from __future__ import annotations

import sqlite3

from codexd.domain.ids import new_id, utc_now_ms
from codexd.errors import ConflictError, NotFoundError, SecurityError
from codexd.storage.records import (
    DiscordIngressCheckpointRecord,
    DiscordIngressTargetRecord,
)
from codexd.storage.sqlite import SQLiteStore

_DISCORD_EPOCH_MS = 1_420_070_400_000


class IngressCheckpointRepository:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def conversation_targets(
        self,
        *,
        guild_id: int,
    ) -> tuple[DiscordIngressTargetRecord, ...]:
        return tuple(
            DiscordIngressTargetRecord(
                discord_guild_id=int(row["discord_guild_id"]),
                discord_channel_id=int(row["discord_thread_id"]),
                scope_kind="conversation_thread",
                conversation_id=str(row["id"]),
                discord_parent_channel_id=int(row["discord_parent_channel_id"]),
            )
            for row in self.store.query_all(
                """
                SELECT id, discord_guild_id, discord_thread_id,
                       discord_parent_channel_id
                FROM conversations
                WHERE state <> 'deleted' AND discord_guild_id = ?
                ORDER BY discord_thread_id
                """,
                (str(guild_id),),
            )
        )

    def ensure(
        self,
        *,
        target: DiscordIngressTargetRecord,
        remote_barrier_id: int | None,
    ) -> DiscordIngressCheckpointRecord:
        _validate_target(target)
        barrier = _snowflake(remote_barrier_id) if remote_barrier_id is not None else None
        now = utc_now_ms()
        with self.store.transaction() as connection:
            existing = connection.execute(
                """
                SELECT * FROM discord_ingress_checkpoints
                WHERE discord_guild_id = ? AND discord_channel_id = ?
                """,
                (str(target.discord_guild_id), str(target.discord_channel_id)),
            ).fetchone()
            if existing is not None:
                _assert_scope(existing, target)
                return _checkpoint(existing)
            _assert_target_origin(connection, target)
            activation = connection.execute(
                """
                SELECT activated_at FROM discord_ingress_feature_state
                WHERE singleton = 1
                """
            ).fetchone()
            if activation is None:
                raise ConflictError("Discord ingress feature activation is missing")
            activation_id = _snowflake_for_ms(int(activation["activated_at"]))
            known = connection.execute(
                """
                SELECT MAX(CAST(discord_message_id AS INTEGER)) AS message_id
                FROM ingress_messages
                WHERE discord_guild_id = ? AND discord_channel_id = ?
                """,
                (str(target.discord_guild_id), str(target.discord_channel_id)),
            ).fetchone()
            known_id = (
                int(known["message_id"])
                if known is not None and known["message_id"] is not None
                else 0
            )
            baseline = max(activation_id, known_id)
            if barrier is not None and baseline > barrier:
                baseline = barrier
            checkpoint_id = new_id()
            connection.execute(
                """
                INSERT INTO discord_ingress_checkpoints(
                    id, discord_guild_id, discord_channel_id, scope_kind,
                    conversation_id, discord_parent_channel_id,
                    last_scanned_message_id, scan_state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'idle', ?, ?)
                """,
                (
                    checkpoint_id,
                    str(target.discord_guild_id),
                    str(target.discord_channel_id),
                    target.scope_kind,
                    target.conversation_id,
                    (
                        str(target.discord_parent_channel_id)
                        if target.discord_parent_channel_id is not None
                        else None
                    ),
                    str(baseline),
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM discord_ingress_checkpoints WHERE id = ?",
                (checkpoint_id,),
            ).fetchone()
            assert row is not None
            return _checkpoint(row)

    def begin_scan(
        self,
        checkpoint_id: str,
        *,
        remote_barrier_id: int | None,
    ) -> DiscordIngressCheckpointRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM discord_ingress_checkpoints WHERE id = ?",
                (checkpoint_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("Discord ingress checkpoint was not found")
            if row["scan_state"] == "blocked":
                return _checkpoint(row)
            if row["in_progress_barrier_id"] is None:
                barrier = (
                    _snowflake(remote_barrier_id)
                    if remote_barrier_id is not None
                    else int(row["last_scanned_message_id"])
                )
                if barrier < int(row["last_scanned_message_id"]):
                    barrier = int(row["last_scanned_message_id"])
                connection.execute(
                    """
                    UPDATE discord_ingress_checkpoints
                    SET in_progress_barrier_id = ?, in_progress_after_id = ?,
                        scan_state = 'scanning', last_scan_started_at = ?,
                        last_error_code = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        str(barrier),
                        row["last_scanned_message_id"],
                        now,
                        now,
                        checkpoint_id,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE discord_ingress_checkpoints
                    SET scan_state = 'scanning', last_scan_started_at = ?,
                        last_error_code = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, checkpoint_id),
                )
            updated = connection.execute(
                "SELECT * FROM discord_ingress_checkpoints WHERE id = ?",
                (checkpoint_id,),
            ).fetchone()
            assert updated is not None
            return _checkpoint(updated)

    def record_progress(
        self,
        checkpoint_id: str,
        *,
        barrier_id: int,
        after_message_id: int,
    ) -> None:
        barrier = _snowflake(barrier_id)
        after = _snowflake(after_message_id)
        if after > barrier:
            raise ConflictError("Discord ingress scan advanced beyond its barrier")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            checkpoint = connection.execute(
                "SELECT * FROM discord_ingress_checkpoints WHERE id = ?",
                (checkpoint_id,),
            ).fetchone()
            if checkpoint is None:
                raise NotFoundError("Discord ingress checkpoint was not found")
            changed = connection.execute(
                """
                UPDATE discord_ingress_checkpoints
                SET in_progress_after_id = ?, updated_at = ?
                WHERE id = ? AND in_progress_barrier_id = ?
                  AND CAST(in_progress_after_id AS INTEGER) <= ?
                """,
                (str(after), now, checkpoint_id, str(barrier), after),
            ).rowcount
            if changed != 1:
                raise ConflictError("Discord ingress scan progress identity changed")

    def complete(
        self,
        checkpoint_id: str,
        *,
        barrier_id: int,
    ) -> DiscordIngressCheckpointRecord:
        barrier = _snowflake(barrier_id)
        now = utc_now_ms()
        with self.store.transaction() as connection:
            checkpoint = connection.execute(
                "SELECT * FROM discord_ingress_checkpoints WHERE id = ?",
                (checkpoint_id,),
            ).fetchone()
            if checkpoint is None:
                raise NotFoundError("Discord ingress checkpoint was not found")
            changed = connection.execute(
                """
                UPDATE discord_ingress_checkpoints
                SET last_scanned_message_id = ?, in_progress_barrier_id = NULL,
                    in_progress_after_id = NULL, scan_state = 'idle',
                    last_scan_completed_at = ?, last_error_code = NULL, updated_at = ?
                WHERE id = ? AND in_progress_barrier_id = ?
                  AND CAST(in_progress_after_id AS INTEGER) <= ?
                """,
                (str(barrier), now, now, checkpoint_id, str(barrier), barrier),
            ).rowcount
            if changed != 1:
                raise ConflictError("Discord ingress scan barrier changed")
            row = connection.execute(
                "SELECT * FROM discord_ingress_checkpoints WHERE id = ?",
                (checkpoint_id,),
            ).fetchone()
            assert row is not None
            return _checkpoint(row)

    def fail(
        self,
        checkpoint_id: str,
        *,
        error_code: str,
        blocked: bool,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            checkpoint = connection.execute(
                "SELECT * FROM discord_ingress_checkpoints WHERE id = ?",
                (checkpoint_id,),
            ).fetchone()
            if checkpoint is None:
                raise NotFoundError("Discord ingress checkpoint was not found")
            changed = connection.execute(
                """
                UPDATE discord_ingress_checkpoints
                SET scan_state = ?, last_error_code = ?, updated_at = ?
                WHERE id = ?
                """,
                ("blocked" if blocked else "retry", error_code[:128], now, checkpoint_id),
            ).rowcount
            if changed != 1:
                raise NotFoundError("Discord ingress checkpoint was not found")
            if blocked:
                project_id = None
                if checkpoint["conversation_id"] is not None:
                    conversation = connection.execute(
                        "SELECT project_id FROM conversations WHERE id = ?",
                        (checkpoint["conversation_id"],),
                    ).fetchone()
                    project_id = (
                        str(conversation["project_id"])
                        if conversation is not None
                        else None
                    )
                connection.execute(
                    """
                    INSERT INTO incidents(
                        id, severity, code, project_id, conversation_id,
                        summary, details_json, occurrence_count,
                        first_seen_at, last_seen_at
                    ) VALUES (?, 'error', 'discord_ingress_reconciliation_blocked',
                              ?, ?, ?, json_object('error_code', ?), 1, ?, ?)
                    """,
                    (
                        new_id(),
                        project_id,
                        checkpoint["conversation_id"],
                        "A Discord channel history checkpoint is blocked",
                        error_code[:128],
                        now,
                        now,
                    ),
                )

    def known_ingress(
        self,
        *,
        discord_message_id: str,
        guild_id: int,
        channel_id: int,
    ) -> tuple[str, str] | None:
        row = self.store.query_one(
            """
            SELECT discord_guild_id, discord_channel_id, state, discovery_kind
            FROM ingress_messages WHERE discord_message_id = ?
            """,
            (discord_message_id,),
        )
        if row is None:
            return None
        if (
            row["discord_guild_id"] != str(guild_id)
            or row["discord_channel_id"] != str(channel_id)
        ):
            raise SecurityError("Discord ingress message scope changed")
        return str(row["state"]), str(row["discovery_kind"])

    def counts(self) -> dict[str, int | str | None]:
        rows = self.store.query_all(
            """
            SELECT scan_state, COUNT(*) AS count,
                   MIN(last_scan_completed_at) AS oldest_completed,
                   MAX(last_scan_completed_at) AS last_completed
            FROM discord_ingress_checkpoints GROUP BY scan_state
            """
        )
        counts = {str(row["scan_state"]): int(row["count"]) for row in rows}
        summary = self.store.query_one(
            """
            SELECT MAX(last_scan_completed_at) AS last_completed,
                   MIN(CAST(last_scanned_message_id AS INTEGER)) AS oldest_cursor
            FROM discord_ingress_checkpoints
            """
        )
        oldest_cursor = (
            int(summary["oldest_cursor"])
            if summary is not None and summary["oldest_cursor"] is not None
            else None
        )
        return {
            "idle": counts.get("idle", 0),
            "scanning": counts.get("scanning", 0),
            "retry": counts.get("retry", 0),
            "blocked": counts.get("blocked", 0),
            "last_completed_at": (
                int(summary["last_completed"])
                if summary is not None and summary["last_completed"] is not None
                else None
            ),
            "oldest_checkpoint_lag_ms": (
                max(
                    0,
                    utc_now_ms()
                    - ((oldest_cursor >> 22) + _DISCORD_EPOCH_MS),
                )
                if oldest_cursor is not None
                else None
            ),
            "last_error_code": (
                str(error["last_error_code"])
                if (
                    error := self.store.query_one(
                        """
                        SELECT last_error_code FROM discord_ingress_checkpoints
                        WHERE last_error_code IS NOT NULL
                        ORDER BY updated_at DESC LIMIT 1
                        """
                    )
                )
                else None
            ),
        }


def _assert_target_origin(
    connection: sqlite3.Connection,
    target: DiscordIngressTargetRecord,
) -> None:
    if target.scope_kind != "conversation_thread":
        return
    row = connection.execute(
        """
        SELECT discord_guild_id, discord_thread_id, discord_parent_channel_id
        FROM conversations WHERE id = ? AND state <> 'deleted'
        """,
        (target.conversation_id,),
    ).fetchone()
    if row is None or (
        int(row["discord_guild_id"]) != target.discord_guild_id
        or int(row["discord_thread_id"]) != target.discord_channel_id
        or int(row["discord_parent_channel_id"])
        != target.discord_parent_channel_id
    ):
        raise SecurityError("Discord reconciliation Conversation origin changed")


def _assert_scope(
    row: sqlite3.Row,
    target: DiscordIngressTargetRecord,
) -> None:
    actual = (
        int(row["discord_guild_id"]),
        int(row["discord_channel_id"]),
        str(row["scope_kind"]),
        str(row["conversation_id"]) if row["conversation_id"] else None,
        (
            int(row["discord_parent_channel_id"])
            if row["discord_parent_channel_id"] is not None
            else None
        ),
    )
    expected = (
        target.discord_guild_id,
        target.discord_channel_id,
        target.scope_kind,
        target.conversation_id,
        target.discord_parent_channel_id,
    )
    if actual != expected:
        raise SecurityError("Discord ingress checkpoint scope changed")


def _validate_target(target: DiscordIngressTargetRecord) -> None:
    _snowflake(target.discord_guild_id)
    _snowflake(target.discord_channel_id)
    if target.scope_kind == "parent_channel":
        if target.conversation_id is not None or target.discord_parent_channel_id is not None:
            raise ConflictError("parent checkpoint cannot bind a Conversation")
        return
    if (
        target.scope_kind != "conversation_thread"
        or not target.conversation_id
        or target.discord_parent_channel_id is None
    ):
        raise ConflictError("Conversation checkpoint scope is incomplete")
    _snowflake(target.discord_parent_channel_id)


def _snowflake(value: int | None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConflictError("Discord reconciliation snowflake is invalid")
    return value


def _snowflake_for_ms(timestamp_ms: int) -> int:
    return max(1, (max(timestamp_ms, _DISCORD_EPOCH_MS) - _DISCORD_EPOCH_MS) << 22)


def _checkpoint(row: sqlite3.Row) -> DiscordIngressCheckpointRecord:
    return DiscordIngressCheckpointRecord(
        id=str(row["id"]),
        discord_guild_id=int(row["discord_guild_id"]),
        discord_channel_id=int(row["discord_channel_id"]),
        scope_kind=str(row["scope_kind"]),
        conversation_id=str(row["conversation_id"]) if row["conversation_id"] else None,
        discord_parent_channel_id=(
            int(row["discord_parent_channel_id"])
            if row["discord_parent_channel_id"] is not None
            else None
        ),
        last_scanned_message_id=int(row["last_scanned_message_id"]),
        in_progress_barrier_id=(
            int(row["in_progress_barrier_id"])
            if row["in_progress_barrier_id"] is not None
            else None
        ),
        in_progress_after_id=(
            int(row["in_progress_after_id"])
            if row["in_progress_after_id"] is not None
            else None
        ),
        scan_state=str(row["scan_state"]),
        last_scan_started_at=(
            int(row["last_scan_started_at"])
            if row["last_scan_started_at"] is not None
            else None
        ),
        last_scan_completed_at=(
            int(row["last_scan_completed_at"])
            if row["last_scan_completed_at"] is not None
            else None
        ),
        last_error_code=str(row["last_error_code"]) if row["last_error_code"] else None,
    )
