from __future__ import annotations

import sqlite3

from codexd.domain.ids import canonical_json, new_id, sha256_text, utc_now_ms
from codexd.errors import ConflictError, NotFoundError
from codexd.storage.records import SideQueryRecord
from codexd.storage.sqlite import SQLiteStore


class SideQueryRepository:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def accept(
        self,
        *,
        interaction_id: str,
        conversation_id: str,
        requested_by_user_id: int,
        question_hash: str,
        question_size: int,
        boot_id: str,
    ) -> tuple[SideQueryRecord, bool]:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM side_queries WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["conversation_id"] != conversation_id
                    or int(existing["requested_by_user_id"])
                    != requested_by_user_id
                    or existing["question_hash"] != question_hash
                    or int(existing["question_size"]) != question_size
                ):
                    raise ConflictError(
                        "Side Query interaction was reused with different scope or input"
                    )
                return _side_query(existing), False
            scope = connection.execute(
                """
                SELECT c.project_id, c.discord_guild_id, c.discord_thread_id
                FROM conversations c
                WHERE c.id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if scope is None:
                raise NotFoundError("Side Query Conversation was not found")
            query_id = new_id()
            try:
                connection.execute(
                    """
                    INSERT INTO side_queries(
                        id, interaction_id, project_id, conversation_id,
                        requested_by_user_id, question_hash, question_size,
                        state, accepted_boot_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'accepted', ?, ?, ?)
                    """,
                    (
                        query_id,
                        interaction_id,
                        scope["project_id"],
                        conversation_id,
                        str(requested_by_user_id),
                        question_hash,
                        question_size,
                        boot_id,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError(
                    "btw_already_running: this user already has an active Side Query"
                ) from exc
            _insert_audit(
                connection,
                query_id=query_id,
                project_id=str(scope["project_id"]),
                conversation_id=conversation_id,
                requested_by_user_id=requested_by_user_id,
                action="side_query.accepted",
                payload={
                    "question_hash": question_hash,
                    "question_size": question_size,
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM side_queries WHERE id = ?",
                (query_id,),
            ).fetchone()
            assert row is not None
            return _side_query(row), True

    def mark_running(self, query_id: str) -> SideQueryRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM side_queries WHERE id = ?",
                (query_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("Side Query was not found")
            if row["state"] == "running":
                return _side_query(row)
            if row["state"] != "accepted":
                raise ConflictError(f"Side Query is already {row['state']}")
            connection.execute(
                """
                UPDATE side_queries
                SET state = 'running', started_at = ?, updated_at = ?
                WHERE id = ? AND state = 'accepted'
                """,
                (now, now, query_id),
            )
            _insert_audit_from_row(
                connection,
                row=row,
                action="side_query.running",
                payload={},
                now=now,
            )
            updated = connection.execute(
                "SELECT * FROM side_queries WHERE id = ?",
                (query_id,),
            ).fetchone()
            assert updated is not None
            return _side_query(updated)

    def finish(
        self,
        query_id: str,
        *,
        state: str,
        terminal_code: str,
        answer_hash: str | None = None,
        answer_size: int | None = None,
        error_code: str | None = None,
    ) -> SideQueryRecord:
        if state not in {"completed", "failed", "interrupted"}:
            raise ValueError("invalid Side Query terminal state")
        if state == "completed" and (not answer_hash or not answer_size):
            raise ValueError("completed Side Query requires answer metadata")
        if state != "completed" and (answer_hash is not None or answer_size is not None):
            raise ValueError("failed Side Query cannot retain answer metadata")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM side_queries WHERE id = ?",
                (query_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("Side Query was not found")
            if row["state"] in {"completed", "failed", "interrupted"}:
                expected = (
                    state,
                    terminal_code,
                    answer_hash,
                    answer_size,
                    error_code,
                )
                actual = (
                    row["state"],
                    row["terminal_code"],
                    row["answer_hash"],
                    row["answer_size"],
                    row["error_code"],
                )
                if actual != expected:
                    raise ConflictError("Side Query already has another result")
                return _side_query(row)
            connection.execute(
                """
                UPDATE side_queries
                SET state = ?, answer_hash = ?, answer_size = ?,
                    terminal_code = ?, error_code = ?, completed_at = ?, updated_at = ?
                WHERE id = ? AND state IN ('accepted', 'running')
                """,
                (
                    state,
                    answer_hash,
                    answer_size,
                    terminal_code,
                    error_code,
                    now,
                    now,
                    query_id,
                ),
            )
            _insert_audit_from_row(
                connection,
                row=row,
                action=f"side_query.{state}",
                payload={
                    "terminal_code": terminal_code,
                    "answer_hash": answer_hash,
                    "answer_size": answer_size,
                    "error_code": error_code,
                },
                now=now,
            )
            updated = connection.execute(
                "SELECT * FROM side_queries WHERE id = ?",
                (query_id,),
            ).fetchone()
            assert updated is not None
            return _side_query(updated)

    def get(self, query_id: str) -> SideQueryRecord:
        row = self.store.query_one(
            "SELECT * FROM side_queries WHERE id = ?",
            (query_id,),
        )
        if row is None:
            raise NotFoundError("Side Query was not found")
        return _side_query(row)


def interrupt_for_restart(
    connection: sqlite3.Connection,
    *,
    current_boot_id: str,
    now: int,
) -> int:
    rows = connection.execute(
        """
        SELECT * FROM side_queries
        WHERE state IN ('accepted', 'running')
          AND accepted_boot_id <> ?
        """,
        (current_boot_id,),
    ).fetchall()
    for row in rows:
        connection.execute(
            """
            UPDATE side_queries
            SET state = 'interrupted', terminal_code = 'daemon_restarted',
                error_code = 'daemon_restarted', completed_at = ?, updated_at = ?
            WHERE id = ? AND state IN ('accepted', 'running')
            """,
            (now, now, row["id"]),
        )
        _insert_audit_from_row(
            connection,
            row=row,
            action="side_query.interrupted",
            payload={"terminal_code": "daemon_restarted"},
            now=now,
        )
    return len(rows)


def _insert_audit_from_row(
    connection: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    action: str,
    payload: dict[str, object],
    now: int,
) -> None:
    _insert_audit(
        connection,
        query_id=str(row["id"]),
        project_id=str(row["project_id"]),
        conversation_id=str(row["conversation_id"]),
        requested_by_user_id=int(row["requested_by_user_id"]),
        action=action,
        payload=payload,
        now=now,
    )


def _insert_audit(
    connection: sqlite3.Connection,
    *,
    query_id: str,
    project_id: str,
    conversation_id: str,
    requested_by_user_id: int,
    action: str,
    payload: dict[str, object],
    now: int,
) -> None:
    connection.execute(
        """
        INSERT INTO audit_log(
            id, actor_kind, actor_id_hash, action, correlation_id,
            project_id, conversation_id, payload_json, occurred_at
        ) VALUES (?, 'discord_user', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_id(),
            sha256_text(f"discord_user:{requested_by_user_id}"),
            action,
            f"side-query:{query_id}",
            project_id,
            conversation_id,
            canonical_json(payload),
            now,
        ),
    )


def _side_query(row: sqlite3.Row) -> SideQueryRecord:
    return SideQueryRecord(
        id=str(row["id"]),
        interaction_id=str(row["interaction_id"]),
        project_id=str(row["project_id"]),
        conversation_id=str(row["conversation_id"]),
        requested_by_user_id=int(row["requested_by_user_id"]),
        question_hash=str(row["question_hash"]),
        question_size=int(row["question_size"]),
        state=str(row["state"]),
        answer_hash=str(row["answer_hash"]) if row["answer_hash"] else None,
        answer_size=int(row["answer_size"]) if row["answer_size"] is not None else None,
        terminal_code=(str(row["terminal_code"]) if row["terminal_code"] else None),
        error_code=str(row["error_code"]) if row["error_code"] else None,
        accepted_boot_id=str(row["accepted_boot_id"]),
        created_at=int(row["created_at"]),
        started_at=int(row["started_at"]) if row["started_at"] is not None else None,
        completed_at=(
            int(row["completed_at"]) if row["completed_at"] is not None else None
        ),
    )
