from __future__ import annotations

import sqlite3
from pathlib import Path, PurePosixPath
from typing import cast

from codexd.domain.ids import canonical_json, new_id, sha256_text, utc_now_ms
from codexd.errors import ConflictError, InvariantError, SecurityError
from codexd.storage.records import (
    OutboundImageInvocationRecord,
    OutboundImageScope,
)
from codexd.storage.sqlite import SQLiteStore


class OutboundImageRepository:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def preflight(
        self,
        *,
        local_turn_id: str,
        runtime_generation: int,
        provider_thread_id: str,
        provider_turn_id: str,
        provider_call_id: str,
        arguments_hash: str,
        configured_guild_id: int,
        configured_owner_user_id: int,
        allowed_user_ids: frozenset[int],
    ) -> tuple[OutboundImageInvocationRecord | None, OutboundImageScope | None]:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            existing = _existing_invocation(
                connection,
                runtime_generation=runtime_generation,
                provider_thread_id=provider_thread_id,
                provider_turn_id=provider_turn_id,
                provider_call_id=provider_call_id,
            )
            if existing is not None:
                _assert_replay_identity(
                    existing,
                    local_turn_id=local_turn_id,
                    arguments_hash=arguments_hash,
                )
                return _outbound_image(existing), None
            scope = _tool_scope(connection, local_turn_id)
            if scope is None:
                raise SecurityError("dynamic image tool local Turn is unavailable")
            authorization_error = _authorization_error(
                scope,
                runtime_generation=runtime_generation,
                provider_thread_id=provider_thread_id,
                provider_turn_id=provider_turn_id,
                configured_guild_id=configured_guild_id,
                configured_owner_user_id=configured_owner_user_id,
                allowed_user_ids=allowed_user_ids,
            )
            if authorization_error is not None:
                record = _insert_failure(
                    connection,
                    scope=scope,
                    local_turn_id=local_turn_id,
                    runtime_generation=runtime_generation,
                    provider_thread_id=provider_thread_id,
                    provider_turn_id=provider_turn_id,
                    provider_call_id=provider_call_id,
                    arguments_hash=arguments_hash,
                    code=authorization_error[0],
                    message=authorization_error[1],
                    now=now,
                )
                return record, None
            return None, _outbound_scope(scope)

    def complete(
        self,
        *,
        local_turn_id: str,
        runtime_generation: int,
        provider_thread_id: str,
        provider_turn_id: str,
        provider_call_id: str,
        arguments_hash: str,
        configured_guild_id: int,
        configured_owner_user_id: int,
        allowed_user_ids: frozenset[int],
        validation_error: tuple[str, str] | None = None,
        relative_path: str | None = None,
        source_sha256: str | None = None,
        normalized_sha256: str | None = None,
        size_bytes: int | None = None,
        width: int | None = None,
        height: int | None = None,
        display_name: str | None = None,
        description: str | None = None,
        retention_until: int | None = None,
    ) -> OutboundImageInvocationRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            existing = _existing_invocation(
                connection,
                runtime_generation=runtime_generation,
                provider_thread_id=provider_thread_id,
                provider_turn_id=provider_turn_id,
                provider_call_id=provider_call_id,
            )
            if existing is not None:
                _assert_replay_identity(
                    existing,
                    local_turn_id=local_turn_id,
                    arguments_hash=arguments_hash,
                )
                return _outbound_image(existing)
            scope = _tool_scope(connection, local_turn_id)
            if scope is None:
                raise SecurityError("dynamic image tool local Turn is unavailable")
            authorization_error = _authorization_error(
                scope,
                runtime_generation=runtime_generation,
                provider_thread_id=provider_thread_id,
                provider_turn_id=provider_turn_id,
                configured_guild_id=configured_guild_id,
                configured_owner_user_id=configured_owner_user_id,
                allowed_user_ids=allowed_user_ids,
            )
            error = authorization_error or validation_error
            if error is not None:
                return _insert_failure(
                    connection,
                    scope=scope,
                    local_turn_id=local_turn_id,
                    runtime_generation=runtime_generation,
                    provider_thread_id=provider_thread_id,
                    provider_turn_id=provider_turn_id,
                    provider_call_id=provider_call_id,
                    arguments_hash=arguments_hash,
                    code=error[0],
                    message=error[1],
                    now=now,
                )
            _validate_artifact_metadata(
                relative_path=relative_path,
                source_sha256=source_sha256,
                normalized_sha256=normalized_sha256,
                size_bytes=size_bytes,
                width=width,
                height=height,
                display_name=display_name,
                description=description,
                retention_until=retention_until,
                now=now,
            )
            ordinal_row = connection.execute(
                """
                SELECT COALESCE(MAX(artifact_ordinal), -1) + 1 AS next_ordinal
                FROM outbound_image_invocations
                WHERE turn_id = ? AND success = 1
                """,
                (local_turn_id,),
            ).fetchone()
            assert ordinal_row is not None
            ordinal = int(ordinal_row["next_ordinal"])
            invocation_id = new_id()
            result = {
                "status": "registered_for_final_delivery",
                "artifact_ref": f"img-{invocation_id[:8]}",
                "display_name": display_name,
                "media_type": "image/png",
                "size_bytes": size_bytes,
            }
            connection.execute(
                """
                INSERT INTO outbound_image_invocations(
                    id, turn_id, runtime_generation, provider_thread_id,
                    provider_turn_id, provider_call_id, arguments_hash,
                    success, result_json, artifact_ordinal, relative_path,
                    source_sha256, normalized_sha256, size_bytes, width, height,
                    media_type, display_name, description, state,
                    retention_until, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?,
                    'image/png', ?, ?, 'registered', ?, ?, ?
                )
                """,
                (
                    invocation_id,
                    local_turn_id,
                    runtime_generation,
                    provider_thread_id,
                    provider_turn_id,
                    provider_call_id,
                    arguments_hash,
                    canonical_json(result),
                    ordinal,
                    relative_path,
                    source_sha256,
                    normalized_sha256,
                    size_bytes,
                    width,
                    height,
                    display_name,
                    description,
                    retention_until,
                    now,
                    now,
                ),
            )
            _insert_audit(
                connection,
                scope=scope,
                action="outbound_image.registered",
                correlation_id=f"dynamic-tool:{invocation_id}",
                payload={
                    "artifact_ref": f"img-{invocation_id[:8]}",
                    "normalized_sha256": normalized_sha256,
                    "size_bytes": size_bytes,
                    "width": width,
                    "height": height,
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM outbound_image_invocations WHERE id = ?",
                (invocation_id,),
            ).fetchone()
            assert row is not None
            return _outbound_image(row)

    def registered_for_turn(
        self,
        turn_id: str,
    ) -> tuple[OutboundImageInvocationRecord, ...]:
        return tuple(
            _outbound_image(row)
            for row in self.store.query_all(
                """
                SELECT * FROM outbound_image_invocations
                WHERE turn_id = ? AND success = 1 AND state = 'registered'
                ORDER BY artifact_ordinal
                """,
                (turn_id,),
            )
        )


def _existing_invocation(
    connection: sqlite3.Connection,
    *,
    runtime_generation: int,
    provider_thread_id: str,
    provider_turn_id: str,
    provider_call_id: str,
) -> sqlite3.Row | None:
    return cast(
        sqlite3.Row | None,
        connection.execute(
            """
        SELECT * FROM outbound_image_invocations
        WHERE runtime_generation = ?
          AND provider_thread_id = ?
          AND provider_turn_id = ?
          AND provider_call_id = ?
        """,
        (
            runtime_generation,
            provider_thread_id,
            provider_turn_id,
            provider_call_id,
            ),
        ).fetchone(),
    )


def _assert_replay_identity(
    row: sqlite3.Row,
    *,
    local_turn_id: str,
    arguments_hash: str,
) -> None:
    if row["turn_id"] != local_turn_id or row["arguments_hash"] != arguments_hash:
        raise ConflictError(
            "dynamic image tool call identity was reused with different input"
        )


def _tool_scope(
    connection: sqlite3.Connection,
    local_turn_id: str,
) -> sqlite3.Row | None:
    return cast(
        sqlite3.Row | None,
        connection.execute(
            """
        SELECT t.id AS turn_id, t.state AS turn_state, t.source_kind,
               t.requested_by_user_id, t.input_message_id,
               t.schedule_fire_id, t.thread_revision_id,
               t.runtime_generation, t.runtime_lease_id, t.provider_turn_id,
               t.started_at, c.id AS conversation_id, c.project_id,
               c.state AS conversation_state, c.active_revision_id,
               c.discord_guild_id, c.discord_thread_id,
               r.provider_thread_id AS revision_provider_thread_id,
               r.state AS revision_state, l.generation AS lease_generation,
               l.state AS lease_state, p.root_path,
               s.created_by_user_id AS schedule_created_by_user_id
        FROM turns t
        JOIN conversations c ON c.id = t.conversation_id
        JOIN projects p ON p.id = c.project_id
        LEFT JOIN thread_revisions r ON r.id = t.thread_revision_id
        LEFT JOIN runtime_leases l ON l.id = t.runtime_lease_id
        LEFT JOIN schedule_fires sf ON sf.id = t.schedule_fire_id
        LEFT JOIN schedules s ON s.id = sf.schedule_id
        WHERE t.id = ?
        """,
            (local_turn_id,),
        ).fetchone(),
    )


def _authorization_error(
    scope: sqlite3.Row,
    *,
    runtime_generation: int,
    provider_thread_id: str,
    provider_turn_id: str,
    configured_guild_id: int,
    configured_owner_user_id: int,
    allowed_user_ids: frozenset[int],
) -> tuple[str, str] | None:
    source = str(scope["source_kind"])
    if source == "discord":
        actor = scope["requested_by_user_id"]
        if actor is None or int(actor) not in allowed_user_ids:
            return "user_not_allowed", "The originating Discord user is not allowed."
        if scope["input_message_id"] is None or scope["schedule_fire_id"] is not None:
            return "scope_mismatch", "The Discord Turn source changed."
    elif source == "schedule":
        if (
            scope["schedule_fire_id"] is None
            or scope["requested_by_user_id"] is not None
            or scope["schedule_created_by_user_id"] is None
            or int(scope["schedule_created_by_user_id"])
            != configured_owner_user_id
        ):
            return "scope_mismatch", "The Schedule Turn source changed."
    else:
        return "tool_not_allowed_for_source", "This Turn cannot publish images."
    if (
        scope["conversation_state"] != "active"
        or scope["turn_state"] not in {"starting", "running"}
    ):
        return "stale_turn", "The originating Turn is no longer active."
    if (
        scope["thread_revision_id"] is None
        or scope["thread_revision_id"] != scope["active_revision_id"]
        or scope["revision_state"] != "active"
        or scope["revision_provider_thread_id"] != provider_thread_id
    ):
        return "stale_thread", "The Codex Thread identity changed."
    if (
        scope["runtime_generation"] != runtime_generation
        or scope["lease_generation"] != runtime_generation
        or scope["lease_state"] != "ready"
    ):
        return "stale_runtime", "The Codex runtime generation changed."
    if (
        scope["provider_turn_id"] is not None
        and scope["provider_turn_id"] != provider_turn_id
    ):
        return "stale_turn", "The Codex Turn identity changed."
    if scope["provider_turn_id"] is None and scope["turn_state"] != "starting":
        return "stale_turn", "The Codex Turn identity is unavailable."
    if int(scope["discord_guild_id"]) != configured_guild_id:
        return "scope_mismatch", "The Discord Conversation scope changed."
    if scope["started_at"] is None:
        return "stale_turn", "The Turn start time is unavailable."
    try:
        root = Path(str(scope["root_path"])).resolve(strict=True)
    except (OSError, RuntimeError):
        return "project_unavailable", "The project root is unavailable."
    if not root.is_dir():
        return "project_unavailable", "The project root is unavailable."
    return None


def _outbound_scope(row: sqlite3.Row) -> OutboundImageScope:
    return OutboundImageScope(
        turn_id=str(row["turn_id"]),
        conversation_id=str(row["conversation_id"]),
        project_id=str(row["project_id"]),
        project_root=Path(str(row["root_path"])),
        turn_started_at=int(row["started_at"]),
    )


def _insert_failure(
    connection: sqlite3.Connection,
    *,
    scope: sqlite3.Row,
    local_turn_id: str,
    runtime_generation: int,
    provider_thread_id: str,
    provider_turn_id: str,
    provider_call_id: str,
    arguments_hash: str,
    code: str,
    message: str,
    now: int,
) -> OutboundImageInvocationRecord:
    invocation_id = new_id()
    result = {
        "status": "error",
        "code": code,
        "message": message[:512],
    }
    connection.execute(
        """
        INSERT INTO outbound_image_invocations(
            id, turn_id, runtime_generation, provider_thread_id,
            provider_turn_id, provider_call_id, arguments_hash,
            success, result_json, state, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, 'rejected', ?, ?)
        """,
        (
            invocation_id,
            local_turn_id,
            runtime_generation,
            provider_thread_id,
            provider_turn_id,
            provider_call_id,
            arguments_hash,
            canonical_json(result),
            now,
            now,
        ),
    )
    _insert_audit(
        connection,
        scope=scope,
        action="outbound_image.rejected",
        correlation_id=f"dynamic-tool:{invocation_id}",
        payload={"code": code, "arguments_hash": arguments_hash},
        now=now,
    )
    row = connection.execute(
        "SELECT * FROM outbound_image_invocations WHERE id = ?",
        (invocation_id,),
    ).fetchone()
    assert row is not None
    return _outbound_image(row)


def _insert_audit(
    connection: sqlite3.Connection,
    *,
    scope: sqlite3.Row,
    action: str,
    correlation_id: str,
    payload: dict[str, object],
    now: int,
) -> None:
    actor = scope["requested_by_user_id"]
    connection.execute(
        """
        INSERT INTO audit_log(
            id, actor_kind, actor_id_hash, action, correlation_id,
            project_id, conversation_id, turn_id, payload_json, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_id(),
            "discord_user" if actor is not None else "system",
            sha256_text(f"discord_user:{actor}") if actor is not None else None,
            action,
            correlation_id,
            scope["project_id"],
            scope["conversation_id"],
            scope["turn_id"],
            canonical_json(payload),
            now,
        ),
    )


def _validate_artifact_metadata(
    *,
    relative_path: str | None,
    source_sha256: str | None,
    normalized_sha256: str | None,
    size_bytes: int | None,
    width: int | None,
    height: int | None,
    display_name: str | None,
    description: str | None,
    retention_until: int | None,
    now: int,
) -> None:
    relative = PurePosixPath(relative_path or "")
    if (
        not relative_path
        or relative.is_absolute()
        or ".." in relative.parts
        or len(relative.parts) < 2
        or not relative_path.endswith(".png")
        or source_sha256 is None
        or len(source_sha256) != 64
        or normalized_sha256 is None
        or len(normalized_sha256) != 64
        or size_bytes is None
        or size_bytes <= 0
        or width is None
        or width <= 0
        or height is None
        or height <= 0
        or not display_name
        or len(display_name) > 128
        or not description
        or len(description) > 1024
        or retention_until is None
        or retention_until <= now
    ):
        raise InvariantError("outbound image metadata is invalid")


def _outbound_image(row: sqlite3.Row) -> OutboundImageInvocationRecord:
    return OutboundImageInvocationRecord(
        id=str(row["id"]),
        turn_id=str(row["turn_id"]),
        runtime_generation=int(row["runtime_generation"]),
        provider_thread_id=str(row["provider_thread_id"]),
        provider_turn_id=str(row["provider_turn_id"]),
        provider_call_id=str(row["provider_call_id"]),
        arguments_hash=str(row["arguments_hash"]),
        success=bool(row["success"]),
        result_json=str(row["result_json"]),
        artifact_ordinal=(
            int(row["artifact_ordinal"])
            if row["artifact_ordinal"] is not None
            else None
        ),
        relative_path=(str(row["relative_path"]) if row["relative_path"] else None),
        source_sha256=(
            str(row["source_sha256"]) if row["source_sha256"] else None
        ),
        normalized_sha256=(
            str(row["normalized_sha256"])
            if row["normalized_sha256"]
            else None
        ),
        size_bytes=(int(row["size_bytes"]) if row["size_bytes"] is not None else None),
        width=int(row["width"]) if row["width"] is not None else None,
        height=int(row["height"]) if row["height"] is not None else None,
        media_type=str(row["media_type"]) if row["media_type"] else None,
        display_name=str(row["display_name"]) if row["display_name"] else None,
        description=str(row["description"]) if row["description"] else None,
        state=str(row["state"]),
        retention_until=(
            int(row["retention_until"])
            if row["retention_until"] is not None
            else None
        ),
    )
