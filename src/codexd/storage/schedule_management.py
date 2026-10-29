"""Owner-scoped schedule controls; timer cursors are not configuration revisions."""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any, cast

from codexd.domain.ids import canonical_json, new_id, utc_now_ms
from codexd.domain.schedules import ScheduleAuditContext
from codexd.errors import ConflictError, NotFoundError, SecurityError
from codexd.runtime.port import DynamicToolCall
from codexd.storage.schedules import (
    ScheduleRepository,
    _dynamic_tool_scope,
    _insert_schedule_audit,
    _schedule,
)


def resolve(connection: sqlite3.Connection, conversation_id: str, reference: str) -> sqlite3.Row:
    reference = reference.strip()
    if not re.fullmatch(r"[0-9a-fA-F-]{4,36}", reference):
        raise ConflictError(
            "Choose a schedule UUID or unique prefix (4+ characters), not a Thread ID"
        )
    rows = connection.execute(
        "SELECT * FROM schedules WHERE conversation_id=? AND lower(id) LIKE ? LIMIT 2",
        (conversation_id, reference.lower() + "%"),
    ).fetchall()
    if not rows:
        raise NotFoundError(
            "Schedule not found in this Conversation; use its original codexD thread"
        )
    if len(rows) != 1:
        raise ConflictError("Schedule prefix is ambiguous; select a longer ID")
    if rows[0]["state"] == "deleted":
        raise ConflictError("Schedule was already deleted")
    return cast(sqlite3.Row, rows[0])


def snapshot(connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    pending = connection.execute(
        "SELECT count(*) FROM turns t JOIN schedule_fires f ON f.id=t.schedule_fire_id "
        "WHERE f.schedule_id=? "
        "AND t.state IN ('queued','starting','running','cancelling')",
        (row["id"],),
    ).fetchone()[0]
    return {
        key: row[key]
        for key in (
            "id",
            "name",
            "state",
            "kind",
            "expression",
            "timezone",
            "next_due_at",
            "config_revision",
        )
    } | {"pending_turns": pending, "observed_at": utc_now_ms(), "scope": "current Conversation"}


def mutate(
    repository: ScheduleRepository,
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    action: str,
    audit: ScheduleAuditContext,
) -> sqlite3.Row:
    now = utc_now_ms()
    if action == "pause" and row["state"] == "paused":
        return row
    due = None
    if action == "resume":
        if row["state"] not in {"paused", "blocked"}:
            raise ConflictError(f"Schedule cannot resume from {row['state']}")
        from codexd.application.schedule_coordinator import next_occurrence, schedule_spec

        target = connection.execute(
            "SELECT c.state,c.active_revision_id,p.root_path FROM conversations c "
            "JOIN projects p ON p.id=c.project_id WHERE c.id=?",
            (row["conversation_id"],),
        ).fetchone()
        if target["state"] != "active" or target["active_revision_id"] is None:
            raise ConflictError("Schedule target Conversation is unavailable")
        repository._require_project_root(target["root_path"])
        occurrence = next_occurrence(schedule_spec(_schedule(row)), now - 1)
        if occurrence is None:
            raise ConflictError("Schedule has no future occurrence")
        due = occurrence.utc_ms
    elif action == "pause":
        if row["state"] != "active":
            raise ConflictError(f"Schedule cannot pause from {row['state']}")
    elif action != "delete":
        raise ConflictError("Unsupported schedule control")
    state = {"pause": "paused", "resume": "active", "delete": "deleted"}[action]
    changed = connection.execute(
        "UPDATE schedules SET state=?,next_due_at=?,version=version+1,updated_at=?, "
        "prompt_text=CASE WHEN ?='deleted' THEN NULL ELSE prompt_text END, "
        "skill_inputs_json=CASE WHEN ?='deleted' THEN NULL ELSE skill_inputs_json END, "
        "deleted_at=CASE WHEN ?='deleted' THEN ? ELSE deleted_at END "
        "WHERE id=? AND version=? AND config_revision=?",
        (
            state,
            due,
            now,
            state,
            state,
            state,
            now,
            row["id"],
            row["version"],
            row["config_revision"],
        ),
    ).rowcount
    if changed != 1:
        raise ConflictError("Schedule changed; refresh the confirmation")
    _insert_schedule_audit(
        connection,
        audit=audit,
        action=f"schedule.{action}",
        schedule_id=row["id"],
        payload={
            "from_version": row["version"],
            "to_version": row["version"] + 1,
            "state": state,
            "config_revision": row["config_revision"],
        },
        now=now,
    )
    return cast(
        sqlite3.Row,
        connection.execute(
            "SELECT * FROM schedules WHERE id=?",
            (row["id"],),
        ).fetchone(),
    )


def control(
    repository: ScheduleRepository,
    conversation_id: str,
    reference: str,
    action: str,
    audit: ScheduleAuditContext,
) -> dict[str, Any]:
    with repository.store.transaction() as connection:
        row = resolve(connection, conversation_id, reference)
        return snapshot(connection, mutate(repository, connection, row, action, audit))


def prepare_delete(
    repository: ScheduleRepository,
    conversation_id: str,
    reference: str,
    owner: int,
) -> tuple[str, dict[str, Any]]:
    with repository.store.transaction() as connection:
        row = resolve(connection, conversation_id, reference)
        confirmation = new_id()
        connection.execute("DELETE FROM schedule_delete_confirmations WHERE expires_at<?",
                           (utc_now_ms() - 86_400_000,))
        connection.execute(
            "INSERT INTO schedule_delete_confirmations VALUES (?,?,?,?,?,?,NULL)",
            (
                confirmation,
                row["id"],
                conversation_id,
                str(owner),
                row["config_revision"],
                utc_now_ms() + 600_000,
            ),
        )
        return confirmation, snapshot(connection, row)


def confirm_delete(
    repository: ScheduleRepository,
    conversation_id: str,
    confirmation: str,
    owner: int,
    audit: ScheduleAuditContext,
) -> None:
    with repository.store.transaction() as connection:
        intent = connection.execute(
            "SELECT * FROM schedule_delete_confirmations WHERE id=?",
            (confirmation,),
        ).fetchone()
        if (
            intent is None
            or intent["conversation_id"] != conversation_id
            or intent["owner_user_id"] != str(owner)
        ):
            raise SecurityError("Confirmation belongs to another owner or Conversation")
        if intent["consumed_at"] is not None:
            return
        if intent["expires_at"] < utc_now_ms():
            raise ConflictError("Confirmation expired; select the schedule again")
        row = resolve(connection, conversation_id, intent["schedule_id"])
        if row["config_revision"] != intent["config_revision"]:
            raise ConflictError("Schedule configuration/state changed; review a new confirmation")
        mutate(repository, connection, row, "delete", audit)
        connection.execute(
            "UPDATE schedule_delete_confirmations SET consumed_at=? WHERE id=?",
            (utc_now_ms(), confirmation),
        )


def tool_call(
    repository: ScheduleRepository,
    call: DynamicToolCall,
    arguments_hash: str,
    owner: int,
    guild: int,
) -> dict[str, Any]:
    with repository.store.transaction() as connection:
        scope = _dynamic_tool_scope(connection, call.local_turn_id)
        if scope is None:
            raise SecurityError("Originating Turn unavailable")
        error = repository._dynamic_schedule_tool_authorization_error(
            scope,
            runtime_generation=call.runtime_generation,
            provider_thread_id=call.provider_thread_id,
            provider_turn_id=call.provider_turn_id,
            namespace=call.namespace or "",
            tool_name="schedule_create",
            configured_owner_user_id=owner,
            configured_guild_id=guild,
        )
        if error:
            return {"status": "error", "code": error[0], "message": error[1]}
        if call.tool not in {"schedule_list", "schedule_show", "schedule_pause"}:
            raise SecurityError("Unsupported management tool")
        identity = (
            call.runtime_generation,
            call.provider_thread_id,
            call.provider_turn_id,
            call.provider_call_id,
        )
        previous = connection.execute(
            "SELECT * FROM schedule_management_calls WHERE runtime_generation=? AND "
            "provider_thread_id=? AND provider_turn_id=? AND provider_call_id=?",
            identity,
        ).fetchone()
        other = connection.execute(
            "SELECT id FROM dynamic_tool_invocations WHERE runtime_generation=? AND "
            "provider_thread_id=? AND provider_turn_id=? AND provider_call_id=?", identity,
        ).fetchone()
        if other is not None:
            raise ConflictError("Tool call identity already used by another tool")
        if previous:
            if (
                previous["arguments_hash"] != arguments_hash
                or previous["tool_name"] != call.tool
                or previous["turn_id"] != call.local_turn_id
            ):
                raise ConflictError("Tool call identity reused with different input")
            return dict(json.loads(previous["result_json"]))
        if call.tool == "schedule_list":
            rows = connection.execute(
                "SELECT * FROM schedules WHERE conversation_id=? "
                "AND state<>'deleted' ORDER BY id LIMIT 26",
                (scope["conversation_id"],),
            ).fetchall()
            result = {
                "status": "ok",
                "schedules": [snapshot(connection, r) for r in rows[:25]],
                "truncated": len(rows) > 25,
            }
        else:
            assert isinstance(call.arguments, dict)
            row = resolve(connection, scope["conversation_id"], call.arguments["schedule_id"])
            if call.tool == "schedule_pause":
                row = mutate(
                    repository,
                    connection,
                    row,
                    "pause",
                    ScheduleAuditContext(
                        "discord_user",
                        str(owner),
                        f"tool:{call.local_turn_id}:{call.provider_call_id}",
                    ),
                )
            result = {
                "status": "ok",
                "schedule": snapshot(connection, row),
                "notice": "Pause stops future triggers only; queued/running Turns are unchanged.",
            }
        connection.execute(
            "INSERT INTO schedule_management_calls VALUES (?,?,?,?,?,?,?,?,?)",
            (
                *identity,
                call.local_turn_id,
                call.tool,
                arguments_hash,
                canonical_json(result),
                utc_now_ms(),
            ),
        )
        return result
