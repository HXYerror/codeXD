from __future__ import annotations

import hmac
import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from codexd.domain.ids import canonical_json, new_id, sha256_text, utc_now_ms
from codexd.domain.schedules import (
    MisfirePolicy,
    ScheduleAuditContext,
    ScheduleKind,
    ScheduleModalSubmission,
    ScheduleState,
    validate_persisted_schedule_spec,
)
from codexd.domain.turns import TurnInput, TurnSkill
from codexd.errors import ConfigurationError, ConflictError, NotFoundError, SecurityError
from codexd.security.redaction import redacted_summary
from codexd.storage.progress import insert_initial_progress
from codexd.storage.records import (
    MaterializedScheduleTurn,
    ScheduleDraftRecord,
    ScheduleFireRecord,
    ScheduleRecord,
)
from codexd.storage.repository import (
    consume_modal_intent_in_transaction,
    mark_command_effect_in_transaction,
)
from codexd.storage.sqlite import SQLiteStore


class ScheduleRepository:
    def __init__(
        self,
        store: SQLiteStore,
        *,
        allowed_roots: tuple[Path, ...] = (),
    ) -> None:
        self.store = store
        # Retain the argument for API compatibility; schedule targets use the
        # same unrestricted full-access path policy as project binding.
        del allowed_roots

    def _project_root_error(self, root_path: str) -> str | None:
        try:
            resolved = Path(root_path).resolve(strict=True)
        except (OSError, RuntimeError):
            return "project_root_unavailable"
        if not resolved.is_dir():
            return "project_root_unavailable"
        return None

    def _require_project_root(self, root_path: str) -> None:
        error = self._project_root_error(root_path)
        if error is not None:
            raise ConflictError(error)

    def create_draft(
        self,
        *,
        conversation_id: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        action: str,
        payload: Mapping[str, Any],
        occurrences: tuple[Mapping[str, Any], ...],
        component_nonce: str,
        expires_at: int,
        schedule_id: str | None = None,
        expected_version: int | None = None,
        modal_submission: ScheduleModalSubmission | None = None,
    ) -> ScheduleDraftRecord:
        if action not in {"create", "update"}:
            raise ConflictError("invalid Schedule draft action")
        if (
            action == "create"
            and (schedule_id is not None or expected_version is not None)
        ) or (
            action == "update"
            and (schedule_id is None or expected_version is None)
        ):
            raise ConflictError("Schedule draft target is invalid")
        now = utc_now_ms()
        draft_id = new_id()
        record: ScheduleDraftRecord | None = None
        modal_expired = False
        with self.store.transaction() as connection:
            modal_record = None
            if modal_submission is not None:
                modal_record, modal_expired = consume_modal_intent_in_transaction(
                    connection,
                    intent_id=modal_submission.intent_id,
                    kind=modal_submission.kind,
                    expires_at=modal_submission.expires_at,
                    nonce=modal_submission.nonce,
                    interaction_id=modal_submission.interaction_id,
                    guild_id=modal_submission.guild_id,
                    channel_id=modal_submission.channel_id,
                    user_id=modal_submission.user_id,
                    now=now,
                )
            if not modal_expired:
                if modal_record is not None:
                    expected_kind = (
                        "schedule_create" if action == "create" else "schedule_update"
                    )
                    if (
                        modal_record.kind != expected_kind
                        or modal_record.conversation_id != conversation_id
                        or modal_record.schedule_id != schedule_id
                        or modal_record.expected_version != expected_version
                        or modal_record.owner_user_id != owner_user_id
                        or modal_record.discord_guild_id != guild_id
                        or modal_record.discord_channel_id != channel_id
                    ):
                        raise SecurityError("Schedule modal target changed")
                record = self._insert_draft(
                    connection,
                    draft_id=draft_id,
                    conversation_id=conversation_id,
                    owner_user_id=owner_user_id,
                    guild_id=guild_id,
                    channel_id=channel_id,
                    action=action,
                    payload=payload,
                    occurrences=occurrences,
                    component_nonce=component_nonce,
                    expires_at=expires_at,
                    schedule_id=schedule_id,
                    expected_version=expected_version,
                    now=now,
                )
                if modal_submission is not None:
                    assert modal_record is not None
                    command, _ = mark_command_effect_in_transaction(
                        connection,
                        interaction_id=modal_submission.interaction_id,
                        effect_kind="schedule_draft",
                        effect_correlation_id=draft_id,
                        turn_id=None,
                        now=now,
                    )
                    if (
                        command.project_id != modal_record.project_id
                        or command.conversation_id != conversation_id
                    ):
                        raise SecurityError("Schedule modal command scope changed")
        if modal_expired:
            raise ConflictError("modal intent expired; run the slash command again")
        assert record is not None
        return record

    def _insert_draft(
        self,
        connection: sqlite3.Connection,
        *,
        draft_id: str,
        conversation_id: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        action: str,
        payload: Mapping[str, Any],
        occurrences: tuple[Mapping[str, Any], ...],
        component_nonce: str,
        expires_at: int,
        schedule_id: str | None,
        expected_version: int | None,
        now: int,
    ) -> ScheduleDraftRecord:
        target = _schedule_target(connection, conversation_id)
        self._require_project_root(str(target["root_path"]))
        if (
            int(target["discord_guild_id"]) != guild_id
            or int(target["discord_thread_id"]) != channel_id
        ):
            raise SecurityError("Schedule draft Discord scope changed")
        if action == "update":
            schedule = connection.execute(
                """
                SELECT id, version
                FROM schedules
                WHERE id = ? AND conversation_id = ?
                  AND state IN ('active', 'paused', 'blocked')
                """,
                (schedule_id, conversation_id),
            ).fetchone()
            if schedule is None:
                raise NotFoundError(f"schedule not found: {schedule_id}")
            if int(schedule["version"]) != expected_version:
                raise ConflictError("schedule was modified concurrently")
        connection.execute(
            """
            UPDATE schedule_drafts
            SET state = 'expired', payload_json = '{}', updated_at = ?
            WHERE state = 'pending' AND expires_at <= ?
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO schedule_drafts(
                id, conversation_id, owner_user_id, action,
                schedule_id, expected_version, payload_json,
                occurrences_json, state, component_nonce_hash,
                expires_at, created_at, updated_at,
                discord_guild_id, discord_channel_id
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?
            )
            """,
            (
                draft_id,
                conversation_id,
                str(owner_user_id),
                action,
                schedule_id,
                expected_version,
                canonical_json(dict(payload)),
                canonical_json([dict(item) for item in occurrences]),
                sha256_text(component_nonce),
                expires_at,
                now,
                now,
                str(guild_id),
                str(channel_id),
            ),
        )
        row = connection.execute(
            "SELECT * FROM schedule_drafts WHERE id = ?",
            (draft_id,),
        ).fetchone()
        assert row is not None
        return _schedule_draft(row)

    def confirm_draft(
        self,
        *,
        draft_id: str,
        component_nonce: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            draft = connection.execute(
                "SELECT * FROM schedule_drafts WHERE id = ?",
                (draft_id,),
            ).fetchone()
            if draft is None:
                raise NotFoundError(f"Schedule draft not found: {draft_id}")
            _assert_draft_identity(
                draft,
                component_nonce=component_nonce,
                owner_user_id=owner_user_id,
                guild_id=guild_id,
                channel_id=channel_id,
            )
            if draft["state"] == "confirmed":
                result = json.loads(str(draft["payload_json"]))
                result_schedule_id = (
                    result.get("schedule_id") if isinstance(result, dict) else None
                )
                if not isinstance(result_schedule_id, str):
                    raise ConflictError("confirmed Schedule draft result is invalid")
                row = connection.execute(
                    "SELECT * FROM schedules WHERE id = ?",
                    (result_schedule_id,),
                ).fetchone()
                if row is None:
                    raise ConflictError("confirmed Schedule draft target is missing")
                return _schedule(row)
            _assert_pending_draft(
                draft,
                component_nonce=component_nonce,
                owner_user_id=owner_user_id,
                guild_id=guild_id,
                channel_id=channel_id,
                now=now,
            )
            target = _schedule_target(connection, str(draft["conversation_id"]))
            self._require_project_root(str(target["root_path"]))
            payload = json.loads(str(draft["payload_json"]))
            if not isinstance(payload, dict):
                raise ConflictError("Schedule draft payload is invalid")
            schedule_id = (
                new_id()
                if draft["action"] == "create"
                else str(draft["schedule_id"])
            )
            try:
                kind = ScheduleKind(str(payload["kind"]))
                policy = MisfirePolicy(str(payload["misfire_policy"]))
                _validate_schedule_definition(
                    name=str(payload["name"]),
                    kind=kind,
                    expression=str(payload["expression"]),
                    timezone=str(payload["timezone"]),
                    misfire_policy=policy,
                    prompt_text=str(payload["prompt_text"]),
                    prompt_hash=str(payload["prompt_hash"]),
                    next_due_at=cast(int | None, payload["next_due_at"]),
                )
            except (KeyError, TypeError, ValueError, ConfigurationError) as exc:
                raise ConflictError(f"Schedule draft payload is invalid: {exc}") from exc
            if draft["action"] == "create":
                try:
                    connection.execute(
                        """
                        INSERT INTO schedules(
                            id, conversation_id, name, kind, expression, timezone,
                            misfire_policy, prompt_text, prompt_hash,
                            state, next_due_at, version, created_by_user_id,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?, ?)
                        """,
                        (
                            schedule_id,
                            draft["conversation_id"],
                            payload["name"],
                            payload["kind"],
                            payload["expression"],
                            payload["timezone"],
                            payload["misfire_policy"],
                            payload["prompt_text"],
                            payload["prompt_hash"],
                            payload["next_due_at"],
                            str(owner_user_id),
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ConflictError(
                        "schedule name already exists in this Conversation"
                    ) from exc
            else:
                try:
                    changed = connection.execute(
                        """
                        UPDATE schedules
                        SET name = ?, kind = ?, expression = ?, timezone = ?,
                            misfire_policy = ?, prompt_text = ?, prompt_hash = ?,
                            next_due_at = CASE
                                WHEN state = 'active' THEN ? ELSE NULL END,
                            version = version + 1, updated_at = ?
                        WHERE id = ? AND conversation_id = ?
                          AND version = ?
                          AND state IN ('active', 'paused', 'blocked')
                        """,
                        (
                            payload["name"],
                            payload["kind"],
                            payload["expression"],
                            payload["timezone"],
                            payload["misfire_policy"],
                            payload["prompt_text"],
                            payload["prompt_hash"],
                            payload["next_due_at"],
                            now,
                            schedule_id,
                            draft["conversation_id"],
                            draft["expected_version"],
                        ),
                    ).rowcount
                except sqlite3.IntegrityError as exc:
                    raise ConflictError(
                        "schedule name already exists in this Conversation"
                    ) from exc
                if changed != 1:
                    raise ConflictError(
                        "schedule was modified concurrently or is terminal"
                    )
            connection.execute(
                """
                UPDATE schedule_drafts
                SET state = 'confirmed', payload_json = ?,
                    updated_at = ?
                WHERE id = ? AND state = 'pending'
                """,
                (
                    canonical_json(
                        {
                            "prompt_hash": payload["prompt_hash"],
                            "schedule_id": schedule_id,
                        }
                    ),
                    now,
                    draft_id,
                ),
            )
            context = _audit_context(
                audit,
                f"schedule:{schedule_id}:confirm:{draft_id}",
            )
            _insert_schedule_audit(
                connection,
                audit=context,
                action="schedule.confirm",
                schedule_id=schedule_id,
                payload={
                    "draft_id_hash": sha256_text(draft_id),
                    "draft_action": str(draft["action"]),
                },
                now=now,
            )
            _insert_schedule_audit(
                connection,
                audit=context,
                action=(
                    "schedule.create"
                    if draft["action"] == "create"
                    else "schedule.update"
                ),
                schedule_id=schedule_id,
                payload={
                    "draft_id_hash": sha256_text(draft_id),
                    "version": (
                        1
                        if draft["action"] == "create"
                        else int(draft["expected_version"]) + 1
                    ),
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM schedules WHERE id = ?",
                (schedule_id,),
            ).fetchone()
            assert row is not None
            return _schedule(row)

    def cancel_draft(
        self,
        *,
        draft_id: str,
        component_nonce: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        audit: ScheduleAuditContext | None = None,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            draft = connection.execute(
                "SELECT * FROM schedule_drafts WHERE id = ?",
                (draft_id,),
            ).fetchone()
            if draft is None:
                raise NotFoundError(f"Schedule draft not found: {draft_id}")
            _assert_draft_identity(
                draft,
                component_nonce=component_nonce,
                owner_user_id=owner_user_id,
                guild_id=guild_id,
                channel_id=channel_id,
            )
            if draft["state"] == "cancelled":
                _mark_draft_command_effect(
                    connection,
                    audit=audit,
                    draft=draft,
                    now=now,
                )
                return
            _assert_pending_draft(
                draft,
                component_nonce=component_nonce,
                owner_user_id=owner_user_id,
                guild_id=guild_id,
                channel_id=channel_id,
                now=now,
            )
            connection.execute(
                """
                UPDATE schedule_drafts
                SET state = 'cancelled', payload_json = '{}', updated_at = ?
                WHERE id = ? AND state = 'pending'
                """,
                (now, draft_id),
            )
            _mark_draft_command_effect(
                connection,
                audit=audit,
                draft=draft,
                now=now,
            )

    def create(
        self,
        *,
        conversation_id: str,
        name: str,
        kind: ScheduleKind,
        expression: str,
        timezone: str,
        misfire_policy: MisfirePolicy,
        prompt_text: str,
        next_due_at: int,
        created_by_user_id: int,
        skill_inputs_json: str | None = None,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        prompt_hash = sha256_text(prompt_text)
        try:
            _validate_schedule_definition(
                name=name,
                kind=kind,
                expression=expression,
                timezone=timezone,
                misfire_policy=misfire_policy,
                prompt_text=prompt_text,
                prompt_hash=prompt_hash,
                next_due_at=next_due_at,
            )
        except ConfigurationError as exc:
            raise ConflictError(f"invalid Schedule definition: {exc}") from exc
        now = utc_now_ms()
        schedule_id = new_id()
        with self.store.transaction() as connection:
            conversation = connection.execute(
                """
                SELECT c.state, c.active_revision_id, p.root_path
                FROM conversations c
                JOIN projects p ON p.id = c.project_id
                WHERE c.id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if conversation is None:
                raise NotFoundError(f"conversation not found: {conversation_id}")
            if (
                conversation["state"] != "active"
                or conversation["active_revision_id"] is None
            ):
                raise ConflictError(
                    "schedule target Conversation has no active revision"
                )
            self._require_project_root(str(conversation["root_path"]))
            try:
                connection.execute(
                    """
                    INSERT INTO schedules(
                        id, conversation_id, name, kind, expression, timezone,
                        misfire_policy, prompt_text, prompt_hash, skill_inputs_json,
                        state, next_due_at, version, created_by_user_id,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?, ?)
                    """,
                    (
                        schedule_id,
                        conversation_id,
                        name,
                        kind.value,
                        expression,
                        timezone,
                        misfire_policy.value,
                        prompt_text,
                        prompt_hash,
                        skill_inputs_json,
                        next_due_at,
                        str(created_by_user_id),
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("schedule name already exists in this Conversation") from exc
            _insert_schedule_audit(
                connection,
                audit=_audit_context(
                    audit,
                    f"schedule:{schedule_id}:create",
                ),
                action="schedule.create",
                schedule_id=schedule_id,
                payload={
                    "kind": kind.value,
                    "misfire_policy": misfire_policy.value,
                    "prompt_hash": prompt_hash,
                    "version": 1,
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
            assert row is not None
            return _schedule(row)

    def get(self, schedule_id: str) -> ScheduleRecord:
        row = self.store.query_one("SELECT * FROM schedules WHERE id = ?", (schedule_id,))
        if row is None:
            raise NotFoundError(f"schedule not found: {schedule_id}")
        return _schedule(row)

    def resolve(self, conversation_id: str, schedule_ref: str) -> ScheduleRecord:
        reference = schedule_ref.strip().lower()
        if len(reference) < 4:
            raise ConflictError("schedule ID prefix must contain at least 4 characters")
        rows = self.store.query_all(
            """
            SELECT * FROM schedules
            WHERE conversation_id = ? AND state <> 'deleted' AND lower(id) LIKE ?
            ORDER BY created_at DESC
            LIMIT 2
            """,
            (conversation_id, f"{reference}%"),
        )
        if not rows:
            raise NotFoundError(f"schedule not found: {schedule_ref}")
        if len(rows) > 1:
            raise ConflictError(f"schedule prefix is ambiguous: {schedule_ref}")
        return _schedule(rows[0])

    def list_for_conversation(self, conversation_id: str) -> tuple[ScheduleRecord, ...]:
        return tuple(
            _schedule(row)
            for row in self.store.query_all(
                """
                SELECT * FROM schedules
                WHERE conversation_id = ? AND state <> 'deleted'
                ORDER BY created_at, id
                """,
                (conversation_id,),
            )
        )

    def list_fires(
        self,
        schedule_id: str,
        *,
        limit: int = 5,
    ) -> tuple[ScheduleFireRecord, ...]:
        if limit < 1 or limit > 20:
            raise ConflictError("Schedule fire limit must be between 1 and 20")
        return tuple(
            ScheduleFireRecord(
                id=str(row["id"]),
                trigger_kind=str(row["trigger_kind"]),
                scheduled_for=(
                    int(row["scheduled_for"])
                    if row["scheduled_for"] is not None
                    else None
                ),
                scheduled_local=(
                    str(row["scheduled_local"])
                    if row["scheduled_local"] is not None
                    else None
                ),
                state=str(row["state"]),
                turn_id=str(row["turn_id"]) if row["turn_id"] is not None else None,
                error_code=(
                    str(row["error_code"])
                    if row["error_code"] is not None
                    else None
                ),
                created_at=int(row["created_at"]),
            )
            for row in self.store.query_all(
                """
                SELECT id, trigger_kind, scheduled_for, scheduled_local,
                       state, turn_id, error_code, created_at
                FROM schedule_fires
                WHERE schedule_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (schedule_id, limit),
            )
        )

    def list_active(self) -> tuple[ScheduleRecord, ...]:
        return tuple(
            _schedule(row)
            for row in self.store.query_all(
                """
                SELECT *
                FROM schedules
                WHERE state = 'active'
                ORDER BY created_at, id
                """
            )
        )

    def validate_active_target(
        self,
        schedule_id: str,
        *,
        expected_version: int,
    ) -> None:
        row = self.store.query_one(
            """
            SELECT s.version, p.root_path
            FROM schedules s
            JOIN conversations c ON c.id = s.conversation_id
            JOIN projects p ON p.id = c.project_id
            WHERE s.id = ? AND s.state = 'active'
            """,
            (schedule_id,),
        )
        if row is None or int(row["version"]) != expected_version:
            raise ConflictError("schedule changed during startup validation")
        error = self._project_root_error(str(row["root_path"]))
        if error is not None:
            raise ConfigurationError(error)

    def due(self, now_ms: int) -> tuple[ScheduleRecord, ...]:
        return tuple(
            _schedule(row)
            for row in self.store.query_all(
                """
                SELECT * FROM schedules
                WHERE state = 'active' AND next_due_at IS NOT NULL AND next_due_at <= ?
                ORDER BY next_due_at, id
                """,
                (now_ms,),
            )
        )

    def set_state(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        state: ScheduleState,
        next_due_at: int | None,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            target = connection.execute(
                """
                SELECT s.state AS schedule_state, c.state AS conversation_state,
                       c.active_revision_id, p.root_path
                FROM schedules s
                JOIN conversations c ON c.id = s.conversation_id
                JOIN projects p ON p.id = c.project_id
                WHERE s.id = ?
                """,
                (schedule_id,),
            ).fetchone()
            if target is None:
                raise NotFoundError(f"schedule not found: {schedule_id}")
            current = ScheduleState(str(target["schedule_state"]))
            if state is ScheduleState.PAUSED:
                if current is not ScheduleState.ACTIVE:
                    raise ConflictError(
                        f"schedule cannot pause from {current.value}"
                    )
            elif state is ScheduleState.ACTIVE:
                if current not in {ScheduleState.PAUSED, ScheduleState.BLOCKED}:
                    raise ConflictError(
                        f"schedule cannot resume from {current.value}"
                    )
            else:
                raise ConflictError(f"unsupported Schedule state target: {state.value}")
            if state is ScheduleState.ACTIVE:
                if next_due_at is None or isinstance(next_due_at, bool) or next_due_at < 0:
                    raise ConflictError("active Schedule requires a valid next due cursor")
                if (
                    target["conversation_state"] != "active"
                    or target["active_revision_id"] is None
                ):
                    raise ConflictError(
                        "schedule target Conversation has no active revision"
                    )
                self._require_project_root(str(target["root_path"]))
            changed = connection.execute(
                """
                UPDATE schedules
                SET state = ?, next_due_at = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ? AND state = ?
                """,
                (
                    state.value,
                    next_due_at,
                    now,
                    schedule_id,
                    expected_version,
                    current.value,
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError("schedule was modified concurrently")
            action = {
                ScheduleState.ACTIVE: "schedule.resume",
                ScheduleState.PAUSED: "schedule.pause",
            }.get(state, "schedule.state")
            _insert_schedule_audit(
                connection,
                audit=_audit_context(
                    audit,
                    f"schedule:{schedule_id}:{action}:{expected_version}",
                ),
                action=action,
                schedule_id=schedule_id,
                payload={
                    "from_version": expected_version,
                    "to_version": expected_version + 1,
                    "state": state.value,
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
            assert row is not None
            return _schedule(row)

    def update(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        kind: ScheduleKind,
        expression: str,
        timezone: str,
        misfire_policy: MisfirePolicy,
        prompt_text: str,
        next_due_at: int | None,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            target = connection.execute(
                """
                SELECT s.name, s.state AS schedule_state,
                       c.state AS conversation_state,
                       c.active_revision_id, p.root_path
                FROM schedules s
                JOIN conversations c ON c.id = s.conversation_id
                JOIN projects p ON p.id = c.project_id
                WHERE s.id = ?
                """,
                (schedule_id,),
            ).fetchone()
            if target is None:
                raise NotFoundError(f"schedule not found: {schedule_id}")
            if target["schedule_state"] not in {"active", "paused", "blocked"}:
                raise ConflictError(
                    f"schedule cannot update from {target['schedule_state']}"
                )
            prompt_hash = sha256_text(prompt_text)
            try:
                _validate_schedule_definition(
                    name=str(target["name"]),
                    kind=kind,
                    expression=expression,
                    timezone=timezone,
                    misfire_policy=misfire_policy,
                    prompt_text=prompt_text,
                    prompt_hash=prompt_hash,
                    next_due_at=next_due_at,
                    require_cursor=target["schedule_state"] == "active",
                )
            except ConfigurationError as exc:
                raise ConflictError(f"invalid Schedule definition: {exc}") from exc
            if (
                target["schedule_state"] == "active"
                and (
                    target["conversation_state"] != "active"
                    or target["active_revision_id"] is None
                )
            ):
                raise ConflictError(
                    "active schedule target Conversation is unavailable"
                )
            if target["schedule_state"] == "active":
                self._require_project_root(str(target["root_path"]))
            changed = connection.execute(
                """
                UPDATE schedules
                SET kind = ?, expression = ?, timezone = ?, misfire_policy = ?,
                    prompt_text = ?, prompt_hash = ?,
                    next_due_at = CASE WHEN state = 'active' THEN ? ELSE NULL END,
                    version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                  AND state IN ('active', 'paused', 'blocked')
                """,
                (
                    kind.value,
                    expression,
                    timezone,
                    misfire_policy.value,
                    prompt_text,
                    prompt_hash,
                    next_due_at,
                    now,
                    schedule_id,
                    expected_version,
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError("schedule was modified concurrently")
            _insert_schedule_audit(
                connection,
                audit=_audit_context(
                    audit,
                    f"schedule:{schedule_id}:update:{expected_version}",
                ),
                action="schedule.update",
                schedule_id=schedule_id,
                payload={
                    "from_version": expected_version,
                    "to_version": expected_version + 1,
                    "kind": kind.value,
                    "misfire_policy": misfire_policy.value,
                    "prompt_hash": prompt_hash,
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
            assert row is not None
            return _schedule(row)

    def delete(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        audit: ScheduleAuditContext | None = None,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE schedules
                SET state = 'deleted', prompt_text = NULL, skill_inputs_json = NULL,
                    next_due_at = NULL, version = version + 1,
                    deleted_at = ?, updated_at = ?
                WHERE id = ? AND version = ? AND state <> 'deleted'
                """,
                (now, now, schedule_id, expected_version),
            ).rowcount
            if changed != 1:
                raise ConflictError("schedule was modified concurrently")
            _insert_schedule_audit(
                connection,
                audit=_audit_context(
                    audit,
                    f"schedule:{schedule_id}:delete:{expected_version}",
                ),
                action="schedule.delete",
                schedule_id=schedule_id,
                payload={
                    "from_version": expected_version,
                    "to_version": expected_version + 1,
                },
                now=now,
            )

    def block(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        reason: str,
        audit: ScheduleAuditContext | None = None,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT s.id, s.conversation_id, s.version, c.project_id,
                       c.discord_thread_id
                FROM schedules s
                JOIN conversations c ON c.id = s.conversation_id
                WHERE s.id = ? AND s.version = ? AND s.state <> 'deleted'
                """,
                (schedule_id, expected_version),
            ).fetchone()
            if row is None:
                exists = connection.execute(
                    "SELECT 1 FROM schedules WHERE id = ?",
                    (schedule_id,),
                ).fetchone()
                if exists is None:
                    raise NotFoundError(f"schedule not found: {schedule_id}")
                raise ConflictError("schedule was modified concurrently")
            _block_schedule(
                connection,
                schedule=row,
                reason=reason,
                audit=_audit_context(
                    audit,
                    f"schedule:{schedule_id}:block:{row['version']}",
                ),
                now=now,
            )

    def record_skipped(
        self,
        *,
        schedule_id: str,
        occurrence_key: str,
        scheduled_for: int,
        scheduled_local: str,
        next_due_at: int | None,
        expected_version: int,
        audit: ScheduleAuditContext | None = None,
    ) -> bool:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            schedule = connection.execute(
                "SELECT * FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
            if (
                schedule is None
                or schedule["state"] != "active"
                or int(schedule["version"]) != expected_version
            ):
                return False
            connection.execute(
                """
                INSERT OR IGNORE INTO schedule_fires(
                    id, schedule_id, occurrence_key, trigger_kind,
                    scheduled_for, scheduled_local, state, created_at
                ) VALUES (?, ?, ?, 'misfire', ?, ?, 'skipped', ?)
                """,
                (
                    new_id(),
                    schedule_id,
                    occurrence_key,
                    scheduled_for,
                    scheduled_local,
                    now,
                ),
            )
            completed = schedule["kind"] == "once" and next_due_at is None
            connection.execute(
                """
                UPDATE schedules
                SET next_due_at = ?, last_due_at = ?,
                    state = CASE WHEN ? THEN 'completed' ELSE state END,
                    prompt_text = CASE WHEN ? THEN NULL ELSE prompt_text END,
                    skill_inputs_json = CASE
                        WHEN ? THEN NULL ELSE skill_inputs_json END,
                    version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (
                    next_due_at,
                    scheduled_for,
                    completed,
                    completed,
                    completed,
                    now,
                    schedule_id,
                    expected_version,
                ),
            )
            _insert_schedule_audit(
                connection,
                audit=_audit_context(
                    audit,
                    f"schedule:{schedule_id}:skip:{occurrence_key}",
                ),
                action="schedule.skip",
                schedule_id=schedule_id,
                payload={
                    "occurrence_key": occurrence_key,
                    "scheduled_for": scheduled_for,
                    "next_due_at": next_due_at,
                },
                now=now,
            )
            return True

    def materialize(
        self,
        *,
        schedule_id: str,
        occurrence_key: str,
        trigger_kind: str,
        scheduled_for: int | None,
        scheduled_local: str,
        next_due_at: int | None,
        expected_version: int | None,
        advance_schedule: bool = True,
        audit: ScheduleAuditContext | None = None,
    ) -> MaterializedScheduleTurn:
        now = utc_now_ms()
        audit_context = _audit_context(
            audit,
            f"schedule:{schedule_id}:{trigger_kind}:{occurrence_key}",
        )
        with self.store.transaction() as connection:
            schedule = connection.execute(
                """
                SELECT s.*, c.state AS conversation_state, c.active_revision_id,
                       c.project_id,
                       c.discord_thread_id,
                       c.model_override, c.reasoning_effort_override,
                       c.reasoning_summary_override,
                       c.personality_override, c.service_tier_override,
                       c.web_search_mode, c.sandbox_profile,
                       p.default_model, p.default_reasoning_effort,
                       p.default_reasoning_summary, p.default_personality,
                       p.default_service_tier,
                       p.default_web_search_mode, p.root_path
                FROM schedules s
                JOIN conversations c ON c.id = s.conversation_id
                JOIN projects p ON p.id = c.project_id
                WHERE s.id = ?
                """,
                (schedule_id,),
            ).fetchone()
            if schedule is None:
                raise NotFoundError(f"schedule not found: {schedule_id}")
            existing = connection.execute(
                """
                SELECT * FROM schedule_fires
                WHERE schedule_id = ? AND occurrence_key = ?
                """,
                (schedule_id, occurrence_key),
            ).fetchone()
            if existing:
                return MaterializedScheduleTurn(
                    fire_id=str(existing["id"]),
                    turn_id=existing["turn_id"],
                    conversation_id=str(schedule["conversation_id"]),
                    fire_state=str(existing["state"]),
                )
            if schedule["state"] != "active":
                raise ConflictError(f"schedule is {schedule['state']}")
            if expected_version is not None and int(schedule["version"]) != expected_version:
                raise ConflictError("schedule was modified concurrently")
            fire_id = new_id()
            target_error: str | None = None
            if (
                schedule["conversation_state"] != "active"
                or schedule["active_revision_id"] is None
            ):
                target_error = "target_unavailable"
            else:
                target_error = self._project_root_error(str(schedule["root_path"]))
            if target_error is not None:
                connection.execute(
                    """
                    INSERT INTO schedule_fires(
                        id, schedule_id, occurrence_key, trigger_kind,
                        scheduled_for, scheduled_local, state, error_code, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'blocked', ?, ?)
                    """,
                    (
                        fire_id,
                        schedule_id,
                        occurrence_key,
                        trigger_kind,
                        scheduled_for,
                        scheduled_local,
                        target_error,
                        now,
                    ),
                )
                _block_schedule(
                    connection,
                    schedule=schedule,
                    reason=target_error,
                    audit=audit_context,
                    now=now,
                )
                _insert_schedule_audit(
                    connection,
                    audit=audit_context,
                    action=_fire_audit_action(trigger_kind),
                    schedule_id=schedule_id,
                    payload={
                        "fire_id": fire_id,
                        "fire_state": "blocked",
                        "occurrence_key": occurrence_key,
                        "scheduled_for": scheduled_for,
                        "trigger_kind": trigger_kind,
                    },
                    now=now,
                )
                return MaterializedScheduleTurn(
                    fire_id, None, str(schedule["conversation_id"]), "blocked"
                )
            prompt = schedule["prompt_text"]
            if not prompt:
                raise ConflictError("schedule prompt is unavailable")
            skills = _decode_skills(schedule["skill_inputs_json"])
            turn_input = TurnInput(text=str(prompt), skill_inputs=skills)
            connection.execute(
                """
                INSERT INTO schedule_fires(
                    id, schedule_id, occurrence_key, trigger_kind,
                    scheduled_for, scheduled_local, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'due', ?)
                """,
                (
                    fire_id,
                    schedule_id,
                    occurrence_key,
                    trigger_kind,
                    scheduled_for,
                    scheduled_local,
                    now,
                ),
            )
            turn_id = new_id()
            connection.execute(
                """
                INSERT INTO turns(
                    id, conversation_id, thread_revision_id, source_kind,
                    schedule_fire_id, state, input_hash, input_summary,
                    queued_input_text,
                    queued_skill_inputs_json, effective_skill_names_json,
                    effective_model, effective_reasoning_effort,
                    effective_reasoning_summary, effective_personality,
                    effective_service_tier,
                    effective_web_search_mode, effective_sandbox,
                    effective_approval_mode, queued_at
                ) VALUES (
                    ?, ?, ?, 'schedule', ?, 'queued',
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    'auto_review', ?
                )
                """,
                (
                    turn_id,
                    schedule["conversation_id"],
                    schedule["active_revision_id"],
                    fire_id,
                    turn_input.input_hash,
                    redacted_summary(
                        prompt,
                        project_root=Path(str(schedule["root_path"])),
                    ),
                    prompt,
                    schedule["skill_inputs_json"],
                    (
                        canonical_json([skill.name for skill in skills])
                        if skills
                        else None
                    ),
                    schedule["model_override"] or schedule["default_model"],
                    schedule["reasoning_effort_override"]
                    or schedule["default_reasoning_effort"],
                    schedule["reasoning_summary_override"]
                    or schedule["default_reasoning_summary"],
                    schedule["personality_override"] or schedule["default_personality"],
                    schedule["service_tier_override"] or schedule["default_service_tier"],
                    schedule["web_search_mode"] or schedule["default_web_search_mode"],
                    schedule["sandbox_profile"],
                    now,
                ),
            )
            insert_initial_progress(
                connection,
                turn_id=turn_id,
                discord_thread_id=schedule["discord_thread_id"],
                sandbox_profile=str(schedule["sandbox_profile"]),
                now=now,
            )
            connection.execute(
                """
                UPDATE schedule_fires
                SET state = 'materialized', turn_id = ?, materialized_at = ?
                WHERE id = ?
                """,
                (turn_id, now, fire_id),
            )
            if advance_schedule:
                completed = schedule["kind"] == "once" and next_due_at is None
                connection.execute(
                    """
                    UPDATE schedules
                    SET next_due_at = ?, last_due_at = ?,
                        state = CASE WHEN ? THEN 'completed' ELSE state END,
                        prompt_text = CASE WHEN ? THEN NULL ELSE prompt_text END,
                        skill_inputs_json = CASE
                            WHEN ? THEN NULL ELSE skill_inputs_json END,
                        version = version + 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        next_due_at,
                        scheduled_for,
                        completed,
                        completed,
                        completed,
                        now,
                        schedule_id,
                    ),
                )
            _insert_schedule_audit(
                connection,
                audit=audit_context,
                action=_fire_audit_action(trigger_kind),
                schedule_id=schedule_id,
                payload={
                    "fire_id": fire_id,
                    "fire_state": "materialized",
                    "occurrence_key": occurrence_key,
                    "scheduled_for": scheduled_for,
                    "trigger_kind": trigger_kind,
                    "turn_id": turn_id,
                },
                now=now,
            )
            return MaterializedScheduleTurn(
                fire_id, turn_id, str(schedule["conversation_id"]), "materialized"
            )


def _decode_skills(value: str | None) -> tuple[TurnSkill, ...]:
    if not value:
        return ()
    decoded = json.loads(value)
    return tuple(
        TurnSkill(
            name=str(item["name"]),
            canonical_path=Path(str(item["canonical_path"])),
            content_hash=str(item["content_hash"]),
        )
        for item in decoded
    )


def _validate_schedule_definition(
    *,
    name: str,
    kind: ScheduleKind,
    expression: str,
    timezone: str,
    misfire_policy: MisfirePolicy,
    prompt_text: str,
    prompt_hash: str,
    next_due_at: int | None,
    require_cursor: bool = True,
) -> None:
    del misfire_policy
    normalized_name = " ".join(name.split())
    if not normalized_name or normalized_name != name or len(name) > 100:
        raise ConfigurationError("schedule name must be canonical and contain 1-100 characters")
    if (
        not prompt_text
        or prompt_text != prompt_text.strip()
        or len(prompt_text.encode("utf-8")) > 16 * 1024
    ):
        raise ConfigurationError(
            "schedule prompt must be canonical and contain at most 16 KiB"
        )
    if sha256_text(prompt_text) != prompt_hash:
        raise ConfigurationError("schedule prompt hash does not match its prompt")
    if require_cursor and next_due_at is None:
        raise ConfigurationError("active Schedule requires a valid next due cursor")
    if next_due_at is not None and (
        isinstance(next_due_at, bool) or not isinstance(next_due_at, int) or next_due_at < 0
    ):
        raise ConfigurationError("Schedule next due cursor is invalid")
    validate_persisted_schedule_spec(kind, expression, timezone)


def _audit_context(
    audit: ScheduleAuditContext | None,
    fallback_correlation_id: str,
) -> ScheduleAuditContext:
    return audit or ScheduleAuditContext.system(fallback_correlation_id)


def _fire_audit_action(trigger_kind: str) -> str:
    return {
        "manual": "schedule.run_now",
        "misfire": "schedule.misfire",
        "timer": "schedule.fire",
    }.get(trigger_kind, "schedule.fire")


def _insert_schedule_audit(
    connection: sqlite3.Connection,
    *,
    audit: ScheduleAuditContext,
    action: str,
    schedule_id: str,
    payload: Mapping[str, Any],
    now: int,
) -> None:
    scope = connection.execute(
        """
        SELECT s.conversation_id, c.project_id
        FROM schedules s
        JOIN conversations c ON c.id = s.conversation_id
        WHERE s.id = ?
        """,
        (schedule_id,),
    ).fetchone()
    if scope is None:
        raise NotFoundError(f"schedule not found: {schedule_id}")
    connection.execute(
        """
        INSERT OR IGNORE INTO audit_log(
            id, actor_kind, actor_id_hash, action, correlation_id,
            project_id, conversation_id, schedule_id, payload_json, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_id(),
            audit.actor_kind,
            (
                sha256_text(f"{audit.actor_kind}:{audit.actor_id}")
                if audit.actor_id is not None
                else None
            ),
            action,
            audit.correlation_id,
            scope["project_id"],
            scope["conversation_id"],
            schedule_id,
            canonical_json(dict(payload)),
            now,
        ),
    )
    _mark_correlated_command_effect(
        connection,
        audit=audit,
        schedule_id=schedule_id,
        now=now,
    )


def _mark_correlated_command_effect(
    connection: sqlite3.Connection,
    *,
    audit: ScheduleAuditContext,
    schedule_id: str,
    now: int,
) -> None:
    if audit.actor_kind != "discord_user":
        return
    intent = connection.execute(
        """
        SELECT state, effect_kind, effect_correlation_id
        FROM command_intents
        WHERE interaction_id = ?
        """,
        (audit.correlation_id,),
    ).fetchone()
    if intent is None or intent["state"] not in {"accepted", "effect_in_flight"}:
        return
    if intent["state"] == "effect_in_flight":
        if (
            intent["effect_kind"] != "schedule_mutation"
            or intent["effect_correlation_id"] != schedule_id
        ):
            raise ConflictError("schedule command effect identity changed")
        return
    changed = connection.execute(
        """
        UPDATE command_intents
        SET state = 'effect_in_flight',
            effect_kind = 'schedule_mutation',
            effect_correlation_id = ?,
            updated_at = ?
        WHERE interaction_id = ? AND state = 'accepted'
        """,
        (schedule_id, now, audit.correlation_id),
    ).rowcount
    if changed != 1:
        raise ConflictError("schedule command intent changed concurrently")


def _mark_draft_command_effect(
    connection: sqlite3.Connection,
    *,
    audit: ScheduleAuditContext | None,
    draft: sqlite3.Row,
    now: int,
) -> None:
    if audit is None or audit.actor_kind != "discord_user":
        return
    existing = connection.execute(
        "SELECT 1 FROM command_intents WHERE interaction_id = ?",
        (audit.correlation_id,),
    ).fetchone()
    if existing is None:
        return
    command, _ = mark_command_effect_in_transaction(
        connection,
        interaction_id=audit.correlation_id,
        effect_kind="schedule_draft_cancel",
        effect_correlation_id=str(draft["id"]),
        turn_id=None,
        now=now,
    )
    scope = connection.execute(
        """
        SELECT c.project_id
        FROM conversations c
        WHERE c.id = ?
        """,
        (draft["conversation_id"],),
    ).fetchone()
    if (
        scope is None
        or command.project_id != scope["project_id"]
        or command.conversation_id != draft["conversation_id"]
    ):
        raise SecurityError("Schedule draft command scope changed")


def _block_schedule(
    connection: sqlite3.Connection,
    *,
    schedule: sqlite3.Row,
    reason: str,
    audit: ScheduleAuditContext,
    now: int,
) -> None:
    schedule_id = str(schedule["id"])
    changed = connection.execute(
        """
        UPDATE schedules
        SET state = 'blocked', next_due_at = NULL,
            version = version + 1, updated_at = ?
        WHERE id = ? AND version = ? AND state <> 'deleted'
        """,
        (now, schedule_id, schedule["version"]),
    ).rowcount
    if changed != 1:
        raise ConflictError("schedule was modified concurrently")
    connection.execute(
        """
        INSERT INTO incidents(
            id, severity, code, project_id, conversation_id,
            schedule_id, summary, details_json, occurrence_count,
            first_seen_at, last_seen_at
        ) VALUES (?, 'error', 'schedule_blocked', ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            new_id(),
            schedule["project_id"],
            schedule["conversation_id"],
            schedule_id,
            "A Schedule was blocked before it could materialize a Turn",
            canonical_json({"reason": reason[:128]}),
            now,
            now,
        ),
    )
    _insert_schedule_audit(
        connection,
        audit=audit,
        action="schedule.block",
        schedule_id=schedule_id,
        payload={
            "from_version": int(schedule["version"]),
            "reason": reason[:128],
        },
        now=now,
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO discord_outbox(
            id, destination_key, operation, payload_json, dedupe_key,
            delivery_marker, state, attempts, next_attempt_at,
            created_at, updated_at
        ) VALUES (?, ?, 'send', ?, ?, ?, 'pending', 0, ?, ?, ?)
        """,
        (
            new_id(),
            f"thread:{schedule['discord_thread_id']}",
            canonical_json(
                {
                    "kind": "schedule_blocked",
                    "schedule_id": schedule_id,
                    "content": (
                        f"Schedule `{schedule_id[:8]}` was blocked "
                        f"(`{reason[:96]}`)."
                    ),
                }
            ),
            f"schedule:{schedule_id}:blocked:{schedule['version']}",
            f"schedule-{schedule_id[:8]}-blocked-{schedule['version']}",
            now,
            now,
            now,
        ),
    )


def _schedule_target(
    connection: sqlite3.Connection,
    conversation_id: str,
) -> sqlite3.Row:
    target = connection.execute(
        """
        SELECT c.state, c.active_revision_id, c.owner_user_id,
               c.discord_guild_id, c.discord_thread_id, p.root_path
        FROM conversations c
        JOIN projects p ON p.id = c.project_id
        WHERE c.id = ?
        """,
        (conversation_id,),
    ).fetchone()
    if target is None:
        raise NotFoundError(f"conversation not found: {conversation_id}")
    if target["state"] != "active" or target["active_revision_id"] is None:
        raise ConflictError("schedule target Conversation has no active revision")
    return cast(sqlite3.Row, target)


def _assert_pending_draft(
    draft: sqlite3.Row,
    *,
    component_nonce: str,
    owner_user_id: int,
    guild_id: int,
    channel_id: int,
    now: int,
) -> None:
    _assert_draft_identity(
        draft,
        component_nonce=component_nonce,
        owner_user_id=owner_user_id,
        guild_id=guild_id,
        channel_id=channel_id,
    )
    if draft["state"] != "pending":
        raise ConflictError(f"Schedule draft is {draft['state']}")
    if int(draft["expires_at"]) <= now:
        raise ConflictError("Schedule draft expired")


def _assert_draft_identity(
    draft: sqlite3.Row,
    *,
    component_nonce: str,
    owner_user_id: int,
    guild_id: int,
    channel_id: int,
) -> None:
    if int(draft["owner_user_id"]) != owner_user_id:
        raise SecurityError("Schedule draft belongs to another owner")
    if (
        draft["discord_guild_id"] is None
        or draft["discord_channel_id"] is None
        or int(draft["discord_guild_id"]) != guild_id
        or int(draft["discord_channel_id"]) != channel_id
    ):
        raise SecurityError("Schedule draft Discord scope changed")
    if not hmac.compare_digest(
        str(draft["component_nonce_hash"]),
        sha256_text(component_nonce),
    ):
        raise ConflictError("Schedule draft component nonce changed")


def _schedule(row: sqlite3.Row) -> ScheduleRecord:
    return ScheduleRecord(
        id=str(row["id"]),
        conversation_id=str(row["conversation_id"]),
        name=str(row["name"]),
        kind=ScheduleKind(row["kind"]),
        expression=str(row["expression"]),
        timezone=str(row["timezone"]),
        misfire_policy=MisfirePolicy(row["misfire_policy"]),
        prompt_text=row["prompt_text"],
        prompt_hash=str(row["prompt_hash"]),
        skill_inputs_json=row["skill_inputs_json"],
        state=ScheduleState(row["state"]),
        next_due_at=row["next_due_at"],
        last_due_at=row["last_due_at"],
        version=int(row["version"]),
        created_by_user_id=int(row["created_by_user_id"]),
    )


def _schedule_draft(row: sqlite3.Row) -> ScheduleDraftRecord:
    return ScheduleDraftRecord(
        id=str(row["id"]),
        conversation_id=str(row["conversation_id"]),
        owner_user_id=int(row["owner_user_id"]),
        discord_guild_id=(
            int(row["discord_guild_id"])
            if row["discord_guild_id"] is not None
            else 0
        ),
        discord_channel_id=(
            int(row["discord_channel_id"])
            if row["discord_channel_id"] is not None
            else 0
        ),
        action=str(row["action"]),
        schedule_id=row["schedule_id"],
        expected_version=row["expected_version"],
        payload_json=str(row["payload_json"]),
        occurrences_json=str(row["occurrences_json"]),
        state=str(row["state"]),
        component_nonce_hash=str(row["component_nonce_hash"]),
        expires_at=int(row["expires_at"]),
    )
