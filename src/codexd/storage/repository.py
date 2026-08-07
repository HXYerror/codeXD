from __future__ import annotations

import hmac
import json
import os
import re
import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from codexd.domain.conversations import (
    ConversationState,
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    WebSearchMode,
)
from codexd.domain.events import NormalizedEvent
from codexd.domain.ids import (
    canonical_json,
    new_id,
    sha256_file,
    sha256_text,
    utc_now_ms,
)
from codexd.domain.turns import (
    InterruptOrigin,
    TurnImage,
    TurnInput,
    TurnSkill,
    TurnSource,
    TurnState,
    assert_turn_transition,
)
from codexd.errors import (
    ConflictError,
    InvariantError,
    NotFoundError,
    SecurityError,
    StorageError,
)
from codexd.security.redaction import redacted_summary
from codexd.storage.progress import (
    insert_initial_progress,
    insert_progress_update,
    insert_prompt_reaction_update,
    supersede_coalesced_outbox,
)
from codexd.storage.records import (
    CommandIntentRecord,
    ConversationRecord,
    IngressMessageRecord,
    ModalIntentRecord,
    OutboxRecord,
    ProjectRecord,
    RenderPlanRecord,
    RuntimeLeaseRecord,
    ThreadRevisionRecord,
    TurnRecord,
)
from codexd.storage.sqlite import SQLiteStore
from codexd.storage.usage import latest_usage_payload


class Repository:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def accept_command_intent(
        self,
        *,
        interaction_id: str,
        command_name: str,
        request: Mapping[str, Any],
        boot_id: str,
        actor_user_id: int,
        project_id: str | None = None,
        conversation_id: str | None = None,
        turn_id: str | None = None,
    ) -> CommandIntentRecord:
        now = utc_now_ms()
        request_hash = sha256_text(canonical_json(dict(request)))
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["command_name"] != command_name
                    or existing["request_hash"] != request_hash
                    or existing["project_id"] != project_id
                    or existing["conversation_id"] != conversation_id
                    or existing["turn_id"] != turn_id
                ):
                    raise ConflictError(
                        "Discord interaction was redelivered with different scope or input"
                    )
                return _command_intent(existing)
            connection.execute(
                """
                INSERT INTO command_intents(
                    interaction_id, command_name, request_hash,
                    project_id, conversation_id, turn_id, state,
                    accepted_boot_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?, ?, ?)
                """,
                (
                    interaction_id,
                    command_name,
                    request_hash,
                    project_id,
                    conversation_id,
                    turn_id,
                    boot_id,
                    now,
                    now,
                ),
            )
            self._insert_audit(
                connection,
                actor_kind="discord_user",
                actor_id=str(actor_user_id),
                action="command.accepted",
                project_id=project_id,
                conversation_id=conversation_id,
                turn_id=turn_id,
                schedule_id=None,
                payload={
                    "command_name": command_name,
                    "interaction_hash": sha256_text(interaction_id),
                    "request_hash": request_hash,
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            assert row is not None
            return _command_intent(row)

    def get_command_intent(self, interaction_id: str) -> CommandIntentRecord:
        row = self.store.query_one(
            "SELECT * FROM command_intents WHERE interaction_id = ?",
            (interaction_id,),
        )
        if row is None:
            raise NotFoundError(f"command intent not found: {interaction_id}")
        return _command_intent(row)

    def create_modal_intent(
        self,
        *,
        kind: str,
        conversation_id: str,
        guild_id: int,
        channel_id: int,
        owner_user_id: int,
        nonce: str,
        expires_at: int,
        turn_id: str | None = None,
        schedule_id: str | None = None,
        expected_version: int | None = None,
    ) -> ModalIntentRecord:
        if kind not in {"schedule_create", "schedule_update", "steer"}:
            raise InvariantError("invalid modal intent kind")
        now = utc_now_ms()
        if expires_at <= now:
            raise InvariantError("modal intent expiry must be in the future")
        intent_id = new_id()
        with self.store.transaction() as connection:
            conversation = connection.execute(
                "SELECT * FROM conversations WHERE id = ?",
                (conversation_id,),
            ).fetchone()
            if conversation is None:
                raise NotFoundError(f"Conversation not found: {conversation_id}")
            if (
                int(conversation["discord_guild_id"]) != guild_id
                or int(conversation["discord_thread_id"]) != channel_id
            ):
                raise SecurityError("modal intent Discord scope does not match")
            if kind == "steer":
                turn = connection.execute(
                    "SELECT conversation_id FROM turns WHERE id = ?",
                    (turn_id,),
                ).fetchone()
                if turn is None or turn["conversation_id"] != conversation_id:
                    raise ConflictError("modal Turn does not belong to Conversation")
            elif kind == "schedule_update":
                schedule = connection.execute(
                    """
                    SELECT conversation_id, version
                    FROM schedules
                    WHERE id = ? AND state <> 'deleted'
                    """,
                    (schedule_id,),
                ).fetchone()
                if (
                    schedule is None
                    or schedule["conversation_id"] != conversation_id
                    or int(schedule["version"]) != expected_version
                ):
                    raise ConflictError("modal Schedule scope or version changed")
            connection.execute(
                """
                INSERT INTO modal_intents(
                    id, kind, project_id, conversation_id, turn_id,
                    schedule_id, expected_version, discord_guild_id,
                    discord_channel_id, owner_user_id, nonce_hash,
                    state, expires_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
                """,
                (
                    intent_id,
                    kind,
                    conversation["project_id"],
                    conversation_id,
                    turn_id,
                    schedule_id,
                    expected_version,
                    str(guild_id),
                    str(channel_id),
                    str(owner_user_id),
                    sha256_text(f"modal:{nonce}"),
                    expires_at,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM modal_intents WHERE id = ?",
                (intent_id,),
            ).fetchone()
            assert row is not None
            return _modal_intent(row)

    def get_modal_intent(self, intent_id: str) -> ModalIntentRecord:
        row = self.store.query_one(
            "SELECT * FROM modal_intents WHERE id = ?",
            (intent_id,),
        )
        if row is None:
            raise NotFoundError("modal intent was not found")
        return _modal_intent(row)

    def consume_modal_intent(
        self,
        *,
        intent_id: str,
        kind: str,
        expires_at: int,
        nonce: str,
        interaction_id: str,
        guild_id: int,
        channel_id: int,
        user_id: int,
    ) -> ModalIntentRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            record, expired = consume_modal_intent_in_transaction(
                connection,
                intent_id=intent_id,
                kind=kind,
                expires_at=expires_at,
                nonce=nonce,
                interaction_id=interaction_id,
                guild_id=guild_id,
                channel_id=channel_id,
                user_id=user_id,
                now=now,
            )
        if expired:
            raise ConflictError("modal intent expired; run the slash command again")
        return record

    def mark_command_effect(
        self,
        interaction_id: str,
        *,
        effect_kind: str,
        effect_correlation_id: str | None = None,
        turn_id: str | None = None,
        actor_user_id: int | None = None,
        audit_action: str | None = None,
        audit_payload: Mapping[str, Any] | None = None,
    ) -> CommandIntentRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            record, started = mark_command_effect_in_transaction(
                connection,
                interaction_id=interaction_id,
                effect_kind=effect_kind,
                effect_correlation_id=effect_correlation_id,
                turn_id=turn_id,
                now=now,
            )
            if audit_action is not None and started:
                self._insert_audit(
                    connection,
                    actor_kind=(
                        "discord_user" if actor_user_id is not None else "system"
                    ),
                    actor_id=(
                        str(actor_user_id) if actor_user_id is not None else None
                    ),
                    action=audit_action,
                    project_id=record.project_id,
                    conversation_id=record.conversation_id,
                    turn_id=turn_id or record.turn_id,
                    schedule_id=None,
                    correlation_id=interaction_id,
                    payload=dict(audit_payload or {}),
                    now=now,
                )
            return record

    def complete_command_intent(
        self,
        interaction_id: str,
        *,
        state: str,
        result: Mapping[str, Any],
        actor_user_id: int | None = None,
    ) -> CommandIntentRecord:
        if state not in {"succeeded", "rejected", "failed", "unknown"}:
            raise InvariantError("invalid terminal command-intent state")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"command intent not found: {interaction_id}")
            terminal_state = state
            terminal_result = dict(result)
            if (
                row["state"] == "effect_in_flight"
                and row["effect_kind"] == "schedule_mutation"
                and state != "succeeded"
                and connection.execute(
                    """
                    SELECT 1 FROM audit_log
                    WHERE correlation_id = ?
                      AND schedule_id = ?
                      AND action LIKE 'schedule.%'
                    LIMIT 1
                    """,
                    (interaction_id, row["effect_correlation_id"]),
                ).fetchone()
                is not None
            ):
                terminal_state = "succeeded"
                terminal_result = {
                    "code": "ok",
                    "message": "Command completed.",
                }
            if (
                row["state"] == "effect_in_flight"
                and row["effect_kind"]
                in {"schedule_draft", "schedule_draft_cancel"}
                and state != "succeeded"
                and _local_command_effect_committed(connection, row)
            ):
                terminal_state = "succeeded"
                terminal_result = {
                    "code": "ok",
                    "message": "Schedule draft command completed.",
                }
            result_json = canonical_json(terminal_result)
            if row["state"] in {"succeeded", "rejected", "failed", "unknown"}:
                if (
                    row["state"] != terminal_state
                    or row["result_json"] != result_json
                ):
                    raise ConflictError("command intent already has another result")
                return _command_intent(row)
            connection.execute(
                """
                UPDATE command_intents
                SET state = ?, result_json = ?, completed_at = ?, updated_at = ?
                WHERE interaction_id = ?
                """,
                (terminal_state, result_json, now, now, interaction_id),
            )
            self._insert_audit(
                connection,
                actor_kind=(
                    "discord_user" if actor_user_id is not None else "system"
                ),
                actor_id=(
                    str(actor_user_id) if actor_user_id is not None else None
                ),
                action=f"command.{terminal_state}",
                project_id=row["project_id"],
                conversation_id=row["conversation_id"],
                turn_id=row["turn_id"],
                schedule_id=None,
                payload={
                    "command_name": row["command_name"],
                    "request_hash": row["request_hash"],
                    "result_code": terminal_result.get("code"),
                },
                now=now,
            )
            updated = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            assert updated is not None
            return _command_intent(updated)

    def mark_command_delivered(self, interaction_id: str) -> CommandIntentRecord:
        return self._set_command_delivery(
            interaction_id,
            delivery="delivered",
            error_code=None,
        )

    def mark_command_delivery_failed(
        self,
        interaction_id: str,
        *,
        error_code: str,
    ) -> CommandIntentRecord:
        intent = self._set_command_delivery(
            interaction_id,
            delivery="failed",
            error_code=error_code,
        )
        self.record_incident(
            severity="warning",
            code="discord_command_delivery_failed",
            summary="A completed Discord command response could not be delivered",
            project_id=intent.project_id,
            conversation_id=intent.conversation_id,
            turn_id=intent.turn_id,
            details={
                "interaction_hash": sha256_text(interaction_id),
                "command_name": intent.command_name,
                "error_code": error_code,
            },
        )
        return intent

    def _set_command_delivery(
        self,
        interaction_id: str,
        *,
        delivery: str,
        error_code: str | None,
    ) -> CommandIntentRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"command intent not found: {interaction_id}")
            if row["state"] != "succeeded" or row["result_json"] is None:
                raise ConflictError("command delivery requires a succeeded intent")
            result = json.loads(str(row["result_json"]))
            if not isinstance(result, dict):
                raise StorageError("command result is not an object")
            current = result.get("delivery")
            if current == "delivered":
                return _command_intent(row)
            if current == "failed" and delivery == "failed":
                return _command_intent(row)
            result["delivery"] = delivery
            if error_code is None:
                result.pop("delivery_error_code", None)
            else:
                result["delivery_error_code"] = error_code
            connection.execute(
                """
                UPDATE command_intents
                SET result_json = ?, updated_at = ?
                WHERE interaction_id = ? AND state = 'succeeded'
                """,
                (canonical_json(result), now, interaction_id),
            )
            updated = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            assert updated is not None
            return _command_intent(updated)

    def record_audit(
        self,
        *,
        actor_kind: str,
        action: str,
        actor_id: str | None = None,
        project_id: str | None = None,
        conversation_id: str | None = None,
        turn_id: str | None = None,
        schedule_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> str:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            return self._insert_audit(
                connection,
                actor_kind=actor_kind,
                actor_id=actor_id,
                action=action,
                project_id=project_id,
                conversation_id=conversation_id,
                turn_id=turn_id,
                schedule_id=schedule_id,
                payload=dict(payload or {}),
                now=now,
            )

    @staticmethod
    def _insert_audit(
        connection: sqlite3.Connection,
        *,
        actor_kind: str,
        actor_id: str | None,
        action: str,
        project_id: str | None,
        conversation_id: str | None,
        turn_id: str | None,
        schedule_id: str | None,
        payload: Mapping[str, Any],
        now: int,
        correlation_id: str | None = None,
    ) -> str:
        audit_id = new_id()
        connection.execute(
            """
            INSERT INTO audit_log(
                id, actor_kind, actor_id_hash, action, project_id,
                conversation_id, turn_id, schedule_id, correlation_id,
                payload_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                audit_id,
                actor_kind,
                sha256_text(f"{actor_kind}:{actor_id}")
                if actor_id is not None
                else None,
                action,
                project_id,
                conversation_id,
                turn_id,
                schedule_id,
                correlation_id,
                canonical_json(dict(payload)),
                now,
            ),
        )
        return audit_id

    def _complete_intent_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        intent: sqlite3.Row,
        state: str,
        result: Mapping[str, Any],
        now: int,
        recovered_from: str | None = None,
    ) -> None:
        if state not in {"succeeded", "rejected", "failed"}:
            raise InvariantError("invalid deterministic command-intent state")
        result_json = canonical_json(dict(result))
        if intent["state"] in {"succeeded", "rejected", "failed", "unknown"}:
            if intent["state"] != state or intent["result_json"] != result_json:
                raise ConflictError("command intent already has another result")
            return
        changed = connection.execute(
            """
            UPDATE command_intents
            SET state = ?, result_json = ?, completed_at = ?, updated_at = ?
            WHERE interaction_id = ?
              AND state IN ('accepted', 'effect_in_flight', 'reconciling')
            """,
            (
                state,
                result_json,
                now,
                now,
                intent["interaction_id"],
            ),
        ).rowcount
        if changed != 1:
            raise ConflictError("command intent changed before deterministic completion")
        payload: dict[str, Any] = {
            "command_name": intent["command_name"],
            "request_hash": intent["request_hash"],
            "result_code": result.get("code"),
        }
        if recovered_from is not None:
            payload["recovered_from"] = recovered_from
        self._insert_audit(
            connection,
            actor_kind="system",
            actor_id=None,
            action=f"command.{state}",
            project_id=intent["project_id"],
            conversation_id=intent["conversation_id"],
            turn_id=intent["turn_id"],
            schedule_id=None,
            correlation_id=str(intent["interaction_id"]),
            payload=payload,
            now=now,
        )

    def _complete_local_command_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        interaction_id: str | None,
        conversation_id: str | None,
        now: int,
    ) -> None:
        if interaction_id is None:
            return
        intent = connection.execute(
            "SELECT * FROM command_intents WHERE interaction_id = ?",
            (interaction_id,),
        ).fetchone()
        if intent is None:
            raise NotFoundError(f"command intent not found: {interaction_id}")
        if (
            conversation_id is not None
            and intent["conversation_id"] != conversation_id
        ):
            raise ConflictError("command intent belongs to another Conversation")
        if intent["state"] != "accepted":
            raise ConflictError(
                f"local command cannot commit while intent is {intent['state']}"
            )
        self._complete_intent_in_transaction(
            connection,
            intent=intent,
            state="succeeded",
            result={"code": "ok", "message": "Command completed."},
            now=now,
        )

    def _resolve_provider_barrier_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        conversation_id: str,
        state: str,
        result: Mapping[str, Any],
        clear_barrier: bool,
        now: int,
    ) -> None:
        conversation = connection.execute(
            """
            SELECT provider_barrier_kind, provider_barrier_intent_id
            FROM conversations
            WHERE id = ?
            """,
            (conversation_id,),
        ).fetchone()
        if conversation is None:
            raise NotFoundError(f"conversation not found: {conversation_id}")
        interaction_id = conversation["provider_barrier_intent_id"]
        if interaction_id is not None:
            intent = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if intent is None:
                raise NotFoundError(f"command intent not found: {interaction_id}")
            if intent["conversation_id"] != conversation_id:
                raise ConflictError(
                    "provider barrier intent belongs to another Conversation"
                )
            self._complete_intent_in_transaction(
                connection,
                intent=intent,
                state=state,
                result=result,
                now=now,
            )
        if clear_barrier:
            connection.execute(
                """
                UPDATE conversations
                SET provider_barrier_kind = NULL,
                    provider_barrier_intent_id = NULL,
                    provider_barrier_since = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, conversation_id),
            )

    def ensure_project(
        self,
        *,
        name: str,
        root_path: Path,
    ) -> ProjectRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = _ensure_project(
                connection,
                name=name,
                root_path=root_path,
                now=now,
            )
            return _project(row)

    def bind_project(
        self,
        *,
        name: str,
        root_path: Path,
        guild_id: int,
        channel_id: int,
        sandbox_profile: SandboxProfile = SandboxProfile.FULL_ACCESS,
        command_interaction_id: str | None = None,
    ) -> ProjectRecord:
        if sandbox_profile is not SandboxProfile.FULL_ACCESS:
            raise InvariantError("codexD sandbox is fixed to full_access")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            project = _ensure_project(
                connection,
                name=name,
                root_path=root_path,
                now=now,
            )
            existing = connection.execute(
                """
                SELECT project_id FROM channel_bindings
                WHERE discord_guild_id = ? AND discord_channel_id = ?
                """,
                (str(guild_id), str(channel_id)),
            ).fetchone()
            if existing is not None:
                if existing["project_id"] != project["id"]:
                    raise ConflictError("Discord channel is already bound to another project")
                self._complete_local_command_in_transaction(
                    connection,
                    interaction_id=command_interaction_id,
                    conversation_id=None,
                    now=now,
                )
                return _project(project)
            try:
                connection.execute(
                    """
                    INSERT INTO channel_bindings(
                        discord_guild_id, discord_channel_id, project_id,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(guild_id),
                        str(channel_id),
                        project["id"],
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError(
                    f"project binding conflicts with existing project: {exc}"
                ) from exc
            self._complete_local_command_in_transaction(
                connection,
                interaction_id=command_interaction_id,
                conversation_id=None,
                now=now,
            )
            return _project(project)

    def project_for_channel(self, guild_id: int, channel_id: int) -> ProjectRecord | None:
        row = self.store.query_one(
            """
            SELECT p.* FROM channel_bindings b
            JOIN projects p ON p.id = b.project_id
            WHERE b.discord_guild_id = ? AND b.discord_channel_id = ?
            """,
            (str(guild_id), str(channel_id)),
        )
        return _project(row) if row else None

    def list_enabled_projects(self) -> tuple[ProjectRecord, ...]:
        return tuple(
            _project(row)
            for row in self.store.query_all(
                "SELECT * FROM projects ORDER BY created_at, id"
            )
        )

    def get_project(self, project_id: str) -> ProjectRecord:
        row = self.store.query_one("SELECT * FROM projects WHERE id = ?", (project_id,))
        if row is None:
            raise NotFoundError(f"project not found: {project_id}")
        return _project(row)

    def unbind_project(
        self,
        *,
        guild_id: int,
        channel_id: int,
        confirmation_name: str,
        command_interaction_id: str | None = None,
    ) -> ProjectRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            project = connection.execute(
                """
                SELECT p.* FROM channel_bindings b
                JOIN projects p ON p.id = b.project_id
                WHERE b.discord_guild_id = ? AND b.discord_channel_id = ?
                """,
                (str(guild_id), str(channel_id)),
            ).fetchone()
            if project is None:
                raise NotFoundError("this channel has no active project binding")
            if confirmation_name != project["name"]:
                raise ConflictError("project confirmation name does not match")
            connection.execute(
                """
                DELETE FROM channel_bindings
                WHERE discord_guild_id = ? AND discord_channel_id = ?
                """,
                (str(guild_id), str(channel_id)),
            )
            connection.execute(
                "UPDATE projects SET updated_at = ? WHERE id = ?",
                (now, project["id"]),
            )
            self._complete_local_command_in_transaction(
                connection,
                interaction_id=command_interaction_id,
                conversation_id=None,
                now=now,
            )
            return _project(project)

    def claim_ingress_message(
        self,
        *,
        discord_message_id: str,
        content_hash: str,
        attachment_manifest_hash: str,
        project_id: str,
        conversation_id: str,
        discord_guild_id: int,
        discord_channel_id: int,
        boot_id: str,
    ) -> tuple[bool, str | None]:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT accepted_content_hash, accepted_attachment_manifest_hash,
                       project_id, conversation_id, discord_guild_id,
                       discord_channel_id, turn_id
                FROM ingress_messages
                WHERE discord_message_id = ?
                """,
                (discord_message_id,),
            ).fetchone()
            if row is not None:
                if (
                    row["accepted_content_hash"] != content_hash
                    or row["accepted_attachment_manifest_hash"]
                    != attachment_manifest_hash
                    or row["project_id"] != project_id
                    or row["conversation_id"] != conversation_id
                    or row["discord_guild_id"] != str(discord_guild_id)
                    or row["discord_channel_id"] != str(discord_channel_id)
                ):
                    raise ConflictError(
                        "Discord message ID was already accepted with different content"
                    )
                turn_id = row["turn_id"]
                return False, str(turn_id) if turn_id is not None else None
            connection.execute(
                """
                INSERT INTO ingress_messages(
                    id, discord_message_id, accepted_content_hash,
                    accepted_attachment_manifest_hash, project_id,
                    conversation_id, discord_guild_id, discord_channel_id,
                    state, accepted_boot_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending_preflight', ?, ?)
                """,
                (
                    new_id(),
                    discord_message_id,
                    content_hash,
                    attachment_manifest_hash,
                    project_id,
                    conversation_id,
                    str(discord_guild_id),
                    str(discord_channel_id),
                    boot_id,
                    now,
                ),
            )
            return True, None

    def request_thread_creation(
        self,
        *,
        discord_message_id: str,
        content_hash: str,
        attachment_manifest_hash: str,
        project_id: str,
        discord_guild_id: int,
        discord_channel_id: int,
        owner_user_id: int,
        boot_id: str,
    ) -> tuple[bool, str]:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            project = connection.execute(
                "SELECT * FROM projects WHERE id = ?",
                (project_id,),
            ).fetchone()
            if project is None:
                raise NotFoundError("thread-creation project is missing")
            existing = connection.execute(
                "SELECT * FROM ingress_messages WHERE discord_message_id = ?",
                (discord_message_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["accepted_content_hash"] != content_hash
                    or existing["accepted_attachment_manifest_hash"]
                    != attachment_manifest_hash
                    or existing["project_id"] != project_id
                    or existing["discord_guild_id"] != str(discord_guild_id)
                    or existing["discord_channel_id"] != str(discord_channel_id)
                ):
                    raise ConflictError(
                        "Discord message ID was already accepted with different content"
                    )
                outbox_id = existing["thread_creation_outbox_id"]
                if outbox_id is None:
                    raise InvariantError(
                        "thread-creation ingress is missing its outbox operation"
                    )
                return False, str(outbox_id)
            ingress_id = new_id()
            outbox_id = new_id()
            payload = {
                "kind": "create_thread",
                "starter_message_id": discord_message_id,
                "expected_thread_id": discord_message_id,
                "project_id": project_id,
                "owner_user_id": owner_user_id,
                "name": f"codex-{ingress_id[:8]}",
            }
            connection.execute(
                """
                INSERT INTO discord_outbox(
                    id, destination_key, operation, payload_json, dedupe_key,
                    delivery_marker, state, attempts, next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, 'create_thread', ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    outbox_id,
                    f"channel:{discord_channel_id}",
                    canonical_json(payload),
                    f"thread-create:{discord_message_id}",
                    f"thread-create:{discord_message_id}",
                    now,
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO ingress_messages(
                    id, discord_message_id, accepted_content_hash,
                    accepted_attachment_manifest_hash, project_id, state,
                    discord_guild_id, discord_channel_id,
                    thread_creation_outbox_id, accepted_boot_id, created_at
                ) VALUES (?, ?, ?, ?, ?, 'pending_thread', ?, ?, ?, ?, ?)
                """,
                (
                    ingress_id,
                    discord_message_id,
                    content_hash,
                    attachment_manifest_hash,
                    project_id,
                    str(discord_guild_id),
                    str(discord_channel_id),
                    outbox_id,
                    boot_id,
                    now,
                ),
            )
            return True, outbox_id

    def finalize_thread_creation(
        self,
        *,
        discord_message_id: str,
        discord_thread_id: int,
        owner_user_id: int,
    ) -> ConversationRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            ingress = connection.execute(
                "SELECT * FROM ingress_messages WHERE discord_message_id = ?",
                (discord_message_id,),
            ).fetchone()
            if ingress is None:
                raise NotFoundError(
                    f"thread-creation ingress not found: {discord_message_id}"
                )
            if ingress["conversation_id"] is not None:
                row = connection.execute(
                    "SELECT * FROM conversations WHERE id = ?",
                    (ingress["conversation_id"],),
                ).fetchone()
                if row is None:
                    raise InvariantError(
                        "thread-creation ingress references a missing Conversation"
                    )
                if int(row["discord_thread_id"]) != discord_thread_id:
                    raise ConflictError(
                        "thread-creation reconciliation returned a different thread"
                    )
                return _conversation(row)
            if ingress["state"] != "pending_thread":
                raise ConflictError(
                    f"thread-creation ingress is {ingress['state']}"
                )
            project = connection.execute(
                "SELECT * FROM projects WHERE id = ?",
                (ingress["project_id"],),
            ).fetchone()
            if project is None:
                raise NotFoundError("thread-creation project is missing")
            existing = connection.execute(
                "SELECT * FROM conversations WHERE discord_thread_id = ?",
                (str(discord_thread_id),),
            ).fetchone()
            if existing is not None:
                if existing["project_id"] != ingress["project_id"]:
                    raise ConflictError("Discord thread belongs to another project")
                if int(existing["owner_user_id"]) != owner_user_id:
                    raise ConflictError(
                        "Discord thread belongs to another Conversation owner"
                    )
                conversation_id = str(existing["id"])
            else:
                conversation_id = new_id()
                connection.execute(
                    """
                    INSERT INTO conversations(
                        id, project_id, discord_thread_id, discord_guild_id,
                        discord_parent_channel_id, owner_user_id, state,
                        web_search_mode, sandbox_profile, last_activity_at,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'uninitialized', 'cached', ?, ?, ?, ?)
                    """,
                    (
                        conversation_id,
                        ingress["project_id"],
                        str(discord_thread_id),
                        ingress["discord_guild_id"],
                        ingress["discord_channel_id"],
                        str(owner_user_id),
                        SandboxProfile.FULL_ACCESS.value,
                        now,
                        now,
                        now,
                    ),
                )
            connection.execute(
                """
                UPDATE ingress_messages
                SET conversation_id = ?, state = 'pending_preflight'
                WHERE id = ? AND state = 'pending_thread'
                """,
                (conversation_id, ingress["id"]),
            )
            row = connection.execute(
                "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
            ).fetchone()
            assert row is not None
            return _conversation(row)

    def get_ingress_message(
        self, discord_message_id: str
    ) -> IngressMessageRecord:
        row = self.store.query_one(
            "SELECT * FROM ingress_messages WHERE discord_message_id = ?",
            (discord_message_id,),
        )
        if row is None:
            raise NotFoundError(f"ingress message not found: {discord_message_id}")
        return _ingress(row)

    def reject_ingress_message(
        self,
        *,
        discord_message_id: str,
        error_code: str,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            connection.execute(
                """
                UPDATE ingress_messages
                SET state = 'rejected', error_code = ?, completed_at = ?
                WHERE discord_message_id = ?
                  AND state IN ('pending_thread', 'pending_preflight')
                """,
                (error_code, now, discord_message_id),
            )

    def create_conversation(
        self,
        *,
        project_id: str,
        discord_thread_id: int,
        discord_guild_id: int,
        discord_parent_channel_id: int,
        owner_user_id: int,
    ) -> ConversationRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM conversations WHERE discord_thread_id = ?",
                (str(discord_thread_id),),
            ).fetchone()
            if existing is not None:
                if existing["project_id"] != project_id:
                    raise ConflictError("Discord thread belongs to another project")
                return _conversation(existing)
            conversation_id = new_id()
            connection.execute(
                """
                INSERT INTO conversations(
                    id, project_id, discord_thread_id, discord_guild_id,
                    discord_parent_channel_id, owner_user_id, state,
                    web_search_mode, sandbox_profile, last_activity_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'uninitialized', ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    project_id,
                    str(discord_thread_id),
                    str(discord_guild_id),
                    str(discord_parent_channel_id),
                    str(owner_user_id),
                    "cached",
                    SandboxProfile.FULL_ACCESS.value,
                    now,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
            ).fetchone()
            assert row is not None
            return _conversation(row)

    def conversation_for_thread(self, thread_id: int) -> ConversationRecord | None:
        row = self.store.query_one(
            """
            SELECT * FROM conversations
            WHERE discord_thread_id = ? AND state <> 'deleted'
            """,
            (str(thread_id),),
        )
        return _conversation(row) if row else None

    def get_conversation(self, conversation_id: str) -> ConversationRecord:
        row = self.store.query_one(
            "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
        )
        if row is None:
            raise NotFoundError(f"conversation not found: {conversation_id}")
        return _conversation(row)

    def active_turn_for_conversation(self, conversation_id: str) -> TurnRecord | None:
        row = self.store.query_one(
            """
            SELECT * FROM turns
            WHERE conversation_id = ?
              AND state IN ('starting', 'running', 'cancelling')
            LIMIT 1
            """,
            (conversation_id,),
        )
        return _turn(row) if row else None

    def list_turns(
        self,
        conversation_id: str,
        *,
        limit: int = 10,
        state: str | None = None,
    ) -> tuple[TurnRecord, ...]:
        if limit < 1 or limit > 50:
            raise InvariantError("Turn list limit must be between 1 and 50")
        if state is not None:
            try:
                normalized_state = TurnState(state).value
            except ValueError as exc:
                raise InvariantError(f"invalid Turn state filter: {state}") from exc
            rows = self.store.query_all(
                """
                SELECT * FROM turns
                WHERE conversation_id = ? AND state = ?
                ORDER BY enqueue_sequence DESC
                LIMIT ?
                """,
                (conversation_id, normalized_state, limit),
            )
        else:
            rows = self.store.query_all(
                """
                SELECT * FROM turns
                WHERE conversation_id = ?
                ORDER BY enqueue_sequence DESC
                LIMIT ?
                """,
                (conversation_id, limit),
            )
        return tuple(
            _turn(row)
            for row in rows
        )

    def latest_turn_for_conversation(
        self,
        conversation_id: str,
    ) -> TurnRecord | None:
        row = self.store.query_one(
            """
            SELECT * FROM turns
            WHERE conversation_id = ?
            ORDER BY enqueue_sequence DESC
            LIMIT 1
            """,
            (conversation_id,),
        )
        return _turn(row) if row is not None else None

    def conversation_turn_summary(self, conversation_id: str) -> dict[str, Any]:
        rows = self.store.query_all(
            """
            SELECT state, COUNT(*) AS count
            FROM turns
            WHERE conversation_id = ?
            GROUP BY state
            """,
            (conversation_id,),
        )
        counts = {str(row["state"]): int(row["count"]) for row in rows}
        last_completed = self.store.query_one(
            """
            SELECT id, ended_at FROM turns
            WHERE conversation_id = ? AND state = 'completed'
            ORDER BY ended_at DESC
            LIMIT 1
            """,
            (conversation_id,),
        )
        return {
            "queued": counts.get("queued", 0),
            "active": sum(
                counts.get(state, 0)
                for state in ("starting", "running", "cancelling")
            ),
            "last_completed_id": (
                str(last_completed["id"]) if last_completed is not None else None
            ),
            "last_completed_at": (
                int(last_completed["ended_at"])
                if last_completed is not None
                and last_completed["ended_at"] is not None
                else None
            ),
        }

    def resolve_turn(self, conversation_id: str, turn_ref: str) -> TurnRecord:
        reference = turn_ref.strip().lower()
        if len(reference) < 4:
            raise InvariantError("Turn ID prefix must contain at least 4 characters")
        rows = self.store.query_all(
            """
            SELECT * FROM turns
            WHERE conversation_id = ? AND lower(id) LIKE ?
            ORDER BY enqueue_sequence DESC
            LIMIT 2
            """,
            (conversation_id, f"{reference}%"),
        )
        if not rows:
            raise NotFoundError(f"Turn not found: {turn_ref}")
        if len(rows) > 1:
            raise ConflictError(f"Turn prefix is ambiguous: {turn_ref}")
        return _turn(rows[0])

    def turn_output(self, turn_id: str) -> str | None:
        row = self.store.query_one(
            "SELECT plain_text FROM message_projections WHERE turn_id = ?",
            (turn_id,),
        )
        return str(row["plain_text"]) if row is not None else None

    def render_plan(self, turn_id: str) -> RenderPlanRecord | None:
        row = self.store.query_one(
            "SELECT * FROM discord_render_plans WHERE turn_id = ?",
            (turn_id,),
        )
        return _render_plan(row) if row is not None else None

    def persist_render_plan(
        self,
        *,
        turn_id: str,
        source_sha256: str,
        plan: Mapping[str, Any],
        retention_until: int,
        incident_codes: Sequence[str] = (),
    ) -> RenderPlanRecord:
        now = utc_now_ms()
        plan_json = canonical_json(dict(plan))
        with self.store.transaction() as connection:
            turn_scope = connection.execute(
                """
                SELECT t.conversation_id, c.project_id
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                WHERE t.id = ?
                """,
                (turn_id,),
            ).fetchone()
            if turn_scope is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO discord_render_plans(
                    turn_id, source_sha256, plan_json, retention_until,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    turn_id,
                    source_sha256,
                    plan_json,
                    retention_until,
                    now,
                    now,
                ),
            ).rowcount
            if inserted:
                for code in dict.fromkeys(incident_codes):
                    if not re.fullmatch(r"[a-z][a-z0-9_]{0,127}", code):
                        raise ValueError("render incident code is invalid")
                    _upsert_incident(
                        connection,
                        severity="warning",
                        code=code,
                        summary="Final response used a bounded rendering fallback",
                        now=now,
                        project_id=str(turn_scope["project_id"]),
                        conversation_id=str(turn_scope["conversation_id"]),
                        turn_id=turn_id,
                        details={"source_sha256": source_sha256},
                    )
            row = connection.execute(
                "SELECT * FROM discord_render_plans WHERE turn_id = ?",
                (turn_id,),
            ).fetchone()
            assert row is not None
            record = _render_plan(row)
            if record.source_sha256 != source_sha256:
                raise ConflictError("Turn final response changed after render planning")
            return record

    def latest_event_payload(
        self, conversation_id: str, kind: str
    ) -> dict[str, Any] | None:
        row = self.store.query_one(
            """
            SELECT payload_json FROM events
            WHERE conversation_id = ? AND kind = ?
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (conversation_id, kind),
        )
        if row is None:
            return None
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise StorageError(f"event payload for {kind} is not an object")
        return payload

    def latest_event_payload_for_turn(
        self,
        turn_id: str,
        kind: str,
    ) -> dict[str, Any] | None:
        row = self.store.query_one(
            """
            SELECT payload_json FROM events
            WHERE turn_id = ? AND kind = ?
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (turn_id, kind),
        )
        if row is None:
            return None
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            raise StorageError(f"event payload for {kind} is not an object")
        return payload

    def turn_recorded_diff(self, turn_id: str) -> str | None:
        aggregate = self.latest_event_payload_for_turn(turn_id, "diff.updated")
        if aggregate is not None:
            diff = aggregate.get("diff")
            if not isinstance(diff, str):
                raise StorageError("diff.updated payload is missing its typed diff")
            return diff or None

        rows = self.store.query_all(
            """
            SELECT payload_json
            FROM events
            WHERE turn_id = ? AND kind = 'file_change.completed'
            ORDER BY sequence
            """,
            (turn_id,),
        )
        sections: list[str] = []
        for row in rows:
            payload = json.loads(str(row["payload_json"]))
            if not isinstance(payload, dict) or not isinstance(
                payload.get("changes"), list
            ):
                raise StorageError(
                    "file_change.completed payload is missing typed changes"
                )
            for raw_change in payload["changes"]:
                if not isinstance(raw_change, dict):
                    raise StorageError("file change entry is not an object")
                path = raw_change.get("path")
                kind = raw_change.get("kind")
                diff = raw_change.get("diff")
                if not all(isinstance(value, str) for value in (path, kind, diff)):
                    raise StorageError("file change entry has an invalid typed schema")
                if diff:
                    sections.append(f"# {kind}: {path}\n{diff}")
        return "\n\n".join(sections) or None

    def turn_event_summary(self, turn_id: str) -> dict[str, Any]:
        kinds = {
            str(row["kind"]): int(row["count"])
            for row in self.store.query_all(
                """
                SELECT kind, COUNT(*) AS count
                FROM events
                WHERE turn_id = ?
                GROUP BY kind
                """,
                (turn_id,),
            )
        }
        incidents = tuple(
            {
                "id": str(row["id"]),
                "severity": str(row["severity"]),
                "code": str(row["code"]),
            }
            for row in self.store.query_all(
                """
                SELECT id, severity, code
                FROM incidents
                WHERE turn_id = ? AND resolved_at IS NULL
                ORDER BY last_seen_at DESC
                LIMIT 5
                """,
                (turn_id,),
            )
        )
        delivered = self.store.query_one(
            """
            SELECT o.discord_message_id
            FROM discord_outbox AS o
            JOIN events AS e ON e.sequence = o.event_sequence
            WHERE e.turn_id = ?
              AND o.state = 'sent'
              AND o.discord_message_id IS NOT NULL
            ORDER BY o.updated_at DESC
            LIMIT 1
            """,
            (turn_id,),
        )
        return {
            "tool_events": sum(
                count for kind, count in kinds.items() if kind.startswith("tool.")
            ),
            "file_events": sum(
                count
                for kind, count in kinds.items()
                if kind.startswith("file_change.") or kind == "diff.updated"
            ),
            "usage_observed": "usage.updated" in kinds,
            "incidents": incidents,
            "discord_message_id": (
                str(delivered["discord_message_id"])
                if delivered is not None
                else None
            ),
        }

    def record_steer_accepted(
        self,
        *,
        turn_id: str,
        instruction_hash: str,
        actor_user_id: int | None,
        interaction_id: str,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            turn = connection.execute(
                """
                SELECT t.state, t.conversation_id, c.project_id
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                WHERE t.id = ?
                """,
                (turn_id,),
            ).fetchone()
            if turn is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            intent = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if intent is None:
                raise NotFoundError(f"command intent not found: {interaction_id}")
            if (
                intent["state"] != "effect_in_flight"
                or intent["effect_kind"] != "turn_steer"
                or intent["effect_correlation_id"] != turn_id
                or intent["conversation_id"] != turn["conversation_id"]
            ):
                raise ConflictError("steer command effect identity changed")
            if turn["state"] in {"starting", "running", "cancelling"}:
                insert_progress_update(
                    connection,
                    turn_id=turn_id,
                    state="running",
                    content="Guidance appended to the active Codex Turn.",
                    now=now,
                )
            self._insert_audit(
                connection,
                actor_kind=(
                    "discord_user" if actor_user_id is not None else "system"
                ),
                actor_id=(
                    str(actor_user_id) if actor_user_id is not None else None
                ),
                action="turn.steer_accepted",
                project_id=str(turn["project_id"]),
                conversation_id=str(turn["conversation_id"]),
                turn_id=turn_id,
                schedule_id=None,
                correlation_id=interaction_id,
                payload={"instruction_hash": instruction_hash},
                now=now,
            )
            self._complete_intent_in_transaction(
                connection,
                intent=intent,
                state="succeeded",
                result={"code": "ok", "message": "Steer accepted."},
                now=now,
            )

    def record_steer_rejected(
        self,
        *,
        turn_id: str,
        instruction_hash: str,
        actor_user_id: int | None,
        interaction_id: str,
        code: str,
        message: str,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            intent = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if intent is None:
                raise NotFoundError(f"command intent not found: {interaction_id}")
            if (
                intent["state"] != "effect_in_flight"
                or intent["effect_kind"] != "turn_steer"
                or intent["effect_correlation_id"] != turn_id
            ):
                raise ConflictError("steer command effect identity changed")
            self._insert_audit(
                connection,
                actor_kind=(
                    "discord_user" if actor_user_id is not None else "system"
                ),
                actor_id=(
                    str(actor_user_id) if actor_user_id is not None else None
                ),
                action="turn.steer_rejected",
                project_id=intent["project_id"],
                conversation_id=intent["conversation_id"],
                turn_id=turn_id,
                schedule_id=None,
                correlation_id=interaction_id,
                payload={
                    "instruction_hash": instruction_hash,
                    "error_code": code,
                },
                now=now,
            )
            self._complete_intent_in_transaction(
                connection,
                intent=intent,
                state="rejected",
                result={"code": code, "message": message},
                now=now,
            )

    def count_conversations_for_project(self, project_id: str) -> int:
        row = self.store.query_one(
            "SELECT COUNT(*) AS count FROM conversations WHERE project_id = ?",
            (project_id,),
        )
        return int(row["count"]) if row is not None else 0

    def latest_runtime_lease(
        self,
        *,
        scope_key: str,
    ) -> RuntimeLeaseRecord | None:
        row = self.store.query_one(
            """
            SELECT * FROM runtime_leases
            WHERE scope_key = ?
            ORDER BY generation DESC
            LIMIT 1
            """,
            (scope_key,),
        )
        return _runtime_lease(row) if row is not None else None

    def runtime_lease_diagnostics(
        self,
        *,
        limit: int = 5,
    ) -> tuple[dict[str, str | int | None], ...]:
        if limit < 1 or limit > 20:
            raise InvariantError("runtime lease limit must be between 1 and 20")
        return tuple(
            {
                "scope_kind": str(row["scope_kind"]),
                "scope_hash": sha256_text(str(row["scope_key"]))[:10],
                "generation": int(row["generation"]),
                "state": str(row["state"]),
                "sdk_version": (
                    str(row["sdk_version"])
                    if row["sdk_version"] is not None
                    else None
                ),
                "runtime_version": (
                    str(row["runtime_version"])
                    if row["runtime_version"] is not None
                    else None
                ),
            }
            for row in self.store.query_all(
                """
                SELECT scope_kind, scope_key, generation, state,
                       sdk_version, runtime_version
                FROM runtime_leases
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        )

    def unresolved_incidents(
        self, *, limit: int = 10
    ) -> tuple[dict[str, str | int], ...]:
        if limit < 1 or limit > 50:
            raise InvariantError("incident limit must be between 1 and 50")
        return tuple(
            {
                "severity": str(row["severity"]),
                "code": str(row["code"]),
                "summary": str(row["summary"]),
                "occurrence_count": int(row["occurrence_count"]),
                "last_seen_at": int(row["last_seen_at"]),
            }
            for row in self.store.query_all(
                """
                SELECT severity, code, summary, occurrence_count, last_seen_at
                FROM incidents
                WHERE resolved_at IS NULL
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        )

    def assert_conversation_mutable(
        self,
        conversation_id: str,
        *,
        reject_active_schedules: bool = False,
    ) -> None:
        with self.store.transaction() as connection:
            conversation = connection.execute(
                """
                SELECT state, provider_barrier_kind
                FROM conversations
                WHERE id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if conversation is None:
                raise NotFoundError(f"conversation not found: {conversation_id}")
            if conversation["state"] == "deleted":
                raise ConflictError("Conversation is deleted")
            if conversation["provider_barrier_kind"] is not None:
                raise ConflictError("Conversation has an active provider barrier")
            _assert_conversation_mutable(
                connection,
                conversation_id,
                reject_active_schedules=reject_active_schedules,
            )

    def list_thread_revisions(
        self, conversation_id: str, *, limit: int | None = None
    ) -> tuple[ThreadRevisionRecord, ...]:
        if limit is not None and limit < 1:
            raise InvariantError("revision list limit must be positive")
        sql = """
            SELECT * FROM thread_revisions
            WHERE conversation_id = ?
            ORDER BY created_at DESC, id DESC
        """
        parameters: tuple[object, ...] = (conversation_id,)
        if limit is not None:
            sql += " LIMIT ?"
            parameters = (conversation_id, limit)
        return tuple(
            _revision(row)
            for row in self.store.query_all(sql, parameters)
        )

    def resolve_thread_revision(
        self, conversation_id: str, revision_ref: str
    ) -> ThreadRevisionRecord:
        reference = revision_ref.strip().lower()
        if len(reference) < 4:
            raise InvariantError("revision ID prefix must contain at least 4 characters")
        rows = self.store.query_all(
            """
            SELECT * FROM thread_revisions
            WHERE conversation_id = ? AND lower(id) LIKE ?
            ORDER BY created_at DESC
            LIMIT 2
            """,
            (conversation_id, f"{reference}%"),
        )
        if not rows:
            raise NotFoundError(f"revision not found: {revision_ref}")
        if len(rows) > 1:
            raise ConflictError(f"revision prefix is ambiguous: {revision_ref}")
        return _revision(rows[0])

    def effective_thread_config(self, conversation_id: str) -> ThreadConfig:
        row = self.store.query_one(
            """
            SELECT c.model_override, c.personality_override,
                   c.service_tier_override, c.web_search_mode, c.sandbox_profile,
                   p.default_model, p.default_personality, p.default_service_tier,
                   p.default_web_search_mode
            FROM conversations c
            JOIN projects p ON p.id = c.project_id
            WHERE c.id = ?
            """,
            (conversation_id,),
        )
        if row is None:
            raise NotFoundError(f"conversation not found: {conversation_id}")
        return ThreadConfig(
            model=row["model_override"] or row["default_model"],
            personality=row["personality_override"] or row["default_personality"],
            sandbox=SandboxProfile.FULL_ACCESS,
            service_tier=row["service_tier_override"] or row["default_service_tier"],
            web_search_mode=WebSearchMode(
                str(row["web_search_mode"] or row["default_web_search_mode"])
            ),
        )

    def update_conversation_preferences(
        self,
        conversation_id: str,
        *,
        model_override: str | object | None = ...,
        reasoning_effort_override: str | object | None = ...,
        reasoning_summary_override: str | object | None = ...,
        personality_override: str | object | None = ...,
        service_tier_override: str | object | None = ...,
        web_search_mode: str | None = None,
        command_interaction_id: str | None = None,
    ) -> ConversationRecord:
        assignments: list[str] = []
        values: list[object] = []
        if model_override is not ...:
            assignments.append("model_override = ?")
            values.append(model_override)
        if reasoning_effort_override is not ...:
            assignments.append("reasoning_effort_override = ?")
            values.append(reasoning_effort_override)
        if reasoning_summary_override is not ...:
            assignments.append("reasoning_summary_override = ?")
            values.append(reasoning_summary_override)
        if personality_override is not ...:
            assignments.append("personality_override = ?")
            values.append(personality_override)
        if service_tier_override is not ...:
            assignments.append("service_tier_override = ?")
            values.append(service_tier_override)
        if web_search_mode is not None:
            try:
                normalized_web_search = WebSearchMode(web_search_mode)
            except ValueError as exc:
                raise InvariantError("invalid web search mode") from exc
            if normalized_web_search is WebSearchMode.PROVIDER_DEFAULT_UNCONTROLLED:
                raise InvariantError("uncontrolled web search cannot be selected explicitly")
            assignments.append("web_search_mode = ?")
            values.append(normalized_web_search.value)
        if not assignments:
            if command_interaction_id is not None:
                now = utc_now_ms()
                with self.store.transaction() as connection:
                    self._complete_local_command_in_transaction(
                        connection,
                        interaction_id=command_interaction_id,
                        conversation_id=conversation_id,
                        now=now,
                    )
            return self.get_conversation(conversation_id)
        assignments.extend(["mailbox_version = mailbox_version + 1", "updated_at = ?"])
        now = utc_now_ms()
        values.append(now)
        values.append(conversation_id)
        with self.store.transaction() as connection:
            changed = connection.execute(
                f"""
                UPDATE conversations
                SET {', '.join(assignments)}
                WHERE id = ? AND state <> 'deleted'
                """,
                tuple(values),
            ).rowcount
            if changed != 1:
                raise NotFoundError(
                    f"conversation not found or deleted: {conversation_id}"
                )
            row = connection.execute(
                "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
            ).fetchone()
            assert row is not None
            if row["active_revision_id"] is not None:
                project = connection.execute(
                    """
                    SELECT default_model, default_personality, default_service_tier,
                           default_web_search_mode
                    FROM projects
                    WHERE id = ?
                    """,
                    (row["project_id"],),
                ).fetchone()
                assert project is not None
                revision_config = ThreadConfig(
                    model=row["model_override"] or project["default_model"],
                    personality=(
                        row["personality_override"] or project["default_personality"]
                    ),
                    sandbox=SandboxProfile.FULL_ACCESS,
                    service_tier=(
                        row["service_tier_override"] or project["default_service_tier"]
                    ),
                    web_search_mode=WebSearchMode(
                        str(
                            row["web_search_mode"]
                            or project["default_web_search_mode"]
                        )
                    ),
                )
                connection.execute(
                    """
                    UPDATE thread_revisions
                    SET thread_config_json = ?
                    WHERE id = ? AND state = 'active'
                    """,
                    (
                        canonical_json(revision_config.as_dict()),
                        row["active_revision_id"],
                    ),
                )
            self._complete_local_command_in_transaction(
                connection,
                interaction_id=command_interaction_id,
                conversation_id=conversation_id,
                now=now,
            )
            return _conversation(row)

    def clear_conversation(
        self,
        conversation_id: str,
        *,
        command_interaction_id: str | None = None,
    ) -> ConversationRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            _assert_conversation_mutable(
                connection, conversation_id, reject_active_schedules=True
            )
            connection.execute(
                """
                UPDATE thread_revisions
                SET state = 'superseded'
                WHERE conversation_id = ? AND state = 'active'
                """,
                (conversation_id,),
            )
            changed = connection.execute(
                """
                UPDATE conversations
                SET state = 'uninitialized', active_revision_id = NULL,
                    provider_barrier_kind = NULL, provider_barrier_intent_id = NULL,
                    provider_barrier_since = NULL, mailbox_version = mailbox_version + 1,
                    updated_at = ?
                WHERE id = ? AND state <> 'deleted'
                """,
                (now, conversation_id),
            ).rowcount
            if changed != 1:
                raise NotFoundError(f"conversation not found or deleted: {conversation_id}")
            row = connection.execute(
                "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
            ).fetchone()
            assert row is not None
            self._complete_local_command_in_transaction(
                connection,
                interaction_id=command_interaction_id,
                conversation_id=conversation_id,
                now=now,
            )
            return _conversation(row)

    def archive_active_revision(
        self,
        conversation_id: str,
        revision_id: str,
        *,
        complete_provider_effect: bool = False,
    ) -> ConversationRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            _assert_conversation_mutable(
                connection, conversation_id, reject_active_schedules=True
            )
            revision = connection.execute(
                """
                SELECT id FROM thread_revisions
                WHERE id = ? AND conversation_id = ? AND state = 'active'
                """,
                (revision_id, conversation_id),
            ).fetchone()
            if revision is None:
                raise ConflictError("revision is no longer active")
            connection.execute(
                """
                UPDATE thread_revisions
                SET state = 'archived', archived_at = ?
                WHERE id = ?
                """,
                (now, revision_id),
            )
            changed = connection.execute(
                """
                UPDATE conversations
                SET state = 'archived', active_revision_id = NULL,
                    mailbox_version = mailbox_version + 1, updated_at = ?
                WHERE id = ? AND state <> 'deleted'
                """,
                (now, conversation_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("Conversation was deleted during archive")
            if complete_provider_effect:
                self._resolve_provider_barrier_in_transaction(
                    connection,
                    conversation_id=conversation_id,
                    state="succeeded",
                    result={"code": "ok", "message": "Command completed."},
                    clear_barrier=True,
                    now=now,
                )
            row = connection.execute(
                "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
            ).fetchone()
            assert row is not None
            return _conversation(row)

    def rename_active_revision(
        self,
        conversation_id: str,
        revision_id: str,
        name: str,
        *,
        complete_provider_effect: bool = False,
    ) -> ThreadRevisionRecord:
        now = utc_now_ms()
        outbox_id = new_id()
        with self.store.transaction() as connection:
            conversation = connection.execute(
                """
                SELECT discord_thread_id
                FROM conversations
                WHERE id = ? AND state = 'active' AND active_revision_id = ?
                """,
                (conversation_id, revision_id),
            ).fetchone()
            if conversation is None:
                raise ConflictError("active Thread revision changed before rename commit")
            changed = connection.execute(
                """
                UPDATE thread_revisions
                SET name = ?
                WHERE id = ? AND conversation_id = ? AND state = 'active'
                """,
                (name, revision_id, conversation_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("active Thread revision changed before rename commit")
            connection.execute(
                """
                INSERT INTO discord_outbox(
                    id, destination_key, operation, payload_json, dedupe_key,
                    delivery_marker, state, attempts, next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, 'edit', ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    outbox_id,
                    f"thread:{int(conversation['discord_thread_id'])}",
                    canonical_json({"kind": "thread_rename", "name": name}),
                    f"thread-rename:{outbox_id}",
                    f"codexd-rename:{outbox_id}",
                    now,
                    now,
                    now,
                ),
            )
            if complete_provider_effect:
                self._resolve_provider_barrier_in_transaction(
                    connection,
                    conversation_id=conversation_id,
                    state="succeeded",
                    result={"code": "ok", "message": "Command completed."},
                    clear_barrier=True,
                    now=now,
                )
            row = connection.execute(
                "SELECT * FROM thread_revisions WHERE id = ?", (revision_id,)
            ).fetchone()
            assert row is not None
            return _revision(row)

    def mark_conversation_deleted(self, discord_thread_id: int) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            conversation = connection.execute(
                """
                SELECT id, project_id FROM conversations
                WHERE discord_thread_id = ? AND state <> 'deleted'
                """,
                (str(discord_thread_id),),
            ).fetchone()
            if conversation is None:
                return
            connection.execute(
                """
                UPDATE conversations
                SET state = 'deleted', mailbox_version = mailbox_version + 1,
                    updated_at = ?
                WHERE discord_thread_id = ? AND state <> 'deleted'
                """,
                (now, str(discord_thread_id)),
            )
            blocked_schedules = connection.execute(
                """
                SELECT id, version
                FROM schedules
                WHERE conversation_id = ? AND state IN ('active', 'paused')
                ORDER BY id
                """,
                (conversation["id"],),
            ).fetchall()
            connection.execute(
                """
                UPDATE schedules
                SET state = 'blocked', next_due_at = NULL,
                    version = version + 1, updated_at = ?
                WHERE conversation_id = ? AND state IN ('active', 'paused')
                """,
                (now, conversation["id"]),
            )
            for schedule in blocked_schedules:
                self._insert_audit(
                    connection,
                    actor_kind="system",
                    actor_id=None,
                    action="schedule.block",
                    project_id=str(conversation["project_id"]),
                    conversation_id=str(conversation["id"]),
                    turn_id=None,
                    schedule_id=str(schedule["id"]),
                    correlation_id=(
                        f"schedule:{schedule['id']}:block:"
                        f"discord_thread_deleted:{schedule['version']}"
                    ),
                    payload={
                        "from_version": int(schedule["version"]),
                        "reason": "discord_thread_deleted",
                    },
                    now=now,
                )
            queued_turns = connection.execute(
                """
                SELECT id
                FROM turns
                WHERE conversation_id = ? AND state = 'queued'
                ORDER BY enqueue_sequence
                """,
                (conversation["id"],),
            ).fetchall()
            for turn in queued_turns:
                connection.execute(
                    """
                    UPDATE turns
                    SET state = 'interrupted',
                        terminal_code = 'discord_thread_deleted',
                        interrupt_origin = 'runtime',
                        interrupt_reason = 'target_deleted',
                        queued_input_text = NULL,
                        queued_skill_inputs_json = NULL,
                        ended_at = ?
                    WHERE id = ? AND state = 'queued'
                    """,
                    (now, turn["id"]),
                )
                self._project_local_terminal(
                    connection,
                    turn_id=str(turn["id"]),
                    target=TurnState.INTERRUPTED,
                    terminal_code="discord_thread_deleted",
                    now=now,
                )
            connection.execute(
                """
                INSERT INTO incidents(
                    id, severity, code, project_id, conversation_id, summary,
                    details_json, occurrence_count, first_seen_at, last_seen_at
                ) VALUES (?, 'warning', 'discord_thread_deleted', ?, ?,
                          'Discord Conversation thread was deleted', '{}', 1, ?, ?)
                """,
                (
                    new_id(),
                    conversation["project_id"],
                    conversation["id"],
                    now,
                    now,
                ),
            )

    def activate_thread_revision(
        self,
        *,
        conversation_id: str,
        identity: ThreadIdentity,
        config: ThreadConfig,
        parent_revision_id: str | None = None,
        restore_conversation_config: bool = False,
        update_web_search_only: bool = False,
        complete_provider_effect: bool = False,
    ) -> ThreadRevisionRecord:
        if (
            identity.requested_thread_id is not None
            and identity.thread_id != identity.requested_thread_id
        ):
            self.block_conversation(
                conversation_id, reason="provider_thread_identity_mismatch"
            )
            raise InvariantError("provider resumed a different thread identity")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            conversation = connection.execute(
                "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
            ).fetchone()
            if conversation is None:
                raise NotFoundError(f"conversation not found: {conversation_id}")
            if conversation["state"] == "deleted":
                raise ConflictError("Conversation is deleted")
            existing = connection.execute(
                "SELECT * FROM thread_revisions WHERE provider_thread_id = ?",
                (identity.thread_id,),
            ).fetchone()
            if existing is not None and existing["conversation_id"] != conversation_id:
                raise ConflictError("provider thread is already attached to another conversation")

            connection.execute(
                """
                UPDATE thread_revisions
                SET state = CASE WHEN state = 'active' THEN 'superseded' ELSE state END
                WHERE conversation_id = ? AND state = 'active'
                """,
                (conversation_id,),
            )
            if existing is None:
                revision_id = new_id()
                connection.execute(
                    """
                    INSERT INTO thread_revisions(
                        id, conversation_id, provider_thread_id, provider_session_id,
                        provider_forked_from_thread_id, provider_parent_thread_id,
                        parent_revision_id, state, thread_config_json, requested_resume_id,
                        provider_version, created_at, activated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
                    """,
                    (
                        revision_id,
                        conversation_id,
                        identity.thread_id,
                        identity.provider_session_id,
                        identity.forked_from_thread_id,
                        identity.parent_thread_id,
                        parent_revision_id,
                        canonical_json(config.as_dict()),
                        identity.requested_thread_id,
                        identity.provider_version,
                        now,
                        now,
                    ),
                )
            else:
                revision_id = str(existing["id"])
                connection.execute(
                    """
                    UPDATE thread_revisions
                    SET state = 'active', activated_at = ?, archived_at = NULL,
                        thread_config_json = ?, requested_resume_id = ?
                    WHERE id = ?
                    """,
                    (
                        now,
                        canonical_json(config.as_dict()),
                        identity.requested_thread_id,
                        revision_id,
                    ),
                )
            connection.execute(
                """
                UPDATE conversations
                SET active_revision_id = ?, state = 'active',
                    model_override = CASE WHEN ? THEN ? ELSE model_override END,
                    personality_override = CASE WHEN ? THEN ? ELSE personality_override END,
                    service_tier_override = CASE WHEN ? THEN ? ELSE service_tier_override END,
                    web_search_mode = CASE WHEN ? OR ? THEN ? ELSE web_search_mode END,
                    sandbox_profile = 'full_access',
                    mailbox_version = mailbox_version + 1,
                    last_activity_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    revision_id,
                    restore_conversation_config,
                    config.model,
                    restore_conversation_config,
                    config.personality,
                    restore_conversation_config,
                    config.service_tier,
                    restore_conversation_config,
                    update_web_search_only,
                    config.web_search_mode.value,
                    now,
                    now,
                    conversation_id,
                ),
            )
            if complete_provider_effect:
                self._resolve_provider_barrier_in_transaction(
                    connection,
                    conversation_id=conversation_id,
                    state="succeeded",
                    result={"code": "ok", "message": "Command completed."},
                    clear_barrier=True,
                    now=now,
                )
            row = connection.execute(
                "SELECT * FROM thread_revisions WHERE id = ?", (revision_id,)
            ).fetchone()
            assert row is not None
            return _revision(row)

    def get_active_revision(self, conversation_id: str) -> ThreadRevisionRecord | None:
        row = self.store.query_one(
            """
            SELECT r.* FROM conversations c
            JOIN thread_revisions r ON r.id = c.active_revision_id
            WHERE c.id = ?
            """,
            (conversation_id,),
        )
        return _revision(row) if row else None

    def create_runtime_lease(
        self,
        *,
        scope_kind: str,
        scope_key: str,
        project_id: str | None,
        environment_hash: str,
    ) -> RuntimeLeaseRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(MAX(generation), 0) + 1 AS generation
                FROM runtime_leases WHERE scope_key = ?
                """,
                (scope_key,),
            ).fetchone()
            generation = int(row["generation"])
            lease_id = new_id()
            connection.execute(
                """
                INSERT INTO runtime_leases(
                    id, scope_kind, scope_key, project_id, generation, state,
                    environment_hash, started_at, heartbeat_at
                ) VALUES (?, ?, ?, ?, ?, 'starting', ?, ?, ?)
                """,
                (
                    lease_id,
                    scope_kind,
                    scope_key,
                    project_id,
                    generation,
                    environment_hash,
                    now,
                    now,
                ),
            )
            return RuntimeLeaseRecord(lease_id, scope_key, generation, "starting")

    def mark_runtime_ready(
        self,
        lease_id: str,
        *,
        sdk_version: str,
        runtime_version: str,
        capability_hash: str,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE runtime_leases
                SET state = 'ready', sdk_version = ?, runtime_version = ?,
                    capability_hash = ?, heartbeat_at = ?
                WHERE id = ? AND state = 'starting'
                """,
                (sdk_version, runtime_version, capability_hash, now, lease_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("runtime lease is not starting")

    def mark_runtime_failed(self, lease_id: str, *, failure_code: str) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE runtime_leases
                SET state = 'failed', failure_code = ?, heartbeat_at = ?, ended_at = ?
                WHERE id = ? AND state = 'starting'
                """,
                (failure_code, now, now, lease_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("runtime lease is not starting")

    def heartbeat_runtime(self, lease_id: str) -> bool:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            return (
                connection.execute(
                    """
                    UPDATE runtime_leases
                    SET heartbeat_at = ?
                    WHERE id = ? AND state = 'ready'
                    """,
                    (now, lease_id),
                ).rowcount
                == 1
            )

    def recent_runtime_failure_count(
        self,
        scope_key: str,
        *,
        since_ms: int,
    ) -> int:
        row = self.store.query_one(
            """
            SELECT COUNT(*) AS count
            FROM runtime_leases
            WHERE scope_key = ?
              AND (
                  state IN ('failed', 'unhealthy')
                  OR (state = 'stopped' AND failure_code IS NOT NULL)
              )
              AND ended_at >= ?
            """,
            (scope_key, since_ms),
        )
        return int(row["count"]) if row is not None else 0

    def mark_runtime_stopping(self, lease_id: str) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            lease = connection.execute(
                "SELECT state FROM runtime_leases WHERE id = ?",
                (lease_id,),
            ).fetchone()
            if lease is None:
                raise NotFoundError(f"runtime lease not found: {lease_id}")
            if lease["state"] in {"stopping", "stopped", "failed"}:
                return
            changed = connection.execute(
                """
                UPDATE runtime_leases
                SET state = 'stopping', heartbeat_at = ?
                WHERE id = ? AND state = 'ready'
                """,
                (now, lease_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("runtime lease is not ready")

    def mark_runtime_stopped(self, lease_id: str) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            lease = connection.execute(
                "SELECT state FROM runtime_leases WHERE id = ?",
                (lease_id,),
            ).fetchone()
            if lease is None:
                raise NotFoundError(f"runtime lease not found: {lease_id}")
            if lease["state"] == "stopped":
                return
            changed = connection.execute(
                """
                UPDATE runtime_leases
                SET state = 'stopped', heartbeat_at = ?, ended_at = ?
                WHERE id = ? AND state IN ('stopping', 'failed')
                """,
                (now, now, lease_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("runtime lease is not stopping or close-failed")

    def mark_runtime_close_failed(
        self,
        lease_id: str,
        *,
        failure_code: str,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            lease = connection.execute(
                "SELECT * FROM runtime_leases WHERE id = ?",
                (lease_id,),
            ).fetchone()
            if lease is None:
                raise NotFoundError(f"runtime lease not found: {lease_id}")
            if lease["state"] == "stopped":
                return
            changed = connection.execute(
                """
                UPDATE runtime_leases
                SET state = 'failed', failure_code = ?, heartbeat_at = ?, ended_at = ?
                WHERE id = ?
                  AND state IN (
                      'starting', 'ready', 'stopping', 'unhealthy', 'failed'
                  )
                """,
                (failure_code, now, now, lease_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("runtime lease cannot record a close failure")
            _upsert_incident(
                connection,
                severity="error",
                code="runtime_close_failed",
                summary="Codex runtime did not close cleanly",
                now=now,
                project_id=(
                    str(lease["project_id"])
                    if lease["project_id"] is not None
                    else None
                ),
                details={
                    "lease_id": lease_id,
                    "generation": int(lease["generation"]),
                    "failure_code": failure_code,
                },
            )

    def mark_runtime_unhealthy(self, lease_id: str, *, failure_code: str) -> tuple[str, ...]:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            lease = connection.execute(
                "SELECT * FROM runtime_leases WHERE id = ?", (lease_id,)
            ).fetchone()
            if lease is None:
                raise NotFoundError(f"runtime lease not found: {lease_id}")
            connection.execute(
                """
                UPDATE runtime_leases
                SET state = 'unhealthy', failure_code = ?, heartbeat_at = ?, ended_at = ?
                WHERE id = ?
                """,
                (failure_code, now, now, lease_id),
            )
            rows = connection.execute(
                """
                SELECT id FROM turns
                WHERE runtime_lease_id = ?
                  AND runtime_generation = ?
                  AND state IN ('starting', 'running', 'cancelling')
                """,
                (lease_id, lease["generation"]),
            ).fetchall()
            turn_ids = tuple(str(row["id"]) for row in rows)
            for turn_id in turn_ids:
                connection.execute(
                    """
                    UPDATE turns
                    SET state = 'interrupted', terminal_code = 'runtime_lost',
                        error_code = ?, interrupt_origin = 'runtime',
                        queued_input_text = NULL,
                        queued_skill_inputs_json = NULL, ended_at = ?
                    WHERE id = ?
                      AND state IN ('starting', 'running', 'cancelling')
                    """,
                    (failure_code, now, turn_id),
                )
                self._project_local_terminal(
                    connection,
                    turn_id=turn_id,
                    target=TurnState.INTERRUPTED,
                    terminal_code="runtime_lost",
                    now=now,
                )
            return turn_ids

    def enqueue_turn(
        self,
        *,
        conversation_id: str,
        source: TurnSource,
        turn_input: TurnInput,
        input_message_id: str | None = None,
        schedule_fire_id: str | None = None,
        ingress_message_id: str | None = None,
    ) -> TurnRecord:
        if source is TurnSource.DISCORD and (not input_message_id or schedule_fire_id):
            raise InvariantError("Discord Turn requires only input_message_id")
        if source is TurnSource.SCHEDULE and (not schedule_fire_id or input_message_id):
            raise InvariantError("Schedule Turn requires only schedule_fire_id")
        if ingress_message_id is not None and (
            source is not TurnSource.DISCORD
            or ingress_message_id != input_message_id
        ):
            raise InvariantError("ingress completion must match a Discord Turn source")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            conversation = connection.execute(
                """
                SELECT c.*, p.default_model, p.default_reasoning_effort,
                       p.default_reasoning_summary, p.default_personality,
                       p.default_service_tier,
                       p.default_web_search_mode, p.root_path
                FROM conversations c
                JOIN projects p ON p.id = c.project_id
                WHERE c.id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if conversation is None:
                raise NotFoundError(f"conversation not found: {conversation_id}")
            if conversation["state"] in {"archived", "blocked", "deleted"}:
                raise ConflictError(f"conversation is {conversation['state']}")

            id_column = "input_message_id" if source is TurnSource.DISCORD else "schedule_fire_id"
            id_value = input_message_id if source is TurnSource.DISCORD else schedule_fire_id
            existing = connection.execute(
                f"SELECT * FROM turns WHERE {id_column} = ?", (id_value,)
            ).fetchone()
            if existing is not None:
                if existing["input_hash"] != turn_input.input_hash:
                    raise ConflictError("duplicate Turn source has different input")
                return _turn(existing)

            turn_id = new_id()
            skill_json = (
                canonical_json([skill.snapshot() for skill in turn_input.skill_inputs])
                if turn_input.skill_inputs
                else None
            )
            skill_names_json = (
                canonical_json([skill.name for skill in turn_input.skill_inputs])
                if turn_input.skill_inputs
                else None
            )
            model = conversation["model_override"] or conversation["default_model"]
            effort = (
                conversation["reasoning_effort_override"]
                or conversation["default_reasoning_effort"]
            )
            summary = (
                conversation["reasoning_summary_override"]
                or conversation["default_reasoning_summary"]
            )
            personality = (
                conversation["personality_override"] or conversation["default_personality"]
            )
            tier = conversation["service_tier_override"] or conversation["default_service_tier"]
            web_search = conversation["web_search_mode"] or conversation["default_web_search_mode"]
            connection.execute(
                """
                INSERT INTO turns(
                    id, conversation_id, thread_revision_id, source_kind,
                    input_message_id, schedule_fire_id, state, input_hash,
                    input_summary, queued_input_text, queued_skill_inputs_json,
                    effective_skill_names_json,
                    effective_model, effective_reasoning_effort,
                    effective_reasoning_summary, effective_personality,
                    effective_service_tier, effective_web_search_mode,
                    effective_sandbox, effective_approval_mode, queued_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, 'queued',
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    'auto_review', ?
                )
                """,
                (
                    turn_id,
                    conversation_id,
                    conversation["active_revision_id"],
                    source.value,
                    input_message_id,
                    schedule_fire_id,
                    turn_input.input_hash,
                    redacted_summary(
                        turn_input.text or "",
                        project_root=Path(str(conversation["root_path"])),
                    ),
                    turn_input.text,
                    skill_json,
                    skill_names_json,
                    model,
                    effort,
                    summary,
                    personality,
                    tier,
                    web_search,
                    conversation["sandbox_profile"],
                    now,
                ),
            )
            if ingress_message_id is not None:
                insert_prompt_reaction_update(
                    connection,
                    turn_id=turn_id,
                    input_message_id=input_message_id,
                    discord_thread_id=conversation["discord_thread_id"],
                    discord_parent_channel_id=conversation[
                        "discord_parent_channel_id"
                    ],
                    state="waiting",
                    now=now,
                )
            progress_outbox_id = insert_initial_progress(
                connection,
                turn_id=turn_id,
                discord_thread_id=conversation["discord_thread_id"],
                sandbox_profile=str(conversation["sandbox_profile"]),
                now=now,
            )
            data_root = self.store.path.parent.resolve()
            for image in turn_input.images:
                try:
                    relative_path = image.canonical_path.resolve(strict=True).relative_to(
                        data_root
                    )
                except (OSError, ValueError) as exc:
                    raise InvariantError(
                        "input image must be stored inside the codexD data directory"
                    ) from exc
                connection.execute(
                    """
                    INSERT INTO attachments(
                        id, turn_id, kind, ordinal, relative_path,
                        source_sha256, normalized_sha256, size_bytes, mime_type,
                        width, height, source_name_sanitized, retention_until, created_at
                    ) VALUES (?, ?, 'input_image', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        image.attachment_id,
                        turn_id,
                        image.ordinal,
                        str(relative_path),
                        image.source_sha256,
                        image.sha256,
                        image.size_bytes,
                        image.media_type,
                        image.width,
                        image.height,
                        image.source_name_sanitized,
                        image.retention_until,
                        now,
                    ),
                )
            connection.execute(
                """
                UPDATE conversations
                SET mailbox_version = mailbox_version + 1,
                    last_activity_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, now, conversation_id),
            )
            if ingress_message_id is not None:
                completed = connection.execute(
                    """
                    UPDATE ingress_messages
                    SET state = 'ready', turn_id = ?, progress_outbox_id = ?,
                        completed_at = ?
                    WHERE discord_message_id = ?
                      AND state IN ('pending_thread', 'pending_preflight')
                    """,
                    (turn_id, progress_outbox_id, now, ingress_message_id),
                ).rowcount
                if completed != 1:
                    raise ConflictError(
                        f"Ingress message {ingress_message_id} is no longer pending"
                    )
            row = connection.execute("SELECT * FROM turns WHERE id = ?", (turn_id,)).fetchone()
            assert row is not None
            return _turn(row)

    def queued_conversation_ids(self) -> tuple[str, ...]:
        return tuple(
            str(row["conversation_id"])
            for row in self.store.query_all(
                """
                SELECT conversation_id, MIN(enqueue_sequence) AS first_queued
                FROM turns
                WHERE state = 'queued'
                GROUP BY conversation_id
                ORDER BY first_queued
                """
            )
        )

    def queued_schedule_turn_ids(self) -> tuple[str, ...]:
        return tuple(
            str(row["id"])
            for row in self.store.query_all(
                """
                SELECT id
                FROM turns
                WHERE state = 'queued' AND source_kind = 'schedule'
                ORDER BY enqueue_sequence
                """
            )
        )

    def next_queued_turn(self, conversation_id: str) -> TurnRecord | None:
        row = self.store.query_one(
            """
            SELECT * FROM turns
            WHERE conversation_id = ? AND state = 'queued'
            ORDER BY enqueue_sequence
            LIMIT 1
            """,
            (conversation_id,),
        )
        return _turn(row) if row else None

    def load_turn_input(self, turn_id: str) -> TurnInput:
        turn = self.get_turn(turn_id)
        if turn.state not in {TurnState.QUEUED, TurnState.STARTING}:
            raise ConflictError("provider input is available only before a Turn is accepted")
        skills_raw = (
            json.loads(turn.queued_skill_inputs_json)
            if turn.queued_skill_inputs_json is not None
            else []
        )
        if not isinstance(skills_raw, list):
            raise ConflictError("queued skill snapshot is invalid")
        skills: list[TurnSkill] = []
        for raw in skills_raw:
            if not isinstance(raw, dict):
                raise ConflictError("queued skill snapshot entry is invalid")
            path = Path(raw["canonical_path"])
            if not path.is_file() or sha256_file(path) != raw["content_hash"]:
                raise ConflictError(f"queued skill input changed: {raw['name']}")
            skills.append(
                TurnSkill(
                    name=str(raw["name"]),
                    canonical_path=path,
                    content_hash=str(raw["content_hash"]),
                )
            )
        images: list[TurnImage] = []
        for row in self.store.query_all(
            """
            SELECT * FROM attachments
            WHERE turn_id = ? AND kind = 'input_image'
            ORDER BY ordinal, id
            """,
            (turn_id,),
        ):
            relative = Path(str(row["relative_path"]))
            if relative.is_absolute() or any(
                part in {"", ".", ".."} for part in relative.parts
            ):
                raise ConflictError(f"queued image path is invalid: {row['id']}")
            data_root = self.store.path.parent.resolve()
            unresolved = data_root.joinpath(*relative.parts)
            current = unresolved
            while current != data_root:
                if current.is_symlink():
                    raise ConflictError(
                        f"queued image path contains a symlink: {row['id']}"
                    )
                current = current.parent
            path = unresolved.resolve(strict=True)
            if not path.is_relative_to(data_root) or not path.is_file():
                raise ConflictError(f"queued image path changed: {row['id']}")
            if sha256_file(path) != row["normalized_sha256"]:
                raise ConflictError(f"queued image attachment changed: {row['id']}")
            images.append(
                TurnImage(
                    attachment_id=str(row["id"]),
                    ordinal=int(row["ordinal"]),
                    canonical_path=path,
                    media_type=str(row["mime_type"]),
                    source_sha256=str(row["source_sha256"]),
                    sha256=str(row["normalized_sha256"]),
                    size_bytes=int(row["size_bytes"]),
                    width=int(row["width"]),
                    height=int(row["height"]),
                    source_name_sanitized=str(row["source_name_sanitized"]),
                    retention_until=int(row["retention_until"]),
                )
            )
        turn_input = TurnInput(
            text=turn.queued_input_text,
            images=tuple(images),
            skill_inputs=tuple(skills),
        )
        if turn_input.input_hash != turn.input_hash:
            raise ConflictError("queued Turn input snapshot hash changed")
        return turn_input

    def add_input_attachment(
        self,
        *,
        turn_id: str,
        attachment_id: str,
        ordinal: int,
        relative_path: str,
        source_sha256: str,
        normalized_sha256: str,
        size_bytes: int,
        mime_type: str,
        width: int,
        height: int,
        source_name_sanitized: str,
        retention_until: int,
    ) -> None:
        with self.store.transaction() as connection:
            connection.execute(
                """
                INSERT INTO attachments(
                    id, turn_id, kind, ordinal, relative_path,
                    source_sha256, normalized_sha256, size_bytes, mime_type,
                    width, height, source_name_sanitized, retention_until, created_at
                ) VALUES (?, ?, 'input_image', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attachment_id,
                    turn_id,
                    ordinal,
                    relative_path,
                    source_sha256,
                    normalized_sha256,
                    size_bytes,
                    mime_type,
                    width,
                    height,
                    source_name_sanitized,
                    retention_until,
                    utc_now_ms(),
                ),
            )

    def get_turn(self, turn_id: str) -> TurnRecord:
        row = self.store.query_one("SELECT * FROM turns WHERE id = ?", (turn_id,))
        if row is None:
            raise NotFoundError(f"Turn not found: {turn_id}")
        return _turn(row)

    def set_provider_barrier(self, conversation_id: str, kind: str) -> None:
        if kind not in {"compact", "external_active", "unknown_effect"}:
            raise InvariantError(f"invalid provider barrier: {kind}")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE conversations
                SET provider_barrier_kind = COALESCE(provider_barrier_kind, ?),
                    provider_barrier_since = COALESCE(provider_barrier_since, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (kind, now, now, conversation_id),
            ).rowcount
            if changed != 1:
                raise NotFoundError(f"conversation not found: {conversation_id}")

    def begin_provider_barrier_effect(
        self,
        *,
        conversation_id: str,
        interaction_id: str,
        kind: str,
        effect_kind: str,
        effect_correlation_id: str | None,
    ) -> None:
        if kind not in {"compact", "unknown_effect"}:
            raise InvariantError("invalid provider command barrier")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            intent = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if intent is None:
                raise NotFoundError(f"command intent not found: {interaction_id}")
            if intent["conversation_id"] != conversation_id:
                raise ConflictError("command intent belongs to another Conversation")
            if intent["state"] not in {"accepted", "effect_in_flight"}:
                raise ConflictError(
                    f"command intent cannot start provider effect from {intent['state']}"
                )
            if intent["state"] == "effect_in_flight" and (
                intent["effect_kind"] != effect_kind
                or intent["effect_correlation_id"] != effect_correlation_id
            ):
                raise ConflictError("command effect identity changed")
            conversation = connection.execute(
                """
                SELECT provider_barrier_kind, provider_barrier_intent_id
                FROM conversations
                WHERE id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if conversation is None:
                raise NotFoundError(f"conversation not found: {conversation_id}")
            if conversation["provider_barrier_kind"] is not None and (
                conversation["provider_barrier_kind"] != kind
                or conversation["provider_barrier_intent_id"] != interaction_id
            ):
                raise ConflictError("Conversation has another provider barrier")
            connection.execute(
                """
                UPDATE command_intents
                SET state = 'effect_in_flight', effect_kind = ?,
                    effect_correlation_id = ?, updated_at = ?
                WHERE interaction_id = ?
                """,
                (effect_kind, effect_correlation_id, now, interaction_id),
            )
            connection.execute(
                """
                UPDATE conversations
                SET provider_barrier_kind = ?, provider_barrier_intent_id = ?,
                    provider_barrier_since = COALESCE(provider_barrier_since, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (kind, interaction_id, now, now, conversation_id),
            )

    def clear_provider_barrier(self, conversation_id: str) -> None:
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE conversations
                SET provider_barrier_kind = NULL,
                    provider_barrier_intent_id = NULL,
                    provider_barrier_since = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (utc_now_ms(), conversation_id),
            ).rowcount
            if changed != 1:
                raise NotFoundError(f"conversation not found: {conversation_id}")

    def resolve_provider_barrier_effect(
        self,
        conversation_id: str,
        *,
        state: str,
        code: str,
        message: str,
        clear_barrier: bool = True,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            self._resolve_provider_barrier_in_transaction(
                connection,
                conversation_id=conversation_id,
                state=state,
                result={"code": code, "message": message},
                clear_barrier=clear_barrier,
                now=now,
            )

    def mark_provider_barrier_outcome_unknown(
        self,
        conversation_id: str,
        *,
        code: str,
        message: str,
        block_conversation: bool = False,
    ) -> None:
        now = utc_now_ms()
        result_json = canonical_json({"code": code, "message": message})
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT c.project_id, c.provider_barrier_kind,
                       c.provider_barrier_intent_id, i.*
                FROM conversations c
                LEFT JOIN command_intents i
                  ON i.interaction_id = c.provider_barrier_intent_id
                WHERE c.id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"conversation not found: {conversation_id}")
            if row["provider_barrier_kind"] not in {"compact", "unknown_effect"}:
                raise ConflictError("Conversation does not have a provider barrier")
            interaction_id = row["provider_barrier_intent_id"]
            if interaction_id is not None:
                if row["state"] in {"succeeded", "rejected", "failed"}:
                    raise ConflictError(
                        "provider command is already deterministically "
                        f"{row['state']}"
                    )
                if row["state"] == "unknown":
                    if row["result_json"] != result_json:
                        raise ConflictError(
                            "provider command already has another unknown result"
                        )
                else:
                    if row["state"] not in {"effect_in_flight", "reconciling"}:
                        raise ConflictError(
                            "provider command cannot become unknown from "
                            f"{row['state']}"
                        )
                    changed = connection.execute(
                        """
                        UPDATE command_intents
                        SET state = 'unknown', result_json = ?,
                            completed_at = ?, updated_at = ?
                        WHERE interaction_id = ?
                          AND state IN ('effect_in_flight', 'reconciling')
                        """,
                        (result_json, now, now, interaction_id),
                    ).rowcount
                    if changed != 1:
                        raise ConflictError(
                            "provider command changed before unknown completion"
                        )
                    self._insert_audit(
                        connection,
                        actor_kind="system",
                        actor_id=None,
                        action="command.unknown",
                        project_id=row["project_id"],
                        conversation_id=conversation_id,
                        turn_id=row["turn_id"],
                        schedule_id=None,
                        correlation_id=str(interaction_id),
                        payload={
                            "command_name": row["command_name"],
                            "request_hash": row["request_hash"],
                            "result_code": code,
                        },
                        now=now,
                    )
            if block_conversation:
                connection.execute(
                    """
                    UPDATE conversations
                    SET state = 'blocked',
                        provider_barrier_kind = 'unknown_effect',
                        updated_at = ?
                    WHERE id = ? AND state <> 'deleted'
                    """,
                    (now, conversation_id),
                )
                self._insert_audit(
                    connection,
                    actor_kind="system",
                    actor_id=None,
                    action="conversation.blocked",
                    project_id=row["project_id"],
                    conversation_id=conversation_id,
                    turn_id=None,
                    schedule_id=None,
                    payload={"reason": code},
                    now=now,
                )

    def provider_barrier_conversation_ids(self) -> tuple[str, ...]:
        return tuple(
            str(row["id"])
            for row in self.store.query_all(
                """
                SELECT id
                FROM conversations
                WHERE provider_barrier_kind IS NOT NULL
                  AND state <> 'deleted'
                ORDER BY provider_barrier_since, id
                """
            )
        )

    def resolve_idle_provider_barrier(self, conversation_id: str) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT c.provider_barrier_kind, c.provider_barrier_intent_id,
                       c.provider_barrier_since, c.discord_thread_id,
                       i.state AS intent_state
                FROM conversations c
                LEFT JOIN command_intents i
                  ON i.interaction_id = c.provider_barrier_intent_id
                WHERE c.id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"conversation not found: {conversation_id}")
            if row["provider_barrier_kind"] is None:
                return
            intent_unknown = row["intent_state"] in {"reconciling", "unknown"}
            if row["intent_state"] == "reconciling":
                connection.execute(
                    """
                    UPDATE command_intents
                    SET state = 'unknown', result_json = ?,
                        completed_at = ?, updated_at = ?
                    WHERE interaction_id = ? AND state = 'reconciling'
                    """,
                    (
                        canonical_json(
                            {
                                "code": "provider_effect_outcome_unknown",
                                "message": (
                                    "The provider Thread is idle and usable, but the "
                                    "pre-restart effect outcome cannot be proven."
                                ),
                            }
                        ),
                        now,
                        now,
                        row["provider_barrier_intent_id"],
                    ),
                )
            connection.execute(
                """
                UPDATE conversations
                SET provider_barrier_kind = NULL,
                    provider_barrier_intent_id = NULL,
                    provider_barrier_since = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, conversation_id),
            )
            outbox_id = new_id()
            barrier_key = (
                str(row["provider_barrier_since"])
                if row["provider_barrier_since"] is not None
                else outbox_id
            )
            content = (
                "Codex Thread is idle and usable again. The exact pre-restart "
                "provider effect outcome remains unknown."
                if intent_unknown
                else "Codex Thread returned to idle; queued Turns may continue."
            )
            connection.execute(
                """
                INSERT INTO discord_outbox(
                    id, destination_key, operation, payload_json, dedupe_key,
                    delivery_marker, state, attempts, next_attempt_at,
                    created_at, updated_at
                ) VALUES (?, ?, 'send', ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    outbox_id,
                    f"thread:{row['discord_thread_id']}",
                    canonical_json({"content": content}),
                    f"provider-barrier:{conversation_id}:{barrier_key}:resolved",
                    f"barrier-{conversation_id[:8]}-{barrier_key}",
                    now,
                    now,
                    now,
                ),
            )

    def block_conversation(self, conversation_id: str, *, reason: str) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE conversations
                SET state = 'blocked', updated_at = ?
                WHERE id = ? AND state <> 'deleted'
                """,
                (now, conversation_id),
            ).rowcount
            if changed != 1:
                raise NotFoundError(f"conversation not found or deleted: {conversation_id}")
            connection.execute(
                """
                INSERT INTO audit_log(
                    id, actor_kind, action, conversation_id, payload_json, occurred_at
                ) VALUES (?, 'system', 'conversation.blocked', ?, ?, ?)
                """,
                (new_id(), conversation_id, canonical_json({"reason": reason}), now),
            )

    def record_incident(
        self,
        *,
        severity: str,
        code: str,
        summary: str,
        project_id: str | None = None,
        conversation_id: str | None = None,
        turn_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> str:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            return _upsert_incident(
                connection,
                severity=severity,
                code=code,
                summary=summary,
                now=now,
                project_id=project_id,
                conversation_id=conversation_id,
                turn_id=turn_id,
                details=details,
            )

    def attach_turn_revision(self, turn_id: str, revision_id: str) -> None:
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT t.state AS turn_state,
                       t.thread_revision_id,
                       t.conversation_id,
                       c.state AS conversation_state,
                       c.active_revision_id,
                       r.conversation_id AS revision_conversation_id,
                       r.state AS revision_state
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                LEFT JOIN thread_revisions r ON r.id = ?
                WHERE t.id = ?
                """,
                (revision_id, turn_id),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            if (
                row["turn_state"] != "queued"
                or row["thread_revision_id"] is not None
                or row["conversation_state"] != "active"
                or row["active_revision_id"] != revision_id
                or row["revision_conversation_id"] != row["conversation_id"]
                or row["revision_state"] != "active"
            ):
                raise ConflictError(
                    "Turn revision is not the active revision for this Conversation"
                )
            changed = connection.execute(
                """
                UPDATE turns SET thread_revision_id = ?
                WHERE id = ? AND state = 'queued' AND thread_revision_id IS NULL
                """,
                (revision_id, turn_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("Turn is not an uninitialized queued Turn")

    def claim_turn(
        self,
        turn_id: str,
        *,
        runtime_lease_id: str,
        runtime_generation: int,
    ) -> TurnRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT t.*, c.project_id,
                       c.state AS conversation_state,
                       c.active_revision_id,
                       r.conversation_id AS revision_conversation_id,
                       r.state AS revision_state
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                LEFT JOIN thread_revisions r ON r.id = t.thread_revision_id
                WHERE t.id = ?
                """,
                (turn_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            assert_turn_transition(TurnState(row["state"]), TurnState.STARTING)
            if row["thread_revision_id"] is None:
                raise InvariantError("Turn must have a thread revision before provider start")
            if (
                row["conversation_state"] != "active"
                or row["active_revision_id"] != row["thread_revision_id"]
                or row["revision_conversation_id"] != row["conversation_id"]
                or row["revision_state"] != "active"
            ):
                raise ConflictError(
                    "Turn does not target the active revision for its Conversation"
                )
            lease = connection.execute(
                "SELECT * FROM runtime_leases WHERE id = ?",
                (runtime_lease_id,),
            ).fetchone()
            if lease is None:
                raise ConflictError("runtime lease does not exist")
            if (
                int(lease["generation"]) != runtime_generation
                or lease["state"] != "ready"
                or not _runtime_lease_matches_project(lease, str(row["project_id"]))
            ):
                raise ConflictError("runtime lease is stale or belongs to another scope")
            newer = connection.execute(
                """
                SELECT 1 FROM runtime_leases
                WHERE scope_kind = ? AND scope_key = ?
                  AND generation > ?
                  AND state IN ('starting', 'ready', 'unhealthy')
                LIMIT 1
                """,
                (
                    lease["scope_kind"],
                    lease["scope_key"],
                    runtime_generation,
                ),
            ).fetchone()
            if newer is not None:
                raise ConflictError("runtime lease generation has been superseded")
            try:
                changed = connection.execute(
                    """
                    UPDATE turns
                    SET state = 'starting', runtime_lease_id = ?,
                        runtime_generation = ?, started_at = ?
                    WHERE id = ? AND state = 'queued'
                    """,
                    (runtime_lease_id, runtime_generation, now, turn_id),
                ).rowcount
            except sqlite3.IntegrityError as exc:
                raise ConflictError("conversation already has an active Turn") from exc
            if changed != 1:
                raise ConflictError("Turn was claimed concurrently")
            updated = connection.execute("SELECT * FROM turns WHERE id = ?", (turn_id,)).fetchone()
            assert updated is not None
            return _turn(updated)

    def mark_turn_running(self, turn_id: str, provider_turn_id: str) -> TurnRecord:
        with self.store.transaction() as connection:
            row = connection.execute("SELECT * FROM turns WHERE id = ?", (turn_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            current = TurnState(row["state"])
            if current not in {TurnState.STARTING, TurnState.CANCELLING}:
                raise ConflictError(f"Turn cannot accept provider identity from {current.value}")
            target = TurnState.RUNNING if current is TurnState.STARTING else TurnState.CANCELLING
            connection.execute(
                """
                UPDATE turns
                SET state = ?, provider_turn_id = ?,
                    queued_input_text = NULL, queued_skill_inputs_json = NULL
                WHERE id = ?
                """,
                (target.value, provider_turn_id, turn_id),
            )
            updated = connection.execute("SELECT * FROM turns WHERE id = ?", (turn_id,)).fetchone()
            assert updated is not None
            return _turn(updated)

    def request_cancel(
        self,
        turn_id: str,
        *,
        origin: InterruptOrigin,
        command_interaction_id: str | None = None,
    ) -> TurnRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute("SELECT * FROM turns WHERE id = ?", (turn_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            state = TurnState(row["state"])
            intent: sqlite3.Row | None = None
            starts_provider_effect = False
            if command_interaction_id is not None:
                intent = connection.execute(
                    "SELECT * FROM command_intents WHERE interaction_id = ?",
                    (command_interaction_id,),
                ).fetchone()
                if intent is None:
                    raise NotFoundError(
                        f"command intent not found: {command_interaction_id}"
                    )
                conversation = connection.execute(
                    "SELECT project_id FROM conversations WHERE id = ?",
                    (row["conversation_id"],),
                ).fetchone()
                assert conversation is not None
                if (
                    intent["conversation_id"] != row["conversation_id"]
                    or intent["project_id"] != conversation["project_id"]
                    or (
                        intent["turn_id"] is not None
                        and intent["turn_id"] != turn_id
                    )
                ):
                    raise ConflictError("cancel command belongs to another Turn scope")
                starts_provider_effect = (
                    state in {TurnState.STARTING, TurnState.RUNNING}
                    and row["interrupt_reason"] is None
                )
                if intent["state"] == "effect_in_flight":
                    if (
                        intent["effect_kind"] != "turn_cancel"
                        or intent["effect_correlation_id"] != turn_id
                    ):
                        raise ConflictError("cancel command effect identity changed")
                elif intent["state"] == "accepted" and starts_provider_effect:
                    connection.execute(
                        """
                        UPDATE command_intents
                        SET state = 'effect_in_flight',
                            effect_kind = 'turn_cancel',
                            effect_correlation_id = ?,
                            turn_id = COALESCE(turn_id, ?),
                            updated_at = ?
                        WHERE interaction_id = ? AND state = 'accepted'
                        """,
                        (
                            turn_id,
                            turn_id,
                            now,
                            command_interaction_id,
                        ),
                    )
                elif intent["state"] != "accepted":
                    raise ConflictError(
                        "cancel command effect cannot start while intent is "
                        f"{intent['state']}"
                    )
            if state.terminal:
                if intent is not None:
                    self._complete_intent_in_transaction(
                        connection,
                        intent=intent,
                        state="succeeded",
                        result={
                            "code": "already_terminal",
                            "message": f"Turn is already {state.value}.",
                        },
                        now=now,
                    )
                return _turn(row)
            if state is TurnState.QUEUED:
                target = TurnState.CANCELLED
                ended_at: int | None = now
            elif state in {TurnState.STARTING, TurnState.RUNNING}:
                target = TurnState.CANCELLING
                ended_at = None
            else:
                target = state
                ended_at = None
            if target is not state:
                assert_turn_transition(state, target)
            connection.execute(
                """
                UPDATE turns
                SET state = ?, interrupt_origin = ?,
                    interrupt_reason = CASE
                        WHEN ? = 'cancelled' THEN 'cancelled_before_start'
                        WHEN interrupt_reason IS NULL THEN 'requested'
                        ELSE interrupt_reason
                    END,
                    ended_at = COALESCE(?, ended_at),
                    terminal_code = CASE WHEN ? = 'cancelled' THEN 'cancelled_before_start'
                                         ELSE terminal_code END,
                    queued_input_text = CASE
                        WHEN ? = 'cancelled' THEN NULL ELSE queued_input_text END,
                    queued_skill_inputs_json = CASE
                        WHEN ? = 'cancelled' THEN NULL ELSE queued_skill_inputs_json END
                WHERE id = ?
                """,
                (
                    target.value,
                    origin.value,
                    target.value,
                    ended_at,
                    target.value,
                    target.value,
                    target.value,
                    turn_id,
                ),
            )
            insert_progress_update(
                connection,
                turn_id=turn_id,
                state=(
                    "terminal"
                    if target is TurnState.CANCELLED
                    else "cancelling"
                ),
                content=(
                    "Cancelled · before provider start"
                    if target is TurnState.CANCELLED
                    else "Cancelling · interrupt requested"
                ),
                now=now,
            )
            if target is TurnState.CANCELLED:
                self._project_local_terminal(
                    connection,
                    turn_id=turn_id,
                    target=target,
                    terminal_code="cancelled_before_start",
                    now=now,
                    progress_already_projected=True,
                )
            if (
                intent is not None
                and intent["state"] == "accepted"
                and not starts_provider_effect
            ):
                self._complete_intent_in_transaction(
                    connection,
                    intent=intent,
                    state="succeeded",
                    result={
                        "code": "ok",
                        "message": (
                            "Turn cancelled before provider start."
                            if target is TurnState.CANCELLED
                            else "Cancellation was already recorded."
                        ),
                    },
                    now=now,
                )
            updated = connection.execute(
                "SELECT * FROM turns WHERE id = ?", (turn_id,)
            ).fetchone()
            assert updated is not None
            return _turn(updated)

    def claim_turn_interrupt(self, turn_id: str) -> bool:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT t.state, t.provider_turn_id, t.interrupt_reason,
                       c.project_id, t.conversation_id
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                WHERE t.id = ?
                """,
                (turn_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            if (
                row["state"] != TurnState.CANCELLING.value
                or row["provider_turn_id"] is None
                or row["interrupt_reason"] != "requested"
            ):
                return False
            changed = connection.execute(
                """
                UPDATE turns
                SET interrupt_reason = 'sending'
                WHERE id = ? AND state = 'cancelling'
                  AND provider_turn_id IS NOT NULL
                  AND interrupt_reason = 'requested'
                """,
                (turn_id,),
            ).rowcount
            if changed != 1:
                return False
            self._insert_audit(
                connection,
                actor_kind="system",
                actor_id=None,
                action="turn.interrupt_sending",
                project_id=row["project_id"],
                conversation_id=row["conversation_id"],
                turn_id=turn_id,
                schedule_id=None,
                payload={},
                now=now,
            )
            return True

    def resolve_turn_interrupt(
        self,
        turn_id: str,
        *,
        outcome: str,
        code: str,
    ) -> TurnRecord:
        if outcome not in {"sent", "failed", "unknown"}:
            raise InvariantError("invalid Turn interrupt outcome")
        now = utc_now_ms()
        reason = "interrupt_sent" if outcome == "sent" else f"interrupt_{outcome}:{code}"
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT t.*, c.project_id
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                WHERE t.id = ?
                """,
                (turn_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            if row["interrupt_reason"] == reason:
                return _turn(row)
            if row["interrupt_reason"] != "sending":
                raise ConflictError(
                    f"Turn interrupt cannot resolve from {row['interrupt_reason']}"
                )
            connection.execute(
                "UPDATE turns SET interrupt_reason = ? WHERE id = ?",
                (reason, turn_id),
            )
            self._insert_audit(
                connection,
                actor_kind="system",
                actor_id=None,
                action=f"turn.interrupt_{outcome}",
                project_id=row["project_id"],
                conversation_id=row["conversation_id"],
                turn_id=turn_id,
                schedule_id=None,
                payload={"code": code},
                now=now,
            )
            updated = connection.execute(
                "SELECT * FROM turns WHERE id = ?",
                (turn_id,),
            ).fetchone()
            assert updated is not None
            return _turn(updated)

    def append_event(
        self,
        *,
        project_id: str,
        conversation_id: str,
        turn_id: str | None,
        runtime_generation: int | None,
        event: NormalizedEvent,
    ) -> int:
        with self.store.transaction() as connection:
            local_index: int | None = None
            if turn_id:
                turn = connection.execute(
                    """
                    SELECT t.state, t.runtime_lease_id, t.runtime_generation,
                           c.project_id, c.id AS conversation_id,
                           rl.state AS lease_state,
                           rl.generation AS lease_generation,
                           rl.scope_kind AS lease_scope_kind,
                           rl.scope_key AS lease_scope_key,
                           rl.project_id AS lease_project_id
                    FROM turns t
                    JOIN conversations c ON c.id = t.conversation_id
                    LEFT JOIN runtime_leases rl ON rl.id = t.runtime_lease_id
                    WHERE t.id = ?
                    """,
                    (turn_id,),
                ).fetchone()
                if turn is None:
                    raise NotFoundError(f"Turn not found: {turn_id}")
                if (
                    str(turn["project_id"]) != project_id
                    or str(turn["conversation_id"]) != conversation_id
                ):
                    raise ConflictError("event scope does not match its Turn")
                if TurnState(str(turn["state"])).terminal:
                    raise ConflictError("provider event arrived after Turn termination")
                if (
                    turn["runtime_lease_id"] is None
                    or turn["runtime_generation"] != runtime_generation
                    or turn["lease_generation"] != runtime_generation
                    or turn["lease_state"] != "ready"
                    or not _runtime_lease_matches_project(
                        turn, str(turn["project_id"]), prefix="lease_"
                    )
                ):
                    raise ConflictError("provider event runtime lease is no longer valid")
                row = connection.execute(
                    """
                    SELECT COALESCE(MAX(local_event_index), 0) + 1 AS next
                    FROM events WHERE turn_id = ?
                    """,
                    (turn_id,),
                ).fetchone()
                local_index = int(row["next"])
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO events(
                        event_id, turn_id, project_id, conversation_id, runtime_generation,
                        provider_event_id, local_event_index, kind, schema_version,
                        payload_json, raw_type, raw_hash, raw_size, occurred_at, recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id(),
                        turn_id,
                        project_id,
                        conversation_id,
                        runtime_generation,
                        event.provider_event_id,
                        local_index,
                        event.kind,
                        event.schema_version,
                        canonical_json(dict(event.payload)),
                        event.raw_type,
                        event.raw_hash,
                        event.raw_size,
                        event.occurred_at,
                        utc_now_ms(),
                    ),
                )
            except sqlite3.IntegrityError:
                if turn_id and event.provider_event_id:
                    row = connection.execute(
                        """
                        SELECT sequence FROM events
                        WHERE turn_id = ? AND provider_event_id = ?
                        """,
                        (turn_id, event.provider_event_id),
                    ).fetchone()
                    if row:
                        return int(row["sequence"])
                raise
            if cursor.lastrowid is None:
                raise StorageError("event insert did not produce a sequence")
            return cursor.lastrowid

    def terminal_turn(
        self,
        turn_id: str,
        *,
        target: TurnState,
        terminal_code: str,
        error_code: str | None = None,
        error_message_redacted: str | None = None,
    ) -> TurnRecord:
        if not target.terminal:
            raise InvariantError("terminal_turn requires a terminal target")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute("SELECT * FROM turns WHERE id = ?", (turn_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            current = TurnState(row["state"])
            if current.terminal:
                self._project_local_terminal(
                    connection,
                    turn_id=turn_id,
                    target=current,
                    terminal_code=str(row["terminal_code"] or terminal_code),
                    now=now,
                )
                return _turn(row)
            assert_turn_transition(current, target)
            connection.execute(
                """
                UPDATE turns
                SET state = ?, terminal_code = ?, error_code = ?,
                    error_message_redacted = ?, ended_at = ?,
                    queued_input_text = NULL, queued_skill_inputs_json = NULL
                WHERE id = ?
                """,
                (
                    target.value,
                    terminal_code,
                    error_code,
                    error_message_redacted,
                    now,
                    turn_id,
                ),
            )
            insert_progress_update(
                connection,
                turn_id=turn_id,
                state="terminal",
                content=f"{target.value.title()} · `{terminal_code}`",
                now=now,
            )
            self._project_local_terminal(
                connection,
                turn_id=turn_id,
                target=target,
                terminal_code=terminal_code,
                now=now,
                progress_already_projected=True,
            )
            updated = connection.execute("SELECT * FROM turns WHERE id = ?", (turn_id,)).fetchone()
            assert updated is not None
            return _turn(updated)

    def interrupt_unreplayable_schedule_turn(
        self,
        turn_id: str,
        *,
        reason_code: str,
    ) -> TurnRecord:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                """
                SELECT t.*, c.project_id, sf.schedule_id
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                LEFT JOIN schedule_fires sf ON sf.id = t.schedule_fire_id
                WHERE t.id = ?
                """,
                (turn_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            current = TurnState(row["state"])
            if current.terminal:
                return _turn(row)
            if current is not TurnState.QUEUED or row["source_kind"] != "schedule":
                raise ConflictError("Turn is not a queued Schedule Turn")
            assert_turn_transition(current, TurnState.INTERRUPTED)
            connection.execute(
                """
                UPDATE turns
                SET state = 'interrupted', terminal_code = ?,
                    error_code = ?, interrupt_origin = 'runtime',
                    interrupt_reason = ?, ended_at = ?,
                    queued_input_text = NULL,
                    queued_skill_inputs_json = NULL
                WHERE id = ? AND state = 'queued'
                """,
                (
                    "schedule_snapshot_not_replayable",
                    reason_code,
                    reason_code,
                    now,
                    turn_id,
                ),
            )
            insert_progress_update(
                connection,
                turn_id=turn_id,
                state="terminal",
                content="Interrupted · `schedule_snapshot_not_replayable`",
                now=now,
            )
            self._project_local_terminal(
                connection,
                turn_id=turn_id,
                target=TurnState.INTERRUPTED,
                terminal_code="schedule_snapshot_not_replayable",
                now=now,
                progress_already_projected=True,
            )
            connection.execute(
                """
                INSERT INTO incidents(
                    id, severity, code, project_id, conversation_id, turn_id,
                    schedule_id, summary, details_json, occurrence_count,
                    first_seen_at, last_seen_at
                ) VALUES (
                    ?, 'error', 'schedule_turn_snapshot_not_replayable',
                    ?, ?, ?, ?, ?, ?, 1, ?, ?
                )
                """,
                (
                    new_id(),
                    row["project_id"],
                    row["conversation_id"],
                    turn_id,
                    row["schedule_id"],
                    "A queued Schedule Turn could not be restored safely",
                    canonical_json({"reason_code": reason_code}),
                    now,
                    now,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM turns WHERE id = ?",
                (turn_id,),
            ).fetchone()
            assert updated is not None
            return _turn(updated)

    @staticmethod
    def _project_local_terminal(
        connection: sqlite3.Connection,
        *,
        turn_id: str,
        target: TurnState,
        terminal_code: str,
        now: int,
        progress_already_projected: bool = False,
    ) -> None:
        scope = connection.execute(
            """
            SELECT t.*, c.discord_thread_id, c.discord_guild_id,
                   c.discord_parent_channel_id, c.project_id
            FROM turns t
            JOIN conversations c ON c.id = t.conversation_id
            WHERE t.id = ?
            """,
            (turn_id,),
        ).fetchone()
        if scope is None:
            raise NotFoundError(f"Turn not found: {turn_id}")
        event_id = f"local-terminal:{turn_id}:{terminal_code}"
        connection.execute(
            """
            INSERT OR IGNORE INTO events(
                event_id, turn_id, project_id, conversation_id,
                runtime_generation, kind, schema_version, payload_json,
                raw_type, raw_hash, raw_size, occurred_at, recorded_at
            ) VALUES (?, ?, ?, ?, ?, 'runtime.local_terminal', 1, ?,
                      'local', ?, 0, ?, ?)
            """,
            (
                event_id,
                turn_id,
                scope["project_id"],
                scope["conversation_id"],
                scope["runtime_generation"],
                canonical_json(
                    {
                        "state": target.value,
                        "terminal_code": terminal_code,
                    }
                ),
                sha256_text(event_id),
                now,
                now,
            ),
        )
        event = connection.execute(
            "SELECT sequence FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        assert event is not None
        sequence = int(event["sequence"])
        insert_prompt_reaction_update(
            connection,
            turn_id=turn_id,
            input_message_id=scope["input_message_id"],
            discord_thread_id=scope["discord_thread_id"],
            discord_parent_channel_id=scope["discord_parent_channel_id"],
            state="completed" if target is TurnState.COMPLETED else "failed",
            now=now,
            event_sequence=sequence,
        )
        note = f"Turn {target.value}: `{terminal_code}`."
        projection = connection.execute(
            "SELECT plain_text FROM message_projections WHERE turn_id = ?",
            (turn_id,),
        ).fetchone()
        plain_text = (
            f"{projection['plain_text']}\n\n{note}"
            if projection is not None and projection["plain_text"]
            else note
        )
        ast = {
            "version": 1,
            "blocks": [{"type": "text", "text": plain_text}],
        }
        if projection is None:
            connection.execute(
                """
                INSERT INTO message_projections(
                    id, turn_id, content_revision, content_ast_json,
                    plain_text, is_final, last_event_sequence
                ) VALUES (?, ?, 1, ?, ?, 1, ?)
                """,
                (new_id(), turn_id, canonical_json(ast), plain_text, sequence),
            )
        else:
            connection.execute(
                """
                UPDATE message_projections
                SET content_revision = content_revision + 1,
                    content_ast_json = ?, plain_text = ?, is_final = 1,
                    last_event_sequence = ?
                WHERE turn_id = ? AND is_final = 0
                """,
                (canonical_json(ast), plain_text, sequence, turn_id),
            )
            final_projection = connection.execute(
                "SELECT plain_text FROM message_projections WHERE turn_id = ?",
                (turn_id,),
            ).fetchone()
            assert final_projection is not None
            plain_text = str(final_projection["plain_text"])
        progress_outbox_id = None
        if not progress_already_projected:
            progress_outbox_id = insert_progress_update(
                connection,
                turn_id=turn_id,
                state="terminal",
                content=f"{target.value.title()} · `{terminal_code}`",
                now=now,
                event_sequence=sequence,
            )
        else:
            progress = connection.execute(
                """
                SELECT o.id
                FROM turn_progress_views v
                JOIN discord_outbox o
                  ON o.dedupe_key = (
                      'turn:' || v.turn_id || ':progress:' || v.content_revision
                  )
                WHERE v.turn_id = ?
                """,
                (turn_id,),
            ).fetchone()
            progress_outbox_id = str(progress["id"]) if progress else None
        connection.execute(
            """
            INSERT OR IGNORE INTO discord_outbox(
                id, event_sequence, destination_key, operation,
                depends_on_outbox_id, payload_json, dedupe_key,
                delivery_marker, state, attempts, next_attempt_at,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'send', ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
            """,
            (
                new_id(),
                sequence,
                f"thread:{scope['discord_thread_id']}",
                progress_outbox_id,
                canonical_json(
                    {
                        "kind": "turn_final",
                        "turn_id": turn_id,
                        "state": target.value,
                        "terminal_code": terminal_code,
                        "model": scope["effective_model"],
                        "reasoning_effort": scope["effective_reasoning_effort"],
                        "started_at": scope["started_at"],
                        "ended_at": now,
                        "input_message_id": scope["input_message_id"],
                        "input_channel_id": (
                            scope["discord_parent_channel_id"]
                            if scope["input_message_id"] is not None
                            and str(scope["input_message_id"])
                            == str(scope["discord_thread_id"])
                            else scope["discord_thread_id"]
                            if scope["input_message_id"] is not None
                            else None
                        ),
                        "discord_guild_id": scope["discord_guild_id"],
                        "usage": latest_usage_payload(
                            connection,
                            turn_id=turn_id,
                            max_sequence=sequence,
                        ),
                        "content_ast": ast,
                        "plain_text": plain_text,
                    }
                ),
                f"turn:{turn_id}:final",
                f"turn-{turn_id[:8]}-final",
                now,
                now,
                now,
            ),
        )

    def enqueue_outbox(
        self,
        *,
        destination_key: str,
        operation: str,
        payload: Mapping[str, Any],
        dedupe_key: str,
        delivery_marker: str,
        event_sequence: int | None = None,
        coalesce_key: str | None = None,
        depends_on_outbox_id: str | None = None,
    ) -> str:
        now = utc_now_ms()
        payload_json = canonical_json(dict(payload))
        with self.store.transaction() as connection:
            existing = connection.execute(
                """
                SELECT id, event_sequence, destination_key, operation,
                       depends_on_outbox_id, payload_json, coalesce_key,
                       delivery_marker
                FROM discord_outbox
                WHERE dedupe_key = ?
                """,
                (dedupe_key,),
            ).fetchone()
            if existing:
                expected = (
                    event_sequence,
                    destination_key,
                    operation,
                    depends_on_outbox_id,
                    payload_json,
                    coalesce_key,
                    delivery_marker,
                )
                actual = (
                    existing["event_sequence"],
                    existing["destination_key"],
                    existing["operation"],
                    existing["depends_on_outbox_id"],
                    existing["payload_json"],
                    existing["coalesce_key"],
                    existing["delivery_marker"],
                )
                if actual != expected:
                    raise InvariantError(
                        "outbox dedupe key was reused for a different operation"
                    )
                return str(existing["id"])
            outbox_id = new_id()
            connection.execute(
                """
                INSERT INTO discord_outbox(
                    id, event_sequence, destination_key, operation,
                    depends_on_outbox_id, payload_json, dedupe_key, coalesce_key,
                    delivery_marker, state, attempts, next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    outbox_id,
                    event_sequence,
                    destination_key,
                    operation,
                    depends_on_outbox_id,
                    payload_json,
                    dedupe_key,
                    coalesce_key,
                    delivery_marker,
                    now,
                    now,
                    now,
                ),
            )
            return outbox_id

    def claim_outbox(
        self,
        *,
        worker_id: str,
        lease_ms: int = 30_000,
    ) -> OutboxRecord | None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            connection.execute(
                """
                UPDATE discord_outbox
                SET state = 'reconciling', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE state = 'sending' AND lease_expires_at < ?
                """,
                (now, now),
            )
            connection.execute(
                """
                UPDATE discord_outbox AS stale
                SET state = 'superseded', updated_at = ?
                WHERE stale.state = 'reconciling'
                  AND stale.coalesce_key IS NOT NULL
                  AND stale.operation <> 'send'
                  AND stale.coalesce_key NOT LIKE 'task-card:%'
                  AND EXISTS (
                      SELECT 1
                      FROM discord_outbox newer
                      WHERE newer.coalesce_key = stale.coalesce_key
                        AND newer.enqueue_sequence > stale.enqueue_sequence
                        AND newer.state <> 'superseded'
                  )
                """,
                (now,),
            )
            row = connection.execute(
                """
                SELECT o.* FROM discord_outbox o
                LEFT JOIN discord_outbox dependency ON dependency.id = o.depends_on_outbox_id
                WHERE o.state IN ('pending', 'retry', 'reconciling')
                  AND o.next_attempt_at <= ?
                  AND (
                      dependency.id IS NULL
                      OR dependency.state IN ('sent', 'dead_letter', 'superseded')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM discord_outbox earlier
                      WHERE earlier.destination_key = o.destination_key
                        AND earlier.enqueue_sequence < o.enqueue_sequence
                        AND earlier.depends_on_outbox_id IS NOT o.id
                        AND earlier.state NOT IN ('sent', 'dead_letter', 'superseded')
                  )
                ORDER BY o.enqueue_sequence
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE discord_outbox
                SET state = 'sending', attempts = attempts + 1,
                    lease_owner = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND state IN ('pending', 'retry', 'reconciling')
                """,
                (worker_id, now + lease_ms, now, row["id"]),
            )
            updated = connection.execute(
                "SELECT * FROM discord_outbox WHERE id = ?", (row["id"],)
            ).fetchone()
            assert updated is not None
            return _outbox(updated, claimed_from_state=str(row["state"]))

    def renew_outbox_lease(
        self,
        outbox_id: str,
        *,
        lease_owner: str,
        lease_attempt: int,
        lease_ms: int = 30_000,
    ) -> bool:
        if lease_ms <= 0:
            raise InvariantError("outbox lease duration must be positive")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE discord_outbox
                SET lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND state = 'sending'
                  AND lease_owner = ? AND attempts = ?
                """,
                (
                    now + lease_ms,
                    now,
                    outbox_id,
                    lease_owner,
                    lease_attempt,
                ),
            ).rowcount
            return changed == 1

    def ack_outbox(
        self,
        outbox_id: str,
        *,
        lease_owner: str,
        lease_attempt: int,
        discord_message_id: str | None = None,
        task_card_view_id: str | None = None,
        turn_progress_id: str | None = None,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE discord_outbox
                SET state = 'sent', discord_message_id = COALESCE(?, discord_message_id),
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE id = ? AND state = 'sending'
                  AND lease_owner = ? AND attempts = ?
                """,
                (
                    discord_message_id,
                    now,
                    outbox_id,
                    lease_owner,
                    lease_attempt,
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError("outbox delivery lease was lost")
            if task_card_view_id is not None and discord_message_id is not None:
                view_changed = connection.execute(
                    """
                    UPDATE task_card_views
                    SET discord_message_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (discord_message_id, now, task_card_view_id),
                ).rowcount
                if view_changed != 1:
                    raise NotFoundError(
                        f"task card view not found: {task_card_view_id}"
                    )
            if turn_progress_id is not None and discord_message_id is not None:
                progress_changed = connection.execute(
                    """
                    UPDATE turn_progress_views
                    SET discord_message_id = ?, updated_at = ?
                    WHERE turn_id = ?
                    """,
                    (discord_message_id, now, turn_progress_id),
                ).rowcount
                if progress_changed != 1:
                    raise NotFoundError(
                        f"Turn progress view not found: {turn_progress_id}"
                    )

    def retry_outbox(
        self,
        outbox_id: str,
        *,
        lease_owner: str,
        lease_attempt: int,
        error_code: str,
        next_attempt_at: int,
        permanent: bool = False,
        incident_code: str | None = None,
        incident_summary: str | None = None,
        incident_details: Mapping[str, Any] | None = None,
    ) -> None:
        target = "dead_letter" if permanent else "retry"
        now = utc_now_ms()
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE discord_outbox
                SET state = ?, last_error_code = ?, next_attempt_at = ?,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE id = ? AND state = 'sending'
                  AND lease_owner = ? AND attempts = ?
                """,
                (
                    target,
                    error_code,
                    next_attempt_at,
                    now,
                    outbox_id,
                    lease_owner,
                    lease_attempt,
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError("outbox delivery lease was lost")
            if incident_code is not None:
                _upsert_incident(
                    connection,
                    severity="warning",
                    code=incident_code,
                    summary=incident_summary or "Discord delivery retry needs attention",
                    details=incident_details,
                    now=now,
                )

    def fail_outbox_permanently(
        self,
        outbox_id: str,
        *,
        lease_owner: str,
        lease_attempt: int,
        error_code: str,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            outbox = connection.execute(
                "SELECT * FROM discord_outbox WHERE id = ?",
                (outbox_id,),
            ).fetchone()
            if outbox is None:
                raise NotFoundError(f"outbox item not found: {outbox_id}")
            changed = connection.execute(
                """
                UPDATE discord_outbox
                SET state = 'dead_letter', last_error_code = ?,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE id = ? AND state = 'sending'
                  AND lease_owner = ? AND attempts = ?
                """,
                (
                    error_code,
                    now,
                    outbox_id,
                    lease_owner,
                    lease_attempt,
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError("outbox delivery lease was lost")

            try:
                payload = json.loads(str(outbox["payload_json"]))
            except json.JSONDecodeError:
                payload = {}
            turn_id = payload.get("turn_id") if isinstance(payload, dict) else None
            scope = None
            if isinstance(turn_id, str):
                scope = connection.execute(
                    """
                    SELECT t.id AS turn_id, c.id AS conversation_id, c.project_id,
                           c.discord_parent_channel_id, c.owner_user_id,
                           sf.schedule_id
                    FROM turns t
                    JOIN conversations c ON c.id = t.conversation_id
                    LEFT JOIN schedule_fires sf ON sf.id = t.schedule_fire_id
                    WHERE t.id = ?
                    """,
                    (turn_id,),
                ).fetchone()
            if scope is None and str(outbox["destination_key"]).startswith("thread:"):
                thread_id = str(outbox["destination_key"]).partition(":")[2]
                scope = connection.execute(
                    """
                    SELECT NULL AS turn_id, c.id AS conversation_id, c.project_id,
                           c.discord_parent_channel_id, c.owner_user_id,
                           NULL AS schedule_id
                    FROM conversations c
                    WHERE c.discord_thread_id = ?
                    """,
                    (thread_id,),
                ).fetchone()

            project_id = str(scope["project_id"]) if scope is not None else None
            conversation_id = (
                str(scope["conversation_id"]) if scope is not None else None
            )
            scoped_turn_id = (
                str(scope["turn_id"])
                if scope is not None and scope["turn_id"] is not None
                else None
            )
            schedule_id = (
                str(scope["schedule_id"])
                if scope is not None and scope["schedule_id"] is not None
                else None
            )
            if conversation_id is not None:
                connection.execute(
                    """
                    UPDATE conversations
                    SET state = 'blocked', updated_at = ?
                    WHERE id = ? AND state IN ('uninitialized', 'active')
                    """,
                    (now, conversation_id),
                )
                connection.execute(
                    """
                    INSERT INTO audit_log(
                        id, actor_kind, action, project_id, conversation_id,
                        turn_id, payload_json, occurred_at
                    ) VALUES (?, 'system', 'conversation.blocked', ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id(),
                        project_id,
                        conversation_id,
                        scoped_turn_id,
                        canonical_json(
                            {
                                "reason": "permanent_discord_delivery_failure",
                                "error_code": error_code,
                                "outbox_id": outbox_id,
                            }
                        ),
                        now,
                    ),
                )
            if schedule_id is not None:
                connection.execute(
                    """
                    UPDATE schedules
                    SET state = 'blocked', next_due_at = NULL,
                        version = version + 1, updated_at = ?
                    WHERE id = ? AND state <> 'deleted'
                    """,
                    (now, schedule_id),
                )
                self._insert_audit(
                    connection,
                    actor_kind="system",
                    actor_id=None,
                    action="schedule.block",
                    project_id=project_id,
                    conversation_id=conversation_id,
                    turn_id=scoped_turn_id,
                    schedule_id=schedule_id,
                    correlation_id=f"outbox:{outbox_id}:schedule_block",
                    payload={
                        "reason": "permanent_discord_delivery_failure",
                        "error_code": error_code,
                        "outbox_id": outbox_id,
                    },
                    now=now,
                )
            if (
                conversation_id is not None
                and scope is not None
                and str(outbox["destination_key"]).startswith("thread:")
            ):
                notice_key = f"conversation:{conversation_id}:delivery-blocked"
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
                        f"channel:{scope['discord_parent_channel_id']}",
                        canonical_json(
                            {
                                "kind": "notice",
                                "level": "error",
                                "title": "Conversation delivery blocked",
                                "content": (
                                    "codexD could not deliver updates to a Conversation "
                                    f"thread (`{error_code}`). The Conversation was blocked; "
                                    "run `/diag status` before retrying."
                                ),
                            }
                        ),
                        notice_key,
                        notice_key,
                        now,
                        now,
                        now,
                    ),
                )
            connection.execute(
                """
                INSERT INTO incidents(
                    id, severity, code, project_id, conversation_id, turn_id,
                    schedule_id, summary, details_json, occurrence_count,
                    first_seen_at, last_seen_at
                ) VALUES (?, 'error', 'discord_delivery_permanent', ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    new_id(),
                    project_id,
                    conversation_id,
                    scoped_turn_id,
                    schedule_id,
                    "Discord delivery failed permanently; durable work was blocked",
                    canonical_json(
                        {"error_code": error_code, "outbox_id": outbox_id}
                    ),
                    now,
                    now,
                ),
            )

    def fail_thread_creation_outbox(
        self,
        outbox_id: str,
        *,
        lease_owner: str,
        lease_attempt: int,
        error_code: str,
    ) -> None:
        now = utc_now_ms()
        notification_id = new_id()
        with self.store.transaction() as connection:
            ingress = connection.execute(
                """
                SELECT i.id, i.discord_message_id, i.discord_channel_id
                FROM ingress_messages i
                WHERE i.thread_creation_outbox_id = ?
                  AND i.state = 'pending_thread'
                """,
                (outbox_id,),
            ).fetchone()
            if ingress is None:
                raise ConflictError(
                    "thread-creation outbox has no pending ingress"
                )
            changed = connection.execute(
                """
                UPDATE discord_outbox
                SET state = 'dead_letter', last_error_code = ?,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE id = ? AND state = 'sending'
                  AND lease_owner = ? AND attempts = ?
                """,
                (
                    error_code,
                    now,
                    outbox_id,
                    lease_owner,
                    lease_attempt,
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError("outbox delivery lease was lost")
            connection.execute(
                """
                UPDATE ingress_messages
                SET state = 'rejected', error_code = ?, completed_at = ?
                WHERE id = ?
                """,
                (error_code, now, ingress["id"]),
            )
            connection.execute(
                """
                INSERT INTO discord_outbox(
                    id, destination_key, operation, payload_json, dedupe_key,
                    delivery_marker, state, attempts, next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, 'send', ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    notification_id,
                    f"channel:{ingress['discord_channel_id']}",
                    canonical_json(
                        {
                            "content": (
                                f"codexD could not create the Conversation thread "
                                f"(`{error_code}`)."
                            )
                        }
                    ),
                    f"thread-create:{ingress['discord_message_id']}:error",
                    f"thread-create:{ingress['discord_message_id']}:error",
                    now,
                    now,
                    now,
                ),
            )

    def turn_progress_message(self, turn_id: str) -> str | None:
        row = self.store.query_one(
            """
            SELECT discord_message_id
            FROM turn_progress_views
            WHERE turn_id = ?
            """,
            (turn_id,),
        )
        if row is None:
            raise NotFoundError(f"Turn progress view not found: {turn_id}")
        value = row["discord_message_id"]
        return str(value) if value is not None else None

    def set_turn_progress_message(self, turn_id: str, message_id: str) -> None:
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE turn_progress_views
                SET discord_message_id = ?, updated_at = ?
                WHERE turn_id = ?
                """,
                (message_id, utc_now_ms(), turn_id),
            ).rowcount
            if changed != 1:
                raise NotFoundError(f"Turn progress view not found: {turn_id}")

    def task_card_message(self, view_id: str) -> str | None:
        row = self.store.query_one(
            "SELECT discord_message_id FROM task_card_views WHERE id = ?", (view_id,)
        )
        if row is None:
            raise NotFoundError(f"task card view not found: {view_id}")
        value = row["discord_message_id"]
        return str(value) if value is not None else None

    def set_task_card_message(self, view_id: str, message_id: str) -> None:
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE task_card_views
                SET discord_message_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (message_id, utc_now_ms(), view_id),
            ).rowcount
            if changed != 1:
                raise NotFoundError(f"task card view not found: {view_id}")

    def update_task_card_display(
        self,
        *,
        view_id: str,
        expected_revision: int,
        action: str,
        component_nonce: str,
        interaction_id: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        message_id: int,
    ) -> str:
        if action not in {"expand", "collapse"}:
            raise InvariantError("invalid task card action")
        now = utc_now_ms()
        with self.store.transaction() as connection:
            intent = connection.execute(
                "SELECT * FROM command_intents WHERE interaction_id = ?",
                (interaction_id,),
            ).fetchone()
            if intent is None:
                raise NotFoundError(f"command intent not found: {interaction_id}")
            if intent["state"] != "accepted":
                raise ConflictError(
                    f"task card command intent is already {intent['state']}"
                )
            row = connection.execute(
                """
                SELECT v.*, t.display_title, t.state, t.operation,
                       t.safe_status_summary, t.model, t.reasoning_effort,
                       c.owner_user_id, c.discord_guild_id
                FROM task_card_views v
                JOIN task_projections t ON t.id = v.task_projection_id
                JOIN turns turn_row ON turn_row.id = t.turn_id
                JOIN conversations c ON c.id = turn_row.conversation_id
                WHERE v.id = ?
                """,
                (view_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"task card view not found: {view_id}")
            if int(row["owner_user_id"]) != owner_user_id:
                raise SecurityError("task card belongs to another owner")
            if int(row["discord_guild_id"]) != guild_id:
                raise SecurityError("task card belongs to another guild")
            if str(row["destination_key"]) != f"thread:{channel_id}":
                raise SecurityError("task card belongs to another channel")
            if not row["discord_message_id"] or int(row["discord_message_id"]) != message_id:
                raise SecurityError("task card message identity changed")
            if int(row["content_revision"]) != expected_revision:
                raise ConflictError("task card revision changed")
            if not hmac.compare_digest(
                str(row["component_nonce_hash"]),
                sha256_text(component_nonce),
            ):
                raise ConflictError("task card component nonce changed")
            state = "expanded" if action == "expand" else "collapsed"
            revision = expected_revision + 1
            next_nonce = new_id()[:12]
            connection.execute(
                """
                UPDATE task_card_views
                SET display_state = ?, content_revision = ?,
                    component_nonce_hash = ?, updated_at = ?
                WHERE id = ? AND content_revision = ?
                """,
                (
                    state,
                    revision,
                    sha256_text(next_nonce),
                    now,
                    view_id,
                    expected_revision,
                ),
            )
            agents = [
                {
                    "label": str(agent["agent_label"]),
                    "state": str(agent["state"]),
                    "message": agent["safe_message"],
                }
                for agent in connection.execute(
                    """
                    SELECT agent_label, state, safe_message
                    FROM task_projection_agents
                    WHERE task_projection_id = ?
                    ORDER BY agent_label, provider_agent_thread_hash
                    """,
                    (row["task_projection_id"],),
                ).fetchall()
            ]
            coalesce_key = f"task-card:{row['task_projection_id']}"
            supersede_coalesced_outbox(
                connection,
                coalesce_key=coalesce_key,
                now=now,
                states=("pending",),
            )
            outbox_id = new_id()
            connection.execute(
                """
                INSERT INTO discord_outbox(
                    id, destination_key, operation, payload_json, dedupe_key,
                    coalesce_key, delivery_marker, state, attempts,
                    next_attempt_at, created_at, updated_at
                ) VALUES (?, ?, 'edit', ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    outbox_id,
                    row["destination_key"],
                    canonical_json(
                        {
                            "kind": "task_card",
                            "view_id": view_id,
                            "title": row["display_title"],
                            "state": row["state"],
                            "status_summary": row["safe_status_summary"],
                            "operation": row["operation"],
                            "model": row["model"],
                            "reasoning_effort": row["reasoning_effort"],
                            "agents": agents,
                            "expanded": state == "expanded",
                            "revision": revision,
                            "nonce": next_nonce,
                        }
                    ),
                    f"task-card:{view_id}:interaction:{interaction_id}",
                    coalesce_key,
                    f"task-{view_id[:8]}-{revision}",
                    now,
                    now,
                    now,
                ),
            )
            result = {
                "code": "ok",
                "message": (
                    "Task card expanded."
                    if action == "expand"
                    else "Task card collapsed."
                ),
                "delivery": "outbox",
            }
            connection.execute(
                """
                UPDATE command_intents
                SET state = 'succeeded', result_json = ?,
                    completed_at = ?, updated_at = ?
                WHERE interaction_id = ? AND state = 'accepted'
                """,
                (canonical_json(result), now, now, interaction_id),
            )
            self._insert_audit(
                connection,
                actor_kind="discord_user",
                actor_id=str(owner_user_id),
                action="command.succeeded",
                project_id=intent["project_id"],
                conversation_id=intent["conversation_id"],
                turn_id=intent["turn_id"],
                schedule_id=None,
                payload={
                    "command_name": intent["command_name"],
                    "request_hash": intent["request_hash"],
                    "result_code": "ok",
                },
                now=now,
            )
            return outbox_id

    def health_counts(self) -> dict[str, int]:
        now = utc_now_ms()
        with self.store.transaction(immediate=False) as connection:
            turns = {
                str(row["state"]): int(row["count"])
                for row in connection.execute(
                    "SELECT state, COUNT(*) AS count FROM turns GROUP BY state"
                ).fetchall()
            }
            outbox = {
                str(row["state"]): int(row["count"])
                for row in connection.execute(
                    "SELECT state, COUNT(*) AS count FROM discord_outbox GROUP BY state"
                ).fetchall()
            }
            barriers = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM conversations
                    WHERE provider_barrier_kind IS NOT NULL
                    """
                ).fetchone()[0]
            )
            schedules = {
                str(row["state"]): int(row["count"])
                for row in connection.execute(
                    "SELECT state, COUNT(*) AS count FROM schedules GROUP BY state"
                ).fetchall()
            }
            oldest_outbox = connection.execute(
                """
                SELECT MIN(created_at) FROM discord_outbox
                WHERE state IN ('pending', 'sending', 'reconciling', 'retry')
                """
            ).fetchone()[0]
            due_at = connection.execute(
                """
                SELECT MIN(next_due_at) FROM schedules
                WHERE state = 'active' AND next_due_at IS NOT NULL
                """
            ).fetchone()[0]
            duration = connection.execute(
                """
                SELECT COALESCE(AVG(ended_at - started_at), 0)
                FROM turns
                WHERE started_at IS NOT NULL AND ended_at IS NOT NULL
                """
            ).fetchone()[0]
            unknown_events = int(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(occurrence_count), 0) FROM incidents
                    WHERE code IN (
                        'unknown_provider_notification',
                        'unknown_provider_item'
                    )
                    """
                ).fetchone()[0]
            )
            attachments_total = int(
                connection.execute("SELECT COUNT(*) FROM attachments").fetchone()[0]
            )
            attachments_cleanup_due = int(
                connection.execute(
                    "SELECT COUNT(*) FROM attachments WHERE retention_until <= ?",
                    (now,),
                ).fetchone()[0]
            )
        return {
            "turns_queued": turns.get("queued", 0),
            "turns_active": sum(
                turns.get(state, 0) for state in ("starting", "running", "cancelling")
            ),
            "outbox_pending": sum(
                outbox.get(state, 0)
                for state in ("pending", "sending", "reconciling", "retry")
            ),
            "outbox_dead_letter": outbox.get("dead_letter", 0),
            "outbox_retry": outbox.get("retry", 0),
            "outbox_oldest_age_ms": (
                max(0, now - int(oldest_outbox))
                if oldest_outbox is not None
                else 0
            ),
            "provider_barriers": barriers,
            "turns_terminal": sum(
                turns.get(state, 0)
                for state in ("completed", "failed", "cancelled", "interrupted")
            ),
            "turns_interrupted": turns.get("interrupted", 0),
            "turn_duration_ms_avg": int(duration),
            "schedules_active": schedules.get("active", 0),
            "schedules_paused": schedules.get("paused", 0),
            "schedules_blocked": schedules.get("blocked", 0),
            "schedule_due_lag_ms": (
                max(0, now - int(due_at)) if due_at is not None else 0
            ),
            "schedule_next_due_at": int(due_at) if due_at is not None else 0,
            "attachments_total": attachments_total,
            "attachments_cleanup_due": attachments_cleanup_due,
            "unknown_provider_events": unknown_events,
        }

    def recover_startup(self, *, current_boot_id: str) -> dict[str, int]:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            interrupted_rows = connection.execute(
                """
                SELECT id, state FROM turns
                WHERE state IN ('starting', 'running', 'cancelling')
                   OR (state = 'queued' AND source_kind = 'discord')
                """,
            ).fetchall()
            for interrupted in interrupted_rows:
                terminal_code = (
                    "daemon_restarted_before_start"
                    if interrupted["state"] == "queued"
                    else "daemon_restarted"
                )
                connection.execute(
                    """
                    UPDATE turns
                    SET state = 'interrupted', terminal_code = ?,
                        interrupt_origin = 'runtime',
                        queued_input_text = NULL,
                        queued_skill_inputs_json = NULL, ended_at = ?
                    WHERE id = ?
                    """,
                    (terminal_code, now, interrupted["id"]),
                )
                self._project_local_terminal(
                    connection,
                    turn_id=str(interrupted["id"]),
                    target=TurnState.INTERRUPTED,
                    terminal_code=terminal_code,
                    now=now,
                )
            discord_turns = len(interrupted_rows)
            outbox = connection.execute(
                """
                UPDATE discord_outbox
                SET state = 'reconciling', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE state = 'sending'
                """,
                (now,),
            ).rowcount
            ingress = connection.execute(
                """
                UPDATE ingress_messages
                SET state = 'rejected',
                    error_code = 'daemon_restarted_before_preflight',
                    completed_at = ?
                WHERE state = 'pending_preflight'
                  AND accepted_boot_id <> ?
                """,
                (now, current_boot_id),
            ).rowcount
            intents = connection.execute(
                """
                UPDATE command_intents
                SET state = 'reconciling', updated_at = ?
                WHERE state = 'effect_in_flight'
                  AND accepted_boot_id <> ?
                """,
                (now, current_boot_id),
            ).rowcount
            abandoned_intents = connection.execute(
                """
                UPDATE command_intents
                SET state = 'failed',
                    result_json = ?,
                    completed_at = ?,
                    updated_at = ?
                WHERE state = 'accepted'
                  AND accepted_boot_id <> ?
                """,
                (
                    canonical_json(
                        {
                            "code": "daemon_restarted_before_effect",
                            "message": (
                                "The daemon restarted before this command began; "
                                "submit it again if it is still needed."
                            ),
                        }
                    ),
                    now,
                    now,
                    current_boot_id,
                ),
            ).rowcount
            unknown_intents = 0
            reconciled_schedule_intents = 0
            reconciled_turn_cancel_intents = 0
            reconciling = connection.execute(
                """
                SELECT i.*, c.provider_barrier_kind, c.provider_barrier_intent_id
                FROM command_intents i
                LEFT JOIN conversations c ON c.id = i.conversation_id
                WHERE i.state = 'reconciling'
                  AND i.accepted_boot_id <> ?
                """,
                (current_boot_id,),
            ).fetchall()
            for intent in reconciling:
                recoverable_schedule_draft = (
                    intent["effect_kind"]
                    in {"schedule_draft", "schedule_draft_cancel"}
                    and _local_command_effect_committed(connection, intent)
                )
                if recoverable_schedule_draft:
                    result = {
                        "code": "ok",
                        "message": "Schedule draft command completed before restart.",
                    }
                    connection.execute(
                        """
                        UPDATE command_intents
                        SET state = 'succeeded', result_json = ?,
                            completed_at = ?, updated_at = ?
                        WHERE interaction_id = ? AND state = 'reconciling'
                        """,
                        (
                            canonical_json(result),
                            now,
                            now,
                            intent["interaction_id"],
                        ),
                    )
                    self._insert_audit(
                        connection,
                        actor_kind="system",
                        actor_id=None,
                        action="command.succeeded",
                        project_id=intent["project_id"],
                        conversation_id=intent["conversation_id"],
                        turn_id=intent["turn_id"],
                        schedule_id=None,
                        correlation_id=str(intent["interaction_id"]),
                        payload={
                            "command_name": intent["command_name"],
                            "request_hash": intent["request_hash"],
                            "result_code": result["code"],
                            "recovered_from": intent["effect_kind"],
                        },
                        now=now,
                    )
                    reconciled_schedule_intents += 1
                    continue
                recoverable_schedule = (
                    intent["effect_kind"] == "schedule_mutation"
                    and intent["effect_correlation_id"] is not None
                    and connection.execute(
                        """
                        SELECT 1 FROM audit_log
                        WHERE correlation_id = ?
                          AND schedule_id = ?
                          AND action LIKE 'schedule.%'
                        LIMIT 1
                        """,
                        (
                            intent["interaction_id"],
                            intent["effect_correlation_id"],
                        ),
                    ).fetchone()
                    is not None
                )
                if recoverable_schedule:
                    result = {
                        "code": "ok",
                        "message": "Command completed.",
                    }
                    connection.execute(
                        """
                        UPDATE command_intents
                        SET state = 'succeeded', result_json = ?,
                            completed_at = ?, updated_at = ?
                        WHERE interaction_id = ? AND state = 'reconciling'
                        """,
                        (
                            canonical_json(result),
                            now,
                            now,
                            intent["interaction_id"],
                        ),
                    )
                    self._insert_audit(
                        connection,
                        actor_kind="system",
                        actor_id=None,
                        action="command.succeeded",
                        project_id=intent["project_id"],
                        conversation_id=intent["conversation_id"],
                        turn_id=intent["turn_id"],
                        schedule_id=str(intent["effect_correlation_id"]),
                        correlation_id=str(intent["interaction_id"]),
                        payload={
                            "command_name": intent["command_name"],
                            "request_hash": intent["request_hash"],
                            "result_code": result["code"],
                            "recovered_from": "schedule_audit",
                        },
                        now=now,
                    )
                    reconciled_schedule_intents += 1
                    continue
                recoverable_turn_cancel = (
                    intent["effect_kind"] == "turn_cancel"
                    and intent["effect_correlation_id"] is not None
                    and connection.execute(
                        """
                        SELECT 1
                        FROM turns t
                        JOIN conversations c ON c.id = t.conversation_id
                        WHERE t.id = ?
                          AND t.conversation_id = ?
                          AND c.project_id = ?
                        """,
                        (
                            intent["effect_correlation_id"],
                            intent["conversation_id"],
                            intent["project_id"],
                        ),
                    ).fetchone()
                    is not None
                )
                if recoverable_turn_cancel:
                    result = {
                        "code": "ok",
                        "message": "Cancel request was committed before restart.",
                    }
                    connection.execute(
                        """
                        UPDATE command_intents
                        SET state = 'succeeded', result_json = ?,
                            completed_at = ?, updated_at = ?
                        WHERE interaction_id = ? AND state = 'reconciling'
                        """,
                        (
                            canonical_json(result),
                            now,
                            now,
                            intent["interaction_id"],
                        ),
                    )
                    self._insert_audit(
                        connection,
                        actor_kind="system",
                        actor_id=None,
                        action="command.succeeded",
                        project_id=intent["project_id"],
                        conversation_id=intent["conversation_id"],
                        turn_id=str(intent["effect_correlation_id"]),
                        schedule_id=None,
                        correlation_id=str(intent["interaction_id"]),
                        payload={
                            "command_name": intent["command_name"],
                            "request_hash": intent["request_hash"],
                            "result_code": result["code"],
                            "recovered_from": "turn_cancel_intent",
                        },
                        now=now,
                    )
                    reconciled_turn_cancel_intents += 1
                    continue
                recoverable_compact = (
                    intent["effect_kind"] == "session_compact"
                    and intent["provider_barrier_kind"] == "compact"
                    and intent["provider_barrier_intent_id"]
                    == intent["interaction_id"]
                )
                if recoverable_compact:
                    continue
                connection.execute(
                    """
                    UPDATE command_intents
                    SET state = 'unknown', result_json = ?,
                        completed_at = ?, updated_at = ?
                    WHERE interaction_id = ? AND state = 'reconciling'
                    """,
                    (
                        canonical_json(
                            {
                                "code": "command_effect_outcome_unknown",
                                "message": (
                                    "codexD cannot safely determine whether the "
                                    "provider effect completed; it was not replayed."
                                ),
                            }
                        ),
                        now,
                        now,
                        intent["interaction_id"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO incidents(
                        id, severity, code, project_id, conversation_id, turn_id,
                        summary, details_json, occurrence_count,
                        first_seen_at, last_seen_at
                    ) VALUES (
                        ?, 'error', 'command_effect_outcome_unknown', ?, ?, ?,
                        ?, ?, 1, ?, ?
                    )
                    """,
                    (
                        new_id(),
                        intent["project_id"],
                        intent["conversation_id"],
                        intent["turn_id"],
                        (
                            "A provider command effect was in flight during daemon "
                            "restart and could not be reconciled"
                        ),
                        canonical_json(
                            {
                                "command_name": intent["command_name"],
                                "effect_kind": intent["effect_kind"],
                                "interaction_hash": sha256_text(
                                    str(intent["interaction_id"])
                                ),
                            }
                        ),
                        now,
                        now,
                    ),
                )
                unknown_intents += 1
            return {
                "interrupted_turns": discord_turns,
                "reconciling_outbox": outbox,
                "rejected_ingress": ingress,
                "reconciling_intents": intents,
                "abandoned_intents": abandoned_intents,
                "reconciled_schedule_intents": reconciled_schedule_intents,
                "reconciled_turn_cancel_intents": reconciled_turn_cancel_intents,
                "unknown_intents": unknown_intents,
            }

    def interrupt_for_shutdown(self) -> int:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            rows = connection.execute(
                """
                SELECT id FROM turns
                WHERE state IN ('starting', 'running', 'cancelling')
                   OR (state = 'queued' AND source_kind = 'discord')
                """,
            ).fetchall()
            for row in rows:
                connection.execute(
                    """
                    UPDATE turns
                    SET state = 'interrupted',
                        terminal_code = 'daemon_shutdown',
                        interrupt_origin = 'shutdown',
                        interrupt_reason = 'daemon_shutdown',
                        queued_input_text = NULL,
                        queued_skill_inputs_json = NULL,
                        ended_at = ?
                    WHERE id = ?
                    """,
                    (now, row["id"]),
                )
                self._project_local_terminal(
                    connection,
                    turn_id=str(row["id"]),
                    target=TurnState.INTERRUPTED,
                    terminal_code="daemon_shutdown",
                    now=now,
                )
            return len(rows)

    def acquire_daemon_lease(
        self,
        *,
        boot_id: str,
        pid: int,
        process_start_token: str,
        stale_before: int,
    ) -> None:
        now = utc_now_ms()
        with self.store.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM daemon_leases WHERE lease_name = 'daemon'"
            ).fetchone()
            if row and int(row["heartbeat_at"]) >= stale_before and row["boot_id"] != boot_id:
                raise ConflictError("another daemon holds the database lease")
            connection.execute(
                """
                INSERT INTO daemon_leases(
                    lease_name, boot_id, pid, process_start_token, acquired_at, heartbeat_at
                ) VALUES ('daemon', ?, ?, ?, ?, ?)
                ON CONFLICT(lease_name) DO UPDATE SET
                    boot_id = excluded.boot_id,
                    pid = excluded.pid,
                    process_start_token = excluded.process_start_token,
                    acquired_at = excluded.acquired_at,
                    heartbeat_at = excluded.heartbeat_at
                """,
                (boot_id, pid, process_start_token, now, now),
            )

    def heartbeat_daemon_lease(self, boot_id: str) -> None:
        with self.store.transaction() as connection:
            changed = connection.execute(
                """
                UPDATE daemon_leases SET heartbeat_at = ?
                WHERE lease_name = 'daemon' AND boot_id = ?
                """,
                (utc_now_ms(), boot_id),
            ).rowcount
            if changed != 1:
                raise ConflictError("daemon database lease was lost")

    def release_daemon_lease(self, boot_id: str) -> None:
        with self.store.transaction() as connection:
            connection.execute(
                "DELETE FROM daemon_leases WHERE lease_name = 'daemon' AND boot_id = ?",
                (boot_id,),
            )


def _local_command_effect_committed(
    connection: sqlite3.Connection,
    intent: sqlite3.Row,
) -> bool:
    effect_kind = intent["effect_kind"]
    draft_id = intent["effect_correlation_id"]
    if effect_kind not in {"schedule_draft", "schedule_draft_cancel"} or draft_id is None:
        return False
    state_clause = (
        "AND d.state = 'cancelled'"
        if effect_kind == "schedule_draft_cancel"
        else ""
    )
    return (
        connection.execute(
            f"""
            SELECT 1
            FROM schedule_drafts d
            JOIN conversations c ON c.id = d.conversation_id
            WHERE d.id = ?
              AND d.conversation_id = ?
              AND c.project_id = ?
              {state_clause}
            """,
            (
                draft_id,
                intent["conversation_id"],
                intent["project_id"],
            ),
        ).fetchone()
        is not None
    )


def consume_modal_intent_in_transaction(
    connection: sqlite3.Connection,
    *,
    intent_id: str,
    kind: str,
    expires_at: int,
    nonce: str,
    interaction_id: str,
    guild_id: int,
    channel_id: int,
    user_id: int,
    now: int,
) -> tuple[ModalIntentRecord, bool]:
    row = connection.execute(
        "SELECT * FROM modal_intents WHERE id = ?",
        (intent_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError("modal intent was not found")
    if (
        row["kind"] != kind
        or int(row["expires_at"]) != expires_at
        or not hmac.compare_digest(
            str(row["nonce_hash"]),
            sha256_text(f"modal:{nonce}"),
        )
    ):
        raise SecurityError("modal intent signature scope does not match")
    if (
        int(row["discord_guild_id"]) != guild_id
        or int(row["discord_channel_id"]) != channel_id
        or int(row["owner_user_id"]) != user_id
    ):
        raise SecurityError("modal submission Discord scope changed")
    if row["state"] == "consumed":
        if row["consumed_interaction_id"] != interaction_id:
            raise ConflictError("modal intent was already consumed")
        return _modal_intent(row), False
    if row["state"] == "expired" or now >= int(row["expires_at"]):
        connection.execute(
            """
            UPDATE modal_intents
            SET state = 'expired'
            WHERE id = ? AND state = 'open'
            """,
            (intent_id,),
        )
        updated = connection.execute(
            "SELECT * FROM modal_intents WHERE id = ?",
            (intent_id,),
        ).fetchone()
        assert updated is not None
        return _modal_intent(updated), True
    changed = connection.execute(
        """
        UPDATE modal_intents
        SET state = 'consumed', consumed_interaction_id = ?,
            consumed_at = ?
        WHERE id = ? AND state = 'open'
        """,
        (interaction_id, now, intent_id),
    ).rowcount
    if changed != 1:
        raise ConflictError("modal intent changed concurrently")
    updated = connection.execute(
        "SELECT * FROM modal_intents WHERE id = ?",
        (intent_id,),
    ).fetchone()
    assert updated is not None
    return _modal_intent(updated), False


def mark_command_effect_in_transaction(
    connection: sqlite3.Connection,
    *,
    interaction_id: str,
    effect_kind: str,
    effect_correlation_id: str | None,
    turn_id: str | None,
    now: int,
) -> tuple[CommandIntentRecord, bool]:
    row = connection.execute(
        "SELECT * FROM command_intents WHERE interaction_id = ?",
        (interaction_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"command intent not found: {interaction_id}")
    if turn_id is not None:
        turn = connection.execute(
            """
            SELECT t.conversation_id, c.project_id
            FROM turns t
            JOIN conversations c ON c.id = t.conversation_id
            WHERE t.id = ?
            """,
            (turn_id,),
        ).fetchone()
        if turn is None:
            raise NotFoundError(f"Turn not found: {turn_id}")
        if (
            row["conversation_id"] != turn["conversation_id"]
            or row["project_id"] != turn["project_id"]
            or (row["turn_id"] is not None and row["turn_id"] != turn_id)
        ):
            raise ConflictError("command intent belongs to another Turn scope")
    if row["state"] == "effect_in_flight":
        if (
            row["effect_kind"] != effect_kind
            or row["effect_correlation_id"] != effect_correlation_id
            or (turn_id is not None and row["turn_id"] != turn_id)
        ):
            raise ConflictError("command effect identity changed")
        return _command_intent(row), False
    if row["state"] != "accepted":
        raise ConflictError(
            f"command effect cannot start while intent is {row['state']}"
        )
    changed = connection.execute(
        """
        UPDATE command_intents
        SET state = 'effect_in_flight', effect_kind = ?,
            effect_correlation_id = ?,
            turn_id = COALESCE(turn_id, ?), updated_at = ?
        WHERE interaction_id = ? AND state = 'accepted'
        """,
        (
            effect_kind,
            effect_correlation_id,
            turn_id,
            now,
            interaction_id,
        ),
    ).rowcount
    if changed != 1:
        raise ConflictError("command intent changed concurrently")
    updated = connection.execute(
        "SELECT * FROM command_intents WHERE interaction_id = ?",
        (interaction_id,),
    ).fetchone()
    assert updated is not None
    return _command_intent(updated), True


def _assert_conversation_mutable(
    connection: sqlite3.Connection,
    conversation_id: str,
    *,
    reject_active_schedules: bool,
) -> None:
    turn = connection.execute(
        """
        SELECT id FROM turns
        WHERE conversation_id = ?
          AND state IN ('queued', 'starting', 'running', 'cancelling')
        LIMIT 1
        """,
        (conversation_id,),
    ).fetchone()
    if turn is not None:
        raise ConflictError("Conversation has a queued or active Turn")
    if reject_active_schedules:
        schedule = connection.execute(
            """
            SELECT id FROM schedules
            WHERE conversation_id = ? AND state = 'active'
            LIMIT 1
            """,
            (conversation_id,),
        ).fetchone()
        if schedule is not None:
            raise ConflictError("Conversation has an active Schedule")


def _runtime_lease_matches_project(
    lease: sqlite3.Row,
    project_id: str,
    *,
    prefix: str = "",
) -> bool:
    scope_kind = str(lease[f"{prefix}scope_kind"])
    scope_key = str(lease[f"{prefix}scope_key"])
    raw_project_id = lease[f"{prefix}project_id"]
    lease_project_id = (
        str(raw_project_id) if raw_project_id is not None else None
    )
    if scope_kind == "shared":
        return scope_key == "shared" and lease_project_id is None
    return (
        scope_kind == "project"
        and scope_key == project_id
        and lease_project_id == project_id
    )


def _ensure_project(
    connection: sqlite3.Connection,
    *,
    name: str,
    root_path: Path,
    now: int,
) -> sqlite3.Row:
    root = str(root_path)
    root_casefold = os.path.normcase(root)
    existing = connection.execute(
        "SELECT * FROM projects WHERE root_path_casefold = ?",
        (root_casefold,),
    ).fetchone()
    if existing is not None:
        if str(existing["root_path"]) != root:
            raise ConflictError("project path conflicts with an existing canonical root")
        return cast(sqlite3.Row, existing)
    project_id = new_id()
    try:
        connection.execute(
            """
            INSERT INTO projects(
                id, name, root_path, root_path_casefold, sandbox_profile,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'full_access', ?, ?)
            """,
            (project_id, name, root, root_casefold, now, now),
        )
    except sqlite3.IntegrityError as exc:
        raise ConflictError(f"project conflicts with existing project: {exc}") from exc
    row = connection.execute(
        "SELECT * FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    assert row is not None
    return cast(sqlite3.Row, row)


def _project(row: sqlite3.Row) -> ProjectRecord:
    return ProjectRecord(
        id=str(row["id"]),
        name=str(row["name"]),
        root_path=Path(row["root_path"]),
        sandbox_profile=SandboxProfile(row["sandbox_profile"]),
    )


def _command_intent(row: sqlite3.Row) -> CommandIntentRecord:
    return CommandIntentRecord(
        interaction_id=str(row["interaction_id"]),
        command_name=str(row["command_name"]),
        request_hash=str(row["request_hash"]),
        project_id=row["project_id"],
        conversation_id=row["conversation_id"],
        turn_id=row["turn_id"],
        state=str(row["state"]),
        result_json=row["result_json"],
        effect_kind=row["effect_kind"],
        effect_correlation_id=row["effect_correlation_id"],
        accepted_boot_id=str(row["accepted_boot_id"]),
    )


def _modal_intent(row: sqlite3.Row) -> ModalIntentRecord:
    return ModalIntentRecord(
        id=str(row["id"]),
        kind=str(row["kind"]),
        project_id=str(row["project_id"]),
        conversation_id=str(row["conversation_id"]),
        turn_id=str(row["turn_id"]) if row["turn_id"] is not None else None,
        schedule_id=(
            str(row["schedule_id"]) if row["schedule_id"] is not None else None
        ),
        expected_version=(
            int(row["expected_version"])
            if row["expected_version"] is not None
            else None
        ),
        discord_guild_id=int(row["discord_guild_id"]),
        discord_channel_id=int(row["discord_channel_id"]),
        owner_user_id=int(row["owner_user_id"]),
        state=str(row["state"]),
        consumed_interaction_id=(
            str(row["consumed_interaction_id"])
            if row["consumed_interaction_id"] is not None
            else None
        ),
        expires_at=int(row["expires_at"]),
    )


def _conversation(row: sqlite3.Row) -> ConversationRecord:
    return ConversationRecord(
        id=str(row["id"]),
        project_id=str(row["project_id"]),
        discord_thread_id=int(row["discord_thread_id"]),
        discord_guild_id=int(row["discord_guild_id"]),
        discord_parent_channel_id=int(row["discord_parent_channel_id"]),
        owner_user_id=int(row["owner_user_id"]),
        state=ConversationState(row["state"]),
        active_revision_id=row["active_revision_id"],
        sandbox_profile=SandboxProfile(row["sandbox_profile"]),
        model_override=row["model_override"],
        reasoning_effort_override=row["reasoning_effort_override"],
        reasoning_summary_override=row["reasoning_summary_override"],
        personality_override=row["personality_override"],
        service_tier_override=row["service_tier_override"],
        web_search_mode=str(row["web_search_mode"]),
        provider_barrier_kind=row["provider_barrier_kind"],
    )


def _revision(row: sqlite3.Row) -> ThreadRevisionRecord:
    return ThreadRevisionRecord(
        id=str(row["id"]),
        conversation_id=str(row["conversation_id"]),
        provider_thread_id=str(row["provider_thread_id"]),
        provider_session_id=str(row["provider_session_id"]),
        provider_forked_from_thread_id=row["provider_forked_from_thread_id"],
        provider_parent_thread_id=row["provider_parent_thread_id"],
        name=row["name"],
        parent_revision_id=row["parent_revision_id"],
        state=str(row["state"]),
        thread_config_json=str(row["thread_config_json"]),
        provider_version=str(row["provider_version"]),
        created_at=int(row["created_at"]),
        activated_at=(
            int(row["activated_at"]) if row["activated_at"] is not None else None
        ),
        archived_at=int(row["archived_at"]) if row["archived_at"] is not None else None,
    )


def _runtime_lease(row: sqlite3.Row) -> RuntimeLeaseRecord:
    return RuntimeLeaseRecord(
        id=str(row["id"]),
        scope_key=str(row["scope_key"]),
        generation=int(row["generation"]),
        state=str(row["state"]),
    )


def _turn(row: sqlite3.Row) -> TurnRecord:
    return TurnRecord(
        id=str(row["id"]),
        conversation_id=str(row["conversation_id"]),
        thread_revision_id=row["thread_revision_id"],
        runtime_lease_id=row["runtime_lease_id"],
        runtime_generation=row["runtime_generation"],
        provider_turn_id=row["provider_turn_id"],
        source_kind=TurnSource(row["source_kind"]),
        state=TurnState(row["state"]),
        interrupt_origin=(
            InterruptOrigin(row["interrupt_origin"]) if row["interrupt_origin"] else None
        ),
        interrupt_reason=row["interrupt_reason"],
        input_message_id=row["input_message_id"],
        schedule_fire_id=row["schedule_fire_id"],
        input_hash=str(row["input_hash"]),
        input_summary=str(row["input_summary"]),
        queued_input_text=row["queued_input_text"],
        queued_skill_inputs_json=row["queued_skill_inputs_json"],
        effective_model=row["effective_model"],
        effective_reasoning_effort=row["effective_reasoning_effort"],
        effective_reasoning_summary=row["effective_reasoning_summary"],
        effective_personality=row["effective_personality"],
        effective_service_tier=row["effective_service_tier"],
        effective_web_search_mode=str(row["effective_web_search_mode"]),
        effective_sandbox=SandboxProfile(row["effective_sandbox"]),
        queued_at=int(row["queued_at"]),
        started_at=int(row["started_at"]) if row["started_at"] is not None else None,
        ended_at=int(row["ended_at"]) if row["ended_at"] is not None else None,
        terminal_code=row["terminal_code"],
        error_code=row["error_code"],
        error_message_redacted=row["error_message_redacted"],
        usage_scope=row["usage_scope"],
    )


def _outbox(
    row: sqlite3.Row, *, claimed_from_state: str | None = None
) -> OutboxRecord:
    lease_owner = row["lease_owner"]
    if lease_owner is None:
        raise InvariantError("claimed outbox item has no lease owner")
    return OutboxRecord(
        id=str(row["id"]),
        destination_key=str(row["destination_key"]),
        operation=str(row["operation"]),
        payload_json=str(row["payload_json"]),
        delivery_marker=str(row["delivery_marker"]),
        state=claimed_from_state or str(row["state"]),
        attempts=int(row["attempts"]),
        lease_owner=str(lease_owner),
    )


def _upsert_incident(
    connection: sqlite3.Connection,
    *,
    severity: str,
    code: str,
    summary: str,
    now: int,
    project_id: str | None = None,
    conversation_id: str | None = None,
    turn_id: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> str:
    existing = connection.execute(
        """
        SELECT id FROM incidents
        WHERE code = ?
          AND project_id IS ?
          AND conversation_id IS ?
          AND turn_id IS ?
          AND resolved_at IS NULL
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        (code, project_id, conversation_id, turn_id),
    ).fetchone()
    if existing is not None:
        connection.execute(
            """
            UPDATE incidents
            SET occurrence_count = occurrence_count + 1,
                last_seen_at = ?, summary = ?, details_json = ?
            WHERE id = ?
            """,
            (
                now,
                summary[:2048],
                canonical_json(dict(details or {})),
                existing["id"],
            ),
        )
        return str(existing["id"])
    incident_id = new_id()
    connection.execute(
        """
        INSERT INTO incidents(
            id, severity, code, project_id, conversation_id, turn_id,
            summary, details_json, occurrence_count, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            incident_id,
            severity,
            code,
            project_id,
            conversation_id,
            turn_id,
            summary[:2048],
            canonical_json(dict(details or {})),
            now,
            now,
        ),
    )
    return incident_id


def _render_plan(row: sqlite3.Row) -> RenderPlanRecord:
    return RenderPlanRecord(
        turn_id=str(row["turn_id"]),
        source_sha256=str(row["source_sha256"]),
        plan_json=str(row["plan_json"]),
        retention_until=int(row["retention_until"]),
    )


def _ingress(row: sqlite3.Row) -> IngressMessageRecord:
    return IngressMessageRecord(
        id=str(row["id"]),
        discord_message_id=str(row["discord_message_id"]),
        accepted_content_hash=str(row["accepted_content_hash"]),
        accepted_attachment_manifest_hash=str(
            row["accepted_attachment_manifest_hash"]
        ),
        project_id=str(row["project_id"]),
        discord_guild_id=int(row["discord_guild_id"]),
        discord_channel_id=int(row["discord_channel_id"]),
        conversation_id=(
            str(row["conversation_id"])
            if row["conversation_id"] is not None
            else None
        ),
        state=str(row["state"]),
        turn_id=str(row["turn_id"]) if row["turn_id"] is not None else None,
        accepted_boot_id=str(row["accepted_boot_id"]),
        error_code=row["error_code"],
    )
