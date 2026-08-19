from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any, cast

from codexd.application.volatile_turns import VolatileTurnStore
from codexd.domain.events import NormalizedEvent
from codexd.domain.ids import canonical_json, new_id, sha256_text, utc_now_ms
from codexd.domain.turns import InterruptOrigin, TurnState, assert_turn_transition
from codexd.errors import InvariantError, NotFoundError, SecurityError, StorageError
from codexd.storage.progress import (
    insert_progress_update,
    insert_prompt_reaction_update,
    supersede_coalesced_outbox,
)
from codexd.storage.sqlite import SQLiteStore
from codexd.storage.usage import (
    USAGE_SCOPE,
    latest_usage_payload,
    validate_usage_payload,
)

_TASK_TERMINAL_STATES = frozenset(
    {"completed", "errored", "interrupted", "shutdown", "not_found"}
)
_AGENT_TERMINAL_STATES = frozenset(
    {"completed", "errored", "interrupted", "shutdown", "not_found"}
)
_DURABLE_EVENT_KINDS = frozenset(
    {
        "file_change.completed",
        "provider.error",
        "provider.unknown",
        "provider.item",
        "model.rerouted",
        "model.safety",
        "model.verification",
        "turn.moderation",
        "turn.completed",
        "turn.failed",
        "turn.interrupted",
        "turn.terminal_unparseable",
        "runtime.stream_interrupted",
        "runtime.stream_lifecycle",
    }
)
_MAX_PROVIDER_EVENT_IDS_PER_TURN = 4096
_MAX_TRACKED_PROVIDER_TURNS = 1024
_VOLATILE_CONTENT_PREFIXES = ("assistant.", "plan.")
_VOLATILE_METADATA_KINDS = frozenset(
    {
        "command.output.delta",
        "diff.updated",
        "file_change.output.delta",
        "reasoning.hidden_delta_discarded",
        "reasoning.summary",
        "usage.updated",
    }
)


@dataclass(frozen=True)
class EventRecordResult:
    sequence: int | None
    terminal: tuple[TurnState, str] | None


class ProjectingEventSink:
    def __init__(
        self,
        store: SQLiteStore,
        *,
        correlation_key: bytes,
        stream_update_ms: int = 1000,
        progress_update_ms: int = 5000,
        task_card_update_ms: int = 5000,
        volatile_turns: VolatileTurnStore | None = None,
    ) -> None:
        if len(correlation_key) < 32:
            raise ValueError("projection correlation key must contain at least 32 bytes")
        if min(stream_update_ms, progress_update_ms, task_card_update_ms) < 1:
            raise ValueError("projection update intervals must be positive")
        self.store = store
        self._correlation_key = correlation_key
        self._stream_update_ms = stream_update_ms
        self._progress_update_ms = progress_update_ms
        self._task_card_update_ms = task_card_update_ms
        self._volatile_turns = volatile_turns or VolatileTurnStore()
        self._provider_event_fingerprints: dict[str, dict[str, tuple[object, ...]]] = {}
        self._provider_event_lock = threading.Lock()
        self._prepare_projection_identity()

    @property
    def stream_update_ms(self) -> int:
        return self._stream_update_ms

    @property
    def volatile_turns(self) -> VolatileTurnStore:
        return self._volatile_turns

    def record(
        self,
        *,
        turn_id: str,
        runtime_generation: int,
        event: NormalizedEvent,
        terminal_state: TurnState | None = None,
        terminal_code: str | None = None,
    ) -> EventRecordResult:
        if terminal_state is None and (
            event.kind.startswith(_VOLATILE_CONTENT_PREFIXES)
            or event.kind in _VOLATILE_METADATA_KINDS
        ):
            accepted = self._record_volatile_event(
                turn_id=turn_id,
                runtime_generation=runtime_generation,
                event=event,
            )
            return EventRecordResult(-1 if accepted else None, None)
        now = utc_now_ms()
        with self.store.transaction() as connection:
            scope = connection.execute(
                """
                SELECT t.*, c.project_id, c.discord_thread_id,
                       c.discord_guild_id, c.discord_parent_channel_id,
                       r.dynamic_tools_enabled,
                       rl.state AS lease_state,
                       rl.generation AS lease_generation
                FROM turns t
                JOIN conversations c ON c.id = t.conversation_id
                LEFT JOIN thread_revisions r ON r.id = t.thread_revision_id
                LEFT JOIN runtime_leases rl ON rl.id = t.runtime_lease_id
                WHERE t.id = ?
                """,
                (turn_id,),
            ).fetchone()
            if scope is None:
                raise NotFoundError(f"Turn not found: {turn_id}")
            if TurnState(str(scope["state"])).terminal:
                if event.kind == "runtime.stream_lifecycle":
                    if (
                        scope["runtime_generation"] != runtime_generation
                        or scope["lease_generation"] != runtime_generation
                    ):
                        self._record_incident(
                            connection,
                            severity="warning",
                            code="stale_runtime_lifecycle",
                            summary=(
                                "Ignored stream lifecycle from a stale runtime generation"
                            ),
                            project_id=str(scope["project_id"]),
                            conversation_id=str(scope["conversation_id"]),
                            turn_id=turn_id,
                            details={
                                "expected_generation": scope["runtime_generation"],
                                "received_generation": runtime_generation,
                            },
                            now=now,
                        )
                        return EventRecordResult(None, None)
                    sequence, inserted = self._append_event(
                        connection,
                        scope,
                        event,
                        now,
                        durable=True,
                    )
                    return EventRecordResult(sequence if inserted else None, None)
                self._record_incident(
                    connection,
                    severity="warning",
                    code="late_terminal_runtime_event",
                    summary="Ignored a provider event received after Turn termination",
                    project_id=str(scope["project_id"]),
                    conversation_id=str(scope["conversation_id"]),
                    turn_id=turn_id,
                    details={
                        "terminal_state": scope["state"],
                        "received_generation": runtime_generation,
                        "event_kind": event.kind,
                    },
                    now=now,
                )
                return EventRecordResult(None, None)
            if (
                scope["runtime_lease_id"] is None
                or scope["runtime_generation"] != runtime_generation
                or scope["lease_generation"] != runtime_generation
                or scope["lease_state"] != "ready"
            ):
                self._record_incident(
                    connection,
                    severity="warning",
                    code="stale_runtime_event",
                    summary="Ignored an event from a stale runtime generation",
                    project_id=str(scope["project_id"]),
                    conversation_id=str(scope["conversation_id"]),
                    turn_id=turn_id,
                    details={
                        "expected_generation": scope["runtime_generation"],
                        "received_generation": runtime_generation,
                        "event_kind": event.kind,
                    },
                    now=now,
                )
                return EventRecordResult(None, None)
            interrupt_origin = (
                InterruptOrigin(str(scope["interrupt_origin"]))
                if scope["interrupt_origin"] is not None
                else None
            )
            terminal = (
                (terminal_state, terminal_code or event.kind)
                if terminal_state is not None
                else terminal_state_for_event(
                    event,
                    interrupt_origin=interrupt_origin,
                )
            )
            durable = (
                terminal is not None
                or event.kind in _DURABLE_EVENT_KINDS
                or event.kind.startswith("review_mode.")
            )
            if (
                not durable
                and event.provider_event_id is not None
                and self._provider_event_seen(str(scope["id"]), event)
            ):
                return EventRecordResult(
                    self._activity_anchor(connection, scope, event, now),
                    None,
                )
            sequence, inserted = self._append_event(
                connection,
                scope,
                event,
                now,
                durable=durable,
            )
            if not inserted:
                return EventRecordResult(sequence, None)
            self._apply_projection(connection, scope, sequence, event, now)
            if terminal is not None:
                self._finalize_turn(
                    connection,
                    scope,
                    sequence,
                    terminal[0],
                    terminal[1],
                    event,
                    now,
                )
                self._forget_provider_events(str(scope["id"]))
        return EventRecordResult(sequence, terminal)

    def _record_volatile_event(
        self,
        *,
        turn_id: str,
        runtime_generation: int,
        event: NormalizedEvent,
    ) -> bool:
        scope = self.store.query_one(
            """
            SELECT t.state, t.runtime_lease_id, t.runtime_generation,
                   rl.state AS lease_state, rl.generation AS lease_generation
            FROM turns t
            LEFT JOIN runtime_leases rl ON rl.id = t.runtime_lease_id
            WHERE t.id = ?
            """,
            (turn_id,),
        )
        if scope is None or TurnState(str(scope["state"])).terminal:
            return False
        if (
            scope["runtime_lease_id"] is None
            or scope["runtime_generation"] != runtime_generation
            or scope["lease_generation"] != runtime_generation
            or scope["lease_state"] != "ready"
        ):
            return False
        if event.provider_event_id is not None and self._provider_event_seen(
            turn_id, event
        ):
            return True
        if event.kind == "usage.updated":
            validate_usage_payload(event.payload)
            self._volatile_turns.save_usage(turn_id, dict(event.payload))
        elif event.kind.startswith(_VOLATILE_CONTENT_PREFIXES) and event.kind != (
            "plan.updated"
        ):
            self._project_content(turn_id, event)
        return True

    def _provider_event_seen(self, turn_id: str, event: NormalizedEvent) -> bool:
        assert event.provider_event_id is not None
        fingerprint: tuple[object, ...] = (
            event.kind,
            event.schema_version,
            canonical_json(dict(event.payload)),
            event.raw_type,
            event.raw_hash,
            event.raw_size,
        )
        with self._provider_event_lock:
            fingerprints = self._provider_event_fingerprints.get(turn_id)
            if fingerprints is None:
                if len(self._provider_event_fingerprints) >= _MAX_TRACKED_PROVIDER_TURNS:
                    oldest_turn = next(iter(self._provider_event_fingerprints))
                    self._provider_event_fingerprints.pop(oldest_turn, None)
                fingerprints = {}
                self._provider_event_fingerprints[turn_id] = fingerprints
            existing = fingerprints.get(event.provider_event_id)
            if existing is not None:
                if existing != fingerprint:
                    raise InvariantError(
                        "provider event ID was reused for different event content"
                    )
                return True
            fingerprints[event.provider_event_id] = fingerprint
            if len(fingerprints) > _MAX_PROVIDER_EVENT_IDS_PER_TURN:
                oldest = next(iter(fingerprints))
                fingerprints.pop(oldest, None)
            return False

    def _forget_provider_events(self, turn_id: str) -> None:
        with self._provider_event_lock:
            self._provider_event_fingerprints.pop(turn_id, None)

    def _append_event(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        event: NormalizedEvent,
        now: int,
        *,
        durable: bool,
    ) -> tuple[int, bool]:
        if not durable:
            return self._activity_anchor(connection, scope, event, now), True
        if event.provider_event_id:
            existing = connection.execute(
                """
                SELECT sequence, runtime_generation, kind, schema_version,
                       payload_json, raw_type, raw_hash, raw_size
                FROM events
                WHERE turn_id = ? AND provider_event_id = ?
                """,
                (scope["id"], event.provider_event_id),
            ).fetchone()
            if existing:
                expected = (
                    int(scope["runtime_generation"]),
                    event.kind,
                    event.schema_version,
                    canonical_json(_durable_event_payload(event)),
                    event.raw_type,
                    event.raw_hash,
                    event.raw_size,
                )
                actual = (
                    int(existing["runtime_generation"]),
                    existing["kind"],
                    int(existing["schema_version"]),
                    existing["payload_json"],
                    existing["raw_type"],
                    existing["raw_hash"],
                    existing["raw_size"],
                )
                if actual != expected:
                    raise InvariantError(
                        "provider event ID was reused for different event content"
                    )
                return int(existing["sequence"]), False
        row = connection.execute(
            """
            SELECT COALESCE(MAX(local_event_index), 0) + 1 AS next
            FROM events WHERE turn_id = ?
            """,
            (scope["id"],),
        ).fetchone()
        local_index = int(row["next"])
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
                scope["id"],
                scope["project_id"],
                scope["conversation_id"],
                scope["runtime_generation"],
                event.provider_event_id,
                local_index,
                event.kind,
                event.schema_version,
                canonical_json(_durable_event_payload(event)),
                event.raw_type,
                event.raw_hash,
                event.raw_size,
                event.occurred_at,
                now,
            ),
        )
        if cursor.lastrowid is None:
            raise StorageError("event insert did not produce a sequence")
        return cursor.lastrowid, True

    @staticmethod
    def _activity_anchor(
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        event: NormalizedEvent,
        now: int,
    ) -> int:
        event_id = f"turn-activity:{scope['id']}"
        existing = connection.execute(
            "SELECT sequence FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if existing is not None:
            return int(existing["sequence"])
        row = connection.execute(
            """
            SELECT COALESCE(MAX(local_event_index), 0) + 1 AS next
            FROM events WHERE turn_id = ?
            """,
            (scope["id"],),
        ).fetchone()
        cursor = connection.execute(
            """
            INSERT INTO events(
                event_id, turn_id, project_id, conversation_id,
                runtime_generation, local_event_index, kind, schema_version,
                payload_json, occurred_at, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'turn.activity', 1, '{}', ?, ?)
            """,
            (
                event_id,
                scope["id"],
                scope["project_id"],
                scope["conversation_id"],
                scope["runtime_generation"],
                int(row["next"]),
                event.occurred_at,
                now,
            ),
        )
        if cursor.lastrowid is None:
            raise StorageError("activity anchor insert did not produce a sequence")
        return int(cursor.lastrowid)

    def _apply_projection(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
        now: int,
    ) -> None:
        if event.kind == "turn.started":
            self._project_progress(
                connection,
                scope,
                sequence,
                state="running",
                content=f"Running · Codex accepted Turn `{str(scope['id'])[:8]}`",
                now=now,
            )
        if event.kind.startswith(
            (
                "command.",
                "file_change.",
                "image_view.",
                "mcp.",
                "dynamic_tool.",
                "web_search.",
                "image_generation.",
                "terminal.",
                "hook.",
                "approval_review.",
            )
        ) and not event.kind.endswith((".delta", ".output.delta")):
            self._project_tool(connection, scope, sequence, event)
            self._project_progress(
                connection,
                scope,
                sequence,
                state="running",
                content=_tool_progress_content(event),
                now=now,
            )
        if event.kind.startswith("sleep."):
            self._project_progress(
                connection,
                scope,
                sequence,
                state="running",
                content=_sleep_progress_content(event),
                now=now,
            )
        if event.kind.startswith("context_compaction."):
            self._project_progress(
                connection,
                scope,
                sequence,
                state="running",
                content="Running · Codex is compacting thread context",
                now=now,
            )
        if event.kind.startswith("thread_goal."):
            status = str(event.payload.get("status") or "cleared")
            self._project_progress(
                connection,
                scope,
                sequence,
                state="running",
                content=f"Running · Codex thread goal: `{status[:64]}`",
                now=now,
            )
        if event.kind in {
            "model.rerouted",
            "model.safety",
            "model.verification",
            "turn.moderation",
        }:
            self._project_policy_notice(
                connection,
                scope,
                sequence,
                event,
                now,
            )
        if event.kind == "provider.error":
            self._project_provider_error(connection, scope, sequence, event, now)
        if event.kind.startswith("review_mode.") and event.payload.get(
            "lifecycle"
        ) == "completed":
            action = "entered" if event.kind.endswith(".entered") else "exited"
            self._insert_outbox(
                connection,
                destination_key=f"thread:{scope['discord_thread_id']}",
                operation="send",
                payload={
                    "kind": "notice",
                    "level": "info",
                    "title": f"Review mode {action}",
                    "content": (
                        "Codex reported a passive review-mode transition. "
                        "codexD did not initiate a review command."
                    ),
                },
                dedupe_key=f"turn:{scope['id']}:review-mode:{sequence}",
                marker=f"turn-{str(scope['id'])[:8]}-review-{sequence}",
                event_sequence=sequence,
                now=now,
            )
        if event.kind == "image_generation.completed" and event.payload.get(
            "has_saved_path"
        ):
            self._record_incident(
                connection,
                severity="warning",
                code="image_generation_attachment_unavailable",
                summary=(
                    "Generated image remained metadata-only because no validated "
                    "staged artifact was available"
                ),
                project_id=str(scope["project_id"]),
                conversation_id=str(scope["conversation_id"]),
                turn_id=str(scope["id"]),
                details={
                    "item_id": event.payload.get("item_id"),
                    "saved_path": event.payload.get("saved_path"),
                },
                now=now,
            )
        if event.kind.startswith("collaboration."):
            self._project_task(connection, scope, sequence, event, now)
            self._project_progress(
                connection,
                scope,
                sequence,
                state="running",
                content="Running · coordinating Codex agents",
                now=now,
            )
        if event.kind in {"provider.unknown", "provider.item"}:
            self._project_unknown(connection, scope, sequence, event, now)

    def _project_unknown(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
        now: int,
    ) -> None:
        code = (
            "unknown_provider_notification"
            if event.kind == "provider.unknown"
            else "unknown_provider_item"
        )
        self._record_incident(
            connection,
            severity="warning",
            code=code,
            summary="Codex emitted a provider payload that this adapter does not recognize",
            project_id=str(scope["project_id"]),
            conversation_id=str(scope["conversation_id"]),
            turn_id=str(scope["id"]),
            details={
                "raw_type": event.raw_type,
                "raw_hash": event.raw_hash,
                "raw_size": event.raw_size,
            },
            now=now,
        )
        self._insert_outbox(
            connection,
            destination_key=f"thread:{scope['discord_thread_id']}",
            operation="send",
            payload={
                "kind": "notice",
                "level": "warning",
                "title": "Unsupported provider event",
                "content": (
                    "Codex reported an unsupported provider event. The Turn is still "
                    f"being observed; diagnostics code: `{code}`."
                )
            },
            dedupe_key=f"turn:{scope['id']}:unknown:{sequence}",
            marker=f"unknown-{sequence}",
            event_sequence=sequence,
            now=now,
        )

    def _project_provider_error(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
        now: int,
    ) -> None:
        retry_count = int(event.payload.get("retry_count") or 0)
        retry_limit = event.payload.get("retry_limit")
        http_status = event.payload.get("http_status")
        connection.execute(
            """
            UPDATE turns
            SET provider_error_code = COALESCE(?, provider_error_code),
                provider_retry_count = MAX(provider_retry_count, ?),
                provider_retry_limit = COALESCE(?, provider_retry_limit),
                provider_http_status = COALESCE(?, provider_http_status)
            WHERE id = ?
            """,
            (
                event.payload.get("failure_code"),
                retry_count,
                retry_limit if isinstance(retry_limit, int) else None,
                http_status if isinstance(http_status, int) else None,
                scope["id"],
            ),
        )
        if event.payload.get("will_retry"):
            suffix = (
                f" `{retry_count}/{retry_limit}`"
                if retry_count and isinstance(retry_limit, int)
                else ""
            )
            self._project_progress(
                connection,
                scope,
                sequence,
                state="running",
                content=f"Running · Codex reconnecting{suffix}",
                now=now,
            )
        if event.payload.get("unknown_typed"):
            self._record_incident(
                connection,
                severity="warning",
                code="provider_error_type_unknown",
                summary="Codex returned an unknown typed provider error",
                project_id=str(scope["project_id"]),
                conversation_id=str(scope["conversation_id"]),
                turn_id=str(scope["id"]),
                details={
                    "typed_code_hash": sha256_text(
                        str(event.payload.get("typed_code") or "unknown")
                    ),
                    "http_status": event.payload.get("http_status"),
                },
                now=now,
            )

    def _project_policy_notice(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
        now: int,
    ) -> None:
        if event.kind == "model.rerouted":
            title = "Model rerouted"
            content = (
                "Codex rerouted this Turn from "
                f"`{str(event.payload.get('from_model') or 'unknown')[:256]}` to "
                f"`{str(event.payload.get('to_model') or 'unknown')[:256]}`."
            )
            level = "warning"
        elif event.kind == "model.safety":
            title = "Model safety buffering"
            content = "Codex updated safety buffering for this Turn."
            level = "warning"
        elif event.kind == "model.verification":
            title = "Model verification"
            content = "Codex reported a model policy verification for this Turn."
            level = "info"
        else:
            title = "Moderation status"
            content = (
                "Codex reported a moderation status update. Raw moderation metadata "
                "was not retained or displayed."
            )
            level = "warning"
        self._insert_outbox(
            connection,
            destination_key=f"thread:{scope['discord_thread_id']}",
            operation="send",
            payload={
                "kind": "notice",
                "level": level,
                "title": title,
                "content": content,
            },
            dedupe_key=f"turn:{scope['id']}:policy:{sequence}",
            marker=f"turn-{str(scope['id'])[:8]}-policy-{sequence}",
            event_sequence=sequence,
            now=now,
        )

    @staticmethod
    def _record_incident(
        connection: sqlite3.Connection,
        *,
        severity: str,
        code: str,
        summary: str,
        project_id: str | None,
        conversation_id: str | None,
        turn_id: str | None,
        details: dict[str, Any],
        now: int,
    ) -> None:
        existing = connection.execute(
            """
            SELECT id FROM incidents
            WHERE code = ? AND project_id IS ? AND conversation_id IS ?
              AND turn_id IS ? AND resolved_at IS NULL
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
                (now, summary[:2048], canonical_json(details), existing["id"]),
            )
            return
        connection.execute(
            """
            INSERT INTO incidents(
                id, severity, code, project_id, conversation_id, turn_id,
                summary, details_json, occurrence_count, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                new_id(),
                severity,
                code,
                project_id,
                conversation_id,
                turn_id,
                summary[:2048],
                canonical_json(details),
                now,
                now,
            ),
        )

    def _project_content(
        self,
        turn_id: str,
        event: NormalizedEvent,
    ) -> None:
        ast = self._volatile_turns.content_ast(turn_id)
        raw_blocks = ast.get("blocks")
        if not isinstance(raw_blocks, list) or not all(
            isinstance(block, dict) for block in raw_blocks
        ):
            raise StorageError("message projection AST has invalid blocks")
        blocks = cast(list[dict[str, Any]], raw_blocks)
        item_id = str(event.payload.get("item_id", event.kind))
        family = "plan" if event.kind.startswith("plan.") else "text"
        block = next(
            (
                candidate
                for candidate in reversed(blocks)
                if candidate.get("item_id") == item_id and candidate.get("kind") == family
            ),
            None,
        )
        completed_text = (
            str(event.payload.get("text", ""))
            if event.kind.endswith(".completed")
            else ""
        )
        raw_phase = event.payload.get("phase")
        completed_phase = str(raw_phase) if raw_phase is not None else None
        if block is None and completed_text:
            block = next(
                (
                    candidate
                    for candidate in reversed(blocks)
                    if candidate.get("kind") == family
                    and not candidate.get("completed")
                    and isinstance(candidate.get("text"), str)
                    and completed_text.startswith(str(candidate["text"]))
                    and candidate.get("phase") in {None, completed_phase}
                ),
                None,
            )
            if block is not None:
                block["item_id"] = item_id
        if block is None:
            block = {
                "kind": family,
                "item_id": item_id,
                "text": "",
                "phase": completed_phase,
                "completed": False,
            }
            blocks.append(block)
        if event.kind.endswith(".delta"):
            block["text"] += str(event.payload.get("text", ""))
        elif event.kind.endswith(".completed"):
            if completed_text:
                block["text"] = completed_text
            block["phase"] = completed_phase
            block["completed"] = True
        self._volatile_turns.save_content_ast(turn_id, ast)

    def _project_tool(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
    ) -> None:
        item_id = str(event.payload.get("item_id", event.kind))
        kind = (
            "command"
            if event.kind == "terminal.interaction"
            else event.kind.split(".", 1)[0]
        )
        existing = connection.execute(
            """
            SELECT label, state, summary_json
            FROM tool_projections
            WHERE turn_id = ? AND kind = ? AND provider_item_id = ?
            """,
            (scope["id"], kind, item_id),
        ).fetchone()
        state = (
            "failed"
            if event.payload.get("status") in {"failed", "declined"}
            else ("completed" if event.kind.endswith(".completed") else "started")
        )
        if existing is not None and str(existing["state"]) in {"completed", "failed"}:
            state = str(existing["state"])
        label = str(
            event.payload.get("tool")
            or event.payload.get("namespace")
            or (existing["label"] if existing is not None else None)
            or kind
        )[:512]
        summary = (
            json.loads(str(existing["summary_json"]))
            if existing is not None
            else {}
        )
        if not isinstance(summary, dict):
            raise StorageError("tool projection summary must be an object")
        summary.update(_tool_projection_payload(event))
        connection.execute(
            """
            INSERT INTO tool_projections(
                id, turn_id, provider_item_id, kind, label,
                state, summary_json, last_event_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(turn_id, kind, provider_item_id) DO UPDATE SET
                label = excluded.label,
                state = excluded.state,
                summary_json = excluded.summary_json,
                last_event_sequence = excluded.last_event_sequence
            """,
            (
                new_id(),
                scope["id"],
                item_id,
                kind,
                label,
                state,
                canonical_json(summary),
                sequence,
            ),
        )

    def _project_progress(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        *,
        state: str,
        content: str | None,
        now: int,
    ) -> str | None:
        return insert_progress_update(
            connection,
            turn_id=str(scope["id"]),
            state=state,
            content=content,
            now=now,
            event_sequence=sequence,
            min_interval_ms=self._progress_update_ms,
        )

    def _project_task(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
        now: int,
    ) -> None:
        if event.payload.get("activity_kind") is not None:
            self._project_subagent_activity(
                connection,
                scope,
                sequence,
                event,
                now,
            )
            return
        item_id = str(event.payload.get("item_id", "unknown"))
        operation = str(event.payload.get("operation", "activity"))[:128]
        if operation == "wait":
            return
        correlation_hash = self._reference_hmac("task", str(scope["id"]), item_id)
        status = str(event.payload.get("status", ""))
        state = _task_state(event.kind, status)
        existing = connection.execute(
            """
            SELECT * FROM task_projections
            WHERE turn_id = ? AND source_type = 'collab_agent_tool_call'
              AND provider_item_id = ?
            """,
            (scope["id"], item_id),
        ).fetchone()
        if existing is not None and str(existing["state"]) in _TASK_TERMINAL_STATES:
            return
        task_id = str(existing["id"]) if existing else new_id()
        title = f"Codex task · {operation.replace('_', ' ')}"[:256]
        sender_hash = self._keyed_provider_reference(
            "sender",
            str(scope["id"]),
            event.payload.get("sender_thread_hash"),
        )
        connection.execute(
            """
            INSERT INTO task_projections(
                id, turn_id, source_type, provider_item_id,
                provider_correlation_hash, operation, tool_status, state,
                display_title, safe_status_summary, sender_thread_hash,
                sender_thread_hash_version,
                model, reasoning_effort, prompt_hash, prompt_size,
                last_event_sequence, created_at, updated_at, ended_at
            ) VALUES (
                ?, ?, 'collab_agent_tool_call', ?, ?, ?, ?, ?, ?, ?, ?, 1,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(turn_id, source_type, provider_item_id) DO UPDATE SET
                operation = excluded.operation,
                tool_status = excluded.tool_status,
                state = excluded.state,
                safe_status_summary = excluded.safe_status_summary,
                sender_thread_hash = COALESCE(
                    excluded.sender_thread_hash,
                    task_projections.sender_thread_hash
                ),
                sender_thread_hash_version = CASE
                    WHEN excluded.sender_thread_hash IS NOT NULL
                    THEN excluded.sender_thread_hash_version
                    ELSE task_projections.sender_thread_hash_version
                END,
                model = COALESCE(excluded.model, task_projections.model),
                reasoning_effort = COALESCE(
                    excluded.reasoning_effort,
                    task_projections.reasoning_effort
                ),
                prompt_hash = COALESCE(
                    excluded.prompt_hash,
                    task_projections.prompt_hash
                ),
                prompt_size = COALESCE(
                    excluded.prompt_size,
                    task_projections.prompt_size
                ),
                last_event_sequence = excluded.last_event_sequence,
                updated_at = excluded.updated_at,
                ended_at = excluded.ended_at
            """,
            (
                task_id,
                scope["id"],
                item_id,
                correlation_hash,
                operation,
                status or None,
                state,
                title,
                f"{operation} · {status or state}"[:512],
                sender_hash,
                event.payload.get("model"),
                event.payload.get("reasoning_effort"),
                event.payload.get("prompt_hash"),
                event.payload.get("prompt_size"),
                sequence,
                now,
                now,
                now if state in _TASK_TERMINAL_STATES else None,
            ),
        )
        raw_agents = event.payload.get("agents")
        agents = (
            [agent for agent in raw_agents if isinstance(agent, dict)]
            if isinstance(raw_agents, list)
            else []
        )
        known_hashes: set[str] = set()
        for agent in agents:
            thread_hash = self._keyed_provider_reference(
                "agent",
                str(scope["id"]),
                agent.get("thread_hash"),
            )
            if thread_hash is None:
                continue
            known_hashes.add(thread_hash)
            existing_agent = connection.execute(
                """
                SELECT agent_label, state
                FROM task_projection_agents
                WHERE task_projection_id = ? AND provider_agent_thread_hash = ?
                """,
                (task_id, thread_hash),
            ).fetchone()
            if (
                existing_agent is not None
                and str(existing_agent["state"]) in _AGENT_TERMINAL_STATES
            ):
                continue
            label = (
                str(existing_agent["agent_label"])
                if existing_agent is not None
                else self._next_agent_label(connection, task_id)
            )
            connection.execute(
                """
                INSERT INTO task_projection_agents(
                    task_projection_id, provider_agent_thread_hash,
                    provider_agent_thread_hash_version, agent_label,
                    state, safe_message, updated_at
                ) VALUES (?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(task_projection_id, provider_agent_thread_hash)
                DO UPDATE SET
                    state = excluded.state,
                    safe_message = excluded.safe_message,
                    updated_at = excluded.updated_at
                """,
                (
                    task_id,
                    thread_hash,
                    label,
                    _agent_state(str(agent.get("status", ""))),
                    None,
                    now,
                ),
            )
        receiver_hashes = event.payload.get("receiver_thread_hashes")
        if isinstance(receiver_hashes, list):
            for raw_hash in receiver_hashes:
                thread_hash = self._keyed_provider_reference(
                    "agent",
                    str(scope["id"]),
                    raw_hash,
                )
                if thread_hash is None or thread_hash in known_hashes:
                    continue
                existing_agent = connection.execute(
                    """
                    SELECT agent_label
                    FROM task_projection_agents
                    WHERE task_projection_id = ? AND provider_agent_thread_hash = ?
                    """,
                    (task_id, thread_hash),
                ).fetchone()
                label = (
                    str(existing_agent["agent_label"])
                    if existing_agent is not None
                    else self._next_agent_label(connection, task_id)
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO task_projection_agents(
                        task_projection_id, provider_agent_thread_hash,
                        provider_agent_thread_hash_version, agent_label,
                        state, updated_at
                    ) VALUES (?, ?, 1, ?, 'pending', ?)
                    """,
                    (task_id, thread_hash, label, now),
                )
        self._render_task_card(
            connection,
            scope,
            sequence,
            task_id,
            now,
        )

    def _project_subagent_activity(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
        now: int,
    ) -> None:
        agent_hash = self._keyed_provider_reference(
            "agent",
            str(scope["id"]),
            event.payload.get("agent_thread_hash"),
        )
        agents = connection.execute(
            """
            SELECT a.task_projection_id, a.state AS agent_state, t.state AS task_state
            FROM task_projection_agents a
            JOIN task_projections t ON t.id = a.task_projection_id
            WHERE t.turn_id = ? AND a.provider_agent_thread_hash = ?
            ORDER BY t.updated_at DESC, t.id
            """,
            (scope["id"], agent_hash),
        ).fetchall()
        if not agents:
            if agent_hash is None:
                self._record_incident(
                    connection,
                    severity="warning",
                    code="subagent_activity_reference_missing",
                    summary="A subagent activity item had no usable agent reference",
                    project_id=str(scope["project_id"]),
                    conversation_id=str(scope["conversation_id"]),
                    turn_id=str(scope["id"]),
                    details={"activity_kind": event.payload.get("activity_kind")},
                    now=now,
                )
                return
            self._project_standalone_subagent(
                connection,
                scope,
                sequence,
                event,
                agent_hash,
                now,
            )
            return
        activity = str(event.payload.get("activity_kind", ""))
        activity_state = _subagent_activity_state(activity)
        detail = None
        for agent in agents:
            if (
                str(agent["task_state"]) in _TASK_TERMINAL_STATES
                or str(agent["agent_state"]) in _AGENT_TERMINAL_STATES
            ):
                continue
            task_id = str(agent["task_projection_id"])
            connection.execute(
                """
                UPDATE task_projection_agents
                SET state = ?, safe_message = COALESCE(?, safe_message), updated_at = ?
                WHERE task_projection_id = ? AND provider_agent_thread_hash = ?
                """,
                (
                    activity_state,
                    detail,
                    now,
                    task_id,
                    agent_hash,
                ),
            )
            connection.execute(
                """
                UPDATE task_projections
                SET safe_status_summary = ?, last_event_sequence = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    _subagent_status_summary(activity, detail),
                    sequence,
                    now,
                    task_id,
                ),
            )
            self._render_task_card(
                connection,
                scope,
                sequence,
                task_id,
                now,
            )

    def _project_standalone_subagent(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        event: NormalizedEvent,
        agent_hash: str,
        now: int,
    ) -> None:
        activity = str(event.payload.get("activity_kind", "unknown"))
        state = _subagent_activity_state(activity)
        detail = None
        existing = connection.execute(
            """
            SELECT *
            FROM task_projections
            WHERE turn_id = ? AND source_type = 'subagent_activity'
              AND provider_correlation_hash = ?
            """,
            (scope["id"], agent_hash),
        ).fetchone()
        if existing is not None and str(existing["state"]) in _TASK_TERMINAL_STATES:
            return
        if existing is None:
            task_id = new_id()
            label = self._next_turn_agent_label(connection, str(scope["id"]))
            connection.execute(
                """
                INSERT INTO task_projections(
                    id, turn_id, source_type, provider_item_id,
                    provider_correlation_hash, operation, tool_status, state,
                    display_title, safe_status_summary,
                    last_event_sequence, created_at, updated_at, ended_at
                ) VALUES (
                    ?, ?, 'subagent_activity', ?, ?, 'activity', ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """,
                (
                    task_id,
                    scope["id"],
                    f"activity:{agent_hash[:24]}",
                    agent_hash,
                    activity,
                    state,
                    f"Codex subagent · {label}",
                    _subagent_status_summary(activity, detail),
                    sequence,
                    now,
                    now,
                    now if state in _TASK_TERMINAL_STATES else None,
                ),
            )
            connection.execute(
                """
                INSERT INTO task_projection_agents(
                    task_projection_id, provider_agent_thread_hash,
                    provider_agent_thread_hash_version, agent_label,
                    state, safe_message, updated_at
                ) VALUES (?, ?, 1, ?, ?, ?, ?)
                """,
                (task_id, agent_hash, label, state, detail, now),
            )
        else:
            task_id = str(existing["id"])
            connection.execute(
                """
                UPDATE task_projections
                SET tool_status = ?, state = ?, safe_status_summary = ?,
                    last_event_sequence = ?, updated_at = ?, ended_at = ?
                WHERE id = ?
                """,
                (
                    activity,
                    state,
                    _subagent_status_summary(activity, detail),
                    sequence,
                    now,
                    now if state in _TASK_TERMINAL_STATES else None,
                    task_id,
                ),
            )
            connection.execute(
                """
                UPDATE task_projection_agents
                SET state = ?, safe_message = COALESCE(?, safe_message), updated_at = ?
                WHERE task_projection_id = ? AND provider_agent_thread_hash = ?
                """,
                (state, detail, now, task_id, agent_hash),
            )
        self._render_task_card(connection, scope, sequence, task_id, now)

    def _render_task_card(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        task_id: str,
        now: int,
    ) -> None:
        task = connection.execute(
            "SELECT * FROM task_projections WHERE id = ?",
            (task_id,),
        ).fetchone()
        assert task is not None
        view = connection.execute(
            "SELECT * FROM task_card_views WHERE task_projection_id = ?", (task_id,)
        ).fetchone()
        nonce = secrets.token_urlsafe(9)
        nonce_hash = sha256_text(nonce)
        if view is None:
            view_id = new_id()
            connection.execute(
                """
                INSERT INTO task_card_views(
                    id, task_projection_id, destination_key, display_state,
                    content_revision, component_nonce_hash, created_at, updated_at
                ) VALUES (?, ?, ?, 'collapsed', 1, ?, ?, ?)
                """,
                (
                    view_id,
                    task_id,
                    f"thread:{scope['discord_thread_id']}",
                    nonce_hash,
                    now,
                    now,
                ),
            )
            revision = 1
        else:
            view_id = str(view["id"])
            revision = int(view["content_revision"]) + 1
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
                (task_id,),
            ).fetchall()
        ]
        expanded = bool(view is not None and view["display_state"] == "expanded")
        visible_payload: dict[str, object] = {
            "kind": "task_card",
            "view_id": view_id,
            "title": task["display_title"],
            "state": task["state"],
            "status_summary": task["safe_status_summary"],
            "operation": task["operation"],
            "model": task["model"],
            "reasoning_effort": task["reasoning_effort"],
            "agents": agents,
            "expanded": expanded,
        }
        previous_card = connection.execute(
            """
            SELECT operation, state, payload_json, next_attempt_at
            FROM discord_outbox
            WHERE coalesce_key = ? AND state <> 'superseded'
            ORDER BY enqueue_sequence DESC
            LIMIT 1
            """,
            (f"task-card:{task_id}",),
        ).fetchone()
        previous_ready_at: int | None = None
        if previous_card is not None:
            previous_payload = json.loads(str(previous_card["payload_json"]))
            if isinstance(previous_payload, dict):
                previous_visible = {
                    key: previous_payload.get(key) for key in visible_payload
                }
                if previous_visible == visible_payload:
                    return
            if (
                str(previous_card["state"]) == "pending"
                and str(previous_card["operation"]) == "send"
            ):
                previous_ready_at = now
            else:
                previous_ready_at = int(previous_card["next_attempt_at"])
        if view is not None:
            revision = int(view["content_revision"]) + 1
            connection.execute(
                """
                UPDATE task_card_views
                SET content_revision = ?, component_nonce_hash = ?, updated_at = ?
                WHERE id = ?
                """,
                (revision, nonce_hash, now, view_id),
            )
        self._insert_outbox(
            connection,
            destination_key=f"thread:{scope['discord_thread_id']}",
            operation="send" if revision == 1 else "edit",
            payload={
                **visible_payload,
                "revision": revision,
                "nonce": nonce,
            },
            dedupe_key=f"task-card:{view_id}:{revision}",
            marker=f"task-{view_id[:8]}-{revision}",
            event_sequence=sequence,
            now=now,
            coalesce_key=f"task-card:{task_id}",
            next_attempt_at=(
                now
                if revision == 1 or task["state"] in _TASK_TERMINAL_STATES
                else (
                    previous_ready_at
                    if previous_ready_at is not None and previous_ready_at > now
                    else now + self._task_card_update_ms
                )
            ),
        )

    def _finalize_turn(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        target: TurnState,
        terminal_code: str,
        event: NormalizedEvent,
        now: int,
    ) -> None:
        current = TurnState(scope["state"])
        if current.terminal:
            return
        assert_turn_transition(current, target)
        connection.execute(
            """
            UPDATE turns
            SET state = ?, terminal_code = ?, error_code = ?, ended_at = ?,
                provider_error_code = COALESCE(?, provider_error_code),
                provider_error_underlying_code = COALESCE(?, provider_error_underlying_code),
                provider_retry_count = MAX(provider_retry_count, ?),
                provider_retry_limit = COALESCE(?, provider_retry_limit),
                provider_http_status = COALESCE(?, provider_http_status),
                queued_input_text = NULL, queued_skill_inputs_json = NULL
            WHERE id = ?
            """,
            (
                target.value,
                terminal_code,
                terminal_code if target is TurnState.FAILED else None,
                now,
                event.payload.get("failure_code"),
                event.payload.get("underlying_typed_code")
                or event.payload.get("typed_code"),
                int(event.payload.get("retry_count") or 0),
                event.payload.get("retry_limit"),
                event.payload.get("http_status"),
                scope["id"],
            ),
        )
        if (
            target is TurnState.FAILED
            and scope["thread_revision_id"] is not None
            and isinstance(event.payload.get("failure_fingerprint"), str)
        ):
            fingerprint = event.payload.get("failure_fingerprint")
            connection.execute(
                """
                UPDATE thread_revisions
                SET consecutive_failure_count = CASE
                        WHEN degraded_fingerprint = ?
                        THEN consecutive_failure_count + 1 ELSE 1 END,
                    degraded_failure_code = ?, degraded_fingerprint = ?,
                    first_failed_at = CASE
                        WHEN degraded_fingerprint = ? THEN first_failed_at ELSE ? END,
                    last_failed_at = ?
                WHERE id = ?
                """,
                (
                    fingerprint,
                    terminal_code,
                    fingerprint,
                    fingerprint,
                    now,
                    now,
                    scope["thread_revision_id"],
                ),
            )
        elif target is TurnState.COMPLETED and scope["thread_revision_id"] is not None:
            connection.execute(
                """
                UPDATE thread_revisions
                SET degraded_failure_code = NULL, degraded_fingerprint = NULL,
                    consecutive_failure_count = 0,
                    first_failed_at = NULL, last_failed_at = NULL
                WHERE id = ?
                """,
                (scope["thread_revision_id"],),
            )
        self._volatile_turns.finalize(
            str(scope["id"]),
            fallback=_terminal_fallback(target),
        )
        self._finalize_open_tasks(
            connection,
            scope,
            sequence,
            target,
            now,
        )
        insert_prompt_reaction_update(
            connection,
            turn_id=str(scope["id"]),
            input_message_id=scope["input_message_id"],
            discord_thread_id=scope["discord_thread_id"],
            discord_parent_channel_id=scope["discord_parent_channel_id"],
            state="completed" if target is TurnState.COMPLETED else "failed",
            now=now,
            event_sequence=sequence,
        )
        progress_outbox_id = self._project_progress(
            connection,
            scope,
            sequence,
            state="terminal",
            content=_terminal_progress(target, terminal_code),
            now=now,
        )
        input_message_id = scope["input_message_id"]
        input_channel_id = (
            scope["discord_parent_channel_id"]
            if input_message_id is not None
            and str(input_message_id) == str(scope["discord_thread_id"])
            else scope["discord_thread_id"]
            if input_message_id is not None
            else None
        )
        turn_id = str(scope["id"])
        usage = self._volatile_turns.usage(turn_id)
        if usage is not None:
            validate_usage_payload(usage)
            connection.execute(
                "UPDATE turns SET usage_scope = ? WHERE id = ?",
                (USAGE_SCOPE, turn_id),
            )
        else:
            usage = latest_usage_payload(
                connection,
                turn_id=turn_id,
                max_sequence=sequence,
            )
        self._insert_outbox(
            connection,
            destination_key=f"thread:{scope['discord_thread_id']}",
            operation="send",
            payload={
                "kind": "turn_final",
                "turn_id": scope["id"],
                "state": target.value,
                "terminal_code": terminal_code,
                "model": scope["effective_model"],
                "reasoning_effort": scope["effective_reasoning_effort"],
                "sandbox": scope["effective_sandbox"],
                "approval_mode": scope["effective_approval_mode"],
                "started_at": scope["started_at"],
                "ended_at": now,
                "input_message_id": input_message_id,
                "input_channel_id": input_channel_id,
                "discord_guild_id": scope["discord_guild_id"],
                "usage": usage,
                "provider_error_code": event.payload.get("failure_code"),
                "provider_error_underlying_code": (
                    event.payload.get("underlying_typed_code")
                    or event.payload.get("typed_code")
                ),
                "provider_retry_count": int(event.payload.get("retry_count") or 0),
                "provider_retry_limit": event.payload.get("retry_limit"),
                "provider_http_status": event.payload.get("http_status"),
                "provider_safe_message": event.payload.get("safe_message"),
                "provider_degraded": self._revision_is_degraded(
                    connection,
                    str(scope["thread_revision_id"])
                    if scope["thread_revision_id"] is not None
                    else None,
                ),
                "content_storage": "volatile",
                "dynamic_tools_enabled": bool(scope["dynamic_tools_enabled"]),
            },
            dedupe_key=f"turn:{scope['id']}:final",
            marker=f"turn-{str(scope['id'])[:8]}-final",
            event_sequence=sequence,
            depends_on_outbox_id=progress_outbox_id,
            now=now,
        )

    @staticmethod
    def _revision_is_degraded(
        connection: sqlite3.Connection,
        revision_id: str | None,
    ) -> bool:
        if revision_id is None:
            return False
        row = connection.execute(
            "SELECT consecutive_failure_count FROM thread_revisions WHERE id = ?",
            (revision_id,),
        ).fetchone()
        return bool(row is not None and int(row["consecutive_failure_count"]) >= 2)

    def _finalize_open_tasks(
        self,
        connection: sqlite3.Connection,
        scope: sqlite3.Row,
        sequence: int,
        target: TurnState,
        now: int,
    ) -> None:
        task_state = {
            TurnState.COMPLETED: "completed",
            TurnState.CANCELLED: "interrupted",
            TurnState.INTERRUPTED: "interrupted",
            TurnState.FAILED: "errored",
        }[target]
        tasks = connection.execute(
            """
            SELECT id
            FROM task_projections
            WHERE turn_id = ?
              AND state NOT IN (
                  'completed', 'errored', 'interrupted', 'shutdown', 'not_found'
              )
            ORDER BY created_at, id
            """,
            (scope["id"],),
        ).fetchall()
        for task in tasks:
            task_id = str(task["id"])
            connection.execute(
                """
                UPDATE task_projections
                SET state = ?, safe_status_summary = ?,
                    last_event_sequence = ?, updated_at = ?, ended_at = ?
                WHERE id = ?
                """,
                (
                    task_state,
                    f"Turn {target.value}"[:512],
                    sequence,
                    now,
                    now,
                    task_id,
                ),
            )
            connection.execute(
                """
                UPDATE task_projection_agents
                SET state = ?, updated_at = ?
                WHERE task_projection_id = ?
                  AND state NOT IN (
                      'completed', 'errored', 'interrupted', 'shutdown', 'not_found'
                  )
                """,
                (task_state, now, task_id),
            )
            self._render_task_card(
                connection,
                scope,
                sequence,
                task_id,
                now,
            )

    @staticmethod
    def _insert_outbox(
        connection: sqlite3.Connection,
        *,
        destination_key: str,
        operation: str,
        payload: dict[str, Any],
        dedupe_key: str,
        marker: str,
        event_sequence: int,
        now: int,
        coalesce_key: str | None = None,
        depends_on_outbox_id: str | None = None,
        next_attempt_at: int | None = None,
    ) -> str:
        if coalesce_key is not None:
            supersede_coalesced_outbox(
                connection,
                coalesce_key=coalesce_key,
                now=now,
                states=("pending",),
            )
        outbox_id = new_id()
        payload_json = canonical_json(payload)
        inserted = connection.execute(
            """
            INSERT OR IGNORE INTO discord_outbox(
                id, event_sequence, destination_key, operation,
                depends_on_outbox_id, payload_json, dedupe_key, coalesce_key,
                delivery_marker, state, attempts, next_attempt_at,
                created_at, updated_at
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
                marker,
                next_attempt_at if next_attempt_at is not None else now,
                now,
                now,
            ),
        ).rowcount
        if inserted == 1:
            return outbox_id
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
        if existing is None:
            raise InvariantError("outbox dedupe conflict did not retain a row")
        expected = (
            event_sequence,
            destination_key,
            operation,
            depends_on_outbox_id,
            payload_json,
            coalesce_key,
            marker,
        )
        actual = (
            int(existing["event_sequence"]),
            existing["destination_key"],
            existing["operation"],
            existing["depends_on_outbox_id"],
            existing["payload_json"],
            existing["coalesce_key"],
            existing["delivery_marker"],
        )
        if actual != expected:
            raise InvariantError(
                "outbox dedupe key was reused for a different projection"
            )
        return str(existing["id"])

    def _prepare_projection_identity(self) -> None:
        now = utc_now_ms()
        fingerprint = self._reference_hmac("projection-key", "v1")
        with self.store.transaction() as connection:
            metadata = connection.execute(
                "SELECT key_fingerprint FROM projection_key_metadata WHERE id = 1"
            ).fetchone()
            if metadata is None:
                connection.execute(
                    """
                    INSERT INTO projection_key_metadata(id, key_fingerprint, created_at)
                    VALUES (1, ?, ?)
                    """,
                    (fingerprint, now),
                )
            elif not hmac.compare_digest(
                str(metadata["key_fingerprint"]),
                fingerprint,
            ):
                raise SecurityError(
                    "projection correlation key does not match durable projections"
                )
            self._migrate_legacy_task_identifiers(connection)

    def _migrate_legacy_task_identifiers(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        rows = connection.execute(
            """
            SELECT a.rowid, a.task_projection_id, a.provider_agent_thread_hash,
                   a.agent_label, t.turn_id
            FROM task_projection_agents a
            JOIN task_projections t ON t.id = a.task_projection_id
            WHERE a.provider_agent_thread_hash_version = 0
            ORDER BY a.task_projection_id, a.rowid
            """
        ).fetchall()
        used_labels: dict[str, set[str]] = {}
        for row in rows:
            task_id = str(row["task_projection_id"])
            labels = used_labels.setdefault(task_id, set())
            label = _normalized_agent_label(str(row["agent_label"]), labels)
            labels.add(label)
            keyed_hash = self._reference_hmac(
                "agent",
                str(row["turn_id"]),
                str(row["provider_agent_thread_hash"]),
            )
            connection.execute(
                """
                UPDATE task_projection_agents
                SET provider_agent_thread_hash = ?,
                    provider_agent_thread_hash_version = 1,
                    agent_label = ?
                WHERE rowid = ?
                """,
                (keyed_hash, label, row["rowid"]),
            )
        sender_rows = connection.execute(
            """
            SELECT id, turn_id, sender_thread_hash
            FROM task_projections
            WHERE sender_thread_hash_version = 0
            """
        ).fetchall()
        for row in sender_rows:
            sender_hash = (
                self._reference_hmac(
                    "sender",
                    str(row["turn_id"]),
                    str(row["sender_thread_hash"]),
                )
                if row["sender_thread_hash"] is not None
                else None
            )
            connection.execute(
                """
                UPDATE task_projections
                SET sender_thread_hash = ?, sender_thread_hash_version = 1
                WHERE id = ?
                """,
                (sender_hash, row["id"]),
            )

    def _reference_hmac(self, domain: str, *parts: str) -> str:
        message = "\0".join((domain, *parts)).encode()
        return hmac.new(
            self._correlation_key,
            message,
            hashlib.sha256,
        ).hexdigest()

    def _keyed_provider_reference(
        self,
        domain: str,
        turn_id: str,
        value: object,
    ) -> str | None:
        if value is None:
            return None
        reference = str(value)
        return self._reference_hmac(domain, turn_id, reference) if reference else None

    @staticmethod
    def _next_agent_label(
        connection: sqlite3.Connection,
        task_id: str,
    ) -> str:
        labels = {
            str(row["agent_label"])
            for row in connection.execute(
                """
                SELECT agent_label
                FROM task_projection_agents
                WHERE task_projection_id = ?
                """,
                (task_id,),
            ).fetchall()
        }
        ordinal = 1
        while f"agent-{ordinal}" in labels:
            ordinal += 1
        return f"agent-{ordinal}"

    @staticmethod
    def _next_turn_agent_label(
        connection: sqlite3.Connection,
        turn_id: str,
    ) -> str:
        labels = {
            str(row["agent_label"])
            for row in connection.execute(
                """
                SELECT a.agent_label
                FROM task_projection_agents a
                JOIN task_projections t ON t.id = a.task_projection_id
                WHERE t.turn_id = ?
                """,
                (turn_id,),
            ).fetchall()
        }
        ordinal = 1
        while f"agent-{ordinal}" in labels:
            ordinal += 1
        return f"agent-{ordinal}"


def _durable_event_payload(event: NormalizedEvent) -> dict[str, Any]:
    payload = event.payload
    if event.kind == "file_change.completed":
        raw_changes = payload.get("changes")
        changes = raw_changes if isinstance(raw_changes, list) else []
        result: dict[str, Any] = {
            "item_id": payload.get("item_id"),
            "status": payload.get("status"),
            "change_count": len(changes),
        }
        result.update(_content_metadata("changes", changes))
        return result
    if event.kind == "provider.error":
        result = {
            "will_retry": bool(payload.get("will_retry")),
            "failure_code": payload.get("failure_code"),
            "typed_code": payload.get("typed_code"),
            "http_status": payload.get("http_status"),
            "retry_count": payload.get("retry_count"),
            "retry_limit": payload.get("retry_limit"),
            "failure_fingerprint": payload.get("failure_fingerprint"),
            "unknown_typed": bool(payload.get("unknown_typed")),
        }
        result.update(_content_metadata("message", payload.get("message")))
        return result
    if event.kind in {
        "turn.completed",
        "turn.failed",
        "turn.interrupted",
        "turn.terminal_unparseable",
    }:
        result = {
            "provider_turn_id": payload.get("provider_turn_id"),
            "status": payload.get("status"),
            "failure_code": payload.get("failure_code"),
            "typed_code": payload.get("typed_code"),
            "http_status": payload.get("http_status"),
            "retry_count": payload.get("retry_count"),
            "retry_limit": payload.get("retry_limit"),
            "failure_fingerprint": payload.get("failure_fingerprint"),
            "unknown_typed": bool(payload.get("unknown_typed")),
        }
        result.update(_content_metadata("error", payload.get("error")))
        return result
    allowed_by_kind = {
        "provider.unknown": ("method", "raw_hash", "raw_size"),
        "provider.item": (
            "item_id",
            "type",
            "lifecycle",
            "raw_hash",
            "raw_size",
        ),
        "model.rerouted": ("from_model", "to_model", "reason"),
        "model.safety": ("model", "faster_model", "show_buffering_ui"),
        "model.verification": ("verifications",),
        "turn.moderation": ("metadata_hash", "metadata_size"),
        "runtime.stream_interrupted": ("code",),
        "runtime.stream_lifecycle": (
            "stream_created",
            "stream_claimed",
            "stream_closed",
            "terminal_notification",
            "duration_ms",
            "failure_code",
            "provider_thread_hash",
            "provider_turn_hash",
        ),
    }
    allowed = allowed_by_kind.get(event.kind)
    if allowed is not None:
        return {key: payload.get(key) for key in allowed if key in payload}
    if event.kind.startswith("review_mode."):
        return {
            key: payload.get(key)
            for key in ("item_id", "review_hash", "review_size", "lifecycle")
            if key in payload
        }
    return {
        "payload_hash": sha256_text(canonical_json(dict(payload))),
        "payload_size": len(canonical_json(dict(payload)).encode()),
    }


def _tool_projection_payload(event: NormalizedEvent) -> dict[str, Any]:
    payload = event.payload
    allowed = (
        "item_id",
        "status",
        "exit_code",
        "duration_ms",
        "tool",
        "namespace",
        "server",
        "success",
        "process_id_hash",
        "stdin_hash",
        "stdin_size",
        "run_hash",
        "event_name",
        "execution_mode",
        "handler_type",
        "scope",
        "entry_count",
        "review_hash",
        "target_item_id",
        "risk_level",
        "action_type",
        "decision_source",
        "result_hash",
        "result_size",
        "revised_prompt_hash",
        "revised_prompt_size",
        "has_saved_path",
    )
    result = {key: payload.get(key) for key in allowed if key in payload}
    for key in ("action", "changes", "command", "error", "message", "output", "query"):
        if key in payload:
            result.update(_content_metadata(key, payload.get(key)))
    return result


def _content_metadata(name: str, value: object) -> dict[str, object]:
    if value is None:
        return {}
    serialized = value if isinstance(value, str) else canonical_json(value)
    return {
        f"{name}_hash": sha256_text(serialized),
        f"{name}_size": len(serialized.encode()),
    }


def terminal_state_for_event(
    event: NormalizedEvent,
    *,
    interrupt_origin: InterruptOrigin | None,
) -> tuple[TurnState, str] | None:
    if event.kind == "turn.completed":
        return TurnState.COMPLETED, "provider_completed"
    if event.kind == "turn.failed":
        code = event.payload.get("failure_code")
        return TurnState.FAILED, code if isinstance(code, str) else "provider_failed"
    if event.kind == "turn.interrupted":
        if interrupt_origin is InterruptOrigin.USER:
            return TurnState.CANCELLED, "user_interrupted"
        return TurnState.INTERRUPTED, "provider_interrupted"
    if event.kind == "turn.terminal_unparseable":
        return TurnState.INTERRUPTED, "provider_terminal_unparseable"
    return None


def _terminal_fallback(state: TurnState) -> str:
    if state is TurnState.COMPLETED:
        return "Codex completed without a final response."
    if state is TurnState.CANCELLED:
        return "Turn cancelled."
    if state is TurnState.INTERRUPTED:
        return "Turn interrupted; it was not replayed."
    return "Turn failed."


def _streaming_preview(source: str) -> str:
    if len(source) <= 1800:
        return source
    return f"…{source[-1799:]}"


def _terminal_progress(state: TurnState, terminal_code: str) -> str:
    label = {
        TurnState.COMPLETED: "Completed",
        TurnState.CANCELLED: "Cancelled",
        TurnState.INTERRUPTED: "Interrupted",
        TurnState.FAILED: "Failed",
    }[state]
    return f"{label} · `{terminal_code}`"


def _tool_progress_content(event: NormalizedEvent) -> str:
    payload = event.payload
    if event.kind == "terminal.interaction":
        return "Running · Codex command terminal interaction"
    if event.kind.startswith("hook."):
        event_name = str(payload.get("event_name") or "hook")[:128]
        return f"Running · Codex hook: `{event_name}`"
    if event.kind.startswith("approval_review."):
        status = str(payload.get("status") or "reviewing")[:64]
        return f"Running · automatic approval review: `{status}`"
    if event.kind.startswith("command."):
        return "Running · Codex command"
    if event.kind.startswith("file_change."):
        return "Running · applying Codex file changes"
    if event.kind.startswith("image_view."):
        return "Running · inspecting an image"
    if event.kind.startswith("web_search."):
        return "Running · Codex web search"
    if event.kind.startswith("image_generation."):
        status = str(payload.get("status") or "running")[:64]
        return f"Running · image generation: `{status}`"
    label = str(payload.get("tool") or payload.get("namespace") or "tool")[:512]
    return f"Running · tool: `{label}`"


def _sleep_progress_content(event: NormalizedEvent) -> str:
    duration = event.payload.get("duration_ms")
    if isinstance(duration, int) and not isinstance(duration, bool) and duration >= 0:
        return f"Running · Codex paused for {duration} ms"
    return "Running · Codex paused briefly"


def _task_state(kind: str, status: str) -> str:
    if kind.endswith(".started"):
        return "running"
    return {
        "completed": "completed",
        "failed": "errored",
        "interrupted": "interrupted",
        "shutdown": "shutdown",
        "notFound": "not_found",
    }.get(status, "unknown")


def _agent_state(status: str) -> str:
    return {
        "pendingInit": "pending",
        "running": "running",
        "interrupted": "interrupted",
        "completed": "completed",
        "errored": "errored",
        "shutdown": "shutdown",
        "notFound": "not_found",
    }.get(status, "pending")


def _subagent_activity_state(activity: str) -> str:
    return {
        "completed": "completed",
        "finished": "completed",
        "interrupted": "interrupted",
        "failed": "errored",
        "errored": "errored",
        "shutdown": "shutdown",
        "notFound": "not_found",
    }.get(activity, "running")


def _subagent_activity_detail(event: NormalizedEvent) -> str | None:
    role = _optional_text(event.payload.get("agent_role"), 128)
    summary = _optional_text(event.payload.get("activity_summary"), 512)
    if role and summary:
        return f"{role} · {summary}"[:512]
    return summary or role


def _subagent_status_summary(activity: str, detail: str | None) -> str:
    return f"{activity} · {detail}"[:512] if detail else f"subagent · {activity}"[:512]


def _normalized_agent_label(value: str, used: set[str]) -> str:
    normalized = value.casefold().replace(" ", "-")
    if normalized.startswith("agent-"):
        suffix = normalized.removeprefix("agent-")
        if suffix.isdigit() and int(suffix) > 0 and normalized not in used:
            return normalized
    ordinal = 1
    while f"agent-{ordinal}" in used:
        ordinal += 1
    return f"agent-{ordinal}"


def _optional_text(value: object, limit: int) -> str | None:
    return str(value)[:limit] if value is not None else None
