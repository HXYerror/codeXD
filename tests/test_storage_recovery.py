from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from conftest import StorageContext

import codexd.storage.projectors as projector_module
from codexd.domain.content_blocks import CodeBlock, TableBlock
from codexd.domain.conversations import (
    ApprovalPolicy,
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    WebSearchMode,
)
from codexd.domain.events import NormalizedEvent
from codexd.domain.schedules import MisfirePolicy, ScheduleKind, ScheduleState
from codexd.domain.turns import (
    InterruptOrigin,
    TurnImage,
    TurnInput,
    TurnSource,
    TurnState,
)
from codexd.errors import ConflictError, InvariantError, SecurityError
from codexd.rendering.markdown import MarkdownContentParser
from codexd.runtime.codex_sdk import _normalize_notification
from codexd.storage.projectors import ProjectingEventSink
from codexd.storage.repository import Repository
from codexd.storage.schedules import ScheduleRepository
from codexd.storage.sqlite import SQLiteStore


def _activate_schedule_target(
    storage_context: StorageContext,
    suffix: str,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id=f"schedule-{suffix}-thread",
            requested_thread_id=None,
            provider_session_id=f"schedule-{suffix}-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )


def _running_turn(
    storage_context: StorageContext,
    suffix: str,
) -> tuple[object, object]:
    repository = storage_context.repository
    _activate_schedule_target(storage_context, suffix)
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text=suffix),
        input_message_id=f"{suffix}-message",
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash=f"{suffix}-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk",
        runtime_version="runtime",
        capability_hash="capabilities",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, f"{suffix}-provider-turn")
    return turn, lease


def test_migration_integrity(storage_context: StorageContext) -> None:
    assert storage_context.store.integrity_check() == "ok"
    assert storage_context.store.foreign_key_check() == ()


def test_outbox_dedupe_rejects_divergent_operation(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    first = repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "first"},
        dedupe_key="same-key",
        delivery_marker="same-marker",
    )

    assert repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "first"},
        dedupe_key="same-key",
        delivery_marker="same-marker",
    ) == first
    with pytest.raises(InvariantError, match="dedupe key"):
        repository.enqueue_outbox(
            destination_key="thread:300",
            operation="send",
            payload={"content": "different"},
            dedupe_key="same-key",
            delivery_marker="same-marker",
        )


def test_provider_event_id_rejects_divergent_content(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "event-id-collision")
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"e" * 32,
    )
    event = NormalizedEvent(
        "command.started",
        {"item_id": "command", "command": "first", "status": "inProgress"},
        provider_event_id="provider-event",
    )

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=event,
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=event,
    )
    with pytest.raises(InvariantError, match="provider event ID"):
        sink.record(
            turn_id=turn.id,
            runtime_generation=lease.generation,
            event=NormalizedEvent(
                "command.completed",
                {"item_id": "command", "command": "different", "status": "completed"},
                provider_event_id="provider-event",
            ),
        )


def test_turn_revision_must_belong_to_active_conversation(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "revision-scope")
    active = storage_context.repository.get_active_revision(
        storage_context.conversation.id
    )
    assert active is not None
    other = storage_context.repository.create_conversation(
        project_id=storage_context.project.id,
        discord_thread_id=301,
        discord_guild_id=100,
        discord_parent_channel_id=200,
        owner_user_id=400,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=other.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="wrong revision"),
        input_message_id="wrong-revision",
    )

    with pytest.raises(ConflictError, match="active revision"):
        storage_context.repository.attach_turn_revision(turn.id, active.id)


def test_tool_progress_updates_are_durably_throttled_and_terminal_is_immediate(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    turn, lease = _running_turn(storage_context, "stream-throttle")
    clock = [10_000]
    monkeypatch.setattr(projector_module, "utc_now_ms", lambda: clock[0])
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = 'sent', updated_at = ?
            WHERE coalesce_key = ?
            """,
            (clock[0], f"turn:{turn.id}:progress"),
        )
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"s" * 32,
        stream_update_ms=1000,
    )

    clock[0] = 10_010
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "command.started",
            {"item_id": "command-first", "command": "first", "status": "inProgress"},
            provider_event_id="stream-first",
        ),
    )
    clock[0] = 10_020
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "command.started",
            {
                "item_id": "command-second",
                "command": "first second",
                "status": "inProgress",
            },
            provider_event_id="stream-second",
        ),
    )
    clock[0] = 10_030
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "turn.completed",
            {
                "provider_turn_id": "stream-throttle-provider-turn",
                "status": "completed",
            },
            provider_event_id="stream-terminal",
        ),
    )

    rows = storage_context.store.query_all(
        """
        SELECT state, next_attempt_at, payload_json
        FROM discord_outbox
        WHERE coalesce_key = ?
        ORDER BY enqueue_sequence
        """,
        (f"turn:{turn.id}:progress",),
    )
    first_stream = json.loads(str(rows[-3]["payload_json"]))
    second_stream = json.loads(str(rows[-2]["payload_json"]))
    terminal = json.loads(str(rows[-1]["payload_json"]))
    assert int(rows[-3]["next_attempt_at"]) == 11_000
    assert int(rows[-2]["next_attempt_at"]) == 11_000
    assert rows[-2]["state"] == "superseded"
    assert int(rows[-1]["next_attempt_at"]) == 10_030
    assert first_stream["content"].endswith("`first`")
    assert second_stream["content"].endswith("`first second`")
    assert terminal["state"] == "terminal"
    assert terminal["plain_text"] == ""


def test_assistant_text_stays_plain_and_does_not_replace_tool_progress(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "plain-assistant-stream")
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"p" * 32,
        stream_update_ms=1000,
    )

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "command.started",
            {
                "item_id": "command",
                "command": "pytest -q",
                "status": "inProgress",
            },
            provider_event_id="plain-command",
        ),
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "assistant.message.delta",
            {"item_id": "answer", "text": "Normal Markdown response"},
            provider_event_id="plain-answer",
        ),
    )
    after_answer = storage_context.store.query_one(
        """
        SELECT payload_json
        FROM discord_outbox
        WHERE coalesce_key = ? AND state <> 'superseded'
        ORDER BY enqueue_sequence DESC
        LIMIT 1
        """,
        (f"turn:{turn.id}:progress",),
    )
    assert after_answer is not None
    answer_payload = json.loads(str(after_answer["payload_json"]))
    assert answer_payload["content"] == "Running · command: `pytest -q`"
    assert answer_payload["plain_text"] == "Normal Markdown response"
    assert "Normal Markdown response" not in answer_payload["content"]

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "file_change.started",
            {"item_id": "change", "status": "inProgress"},
            provider_event_id="plain-file-change",
        ),
    )
    after_tool = storage_context.store.query_one(
        """
        SELECT payload_json
        FROM discord_outbox
        WHERE coalesce_key = ? AND state <> 'superseded'
        ORDER BY enqueue_sequence DESC
        LIMIT 1
        """,
        (f"turn:{turn.id}:progress",),
    )
    assert after_tool is not None
    tool_payload = json.loads(str(after_tool["payload_json"]))
    assert tool_payload["content"] == "Running · applying Codex file changes"
    assert tool_payload["plain_text"] == "Normal Markdown response"


def test_completed_assistant_item_coalesces_matching_delta_item(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "assistant-item-coalesce")
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"a" * 32,
        stream_update_ms=1000,
    )
    text = "One commentary message, not two."
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "assistant.text.started",
            {"item_id": "delta-item", "phase": "commentary", "text": ""},
            provider_event_id="assistant-delta-started",
        ),
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "assistant.text.delta",
            {"item_id": "delta-item", "text": text[:16]},
            provider_event_id="assistant-delta",
        ),
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "assistant.text.completed",
            {
                "item_id": "completed-item",
                "phase": "commentary",
                "text": text,
            },
            provider_event_id="assistant-completed",
        ),
    )

    projection = storage_context.store.query_one(
        "SELECT content_ast_json, plain_text FROM message_projections WHERE turn_id = ?",
        (turn.id,),
    )
    assert projection is not None
    ast = json.loads(str(projection["content_ast_json"]))
    text_blocks = [block for block in ast["blocks"] if block["kind"] == "text"]
    assert text_blocks == [
        {
            "kind": "text",
            "item_id": "completed-item",
            "text": text,
            "phase": "commentary",
            "completed": True,
        }
    ]
    assert projection["plain_text"] == text
    latest_progress = storage_context.store.query_one(
        """
        SELECT payload_json
        FROM discord_outbox
        WHERE coalesce_key = ? AND state <> 'superseded'
        ORDER BY enqueue_sequence DESC
        LIMIT 1
        """,
        (f"turn:{turn.id}:progress",),
    )
    assert latest_progress is not None
    assert json.loads(str(latest_progress["payload_json"]))["plain_text"] == text


def test_independent_completed_assistant_items_keep_repeated_text(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "assistant-repeated-text")
    sink = ProjectingEventSink(storage_context.store, correlation_key=b"r" * 32)
    for item_id in ("first-item", "second-item"):
        sink.record(
            turn_id=turn.id,
            runtime_generation=lease.generation,
            event=NormalizedEvent(
                "assistant.text.completed",
                {
                    "item_id": item_id,
                    "phase": "commentary",
                    "text": "Intentionally repeated.",
                },
                provider_event_id=f"{item_id}-completed",
            ),
        )

    projection = storage_context.store.query_one(
        "SELECT content_ast_json, plain_text FROM message_projections WHERE turn_id = ?",
        (turn.id,),
    )
    assert projection is not None
    ast = json.loads(str(projection["content_ast_json"]))
    assert [block["item_id"] for block in ast["blocks"]] == [
        "first-item",
        "second-item",
    ]
    assert projection["plain_text"] == (
        "Intentionally repeated.\n\nIntentionally repeated."
    )


def test_streaming_projection_survives_every_delta_boundary_and_restart(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "stream-restart")
    source = (
        "Before\n\n```python\nprint('x')\n```\n\n"
        "| A | B |\n| --- | --- |\n| 中， | 😀 |\n"  # noqa: RUF001
    )
    prefix = ""
    for index, character in enumerate(source):
        ProjectingEventSink(
            storage_context.store,
            correlation_key=b"d" * 32,
            stream_update_ms=1500,
        ).record(
            turn_id=turn.id,
            runtime_generation=lease.generation,
            event=NormalizedEvent(
                "assistant.message.delta",
                {"item_id": "answer", "text": character},
                provider_event_id=f"delta-{index}",
            ),
        )
        prefix += character
        projection = storage_context.store.query_one(
            "SELECT plain_text FROM message_projections WHERE turn_id = ?",
            (turn.id,),
        )
        assert projection is not None
        assert projection["plain_text"] == prefix
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"d" * 32,
        stream_update_ms=1500,
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "assistant.message.completed",
            {"item_id": "answer", "phase": "final_answer", "text": source},
            provider_event_id="answer-completed",
        ),
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "turn.completed",
            {
                "provider_turn_id": "stream-restart-provider-turn",
                "status": "completed",
            },
            provider_event_id="turn-completed",
        ),
    )

    projection = storage_context.store.query_one(
        "SELECT plain_text, is_final FROM message_projections WHERE turn_id = ?",
        (turn.id,),
    )
    assert projection is not None
    assert projection["plain_text"] == source
    assert projection["is_final"] == 1
    blocks = MarkdownContentParser().parse(str(projection["plain_text"]))
    assert any(isinstance(block, CodeBlock) for block in blocks)
    assert any(isinstance(block, TableBlock) for block in blocks)


def test_render_fallback_incident_is_atomic_with_durable_plan(
    storage_context: StorageContext,
) -> None:
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="render incident"),
        input_message_id="render-incident-message",
    )
    plan = {"version": 3, "messages": [], "attachments": [], "incident_codes": []}

    for _ in range(2):
        storage_context.repository.persist_render_plan(
            turn_id=turn.id,
            source_sha256="a" * 64,
            plan=plan,
            retention_until=999_999_999_999_999,
            incident_codes=("table_font_coverage_missing",),
        )

    incident = storage_context.store.query_one(
        """
        SELECT code, occurrence_count
        FROM incidents
        WHERE turn_id = ? AND code = 'table_font_coverage_missing'
        """,
        (turn.id,),
    )
    assert incident is not None
    assert incident["occurrence_count"] == 1


def test_optional_provider_item_families_project_without_unknown_fallback(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "optional-items")
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"o" * 32,
    )
    events = (
        NormalizedEvent(
            "sleep.completed",
            {"item_id": "sleep", "duration_ms": 250},
            provider_event_id="sleep-completed",
        ),
        NormalizedEvent(
            "context_compaction.completed",
            {"item_id": "compact"},
            provider_event_id="compact-completed",
        ),
        NormalizedEvent(
            "review_mode.entered",
            {
                "item_id": "review",
                "review_hash": "a" * 64,
                "review_size": 10,
                "lifecycle": "completed",
            },
            provider_event_id="review-completed",
        ),
        NormalizedEvent(
            "image_generation.completed",
            {
                "item_id": "image",
                "status": "completed",
                "result_hash": "b" * 64,
                "result_size": 20,
                "revised_prompt_hash": "c" * 64,
                "revised_prompt_size": 30,
                "saved_path": "<project>/generated.png",
                "has_saved_path": True,
            },
            provider_event_id="image-completed",
        ),
    )

    for event in events:
        sink.record(
            turn_id=turn.id,
            runtime_generation=lease.generation,
            event=event,
        )

    tool = storage_context.store.query_one(
        """
        SELECT kind, state, summary_json
        FROM tool_projections
        WHERE turn_id = ? AND provider_item_id = 'image'
        """,
        (turn.id,),
    )
    assert tool is not None
    assert (tool["kind"], tool["state"]) == ("image_generation", "completed")
    assert "<project>/generated.png" in str(tool["summary_json"])
    incident = storage_context.store.query_one(
        """
        SELECT code FROM incidents
        WHERE turn_id = ?
          AND code = 'image_generation_attachment_unavailable'
        """,
        (turn.id,),
    )
    assert incident is not None
    notice = storage_context.store.query_one(
        """
        SELECT payload_json FROM discord_outbox
        WHERE dedupe_key LIKE ?
        """,
        (f"turn:{turn.id}:review-mode:%",),
    )
    assert notice is not None
    assert json.loads(str(notice["payload_json"]))["title"] == "Review mode entered"
    unknown = storage_context.store.query_one(
        """
        SELECT 1 FROM incidents
        WHERE turn_id = ?
          AND code IN ('unknown_provider_notification', 'unknown_provider_item')
        """,
        (turn.id,),
    )
    assert unknown is None


def test_routed_event_families_project_without_unknown_fallback(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "routed-events")
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"r" * 32,
    )
    events = (
        NormalizedEvent(
            "command.started",
            {"item_id": "command", "command": "printf ok", "status": "running"},
            provider_event_id="command-started",
        ),
        NormalizedEvent(
            "command.completed",
            {"item_id": "command", "command": "printf ok", "status": "completed"},
            provider_event_id="command-completed",
        ),
        NormalizedEvent(
            "terminal.interaction",
            {
                "item_id": "command",
                "process_id_hash": "a" * 64,
                "stdin_hash": "b" * 64,
                "stdin_size": 1,
            },
            provider_event_id="terminal-interaction",
        ),
        NormalizedEvent(
            "file_change.started",
            {"item_id": "patch", "status": "running", "changes": []},
            provider_event_id="patch-started",
        ),
        NormalizedEvent(
            "file_change.patch.updated",
            {
                "item_id": "patch",
                "changes": [{"path": "<project>/a.py", "diff": "+x"}],
            },
            provider_event_id="patch-updated",
        ),
        NormalizedEvent(
            "mcp.started",
            {
                "item_id": "mcp",
                "server": "server",
                "tool": "tool",
                "status": "running",
            },
            provider_event_id="mcp-started",
        ),
        NormalizedEvent(
            "mcp.progress",
            {"item_id": "mcp", "message": "working"},
            provider_event_id="mcp-progress",
        ),
        NormalizedEvent(
            "hook.started",
            {
                "item_id": "hook-hash",
                "event_name": "preToolUse",
                "status": "running",
            },
            provider_event_id="hook-started",
        ),
        NormalizedEvent(
            "hook.completed",
            {
                "item_id": "hook-hash",
                "event_name": "preToolUse",
                "status": "completed",
            },
            provider_event_id="hook-completed",
        ),
        NormalizedEvent(
            "approval_review.started",
            {
                "item_id": "review-hash",
                "risk_level": "high",
                "status": "inProgress",
            },
            provider_event_id="review-started",
        ),
        NormalizedEvent(
            "approval_review.completed",
            {
                "item_id": "review-hash",
                "risk_level": "high",
                "status": "approved",
            },
            provider_event_id="review-completed",
        ),
        NormalizedEvent(
            "context_compaction.completed",
            {"provider_turn_id": "provider-turn"},
            provider_event_id="context-compacted",
        ),
        NormalizedEvent(
            "thread_goal.updated",
            {
                "status": "active",
                "objective_hash": "c" * 64,
                "objective_size": 10,
            },
            provider_event_id="goal-updated",
        ),
        NormalizedEvent(
            "thread_goal.cleared",
            {},
            provider_event_id="goal-cleared",
        ),
        NormalizedEvent(
            "model.rerouted",
            {"from_model": "a", "to_model": "b", "reason": "policy"},
            provider_event_id="model-rerouted",
        ),
        NormalizedEvent(
            "model.safety",
            {"model": "b", "show_buffering_ui": True},
            provider_event_id="model-safety",
        ),
        NormalizedEvent(
            "model.verification",
            {"verifications": ["trusted"]},
            provider_event_id="model-verification",
        ),
        NormalizedEvent(
            "turn.moderation",
            {"metadata_hash": "d" * 64, "metadata_size": 10},
            provider_event_id="turn-moderation",
        ),
    )

    for event in events:
        sink.record(
            turn_id=turn.id,
            runtime_generation=lease.generation,
            event=event,
        )

    tools = storage_context.store.query_all(
        """
        SELECT provider_item_id, kind, label, state, summary_json
        FROM tool_projections
        WHERE turn_id = ?
        ORDER BY kind, provider_item_id
        """,
        (turn.id,),
    )
    indexed = {
        (str(row["kind"]), str(row["provider_item_id"])): row for row in tools
    }
    command = indexed[("command", "command")]
    assert (command["label"], command["state"]) == ("printf ok", "completed")
    command_summary = json.loads(str(command["summary_json"]))
    assert command_summary["command"] == "printf ok"
    assert command_summary["stdin_hash"] == "b" * 64
    assert indexed[("file_change", "patch")]["state"] == "started"
    assert indexed[("mcp", "mcp")]["label"] == "tool"
    assert indexed[("hook", "hook-hash")]["state"] == "completed"
    assert indexed[("approval_review", "review-hash")]["state"] == "completed"

    policy_notices = storage_context.store.query_all(
        """
        SELECT payload_json FROM discord_outbox
        WHERE dedupe_key LIKE ?
        ORDER BY enqueue_sequence
        """,
        (f"turn:{turn.id}:policy:%",),
    )
    assert [json.loads(str(row["payload_json"]))["title"] for row in policy_notices] == [
        "Model rerouted",
        "Model safety buffering",
        "Model verification",
        "Moderation status",
    ]
    unknown = storage_context.store.query_one(
        """
        SELECT 1 FROM incidents
        WHERE turn_id = ?
          AND code IN ('unknown_provider_notification', 'unknown_provider_item')
        """,
        (turn.id,),
    )
    assert unknown is None


def test_channel_binding_migration_preserves_legacy_conversation_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.sqlite as sqlite_module

    migrations = sqlite_module._load_migrations()
    with SQLiteStore(tmp_path / "channel-binding-upgrade.sqlite3") as store:
        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 9),
        )
        assert store.migrate() == 8
        with store.transaction() as connection:
            connection.execute(
                """
                INSERT INTO projects(
                    id, name, root_path, root_path_casefold,
                    discord_guild_id, discord_channel_id, enabled,
                    sandbox_profile, created_at, updated_at
                ) VALUES (
                    'legacy-project', 'legacy', ?, ?, '10', '20', 0,
                    'read_only', 1, 1
                )
                """,
                (str(tmp_path), str(tmp_path)),
            )
            connection.execute(
                """
                INSERT INTO conversations(
                    id, project_id, discord_thread_id, owner_user_id, state,
                    web_search_mode, sandbox_profile, last_activity_at,
                    created_at, updated_at
                ) VALUES (
                    'legacy-conversation', 'legacy-project', '30', '40',
                    'uninitialized', 'cached', 'read_only', 1, 1, 1
                )
                """
            )

        monkeypatch.setattr(sqlite_module, "_load_migrations", lambda: migrations)
        assert store.migrate() == 14
        assert store.foreign_key_check() == ()
        repository = Repository(store)
        project = repository.get_project("legacy-project")
        conversation = repository.get_conversation("legacy-conversation")
        columns = {
            str(row["name"]) for row in store.query_all("PRAGMA table_info(projects)")
        }

        assert "enabled" not in columns
        assert "discord_channel_id" not in columns
        assert project.sandbox_profile is SandboxProfile.FULL_ACCESS
        assert conversation.sandbox_profile is SandboxProfile.FULL_ACCESS
        assert conversation.discord_guild_id == 10
        assert conversation.discord_parent_channel_id == 20
        assert repository.project_for_channel(10, 20) is None
        turn = repository.enqueue_turn(
            conversation_id=conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="still usable"),
            input_message_id="legacy-after-upgrade",
        )
        assert turn.conversation_id == conversation.id


def test_component_scope_migrations_expire_and_guard_legacy_pending_drafts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.sqlite as sqlite_module

    migrations = sqlite_module._load_migrations()
    with SQLiteStore(tmp_path / "component-scope-upgrade.sqlite3") as store:
        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 12),
        )
        assert store.migrate() == 11
        repository = Repository(store)
        root = tmp_path / "legacy-component-project"
        root.mkdir()
        project = repository.bind_project(
            name="legacy-component",
            root_path=root,
            guild_id=100,
            channel_id=200,
            sandbox_profile=SandboxProfile.FULL_ACCESS,
        )
        conversation = repository.create_conversation(
            project_id=project.id,
            discord_thread_id=300,
            discord_guild_id=100,
            discord_parent_channel_id=200,
            owner_user_id=400,
        )
        with store.transaction() as connection:
            connection.execute(
                """
                INSERT INTO schedule_drafts(
                    id, conversation_id, owner_user_id, action,
                    payload_json, occurrences_json, state,
                    component_nonce_hash, expires_at, created_at, updated_at
                ) VALUES (
                    'legacy-draft', ?, '400', 'create',
                    '{}', '[]', 'pending', 'nonce', 9999999999999, 1, 1
                )
                """,
                (conversation.id,),
            )

        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 13),
        )
        assert store.migrate() == 12
        row = store.query_one(
            """
            SELECT state, discord_guild_id, discord_channel_id
            FROM schedule_drafts WHERE id = 'legacy-draft'
            """
        )
        assert row is not None
        assert (row["state"], row["discord_guild_id"], row["discord_channel_id"]) == (
            "expired",
            None,
            None,
        )
        with store.transaction() as connection:
            connection.execute(
                "UPDATE schedule_drafts SET state = 'pending' WHERE id = 'legacy-draft'"
            )

        monkeypatch.setattr(sqlite_module, "_load_migrations", lambda: migrations)
        assert store.migrate() == 14
        row = store.query_one(
            """
            SELECT state, discord_guild_id, discord_channel_id
            FROM schedule_drafts WHERE id = 'legacy-draft'
            """
        )
        assert row is not None
        assert (row["state"], row["discord_guild_id"], row["discord_channel_id"]) == (
            "expired",
            None,
            None,
        )
        with (
            pytest.raises(sqlite3.IntegrityError, match="requires Discord scope"),
            store.transaction() as connection,
        ):
            connection.execute(
                "UPDATE schedule_drafts SET state = 'pending' WHERE id = 'legacy-draft'"
            )


def test_queued_turn_cancel_intent_completes_before_startup_recovery(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="cancel before provider start"),
        input_message_id="recover-cancel",
    )
    repository.accept_command_intent(
        interaction_id="recover-cancel-command",
        command_name="turn cancel",
        request={"turn": turn.id},
        boot_id="old-boot",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        turn_id=turn.id,
    )

    cancelled = repository.request_cancel(
        turn.id,
        origin=InterruptOrigin.USER,
        command_interaction_id="recover-cancel-command",
    )
    assert cancelled.state is TurnState.CANCELLED
    completed = repository.get_command_intent("recover-cancel-command")
    assert completed.state == "succeeded"
    assert json.loads(completed.result_json or "{}")["code"] == "ok"

    recovered = repository.recover_startup(current_boot_id="new-boot")

    intent = repository.get_command_intent("recover-cancel-command")
    assert intent.state == "succeeded"
    assert json.loads(intent.result_json or "{}")["code"] == "ok"
    assert recovered["reconciled_turn_cancel_intents"] == 0
    assert recovered["unknown_intents"] == 0


def test_turn_enqueue_sequence_migration_backfills_existing_turns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.sqlite as sqlite_module

    migrations = sqlite_module._load_migrations()
    database = tmp_path / "upgrade.sqlite3"
    with SQLiteStore(database) as store:
        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 5),
        )
        assert store.migrate() == 4
        with store.transaction() as connection:
            connection.execute(
                """
                INSERT INTO projects(
                    id, name, root_path, root_path_casefold,
                    discord_guild_id, discord_channel_id, sandbox_profile,
                    created_at, updated_at
                ) VALUES (
                    'upgrade-project', 'upgrade', ?, ?, '1', '2',
                    'full_access', 1, 1
                )
                """,
                (str(tmp_path), str(tmp_path)),
            )
            connection.execute(
                """
                INSERT INTO conversations(
                    id, project_id, discord_thread_id, owner_user_id, state,
                    web_search_mode, sandbox_profile, last_activity_at,
                    created_at, updated_at
                ) VALUES (
                    'upgrade-conversation', 'upgrade-project', '3', '4',
                    'uninitialized', 'cached', 'full_access', 1, 1, 1
                )
                """
            )
            for index, text in enumerate(("first", "second"), start=1):
                connection.execute(
                    """
                    INSERT INTO turns(
                        id, conversation_id, source_kind, input_message_id,
                        state, input_hash, queued_input_text,
                        effective_web_search_mode, effective_sandbox,
                        effective_approval_mode, queued_at
                    ) VALUES (?, 'upgrade-conversation', 'discord', ?, 'queued',
                              ?, ?, 'cached', 'full_access', 'auto_review', ?)
                    """,
                    (
                        f"upgrade-turn-{index}",
                        f"upgrade-message-{index}",
                        f"hash-{index}",
                        text,
                        index,
                    ),
                )

        monkeypatch.setattr(sqlite_module, "_load_migrations", lambda: migrations)
        assert store.migrate() == 14
        repository = Repository(store)
        third = repository.enqueue_turn(
            conversation_id="upgrade-conversation",
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="third"),
            input_message_id="upgrade-third",
        )
        rows = store.query_all(
            """
            SELECT id, enqueue_sequence
            FROM turns
            ORDER BY enqueue_sequence
            """
        )

    assert [row["id"] for row in rows] == [
        "upgrade-turn-1",
        "upgrade-turn-2",
        third.id,
    ]
    assert [int(row["enqueue_sequence"]) for row in rows] == [1, 2, 3]


def test_schedule_fire_turn_fk_upgrade_preserves_pairs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.sqlite as sqlite_module

    migrations = sqlite_module._load_migrations()
    with SQLiteStore(tmp_path / "schedule-fk-upgrade.sqlite3") as store:
        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 7),
        )
        assert store.migrate() == 6
        with store.transaction() as connection:
            connection.execute(
                """
                INSERT INTO projects(
                    id, name, root_path, root_path_casefold,
                    discord_guild_id, discord_channel_id, sandbox_profile,
                    created_at, updated_at
                ) VALUES (
                    'schedule-project', 'schedule-upgrade', ?, ?, '10', '20',
                    'full_access', 1, 1
                )
                """,
                (str(tmp_path), str(tmp_path)),
            )
            connection.execute(
                """
                INSERT INTO conversations(
                    id, project_id, discord_thread_id, owner_user_id, state,
                    web_search_mode, sandbox_profile, last_activity_at,
                    created_at, updated_at
                ) VALUES (
                    'schedule-conversation', 'schedule-project', '30', '40',
                    'active', 'cached', 'full_access', 1, 1, 1
                )
                """
            )
            connection.execute(
                """
                INSERT INTO schedules(
                    id, conversation_id, name, kind, expression, timezone,
                    misfire_policy, prompt_text, prompt_hash, state,
                    next_due_at, version, created_by_user_id, created_at, updated_at
                ) VALUES (
                    'schedule-upgrade', 'schedule-conversation', 'upgrade-fire',
                    'cron', '* * * * *', 'UTC', 'latest', 'upgrade', 'hash',
                    'active', 60000, 1, '40', 1, 1
                )
                """
            )
            connection.execute(
                """
                INSERT INTO schedule_fires(
                    id, schedule_id, occurrence_key, trigger_kind,
                    scheduled_for, scheduled_local, state, created_at
                ) VALUES (
                    'schedule-fire', 'schedule-upgrade', '60000', 'timer',
                    60000, '1970-01-01T00:01:00+00:00', 'materialized', 1
                )
                """
            )
            connection.execute(
                """
                INSERT INTO turns(
                    id, conversation_id, source_kind, schedule_fire_id, state,
                    input_hash, queued_input_text, effective_web_search_mode,
                    effective_sandbox, effective_approval_mode, queued_at
                ) VALUES (
                    'schedule-turn', 'schedule-conversation', 'schedule',
                    'schedule-fire', 'queued', 'hash', 'upgrade', 'cached',
                    'full_access', 'auto_review', 1
                )
                """
            )
            connection.execute(
                "UPDATE schedule_fires SET turn_id = 'schedule-turn' "
                "WHERE id = 'schedule-fire'"
            )

        monkeypatch.setattr(sqlite_module, "_load_migrations", lambda: migrations)
        assert store.migrate() == 14
        assert store.foreign_key_check() == ()
        fire_fks = store.query_all("PRAGMA foreign_key_list(schedule_fires)")
        assert any(
            row["from"] == "turn_id"
            and row["table"] == "turns"
            and row["on_delete"] == "CASCADE"
            for row in fire_fks
        )
        fire = store.query_one(
            "SELECT id, turn_id FROM schedule_fires WHERE schedule_id = ?",
            ("schedule-upgrade",),
        )
        assert fire is not None
        assert fire["turn_id"] == "schedule-turn"

        repository = Repository(store)
        discord_turn = repository.enqueue_turn(
            conversation_id="schedule-conversation",
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="not this fire"),
            input_message_id="not-this-fire",
        )
        with (
            pytest.raises(sqlite3.IntegrityError, match="pairing"),
            store.transaction() as connection,
        ):
            connection.execute(
                "UPDATE schedule_fires SET turn_id = ? WHERE id = ?",
                (discord_turn.id, fire["id"]),
            )
        with (
            pytest.raises(sqlite3.IntegrityError, match="pairing is immutable"),
            store.transaction() as connection,
        ):
            connection.execute(
                "UPDATE schedule_fires SET turn_id = NULL WHERE id = ?",
                (fire["id"],),
            )


def test_startup_never_replays_provider_or_discord_turns(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    conversation = storage_context.conversation
    first = repository.enqueue_turn(
        conversation_id=conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="first"),
        input_message_id="message-1",
    )
    second = repository.enqueue_turn(
        conversation_id=conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="second"),
        input_message_id="message-2",
    )
    revision = repository.activate_thread_revision(
        conversation_id=conversation.id,
        identity=ThreadIdentity(
            thread_id="provider-thread",
            provider_session_id="provider-session",
            requested_thread_id=None,
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
            approval_mode=ApprovalPolicy.AUTO_REVIEW,
            service_tier=None,
            web_search_mode=WebSearchMode.CACHED,
        ),
    )
    repository.attach_turn_revision(first.id, revision.id)
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk",
        runtime_version="runtime",
        capability_hash="capabilities",
    )
    repository.claim_turn(
        first.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )

    schedules = ScheduleRepository(storage_context.store)
    schedule = schedules.create(
        conversation_id=conversation.id,
        name="durable",
        kind=ScheduleKind.CRON,
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="scheduled",
        next_due_at=1,
        created_by_user_id=400,
    )
    scheduled = schedules.materialize(
        schedule_id=schedule.id,
        occurrence_key="occurrence-1",
        trigger_kind="timer",
        scheduled_for=1,
        scheduled_local="1970-01-01T00:00:00+00:00",
        next_due_at=3_600_001,
        expected_version=1,
    )
    assert scheduled.turn_id is not None

    recovered = repository.recover_startup(current_boot_id="new-boot")

    assert recovered["interrupted_turns"] == 2
    assert repository.get_turn(first.id).state is TurnState.INTERRUPTED
    assert repository.get_turn(second.id).state is TurnState.INTERRUPTED
    assert repository.get_turn(scheduled.turn_id).state is TurnState.QUEUED
    for turn_id in (first.id, second.id):
        assert storage_context.store.query_one(
            "SELECT 1 FROM message_projections WHERE turn_id = ? AND is_final = 1",
            (turn_id,),
        )
        assert storage_context.store.query_one(
            "SELECT 1 FROM discord_outbox WHERE dedupe_key = ?",
            (f"turn:{turn_id}:final",),
        )


def test_daemon_lease_requires_staleness(storage_context: StorageContext) -> None:
    repository = storage_context.repository
    repository.acquire_daemon_lease(
        boot_id="boot-1",
        pid=1,
        process_start_token="one",
        stale_before=0,
    )

    try:
        repository.acquire_daemon_lease(
            boot_id="boot-2",
            pid=2,
            process_start_token="two",
            stale_before=0,
        )
    except ConflictError:
        pass
    else:
        raise AssertionError("a fresh daemon lease must not be replaced")

    repository.release_daemon_lease("boot-1")


def test_recovered_outbox_is_claimed_for_reconciliation(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    record = repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "hello"},
        dedupe_key="reconcile-me",
        delivery_marker="reconcile-me",
    )
    claimed = repository.claim_outbox(worker_id="old-worker", lease_ms=30_000)
    assert claimed is not None
    assert claimed.id == record

    recovered = repository.recover_startup(current_boot_id="new-boot")
    assert recovered["reconciling_outbox"] == 1

    reclaimed = repository.claim_outbox(worker_id="new-worker", lease_ms=30_000)
    assert reclaimed is not None
    assert reclaimed.id == record
    assert reclaimed.attempts == 2


def test_reclaimed_outbox_rejects_every_stale_worker_mutation(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.repository as repository_module

    repository = storage_context.repository
    monkeypatch.setattr(repository_module, "utc_now_ms", lambda: 100)
    outbox_id = repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "fenced"},
        dedupe_key="fenced-outbox",
        delivery_marker="fenced-outbox",
    )
    first = repository.claim_outbox(worker_id="worker-a", lease_ms=10)
    assert first is not None

    monkeypatch.setattr(repository_module, "utc_now_ms", lambda: 120)
    second = repository.claim_outbox(worker_id="worker-b", lease_ms=10)
    assert second is not None
    assert second.id == outbox_id
    assert second.attempts == first.attempts + 1

    with pytest.raises(ConflictError, match="lease was lost"):
        repository.ack_outbox(
            outbox_id,
            lease_owner=first.lease_owner,
            lease_attempt=first.attempts,
        )
    with pytest.raises(ConflictError, match="lease was lost"):
        repository.retry_outbox(
            outbox_id,
            lease_owner=first.lease_owner,
            lease_attempt=first.attempts,
            error_code="stale_retry",
            next_attempt_at=0,
        )
    with pytest.raises(ConflictError, match="lease was lost"):
        repository.fail_outbox_permanently(
            outbox_id,
            lease_owner=first.lease_owner,
            lease_attempt=first.attempts,
            error_code="stale_failure",
        )

    repository.ack_outbox(
        outbox_id,
        lease_owner=second.lease_owner,
        lease_attempt=second.attempts,
        discord_message_id="delivered",
    )
    row = storage_context.store.query_one(
        "SELECT state, discord_message_id FROM discord_outbox WHERE id = ?",
        (outbox_id,),
    )
    assert row is not None
    assert (row["state"], row["discord_message_id"]) == ("sent", "delivered")


def test_reclaimed_thread_creation_rejects_stale_permanent_failure(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.repository as repository_module

    repository = storage_context.repository
    monkeypatch.setattr(repository_module, "utc_now_ms", lambda: 100)
    repository.request_thread_creation(
        discord_message_id="fenced-thread",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text="reclaimed request",
        has_image_attachment=False,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )
    first = repository.claim_outbox(worker_id="worker-a", lease_ms=10)
    assert first is not None

    monkeypatch.setattr(repository_module, "utc_now_ms", lambda: 120)
    second = repository.claim_outbox(worker_id="worker-b", lease_ms=10)
    assert second is not None
    with pytest.raises(ConflictError, match="lease was lost"):
        repository.fail_thread_creation_outbox(
            first.id,
            lease_owner=first.lease_owner,
            lease_attempt=first.attempts,
            error_code="stale_failure",
        )

    repository.fail_thread_creation_outbox(
        second.id,
        lease_owner=second.lease_owner,
        lease_attempt=second.attempts,
        error_code="discord_forbidden",
    )
    assert repository.get_ingress_message("fenced-thread").state == "rejected"


def test_project_unbind_preserves_existing_turns_and_schedules(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "unbind")
    schedules = ScheduleRepository(storage_context.store)
    schedule = schedules.create(
        conversation_id=storage_context.conversation.id,
        name="active",
        kind=ScheduleKind.CRON,
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="scheduled",
        next_due_at=1,
        created_by_user_id=400,
    )

    project = storage_context.repository.unbind_project(
        guild_id=100,
        channel_id=200,
        confirmation_name="test",
    )
    assert project.id == storage_context.project.id
    assert storage_context.repository.project_for_channel(100, 200) is None

    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="after unbind"),
        input_message_id="after-unbind",
    )
    assert turn.conversation_id == storage_context.conversation.id
    assert schedules.get(schedule.id).state is ScheduleState.ACTIVE


def test_ingress_completion_is_atomic_with_turn_enqueue(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    claimed, _turn_id = repository.claim_ingress_message(
        discord_message_id="atomic-message",
        content_hash="content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        discord_guild_id=100,
        discord_channel_id=200,
        boot_id="boot",
    )
    assert claimed

    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="atomic"),
        input_message_id="atomic-message",
        ingress_message_id="atomic-message",
    )

    ingress = storage_context.store.query_one(
        "SELECT state, turn_id FROM ingress_messages WHERE discord_message_id = ?",
        ("atomic-message",),
    )
    assert ingress is not None
    assert ingress["state"] == "ready"
    assert ingress["turn_id"] == turn.id
    reaction = storage_context.store.query_one(
        """
        SELECT destination_key, operation, state, payload_json
        FROM discord_outbox
        WHERE coalesce_key = ?
        """,
        (f"turn:{turn.id}:prompt-reaction",),
    )
    assert reaction is not None
    assert reaction["destination_key"] == (
        f"thread:{storage_context.conversation.discord_thread_id}"
    )
    assert reaction["operation"] == "edit"
    assert reaction["state"] == "pending"
    assert json.loads(str(reaction["payload_json"])) == {
        "kind": "prompt_reaction",
        "message_id": "atomic-message",
        "state": "waiting",
        "turn_id": turn.id,
    }

    repository.terminal_turn(
        turn.id,
        target=TurnState.CANCELLED,
        terminal_code="test_cancelled",
    )
    terminal_reactions = storage_context.store.query_all(
        """
        SELECT state, payload_json
        FROM discord_outbox
        WHERE coalesce_key = ?
        ORDER BY enqueue_sequence
        """,
        (f"turn:{turn.id}:prompt-reaction",),
    )
    assert [
        (row["state"], json.loads(str(row["payload_json"]))["state"])
        for row in terminal_reactions
    ] == [("superseded", "waiting"), ("pending", "failed")]


def test_starter_prompt_reaction_targets_parent_channel(
    storage_context: StorageContext,
) -> None:
    message_id = str(storage_context.conversation.discord_thread_id)
    claimed, _turn_id = storage_context.repository.claim_ingress_message(
        discord_message_id=message_id,
        content_hash="content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        discord_guild_id=storage_context.conversation.discord_guild_id,
        discord_channel_id=storage_context.conversation.discord_parent_channel_id,
        boot_id="boot",
    )
    assert claimed
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="starter"),
        input_message_id=message_id,
        ingress_message_id=message_id,
    )

    reaction = storage_context.store.query_one(
        """
        SELECT destination_key
        FROM discord_outbox
        WHERE coalesce_key = ?
        """,
        (f"turn:{turn.id}:prompt-reaction",),
    )
    assert reaction is not None
    assert reaction["destination_key"] == (
        f"channel:{storage_context.conversation.discord_parent_channel_id}"
    )


def test_ingress_enqueue_rolls_back_when_snapshot_persistence_fails(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    claimed, _turn_id = repository.claim_ingress_message(
        discord_message_id="rollback-message",
        content_hash="content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        discord_guild_id=100,
        discord_channel_id=200,
        boot_id="boot",
    )
    assert claimed
    missing_image = storage_context.store.path.parent / "missing-input.png"

    with pytest.raises(InvariantError, match="input image must be stored"):
        repository.enqueue_turn(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(
                text="rollback",
                images=(
                    TurnImage(
                        attachment_id="missing-input",
                        ordinal=0,
                        canonical_path=missing_image,
                        media_type="image/png",
                        source_sha256="source",
                        sha256="normalized",
                        size_bytes=1,
                        width=1,
                        height=1,
                        source_name_sanitized="missing.png",
                        retention_until=9_999_999_999_999,
                    ),
                ),
            ),
            input_message_id="rollback-message",
            ingress_message_id="rollback-message",
        )

    ingress = repository.get_ingress_message("rollback-message")
    assert ingress.state == "pending_preflight"
    assert ingress.turn_id is None
    assert storage_context.store.query_one(
        "SELECT 1 FROM turns WHERE input_message_id = ?",
        ("rollback-message",),
    ) is None


def test_discord_thread_delete_tombstones_pending_work(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    _activate_schedule_target(storage_context, "delete")
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="pending"),
        input_message_id="pending-delete",
    )
    schedules = ScheduleRepository(storage_context.store)
    schedule = schedules.create(
        conversation_id=storage_context.conversation.id,
        name="delete-target",
        kind=ScheduleKind.CRON,
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="scheduled",
        next_due_at=1,
        created_by_user_id=400,
    )

    repository.mark_conversation_deleted(
        storage_context.conversation.discord_thread_id
    )

    assert repository.get_conversation(
        storage_context.conversation.id
    ).state.value == "deleted"
    assert repository.get_turn(turn.id).state is TurnState.INTERRUPTED
    assert storage_context.store.query_one(
        """
        SELECT 1 FROM events
        WHERE turn_id = ? AND kind = 'runtime.local_terminal'
        """,
        (turn.id,),
    )
    assert storage_context.store.query_one(
        """
        SELECT 1 FROM message_projections
        WHERE turn_id = ? AND is_final = 1
        """,
        (turn.id,),
    )
    assert storage_context.store.query_one(
        "SELECT 1 FROM discord_outbox WHERE dedupe_key = ?",
        (f"turn:{turn.id}:final",),
    )
    assert schedules.get(schedule.id).state is ScheduleState.BLOCKED
    assert storage_context.store.query_one(
        """
        SELECT 1 FROM audit_log
        WHERE schedule_id = ? AND action = 'schedule.block'
          AND actor_kind = 'system'
          AND correlation_id LIKE '%discord_thread_deleted%'
        """,
        (schedule.id,),
    )
    assert repository.conversation_for_thread(
        storage_context.conversation.discord_thread_id
    ) is None


def test_outbox_preserves_destination_order_for_equal_timestamps(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.repository as repository_module

    monkeypatch.setattr(repository_module, "utc_now_ms", lambda: 100)
    repository = storage_context.repository
    first = repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "first"},
        dedupe_key="ordered-first",
        delivery_marker="ordered-first",
    )
    second = repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "second"},
        dedupe_key="ordered-second",
        delivery_marker="ordered-second",
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE discord_outbox SET rowid = 1000 WHERE id = ?",
            (first,),
        )
        connection.execute(
            "UPDATE discord_outbox SET rowid = 500 WHERE id = ?",
            (second,),
        )
    claimed = repository.claim_outbox(worker_id="worker", lease_ms=30_000)
    assert claimed is not None
    assert claimed.id == first
    repository.retry_outbox(
        claimed.id,
        lease_owner=claimed.lease_owner,
        lease_attempt=claimed.attempts,
        error_code="retry",
        next_attempt_at=200,
        permanent=False,
    )

    assert repository.claim_outbox(worker_id="worker", lease_ms=30_000) is None


def test_turn_queue_preserves_insert_order_for_equal_timestamps(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.repository as repository_module

    monkeypatch.setattr(repository_module, "utc_now_ms", lambda: 100)
    identifiers = iter(("z-turn", "a-turn"))
    monkeypatch.setattr(repository_module, "new_id", lambda: next(identifiers))
    repository = storage_context.repository
    first = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="first"),
        input_message_id="ordered-message-1",
    )
    second = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="second"),
        input_message_id="ordered-message-2",
    )

    assert repository.next_queued_turn(storage_context.conversation.id) == first
    rows = storage_context.store.query_all(
        """
        SELECT id, enqueue_sequence
        FROM turns
        WHERE id IN (?, ?)
        ORDER BY enqueue_sequence
        """,
        (first.id, second.id),
    )
    assert [row["id"] for row in rows] == [first.id, second.id]
    assert int(rows[0]["enqueue_sequence"]) < int(rows[1]["enqueue_sequence"])


def test_thread_creation_intent_is_durable_and_idempotent(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    created, outbox_id = repository.request_thread_creation(
        discord_message_id="301",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text="inspect durable storage",
        has_image_attachment=False,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )

    assert created
    ingress = repository.get_ingress_message("301")
    assert ingress.state == "pending_thread"
    assert ingress.conversation_id is None
    outbox = repository.claim_outbox(worker_id="worker")
    assert outbox is not None
    assert outbox.id == outbox_id
    assert outbox.operation == "create_thread"
    payload = json.loads(outbox.payload_json)
    assert set(payload) == {
        "expected_thread_id",
        "kind",
        "name",
        "owner_user_id",
        "project_id",
        "starter_message_id",
    }
    assert payload["name"] == f"inspect durable storage · {ingress.id[:4]}"
    assert 1 <= len(payload["name"]) <= 100

    conversation = repository.finalize_thread_creation(
        discord_message_id="301",
        discord_thread_id=301,
        owner_user_id=400,
    )
    assert conversation.discord_thread_id == 301
    assert conversation.sandbox_profile.value == "full_access"
    assert repository.get_ingress_message("301").state == "pending_preflight"

    duplicate, duplicate_outbox = repository.request_thread_creation(
        discord_message_id="301",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text="this duplicate must not replace the original title",
        has_image_attachment=True,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )
    assert not duplicate
    assert duplicate_outbox == outbox_id
    persisted = storage_context.store.query_one(
        "SELECT payload_json FROM discord_outbox WHERE id = ?",
        (outbox_id,),
    )
    assert persisted is not None
    assert persisted["payload_json"] == outbox.payload_json
    assert (
        repository.finalize_thread_creation(
            discord_message_id="301",
            discord_thread_id=301,
            owner_user_id=400,
        ).id
        == conversation.id
    )


def test_fresh_image_only_thread_creation_uses_image_title(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository

    created, _ = repository.request_thread_creation(
        discord_message_id="306",
        content_hash="content",
        attachment_manifest_hash="image-attachments",
        first_request_text="",
        has_image_attachment=True,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )

    assert created
    ingress = repository.get_ingress_message("306")
    outbox = repository.claim_outbox(worker_id="worker")
    assert outbox is not None
    payload = json.loads(outbox.payload_json)
    assert payload["name"] == f"图片任务 · {ingress.id[:4]}"


def test_thread_creation_title_is_redacted_bounded_and_only_persists_safe_name(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    secret = "provider-secret-value"
    github_pat = "github_pat_" + "a" * 22 + "_" + "b" * 59
    raw_request = (
        f"rotate {github_pat} inspect {storage_context.root}/private "
        f"OPENAI_API_KEY={secret} "
        + "界🙂" * 100
    )

    repository.request_thread_creation(
        discord_message_id="305",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text=raw_request,
        has_image_attachment=False,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )

    ingress = repository.get_ingress_message("305")
    outbox = repository.claim_outbox(worker_id="worker")
    assert outbox is not None
    payload = json.loads(outbox.payload_json)
    summary, suffix = payload["name"].rsplit(" · ", 1)
    assert suffix == ingress.id[:4]
    assert 1 <= len(summary) <= 72
    assert len(payload["name"]) <= 100
    assert raw_request not in outbox.payload_json
    assert str(storage_context.root) not in outbox.payload_json
    assert secret not in outbox.payload_json
    assert github_pat not in outbox.payload_json
    assert "<redacted>" in payload["name"]
    assert set(payload) == {
        "expected_thread_id",
        "kind",
        "name",
        "owner_user_id",
        "project_id",
        "starter_message_id",
    }


def test_thread_creation_title_redacts_credential_completed_by_truncation(
    storage_context: StorageContext,
) -> None:
    boundary_credential = "ghp_" + "a" * 36
    prefix = "x" * (68 - len(boundary_credential)) + " "
    raw_request = prefix + boundary_credential + "zzzz"
    assert len(prefix + boundary_credential) == 69
    assert len(raw_request) > 72

    storage_context.repository.request_thread_creation(
        discord_message_id="307",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text=raw_request,
        has_image_attachment=False,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )

    outbox = storage_context.repository.claim_outbox(worker_id="worker")
    assert outbox is not None
    payload = json.loads(outbox.payload_json)
    assert boundary_credential not in payload["name"]
    assert "ghp_" not in payload["name"]
    assert "<redacted>" in payload["name"]
    assert raw_request not in outbox.payload_json


def test_permanent_thread_creation_failure_rejects_ingress(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.request_thread_creation(
        discord_message_id="303",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text="permanent failure request",
        has_image_attachment=False,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )
    outbox = repository.claim_outbox(worker_id="worker")
    assert outbox is not None

    repository.fail_thread_creation_outbox(
        outbox.id,
        lease_owner=outbox.lease_owner,
        lease_attempt=outbox.attempts,
        error_code="discord_forbidden",
    )

    ingress = repository.get_ingress_message("303")
    assert ingress.state == "rejected"
    assert ingress.error_code == "discord_forbidden"
    notification = repository.claim_outbox(worker_id="worker")
    assert notification is not None
    assert notification.operation == "send"
    assert "discord_forbidden" in notification.payload_json


def test_local_terminal_projection_is_complete_and_unblocked_by_dead_progress(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="cancel before start"),
        input_message_id="cancel-before-start",
    )

    cancelled = repository.request_cancel(turn.id, origin=InterruptOrigin.USER)

    assert cancelled.state is TurnState.CANCELLED
    event = storage_context.store.query_one(
        "SELECT kind FROM events WHERE turn_id = ?",
        (turn.id,),
    )
    projection = storage_context.store.query_one(
        "SELECT plain_text, is_final FROM message_projections WHERE turn_id = ?",
        (turn.id,),
    )
    final = storage_context.store.query_one(
        "SELECT id, depends_on_outbox_id FROM discord_outbox WHERE dedupe_key = ?",
        (f"turn:{turn.id}:final",),
    )
    assert event is not None and event["kind"] == "runtime.local_terminal"
    assert projection is not None and projection["is_final"] == 1
    assert "cancelled_before_start" in projection["plain_text"]
    assert final is not None and final["depends_on_outbox_id"] is not None

    progress = repository.claim_outbox(worker_id="worker")
    assert progress is not None
    assert progress.id == final["depends_on_outbox_id"]
    repository.retry_outbox(
        progress.id,
        lease_owner=progress.lease_owner,
        lease_attempt=progress.attempts,
        error_code="discord_forbidden",
        next_attempt_at=0,
        permanent=True,
    )
    claimed_final = repository.claim_outbox(worker_id="worker")
    assert claimed_final is not None
    assert claimed_final.id == final["id"]


def test_terminal_progress_reconciles_running_send_before_terminal_revision(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="cancel while progress send is in flight"),
        input_message_id="cancel-progress-race",
    )
    running = repository.claim_outbox(worker_id="old-worker")
    assert running is not None
    repository.request_cancel(turn.id, origin=InterruptOrigin.USER)

    repository.recover_startup(current_boot_id="new-boot")
    recovered = repository.claim_outbox(worker_id="new-worker")

    assert recovered is not None
    assert recovered.id == running.id
    assert recovered.state == "reconciling"
    repository.ack_outbox(
        recovered.id,
        lease_owner=recovered.lease_owner,
        lease_attempt=recovered.attempts,
        discord_message_id="reconciled-progress",
    )
    terminal = repository.claim_outbox(worker_id="new-worker")
    assert terminal is not None
    assert json.loads(terminal.payload_json)["state"] == "terminal"
    sent = storage_context.store.query_one(
        "SELECT state FROM discord_outbox WHERE id = ?",
        (running.id,),
    )
    assert sent is not None
    assert sent["state"] == "sent"


def test_permanent_discord_failure_blocks_conversation_and_records_incident(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="delivery failure"),
        input_message_id="delivery-failure",
    )
    outbox = repository.claim_outbox(worker_id="worker")
    assert outbox is not None

    repository.fail_outbox_permanently(
        outbox.id,
        lease_owner=outbox.lease_owner,
        lease_attempt=outbox.attempts,
        error_code="discord_forbidden",
    )

    assert repository.get_conversation(turn.conversation_id).state.value == "blocked"
    incident = storage_context.store.query_one(
        "SELECT code, turn_id FROM incidents WHERE conversation_id = ?",
        (turn.conversation_id,),
    )
    assert incident is not None
    assert (incident["code"], incident["turn_id"]) == (
        "discord_delivery_permanent",
        turn.id,
    )
    notice = storage_context.store.query_one(
        "SELECT destination_key FROM discord_outbox WHERE dedupe_key = ?",
        (f"conversation:{turn.conversation_id}:delivery-blocked",),
    )
    assert notice is not None
    assert notice["destination_key"] == "channel:200"


def test_queued_image_snapshot_rejects_symlink_replacement(
    storage_context: StorageContext,
) -> None:
    source = storage_context.store.path.parent / "queued-image.png"
    replacement = storage_context.store.path.parent / "replacement.png"
    content = b"immutable image bytes"
    source.write_bytes(content)
    replacement.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(
            text="inspect image",
            images=(
                TurnImage(
                    attachment_id="queued-image",
                    ordinal=0,
                    canonical_path=source,
                    media_type="image/png",
                    source_sha256=digest,
                    sha256=digest,
                    size_bytes=len(content),
                    width=1,
                    height=1,
                    source_name_sanitized="queued-image.png",
                    retention_until=9_999_999_999_999,
                ),
            ),
        ),
        input_message_id="queued-image-message",
    )
    source.unlink()
    source.symlink_to(replacement)

    with pytest.raises(ConflictError, match="symlink"):
        storage_context.repository.load_turn_input(turn.id)


def test_runtime_loss_and_shutdown_create_terminal_projections(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="terminal-projection-thread",
            requested_thread_id=None,
            provider_session_id="terminal-projection-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    runtime_turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="runtime loss"),
        input_message_id="runtime-loss",
    )
    shutdown_turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="shutdown"),
        input_message_id="shutdown",
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk",
        runtime_version="runtime",
        capability_hash="capabilities",
    )
    repository.claim_turn(
        runtime_turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )

    assert repository.mark_runtime_unhealthy(
        lease.id,
        failure_code="runtime_crashed",
    ) == (runtime_turn.id,)
    assert repository.interrupt_for_shutdown() == 1

    for turn_id, code in (
        (runtime_turn.id, "runtime_lost"),
        (shutdown_turn.id, "daemon_shutdown"),
    ):
        terminal = repository.get_turn(turn_id)
        assert terminal.state is TurnState.INTERRUPTED
        assert terminal.terminal_code == code
        projection = storage_context.store.query_one(
            "SELECT plain_text, is_final FROM message_projections WHERE turn_id = ?",
            (turn_id,),
        )
        assert projection is not None
        assert projection["is_final"] == 1
        assert code in projection["plain_text"]
        assert storage_context.store.query_one(
            "SELECT 1 FROM discord_outbox WHERE dedupe_key = ?",
            (f"turn:{turn_id}:final",),
        )


def test_runtime_claim_and_events_are_fenced_after_lease_invalidation(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="fenced-runtime-thread",
            requested_thread_id=None,
            provider_session_id="fenced-runtime-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="runtime fencing"),
        input_message_id="runtime-fencing",
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk",
        runtime_version="runtime",
        capability_hash="capabilities",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_runtime_unhealthy(lease.id, failure_code="runtime_crashed")
    before = storage_context.store.query_one(
        "SELECT plain_text, content_revision FROM message_projections WHERE turn_id = ?",
        (turn.id,),
    )
    assert before is not None

    recorded = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"x" * 32,
    ).record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "assistant.message.completed",
            {"item_id": "late", "phase": "final_answer", "text": "LATE"},
            provider_event_id="late-after-runtime-loss",
        ),
    )

    after = storage_context.store.query_one(
        "SELECT plain_text, content_revision FROM message_projections WHERE turn_id = ?",
        (turn.id,),
    )
    assert recorded.sequence is None
    assert after is not None
    assert dict(after) == dict(before)
    assert storage_context.store.query_one(
        """
        SELECT 1 FROM events
        WHERE turn_id = ? AND provider_event_id = 'late-after-runtime-loss'
        """,
        (turn.id,),
    ) is None
    assert storage_context.store.query_one(
        """
        SELECT 1 FROM incidents
        WHERE turn_id = ? AND code = 'late_terminal_runtime_event'
        """,
        (turn.id,),
    )
    queued = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="must not claim"),
        input_message_id="invalid-runtime-claim",
    )
    with pytest.raises(ConflictError, match="runtime lease is stale"):
        repository.claim_turn(
            queued.id,
            runtime_lease_id=lease.id,
            runtime_generation=lease.generation,
        )
    with pytest.raises(ConflictError, match="runtime lease is stale"):
        repository.claim_turn(
            queued.id,
            runtime_lease_id=lease.id,
            runtime_generation=lease.generation + 1,
        )


def test_adapter_redaction_reaches_projection_and_outbox(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="redaction-thread",
            requested_thread_id=None,
            provider_session_id="redaction-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="redact provider output"),
        input_message_id="redaction-message",
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk",
        runtime_version="runtime",
        capability_hash="capabilities",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "provider-turn")
    secret = "sk-0123456789abcdef0123456789abcdef"
    provider_event = _normalize_notification(
        cast(
            Any,
            SimpleNamespace(
                method="item/completed",
                payload=SimpleNamespace(
                    item=SimpleNamespace(
                        root=SimpleNamespace(
                            type="agentMessage",
                            id="assistant",
                            text=f"answer {secret}",
                            phase="final_answer",
                        )
                    )
                ),
            ),
        ),
        cwd=storage_context.root,
    )
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"x" * 32,
    )

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=provider_event,
    )
    completed = sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "turn.completed",
            {"provider_turn_id": "provider-turn", "status": "completed"},
            provider_event_id="provider-turn-completed",
        ),
    )

    persisted = "\n".join(
        str(row[0])
        for row in storage_context.store.query_all(
            """
            SELECT payload_json FROM events WHERE turn_id = ?
            UNION ALL
            SELECT plain_text FROM message_projections WHERE turn_id = ?
            UNION ALL
            SELECT payload_json FROM discord_outbox WHERE dedupe_key = ?
            """,
            (turn.id, turn.id, f"turn:{turn.id}:final"),
        )
    )
    assert completed.terminal == (TurnState.COMPLETED, "provider_completed")
    assert secret not in persisted
    assert "<redacted>" in persisted


def test_turn_input_summary_is_redacted_and_bounded(
    storage_context: StorageContext,
) -> None:
    secret = "sk-0123456789abcdef0123456789abcdef"
    prompt = (
        f"inspect {storage_context.root}\n"
        f"Authorization: Bearer {secret}\n"
        + ("long context " * 30)
    )

    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text=prompt),
        input_message_id="redacted-summary",
    )

    assert secret not in turn.input_summary
    assert str(storage_context.root) not in turn.input_summary
    assert "<project>" in turn.input_summary
    assert "Authorization:" in turn.input_summary
    assert "<redacted>" in turn.input_summary
    assert "\n" not in turn.input_summary
    assert len(turn.input_summary) <= 180
    assert turn.input_summary.endswith("...")


def test_turn_recorded_diff_falls_back_to_completed_file_changes(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="diff-thread",
            requested_thread_id=None,
            provider_session_id="diff-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="diff-runtime",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk-test",
        runtime_version="runtime-test",
        capability_hash="diff-capabilities",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="record changes"),
        input_message_id="diff-input",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "diff-provider-turn")
    repository.append_event(
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="file_change.completed",
            provider_event_id="file-change-completed",
            payload={
                "item_id": "change-1",
                "status": "completed",
                "changes": [
                    {
                        "path": "src/example.py",
                        "kind": "update",
                        "diff": "@@ -1 +1 @@\n-old\n+new",
                    }
                ],
            },
        ),
    )

    assert repository.turn_recorded_diff(turn.id) == (
        "# update: src/example.py\n@@ -1 +1 @@\n-old\n+new"
    )

    repository.append_event(
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="diff.updated",
            provider_event_id="aggregate-diff",
            payload={"diff": "diff --git a/src/example.py b/src/example.py\n+aggregate"},
        ),
    )
    assert repository.turn_recorded_diff(turn.id) == (
        "diff --git a/src/example.py b/src/example.py\n+aggregate"
    )


def test_projection_key_change_fails_closed(
    storage_context: StorageContext,
) -> None:
    ProjectingEventSink(storage_context.store, correlation_key=b"a" * 32)

    with pytest.raises(
        SecurityError,
        match="projection correlation key does not match",
    ):
        ProjectingEventSink(storage_context.store, correlation_key=b"b" * 32)


def test_provider_terminal_correlation_and_usage_are_scoped(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    _activate_schedule_target(storage_context, "terminal-correlation")
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="terminal-correlation-runtime",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk-test",
        runtime_version="runtime-test",
        capability_hash="terminal-correlation-capabilities",
    )
    sink = ProjectingEventSink(storage_context.store, correlation_key=b"u" * 32)
    usage = {
        "last": {
            "input_tokens": 100,
            "output_tokens": 20,
            "cached_input_tokens": 10,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        },
        "total": {
            "input_tokens": 1_000,
            "output_tokens": 200,
            "cached_input_tokens": 100,
            "reasoning_output_tokens": 50,
            "total_tokens": 1_200,
        },
        "model_context_window": 1_000_000,
    }

    def complete_turn(
        turn_id: str,
        *,
        suffix: str,
        observed_usage: dict[str, object] | None = None,
    ) -> dict[str, object]:
        repository.claim_turn(
            turn_id,
            runtime_lease_id=lease.id,
            runtime_generation=lease.generation,
        )
        repository.mark_turn_running(turn_id, f"provider-{suffix}")
        if observed_usage is not None:
            sink.record(
                turn_id=turn_id,
                runtime_generation=lease.generation,
                event=NormalizedEvent(
                    kind="usage.updated",
                    provider_event_id=f"usage-{suffix}",
                    payload=observed_usage,
                ),
            )
        sink.record(
            turn_id=turn_id,
            runtime_generation=lease.generation,
            event=NormalizedEvent(
                kind="assistant.message.completed",
                provider_event_id=f"answer-{suffix}",
                payload={
                    "item_id": f"answer-{suffix}",
                    "phase": "final_answer",
                    "text": f"answer {suffix}",
                },
            ),
        )
        sink.record(
            turn_id=turn_id,
            runtime_generation=lease.generation,
            event=NormalizedEvent(
                kind="turn.completed",
                provider_event_id=f"completed-{suffix}",
                payload={
                    "provider_turn_id": f"provider-{suffix}",
                    "status": "completed",
                },
            ),
        )
        row = storage_context.store.query_one(
            "SELECT payload_json FROM discord_outbox WHERE dedupe_key = ?",
            (f"turn:{turn_id}:final",),
        )
        assert row is not None
        payload = json.loads(str(row["payload_json"]))
        assert isinstance(payload, dict)
        return payload

    starter_turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="starter"),
        input_message_id="300",
    )
    repository.claim_turn(
        starter_turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(starter_turn.id, "provider-starter")
    with pytest.raises(InvariantError, match=r"last\.input_tokens"):
        sink.record(
            turn_id=starter_turn.id,
            runtime_generation=lease.generation,
            event=NormalizedEvent(
                kind="usage.updated",
                provider_event_id="usage-malformed",
                payload={
                    "last": {
                        "input_tokens": True,
                        "output_tokens": 0,
                        "cached_input_tokens": 0,
                        "reasoning_output_tokens": 0,
                        "total_tokens": 0,
                    },
                    "total": usage["total"],
                },
            ),
        )
    assert storage_context.store.query_one(
        """
        SELECT 1 FROM events
        WHERE turn_id = ? AND provider_event_id = 'usage-malformed'
        """,
        (starter_turn.id,),
    ) is None
    sink.record(
        turn_id=starter_turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="usage.updated",
            provider_event_id="usage-starter",
            payload=usage,
        ),
    )
    sink.record(
        turn_id=starter_turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="assistant.message.completed",
            provider_event_id="answer-starter",
            payload={
                "item_id": "answer-starter",
                "phase": "final_answer",
                "text": "answer starter",
            },
        ),
    )
    sink.record(
        turn_id=starter_turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="turn.completed",
            provider_event_id="completed-starter",
            payload={
                "provider_turn_id": "provider-starter",
                "status": "completed",
            },
        ),
    )
    starter_row = storage_context.store.query_one(
        "SELECT payload_json FROM discord_outbox WHERE dedupe_key = ?",
        (f"turn:{starter_turn.id}:final",),
    )
    assert starter_row is not None
    starter_payload = json.loads(str(starter_row["payload_json"]))
    assert starter_payload["input_message_id"] == "300"
    assert starter_payload["input_channel_id"] == "200"
    assert starter_payload["discord_guild_id"] == "100"
    assert starter_payload["usage"] == usage
    assert repository.get_turn(starter_turn.id).usage_scope == (
        "provider_last_and_thread_total"
    )

    thread_turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="thread reply"),
        input_message_id="901",
    )
    thread_payload = complete_turn(thread_turn.id, suffix="thread")
    assert thread_payload["input_message_id"] == "901"
    assert thread_payload["input_channel_id"] == "300"
    assert thread_payload["discord_guild_id"] == "100"
    assert thread_payload["usage"] is None

    schedules = ScheduleRepository(storage_context.store)
    schedule = schedules.create(
        conversation_id=storage_context.conversation.id,
        name="terminal-correlation",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="scheduled turn",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    materialized = schedules.materialize(
        schedule_id=schedule.id,
        occurrence_key="60000",
        trigger_kind="timer",
        scheduled_for=60_000,
        scheduled_local="1970-01-01T00:01:00+00:00",
        next_due_at=120_000,
        expected_version=schedule.version,
    )
    assert materialized.turn_id is not None
    schedule_payload = complete_turn(materialized.turn_id, suffix="schedule")
    assert schedule_payload["input_message_id"] is None
    assert schedule_payload["input_channel_id"] is None
    assert schedule_payload["discord_guild_id"] == "100"
    assert schedule_payload["usage"] is None


def test_task_card_unknown_send_reconciles_before_newer_revision(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    _activate_schedule_target(storage_context, "task-card-reconcile")
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="task-card-reconcile-runtime",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk-test",
        runtime_version="runtime-test",
        capability_hash="task-card-reconcile-capabilities",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="recover task card"),
        input_message_id="task-card-reconcile-input",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "task-card-reconcile-provider-turn")
    sink = ProjectingEventSink(storage_context.store, correlation_key=b"r" * 32)
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.started",
            provider_event_id="task-reconcile-started",
            payload={
                "item_id": "provider-task-reconcile",
                "operation": "spawnAgent",
                "status": "inProgress",
            },
        ),
    )
    task = storage_context.store.query_one(
        """
        SELECT t.id, v.id AS view_id, o.id AS outbox_id
        FROM task_projections t
        JOIN task_card_views v ON v.task_projection_id = t.id
        JOIN discord_outbox o ON o.coalesce_key = 'task-card:' || t.id
        WHERE t.turn_id = ?
        """,
        (turn.id,),
    )
    assert task is not None
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = 'sent'
            WHERE coalesce_key = ?
            """,
            (f"turn:{turn.id}:progress",),
        )
    first_claim = repository.claim_outbox(worker_id="worker-first", lease_ms=-1)
    assert first_claim is not None
    assert first_claim.id == task["outbox_id"]
    assert first_claim.state == "pending"

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.completed",
            provider_event_id="task-reconcile-completed",
            payload={
                "item_id": "provider-task-reconcile",
                "operation": "spawnAgent",
                "status": "completed",
            },
        ),
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = 'sent'
            WHERE coalesce_key = ?
            """,
            (f"turn:{turn.id}:progress",),
        )
    recovered = repository.claim_outbox(worker_id="worker-recovered")
    assert recovered is not None
    assert recovered.id == first_claim.id
    assert recovered.state == "reconciling"
    repository.ack_outbox(
        recovered.id,
        lease_owner="worker-recovered",
        lease_attempt=recovered.attempts,
        discord_message_id="501",
        task_card_view_id=str(task["view_id"]),
    )
    assert repository.task_card_message(str(task["view_id"])) == "501"

    latest = repository.claim_outbox(worker_id="worker-latest")
    assert latest is not None
    assert latest.id != recovered.id
    assert latest.operation == "edit"
    assert latest.state == "pending"
    latest_payload = json.loads(latest.payload_json)
    assert latest_payload["revision"] == 2
    assert latest_payload["state"] == "completed"


def test_activity_only_subagents_create_and_finalize_task_cards(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "activity-only-subagents")
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"u" * 32,
    )

    for ordinal in range(1, 4):
        for suffix in ("started", "completed"):
            sink.record(
                turn_id=turn.id,
                runtime_generation=lease.generation,
                event=NormalizedEvent(
                    f"collaboration.{suffix}",
                    {
                        "item_id": f"activity-{ordinal}",
                        "operation": "activity",
                        "activity_kind": "started",
                        "agent_thread_hash": f"agent-thread-{ordinal}",
                        "agent_role": f"role-{ordinal}",
                        "activity_summary": f"Task {ordinal}",
                    },
                    provider_event_id=f"activity-{ordinal}-{suffix}",
                ),
            )

    projected = storage_context.store.query_all(
        """
        SELECT t.id, t.source_type, t.state, t.display_title,
               t.safe_status_summary, a.agent_label,
               a.state AS agent_state, a.safe_message,
               v.content_revision
        FROM task_projections t
        JOIN task_projection_agents a ON a.task_projection_id = t.id
        JOIN task_card_views v ON v.task_projection_id = t.id
        WHERE t.turn_id = ?
        ORDER BY a.agent_label
        """,
        (turn.id,),
    )
    assert len({str(row["id"]) for row in projected}) == 3
    assert {int(row["content_revision"]) for row in projected} == {2}
    assert [
        {
            "source_type": row["source_type"],
            "state": row["state"],
            "display_title": row["display_title"],
            "safe_status_summary": row["safe_status_summary"],
            "agent_label": row["agent_label"],
            "agent_state": row["agent_state"],
            "safe_message": row["safe_message"],
        }
        for row in projected
    ] == [
        {
            "source_type": "subagent_activity",
            "state": "running",
            "display_title": f"Codex subagent · agent-{ordinal}",
            "safe_status_summary": f"started · role-{ordinal} · Task {ordinal}",
            "agent_label": f"agent-{ordinal}",
            "agent_state": "running",
            "safe_message": f"role-{ordinal} · Task {ordinal}",
        }
        for ordinal in range(1, 4)
    ]
    assert storage_context.store.query_one(
        "SELECT id FROM incidents WHERE turn_id = ?",
        (turn.id,),
    ) is None

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "collaboration.completed",
            {
                "item_id": "activity-2-interacted",
                "operation": "activity",
                "activity_kind": "interacted",
                "agent_thread_hash": "agent-thread-2",
                "agent_role": "role-2",
                "activity_summary": "Updated task 2",
            },
            provider_event_id="activity-2-interacted",
        ),
    )
    assert storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM task_projections WHERE turn_id = ?",
        (turn.id,),
    )["count"] == 3
    updated = storage_context.store.query_one(
        """
        SELECT t.safe_status_summary, a.safe_message
        FROM task_projections t
        JOIN task_projection_agents a ON a.task_projection_id = t.id
        WHERE t.turn_id = ? AND a.agent_label = 'agent-2'
        """,
        (turn.id,),
    )
    assert updated is not None
    assert dict(updated) == {
        "safe_status_summary": "interacted · role-2 · Updated task 2",
        "safe_message": "role-2 · Updated task 2",
    }

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "turn.completed",
            {
                "provider_turn_id": "activity-only-subagents-provider-turn",
                "status": "completed",
            },
            provider_event_id="activity-only-subagents-terminal",
        ),
    )
    terminal = storage_context.store.query_all(
        """
        SELECT t.state, a.state AS agent_state
        FROM task_projections t
        JOIN task_projection_agents a ON a.task_projection_id = t.id
        WHERE t.turn_id = ?
        ORDER BY a.agent_label
        """,
        (turn.id,),
    )
    assert [dict(row) for row in terminal] == [
        {"state": "completed", "agent_state": "completed"},
        {"state": "completed", "agent_state": "completed"},
        {"state": "completed", "agent_state": "completed"},
    ]


def test_repeated_collaboration_wait_items_do_not_create_task_cards(
    storage_context: StorageContext,
) -> None:
    turn, lease = _running_turn(storage_context, "collaboration-wait")
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"w" * 32,
    )
    for ordinal in range(3):
        for suffix, status in (("started", "inProgress"), ("completed", "completed")):
            sink.record(
                turn_id=turn.id,
                runtime_generation=lease.generation,
                event=NormalizedEvent(
                    f"collaboration.{suffix}",
                    {
                        "item_id": f"wait-{ordinal}",
                        "operation": "wait",
                        "status": status,
                    },
                    provider_event_id=f"wait-{ordinal}-{suffix}",
                ),
            )

    assert storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM task_projections WHERE turn_id = ?",
        (turn.id,),
    )["count"] == 0
    task_outbox = storage_context.store.query_one(
        """
        SELECT COUNT(*) AS count
        FROM discord_outbox
        WHERE json_extract(payload_json, '$.kind') = 'task_card'
        """,
    )
    assert task_outbox is not None
    assert task_outbox["count"] == 0


def test_task_card_scope_and_terminal_revision_are_durable(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="task-card-thread",
            requested_thread_id=None,
            provider_session_id="task-card-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="task-card-runtime",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk-test",
        runtime_version="runtime-test",
        capability_hash="task-card-capabilities",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="coordinate agents"),
        input_message_id="task-card-input",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "task-card-provider-turn")
    correlation_key = b"t" * 32
    sink = ProjectingEventSink(
        storage_context.store,
        correlation_key=correlation_key,
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.started",
            provider_event_id="task-started",
            payload={
                "item_id": "provider-task",
                "operation": "spawnAgent",
                "status": "inProgress",
                "receiver_thread_hashes": ["agent-thread-hash"],
                "sender_thread_hash": "sender-thread-hash",
                "agents": [
                    {
                        "thread_hash": "agent-thread-hash",
                        "status": "running",
                        "message": "Running focused checks",
                    }
                ],
            },
        ),
    )
    initial = storage_context.store.query_one(
        """
        SELECT t.id AS task_id, t.provider_correlation_hash,
               t.sender_thread_hash, t.sender_thread_hash_version,
               v.id, v.content_revision, o.payload_json,
               o.coalesce_key, o.state AS outbox_state
        FROM task_card_views v
        JOIN task_projections t ON t.id = v.task_projection_id
        JOIN discord_outbox o
          ON json_extract(o.payload_json, '$.view_id') = v.id
        WHERE t.turn_id = ? AND t.provider_item_id = 'provider-task'
        ORDER BY v.content_revision DESC
        LIMIT 1
        """,
        (turn.id,),
    )
    assert initial is not None
    view_id = str(initial["id"])
    task_id = str(initial["task_id"])
    initial_payload = json.loads(str(initial["payload_json"]))
    expected_task_hash = hmac.new(
        correlation_key,
        f"task\0{turn.id}\0provider-task".encode(),
        hashlib.sha256,
    ).hexdigest()
    expected_sender_hash = hmac.new(
        correlation_key,
        f"sender\0{turn.id}\0sender-thread-hash".encode(),
        hashlib.sha256,
    ).hexdigest()
    expected_agent_hash = hmac.new(
        correlation_key,
        f"agent\0{turn.id}\0agent-thread-hash".encode(),
        hashlib.sha256,
    ).hexdigest()
    assert initial["provider_correlation_hash"] == expected_task_hash
    assert initial["sender_thread_hash"] == expected_sender_hash
    assert initial["sender_thread_hash_version"] == 1
    assert initial["coalesce_key"] == f"task-card:{task_id}"
    assert initial["outbox_state"] == "pending"
    initial_agent = storage_context.store.query_one(
        """
        SELECT provider_agent_thread_hash, provider_agent_thread_hash_version,
               agent_label, state, safe_message
        FROM task_projection_agents
        WHERE task_projection_id = ?
        """,
        (task_id,),
    )
    assert initial_agent is not None
    assert dict(initial_agent) == {
        "provider_agent_thread_hash": expected_agent_hash,
        "provider_agent_thread_hash_version": 1,
        "agent_label": "agent-1",
        "state": "running",
        "safe_message": "Running focused checks",
    }
    repository.set_task_card_message(view_id, "501")

    for index, overrides in enumerate(
        (
            {"owner_user_id": 401},
            {"guild_id": 101},
            {"channel_id": 301},
            {"message_id": 502},
        ),
        start=1,
    ):
        interaction_id = f"task-scope-{index}"
        repository.accept_command_intent(
            interaction_id=interaction_id,
            command_name="task card expand",
            request={"case": index},
            boot_id="task-card-test",
            actor_user_id=400,
            project_id=storage_context.project.id,
            conversation_id=storage_context.conversation.id,
            turn_id=turn.id,
        )
        arguments = {
            "view_id": view_id,
            "expected_revision": 1,
            "action": "expand",
            "component_nonce": str(initial_payload["nonce"]),
            "interaction_id": interaction_id,
            "owner_user_id": 400,
            "guild_id": 100,
            "channel_id": 300,
            "message_id": 501,
        }
        arguments.update(overrides)
        with pytest.raises(SecurityError):
            Repository(storage_context.store).update_task_card_display(**arguments)

    valid_interaction_id = "task-scope-valid"
    repository.accept_command_intent(
        interaction_id=valid_interaction_id,
        command_name="task card expand",
        request={"case": "valid"},
        boot_id="task-card-test",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        turn_id=turn.id,
    )
    interaction_outbox_id = repository.update_task_card_display(
        view_id=view_id,
        expected_revision=1,
        action="expand",
        component_nonce=str(initial_payload["nonce"]),
        interaction_id=valid_interaction_id,
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        message_id=501,
    )
    interaction_outbox = storage_context.store.query_one(
        "SELECT state, coalesce_key FROM discord_outbox WHERE id = ?",
        (interaction_outbox_id,),
    )
    assert interaction_outbox is not None
    assert dict(interaction_outbox) == {
        "state": "pending",
        "coalesce_key": f"task-card:{task_id}",
    }

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.started",
            provider_event_id="task-progress",
            payload={
                "item_id": "provider-task",
                "operation": "spawnAgent",
                "status": "inProgress",
                "agents": [
                    {
                        "thread_hash": "agent-a-thread-hash",
                        "status": "running",
                        "message": "Running new checks",
                    },
                    {
                        "thread_hash": "agent-thread-hash",
                        "status": "completed",
                        "message": "Focused checks complete",
                    },
                ],
            },
        ),
    )
    projected_agents = storage_context.store.query_all(
        """
        SELECT agent_label, provider_agent_thread_hash, state, safe_message
        FROM task_projection_agents
        WHERE task_projection_id = ?
        ORDER BY agent_label
        """,
        (task_id,),
    )
    expected_agent_a_hash = hmac.new(
        correlation_key,
        f"agent\0{turn.id}\0agent-a-thread-hash".encode(),
        hashlib.sha256,
    ).hexdigest()
    assert [dict(agent) for agent in projected_agents] == [
        {
            "agent_label": "agent-1",
            "provider_agent_thread_hash": expected_agent_hash,
            "state": "completed",
            "safe_message": "Focused checks complete",
        },
        {
            "agent_label": "agent-2",
            "provider_agent_thread_hash": expected_agent_a_hash,
            "state": "running",
            "safe_message": "Running new checks",
        },
    ]

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.completed",
            provider_event_id="task-completed",
            payload={
                "item_id": "provider-task",
                "operation": "spawnAgent",
                "status": "completed",
            },
        ),
    )
    terminal = storage_context.store.query_one(
        """
        SELECT t.state, v.content_revision
        FROM task_projections t
        JOIN task_card_views v ON v.task_projection_id = t.id
        WHERE t.turn_id = ? AND t.provider_item_id = 'provider-task'
        """,
        (turn.id,),
    )
    assert terminal is not None
    terminal_revision = int(terminal["content_revision"])
    assert terminal["state"] == "completed"

    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.completed",
            provider_event_id="task-conflicting-terminal",
            payload={
                "item_id": "provider-task",
                "operation": "spawnAgent",
                "status": "failed",
            },
        ),
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.started",
            provider_event_id="task-late-progress",
            payload={
                "item_id": "provider-task",
                "operation": "spawnAgent",
                "status": "inProgress",
            },
        ),
    )
    sink.record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            kind="collaboration.activity",
            provider_event_id="task-late-agent",
            payload={
                "activity_kind": "started",
                "agent_thread_hash": "agent-thread-hash",
            },
        ),
    )
    after_late = storage_context.store.query_one(
        """
        SELECT t.state, v.content_revision
        FROM task_projections t
        JOIN task_card_views v ON v.task_projection_id = t.id
        WHERE t.turn_id = ? AND t.provider_item_id = 'provider-task'
        """,
        (turn.id,),
    )
    assert after_late is not None
    assert (after_late["state"], after_late["content_revision"]) == (
        "completed",
        terminal_revision,
    )
    task_outbox = storage_context.store.query_all(
        """
        SELECT state, json_extract(payload_json, '$.revision') AS revision
        FROM discord_outbox
        WHERE coalesce_key = ?
        ORDER BY enqueue_sequence
        """,
        (f"task-card:{task_id}",),
    )
    assert [dict(row) for row in task_outbox] == [
        {"state": "superseded", "revision": 1},
        {"state": "superseded", "revision": 2},
        {"state": "superseded", "revision": 3},
        {"state": "pending", "revision": terminal_revision},
    ]


def test_terminal_interrupt_uses_transactional_cancel_origin(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="cancel-thread",
            requested_thread_id=None,
            provider_session_id="cancel-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="cancel provider turn"),
        input_message_id="cancel-message",
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk",
        runtime_version="runtime",
        capability_hash="capabilities",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "provider-turn")
    repository.request_cancel(turn.id, origin=InterruptOrigin.USER)

    recorded = ProjectingEventSink(
        storage_context.store,
        correlation_key=b"x" * 32,
    ).record(
        turn_id=turn.id,
        runtime_generation=lease.generation,
        event=NormalizedEvent(
            "turn.interrupted",
            {"provider_turn_id": "provider-turn", "status": "interrupted"},
            provider_event_id="provider-turn-interrupted",
        ),
    )

    terminal = repository.get_turn(turn.id)
    assert recorded.terminal == (TurnState.CANCELLED, "user_interrupted")
    assert terminal.state is TurnState.CANCELLED
    assert terminal.terminal_code == "user_interrupted"
