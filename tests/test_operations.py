from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest
from conftest import StorageContext

from codexd.config import AppConfig, RetentionConfig
from codexd.domain.conversations import (
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
)
from codexd.domain.ids import canonical_json, new_id, sha256_text, utc_now_ms
from codexd.domain.schedules import MisfirePolicy, ScheduleKind
from codexd.domain.turns import InterruptOrigin, TurnImage, TurnInput, TurnSource
from codexd.errors import StorageError
from codexd.paths import AppPaths
from codexd.service.diagnostics import export_diagnostics
from codexd.service.doctor import run_doctor
from codexd.service.manager import ServiceStatus
from codexd.storage.retention import run_retention
from codexd.storage.schedules import ScheduleRepository
from codexd.storage.sqlite import SQLiteStore


def test_sqlite_backup_checkpoints_and_verifies(tmp_path: Path) -> None:
    source = tmp_path / "data" / "codexd.sqlite3"
    backup = tmp_path / "backups" / "codexd.sqlite3"
    with SQLiteStore(source) as store:
        version = store.migrate()
        result = store.backup(backup)

    assert result == backup.resolve()
    with SQLiteStore(backup) as restored:
        assert restored.integrity_check() == "ok"
        assert restored.foreign_key_check() == ()
        assert restored.query_one(
            "SELECT MAX(version) AS version FROM schema_migrations"
        )["version"] == version
    if os.name != "nt":
        assert backup.stat().st_mode & 0o077 == 0


def test_sqlite_backup_rejects_fk_or_migration_corruption(tmp_path: Path) -> None:
    fk_source = tmp_path / "fk-source.sqlite3"
    fk_backup = tmp_path / "fk-backup.sqlite3"
    with SQLiteStore(fk_source) as store:
        store.migrate()
        store.connection.execute("PRAGMA foreign_keys = OFF")
        store.connection.execute(
            """
            INSERT INTO audit_log(
                id, actor_kind, action, project_id, payload_json, occurred_at
            ) VALUES ('invalid-fk', 'system', 'backup.test', 'missing-project', '{}', 1)
            """
        )
        store.connection.execute("PRAGMA foreign_keys = ON")
        assert store.integrity_check() == "ok"
        assert store.foreign_key_check()
        with pytest.raises(StorageError, match="foreign key check failed"):
            store.backup(fk_backup)
    assert not fk_backup.exists()

    checksum_source = tmp_path / "checksum-source.sqlite3"
    checksum_backup = tmp_path / "checksum-backup.sqlite3"
    with SQLiteStore(checksum_source) as store:
        store.migrate()
        store.connection.execute(
            """
            UPDATE schema_migrations
            SET checksum = 'invalid'
            WHERE version = (SELECT MAX(version) FROM schema_migrations)
            """
        )
        with pytest.raises(StorageError, match="migration checksum mismatch"):
            store.backup(checksum_backup)
    assert not checksum_backup.exists()


def test_diagnostics_bundle_is_redacted_by_default(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    paths = AppPaths(tmp_path / "data", tmp_path / "logs")
    paths.ensure()
    config = AppConfig(paths=paths)
    with SQLiteStore(paths.database) as store:
        store.migrate()
    paths.log_file.write_text(
        "\n".join(
            (
                '{"message":"token=super-secret-value","path":"/Users/alice/work"}',
                '{"message":"Authorization: Basic basic-secret"}',
                '{"message":"OPENAI_API_KEY=environment-secret"}',
                '{"message":"--password command-secret"}',
                (
                    '{"message":"https://example.invalid/?'
                    'access_token=query-secret"}'
                ),
                "",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "codexd.service.diagnostics.service_status",
        lambda _config: ServiceStatus(
            installed=True,
            heartbeat="fresh",
            process="running",
            service_manager="loaded",
            database_lease="fresh",
            boot_id="boot",
        ),
    )

    bundle = export_diagnostics(config)

    with zipfile.ZipFile(bundle) as archive:
        names = set(archive.namelist())
        content = b"\n".join(archive.read(name) for name in names)
    assert "content.json" not in names
    assert {
        "manifest.json",
        "health.json",
        "versions.json",
        "capabilities.json",
        "config.redacted.toml",
        "database-schema.txt",
        "database-integrity.txt",
        "incidents.json",
        "logs.tail.jsonl",
        "service-status.txt",
    } <= names
    assert b"super-secret-value" not in content
    assert b"basic-secret" not in content
    assert b"environment-secret" not in content
    assert b"command-secret" not in content
    assert b"query-secret" not in content
    assert b"/Users/alice" not in content


def test_doctor_fails_when_discord_startup_prerequisites_are_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = AppPaths(tmp_path / "data", tmp_path / "logs")
    paths.ensure()
    monkeypatch.setattr(
        "codexd.service.doctor.SecretStore.discord_token",
        lambda _self: None,
    )

    result = run_doctor(
        AppConfig(paths=paths),
        expected_environment=dict(os.environ),
    )

    assert result == 1
    output = capsys.readouterr().out
    assert '"discord_config"' in output
    assert '"discord_secret"' in output
    assert '"state": "missing"' in output


def test_diagnostics_included_content_is_still_redacted(
    storage_context: StorageContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = AppPaths(storage_context.store.path.parent, tmp_path / "logs")
    paths.ensure()
    config = AppConfig(paths=paths)
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="diagnostic redaction"),
        input_message_id="diagnostic-redaction",
    )
    secret = "sk-diagnostic-secret-value"
    now = utc_now_ms()
    with storage_context.store.transaction() as connection:
        event = connection.execute(
            """
            INSERT INTO events(
                event_id, turn_id, project_id, conversation_id,
                local_event_index, kind, schema_version, payload_json,
                occurred_at, recorded_at
            ) VALUES (?, ?, ?, ?, 1, 'assistant.text.completed', 1, ?, ?, ?)
            """,
            (
                new_id(),
                turn.id,
                storage_context.project.id,
                storage_context.conversation.id,
                canonical_json(
                    {
                        "authorization": f"Bearer {secret}",
                        "text": f"OPENAI_API_KEY={secret}",
                    }
                ),
                now,
                now,
            ),
        )
        assert event.lastrowid is not None
        connection.execute(
            """
            INSERT INTO message_projections(
                id, turn_id, content_revision, content_ast_json,
                plain_text, is_final, last_event_sequence
            ) VALUES (?, ?, 1, '[]', ?, 1, ?)
            """,
            (
                new_id(),
                turn.id,
                f"Authorization: Bearer {secret}",
                event.lastrowid,
            ),
        )
    monkeypatch.setattr(
        "codexd.service.diagnostics.service_status",
        lambda _config: ServiceStatus(
            installed=True,
            heartbeat="fresh",
            process="running",
            service_manager="loaded",
            database_lease="fresh",
            boot_id="boot",
        ),
    )

    bundle = export_diagnostics(config, include_content=True)

    with zipfile.ZipFile(bundle) as archive:
        payload = archive.read("content.json")
    assert secret.encode() not in payload
    assert b"<redacted>" in payload


def test_retention_preserves_active_and_removes_terminal_artifacts(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="retention-thread",
            requested_thread_id=None,
            provider_session_id="retention-session",
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
    input_path = storage_context.store.path.parent / "attachments" / "input" / "old.png"
    input_path.parent.mkdir(mode=0o700, parents=True)
    input_path.write_bytes(b"png")
    image = TurnImage(
        attachment_id="retention-image",
        ordinal=0,
        canonical_path=input_path,
        media_type="image/png",
        source_sha256=sha256_text("source"),
        sha256=sha256_text("png"),
        size_bytes=3,
        width=1,
        height=1,
        source_name_sanitized="old.png",
        retention_until=1,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="retain", images=(image,)),
        input_message_id="retention-message",
    )

    first = run_retention(
        storage_context.store,
        AppPaths(storage_context.store.path.parent, storage_context.store.path.parent / "logs"),
        RetentionConfig(),
        now_ms=utc_now_ms(),
    )
    assert first.input_attachments == 0
    assert input_path.exists()

    storage_context.repository.request_cancel(
        turn.id,
        origin=InterruptOrigin.USER,
    )
    render_root = storage_context.store.path.parent / "attachments" / "render"
    render_path = render_root / turn.id / "table.md"
    render_path.parent.mkdir(mode=0o700, parents=True)
    render_path.write_bytes(b"table")
    plan = {
        "version": 1,
        "messages": ["done"],
        "attachments": [
            {
                "filename": "table.md",
                "relative_path": f"{turn.id}/table.md",
                "description": "table",
                "sha256": sha256_text("table"),
                "size_bytes": 5,
            }
        ],
    }
    storage_context.repository.persist_render_plan(
        turn_id=turn.id,
        source_sha256=sha256_text("done"),
        plan=plan,
        retention_until=1,
    )
    now = utc_now_ms()
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            INSERT INTO discord_outbox(
                id, destination_key, operation, payload_json, dedupe_key,
                delivery_marker, state, attempts, next_attempt_at,
                created_at, updated_at
            ) VALUES (
                'retention-final', 'thread:300', 'send', ?, 'retention-final',
                'retention-final', 'sent', 1, ?, ?, ?
            )
            """,
            (
                canonical_json(
                    {
                        "kind": "turn_final",
                        "turn_id": turn.id,
                        "plain_text": "done",
                    }
                ),
                now,
                now,
                now,
            ),
        )

    result = run_retention(
        storage_context.store,
        AppPaths(storage_context.store.path.parent, storage_context.store.path.parent / "logs"),
        RetentionConfig(),
        now_ms=now,
    )

    assert result.input_attachments == 1
    assert result.render_plans == 1
    assert not input_path.exists()
    assert not render_path.exists()


def test_retention_sweeps_old_unreferenced_artifacts(
    storage_context: StorageContext,
) -> None:
    paths = AppPaths(
        storage_context.store.path.parent,
        storage_context.store.path.parent / "logs",
    )
    orphan = paths.attachments / "input" / "orphan.png"
    orphan.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    orphan.write_bytes(b"orphan")
    os.utime(orphan, (1, 1))

    result = run_retention(
        storage_context.store,
        paths,
        RetentionConfig(),
        now_ms=utc_now_ms(),
    )

    assert result.orphan_artifacts == 1
    assert not orphan.exists()


def test_retention_redacts_old_final_content_and_audit(
    storage_context: StorageContext,
) -> None:
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="sensitive old content"),
        input_message_id="old-content",
    )
    storage_context.repository.request_cancel(
        turn.id,
        origin=InterruptOrigin.USER,
    )
    now = utc_now_ms()
    old = now - 181 * 24 * 60 * 60 * 1000
    incident_id = storage_context.repository.record_incident(
        severity="warning",
        code="old_resolved_incident",
        summary="old",
        conversation_id=storage_context.conversation.id,
        turn_id=turn.id,
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            INSERT INTO audit_log(
                id, actor_kind, action, conversation_id,
                payload_json, occurred_at
            ) VALUES (?, 'system', 'retention.test', ?, '{}', ?)
            """,
            (new_id(), storage_context.conversation.id, old),
        )
        connection.execute(
            "UPDATE turns SET ended_at = ? WHERE id = ?",
            (old, turn.id),
        )
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = CASE
                    WHEN state = 'superseded' THEN state
                    ELSE 'sent'
                END,
                updated_at = ?
            WHERE json_extract(payload_json, '$.turn_id') = ?
            """,
            (old, turn.id),
        )
        connection.execute(
            "UPDATE audit_log SET occurred_at = ?",
            (old,),
        )
        connection.execute(
            "UPDATE incidents SET resolved_at = ? WHERE id = ?",
            (old, incident_id),
        )

    result = run_retention(
        storage_context.store,
        AppPaths(
            storage_context.store.path.parent,
            storage_context.store.path.parent / "logs",
        ),
        RetentionConfig(),
        now_ms=now,
    )

    assert result.final_projections == 1
    assert result.outbox_payloads >= 2
    assert result.audit_entries >= 1
    assert result.incidents == 1
    assert storage_context.store.query_one(
        "SELECT 1 FROM message_projections WHERE turn_id = ?",
        (turn.id,),
    ) is None
    payloads = storage_context.store.query_all(
        "SELECT payload_json FROM discord_outbox WHERE dedupe_key LIKE ?",
        (f"turn:{turn.id}:%",),
    )
    assert all("sensitive old content" not in row["payload_json"] for row in payloads)


def test_retention_expires_and_redacts_untouched_schedule_draft(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="expired-draft-thread",
            requested_thread_id=None,
            provider_session_id="expired-draft-session",
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
    draft = ScheduleRepository(storage_context.store).create_draft(
        conversation_id=storage_context.conversation.id,
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        action="create",
        payload={"prompt_text": "sensitive scheduled prompt"},
        occurrences=(),
        component_nonce="expired-draft",
        expires_at=1,
    )
    now = utc_now_ms()

    run_retention(
        storage_context.store,
        AppPaths(
            storage_context.store.path.parent,
            storage_context.store.path.parent / "logs",
        ),
        RetentionConfig(),
        now_ms=now,
    )

    row = storage_context.store.query_one(
        "SELECT state, payload_json FROM schedule_drafts WHERE id = ?",
        (draft.id,),
    )
    assert row is not None
    assert row["state"] == "expired"
    assert row["payload_json"] == "{}"


def test_retention_compacts_then_expires_tool_output_detail(
    storage_context: StorageContext,
) -> None:
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="tool retention"),
        input_message_id="tool-retention",
    )
    storage_context.repository.request_cancel(
        turn.id,
        origin=InterruptOrigin.USER,
    )
    now = utc_now_ms()
    old = now - 31 * 24 * 60 * 60 * 1000
    with storage_context.store.transaction() as connection:
        cursor = connection.execute(
            """
            INSERT INTO events(
                event_id, turn_id, project_id, conversation_id,
                local_event_index, kind, schema_version, payload_json,
                occurred_at, recorded_at
            ) VALUES (?, ?, ?, ?, 1, 'command.output.delta', 1, ?, ?, ?)
            """,
            (
                new_id(),
                turn.id,
                storage_context.project.id,
                storage_context.conversation.id,
                canonical_json({"text": "sensitive tool output"}),
                old,
                old,
            ),
        )
        assert cursor.lastrowid is not None
        tool_event_sequence = int(cursor.lastrowid)
        connection.execute(
            """
            INSERT INTO tool_projections(
                id, turn_id, provider_item_id, kind, label, state,
                summary_json, last_event_sequence
            ) VALUES (?, ?, 'tool-retention', 'command', 'tool',
                      'completed', ?, ?)
            """,
            (
                new_id(),
                turn.id,
                canonical_json({"text": "sensitive tool output"}),
                tool_event_sequence,
            ),
        )
        connection.execute(
            "UPDATE turns SET ended_at = ? WHERE id = ?",
            (old, turn.id),
        )

    paths = AppPaths(
        storage_context.store.path.parent,
        storage_context.store.path.parent / "logs",
    )
    run_retention(
        storage_context.store,
        paths,
        RetentionConfig(),
        now_ms=now,
    )

    event = storage_context.store.query_one(
        "SELECT payload_json FROM events WHERE sequence = ?",
        (tool_event_sequence,),
    )
    projection = storage_context.store.query_one(
        "SELECT summary_json FROM tool_projections WHERE turn_id = ?",
        (turn.id,),
    )
    assert event is not None and "sensitive tool output" not in event["payload_json"]
    assert projection is not None
    assert "sensitive tool output" not in projection["summary_json"]

    run_retention(
        storage_context.store,
        paths,
        RetentionConfig(),
        now_ms=now + 61 * 24 * 60 * 60 * 1000,
    )

    assert storage_context.store.query_one(
        "SELECT 1 FROM tool_projections WHERE turn_id = ?",
        (turn.id,),
    ) is None
    assert storage_context.store.query_one(
        "SELECT 1 FROM events WHERE sequence = ?",
        (tool_event_sequence,),
    ) is None


def test_retention_tombstones_then_deletes_terminal_turn_and_linked_fire(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="turn-retention-thread",
            requested_thread_id=None,
            provider_session_id="turn-retention-session",
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
    schedules = ScheduleRepository(storage_context.store)
    schedule = schedules.create(
        conversation_id=storage_context.conversation.id,
        name="turn-retention",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="retained prompt",
        next_due_at=1,
        created_by_user_id=400,
    )
    materialized = schedules.materialize(
        schedule_id=schedule.id,
        occurrence_key="1",
        trigger_kind="timer",
        scheduled_for=1,
        scheduled_local="1970-01-01T00:00:00+00:00",
        next_due_at=60_001,
        expected_version=schedule.version,
    )
    assert materialized.turn_id is not None
    storage_context.repository.request_cancel(
        materialized.turn_id,
        origin=InterruptOrigin.USER,
    )
    incident_id = storage_context.repository.record_incident(
        severity="warning",
        code="retained-turn-history",
        summary="retain incident scope",
        conversation_id=storage_context.conversation.id,
        turn_id=materialized.turn_id,
    )
    recoverable = schedules.materialize(
        schedule_id=schedule.id,
        occurrence_key="60001",
        trigger_kind="timer",
        scheduled_for=60_001,
        scheduled_local="1970-01-01T00:01:00+00:00",
        next_due_at=120_001,
        expected_version=schedule.version + 1,
    )
    assert recoverable.turn_id is not None
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE turns SET ended_at = 1 WHERE id = ?",
            (materialized.turn_id,),
        )
        connection.execute(
            "UPDATE events SET recorded_at = 1 WHERE turn_id = ?",
            (materialized.turn_id,),
        )
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = CASE
                    WHEN state = 'superseded' THEN state
                    ELSE 'sent'
                END,
                updated_at = 1
            WHERE json_extract(payload_json, '$.turn_id') = ?
            """,
            (materialized.turn_id,),
        )
    paths = AppPaths(
        storage_context.store.path.parent,
        storage_context.store.path.parent / "logs",
    )

    first = run_retention(
        storage_context.store,
        paths,
        RetentionConfig(events_days=90),
        now_ms=91 * 24 * 60 * 60 * 1000,
    )

    retained_turn = storage_context.store.query_one(
        """
        SELECT retained_at, provider_turn_id, effective_skill_names_json
        FROM turns WHERE id = ?
        """,
        (materialized.turn_id,),
    )
    retained_fire = storage_context.store.query_one(
        "SELECT retained_at, scheduled_local FROM schedule_fires WHERE turn_id = ?",
        (materialized.turn_id,),
    )
    assert first.terminal_turn_tombstones == 1
    assert retained_turn is not None and retained_turn["retained_at"] is not None
    assert retained_turn["provider_turn_id"] is None
    assert retained_turn["effective_skill_names_json"] is None
    assert retained_fire is not None and retained_fire["retained_at"] is not None
    assert retained_fire["scheduled_local"] == "[retained]"
    assert storage_context.store.query_one(
        "SELECT 1 FROM message_projections WHERE turn_id = ?",
        (materialized.turn_id,),
    )
    assert storage_context.store.query_one(
        "SELECT 1 FROM turns WHERE id = ? AND state = 'queued'",
        (recoverable.turn_id,),
    )

    second = run_retention(
        storage_context.store,
        paths,
        RetentionConfig(events_days=90),
        now_ms=181 * 24 * 60 * 60 * 1000,
    )

    assert second.terminal_turns == 1
    assert second.schedule_fires >= 1
    assert storage_context.store.query_one(
        "SELECT 1 FROM turns WHERE id = ?",
        (materialized.turn_id,),
    ) is None
    assert storage_context.store.query_one(
        "SELECT 1 FROM schedule_fires WHERE turn_id = ?",
        (materialized.turn_id,),
    ) is None
    incident = storage_context.store.query_one(
        "SELECT turn_id, conversation_id FROM incidents WHERE id = ?",
        (incident_id,),
    )
    assert incident is not None
    assert incident["turn_id"] is None
    assert incident["conversation_id"] == storage_context.conversation.id
    assert storage_context.store.query_one(
        "SELECT 1 FROM turns WHERE id = ? AND state = 'queued'",
        (recoverable.turn_id,),
    )
    assert storage_context.store.foreign_key_check() == ()
