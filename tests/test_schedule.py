from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from conftest import StorageContext

from codexd.application.schedule_coordinator import (
    ScheduleCoordinator,
    next_occurrence,
    parse_schedule_spec,
)
from codexd.domain.conversations import (
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
)
from codexd.domain.ids import sha256_text, utc_now_ms
from codexd.domain.schedules import (
    MisfirePolicy,
    ScheduleAuditContext,
    ScheduleKind,
    ScheduleModalSubmission,
    ScheduleState,
)
from codexd.errors import (
    ConfigurationError,
    ConflictError,
    InvariantError,
    SecurityError,
)
from codexd.security.signing import ComponentSigner
from codexd.storage import schedules as schedules_module
from codexd.storage.repository import Repository
from codexd.storage.schedules import ScheduleRepository


def _milliseconds(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _activate_schedule_target(
    storage_context: StorageContext,
    suffix: str,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id=f"{suffix}-thread",
            requested_thread_id=None,
            provider_session_id=f"{suffix}-session",
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


def test_spring_forward_skips_nonexistent_local_time() -> None:
    spec = parse_schedule_spec(
        "cron", "30 2 * * *", "America/New_York", "all"
    )
    after = _milliseconds(datetime(2025, 3, 8, 8, tzinfo=UTC))

    occurrence = next_occurrence(spec, after)

    assert occurrence is not None
    assert occurrence.local_display.startswith("2025-03-10T02:30:00")


def test_fall_back_materializes_both_folds() -> None:
    spec = parse_schedule_spec(
        "cron", "30 1 * * *", "America/New_York", "all"
    )
    after = _milliseconds(datetime(2025, 11, 2, 4, tzinfo=UTC))

    first = next_occurrence(spec, after)
    assert first is not None
    second = next_occurrence(spec, first.utc_ms)

    assert second is not None
    assert first.local_display.startswith("2025-11-02T01:30:00-04:00")
    assert second.local_display.startswith("2025-11-02T01:30:00-05:00")
    assert second.utc_ms - first.utc_ms == 3_600_000


@pytest.mark.asyncio
async def test_run_now_is_idempotent_by_interaction(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="schedule-thread",
            requested_thread_id=None,
            provider_session_id="schedule-session",
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
    wakeups: list[str] = []

    async def wake(conversation_id: str) -> None:
        wakeups.append(conversation_id)

    coordinator = ScheduleCoordinator(
        repository=ScheduleRepository(storage_context.store),
        wake_conversation=wake,
    )
    schedule = await coordinator.create(
        conversation_id=storage_context.conversation.id,
        name="manual",
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="inspect repository",
        owner_user_id=400,
        now_ms=1,
    )
    storage_context.repository.update_conversation_preferences(
        storage_context.conversation.id,
        reasoning_summary_override="detailed",
    )

    first = await coordinator.run_now(schedule.id, interaction_id="interaction-1")
    second = await coordinator.run_now(schedule.id, interaction_id="interaction-1")

    assert first is not None
    assert second == first
    assert (
        storage_context.repository.get_turn(first).effective_reasoning_summary
        == "detailed"
    )
    assert storage_context.repository.get_turn(first).input_summary == "inspect repository"
    assert wakeups == [
        storage_context.conversation.id,
        storage_context.conversation.id,
    ]


@pytest.mark.asyncio
async def test_schedule_mutation_intent_recovers_from_correlated_audit(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "intent-recovery")

    async def wake(_conversation_id: str) -> None:
        return None

    schedule_repository = ScheduleRepository(storage_context.store)
    coordinator = ScheduleCoordinator(
        repository=schedule_repository,
        wake_conversation=wake,
    )
    schedule = await coordinator.create(
        conversation_id=storage_context.conversation.id,
        name="recoverable",
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="inspect repository",
        owner_user_id=400,
        now_ms=1,
    )
    interaction_id = "schedule-pause-interaction"
    storage_context.repository.accept_command_intent(
        interaction_id=interaction_id,
        command_name="schedule pause",
        request={"schedule_id": schedule.id, "version": schedule.version},
        boot_id="old-boot",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
    )

    paused = await coordinator.pause(
        schedule.id,
        expected_version=schedule.version,
        audit=ScheduleAuditContext.discord_user(
            user_id=400,
            interaction_id=interaction_id,
        ),
    )

    in_flight = storage_context.repository.get_command_intent(interaction_id)
    assert paused.state is ScheduleState.PAUSED
    assert in_flight.state == "effect_in_flight"
    assert in_flight.effect_kind == "schedule_mutation"
    assert in_flight.effect_correlation_id == schedule.id

    completed_after_error = storage_context.repository.complete_command_intent(
        interaction_id,
        state="failed",
        result={"code": "internal_error"},
        actor_user_id=400,
    )
    assert completed_after_error.state == "succeeded"

    recovery_interaction_id = "schedule-resume-interaction"
    storage_context.repository.accept_command_intent(
        interaction_id=recovery_interaction_id,
        command_name="schedule resume",
        request={"schedule_id": paused.id, "version": paused.version},
        boot_id="old-boot",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
    )
    resumed = await coordinator.resume(
        paused.id,
        expected_version=paused.version,
        audit=ScheduleAuditContext.discord_user(
            user_id=400,
            interaction_id=recovery_interaction_id,
        ),
    )
    assert resumed.state is ScheduleState.ACTIVE

    recovered = storage_context.repository.recover_startup(
        current_boot_id="new-boot"
    )
    completed = storage_context.repository.get_command_intent(
        recovery_interaction_id
    )

    assert recovered["reconciled_schedule_intents"] == 1
    assert recovered["unknown_intents"] == 0
    assert completed.state == "succeeded"
    assert json.loads(completed.result_json or "{}")["code"] == "ok"


@pytest.mark.asyncio
async def test_schedule_update_and_short_id_resolution(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="editable-thread",
            requested_thread_id=None,
            provider_session_id="editable-session",
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

    async def wake(_conversation_id: str) -> None:
        return None

    repository = ScheduleRepository(storage_context.store)
    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    schedule = await coordinator.create(
        conversation_id=storage_context.conversation.id,
        name="editable",
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="before",
        owner_user_id=400,
        now_ms=1,
    )

    resolved = repository.resolve(
        storage_context.conversation.id, schedule.id[:8]
    )
    updated = await coordinator.update(
        resolved.id,
        expected_version=resolved.version,
        expression="30 * * * *",
        prompt_text="after",
        now_ms=1,
    )

    assert updated.expression == "30 * * * *"
    assert updated.prompt_text == "after"
    assert updated.version == schedule.version + 1


@pytest.mark.asyncio
async def test_schedule_draft_manager_need_not_be_conversation_creator(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="draft-thread",
            requested_thread_id=None,
            provider_session_id="draft-session",
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

    async def wake(_conversation_id: str) -> None:
        return None

    repository = ScheduleRepository(storage_context.store)
    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    draft = await coordinator.create_draft(
        conversation_id=storage_context.conversation.id,
        name="nightly review",
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="Review the repository",
        owner_user_id=401,
        guild_id=100,
        channel_id=300,
        component_nonce="draft-nonce",
    )

    assert len(json.loads(draft.occurrences_json)) == 3
    assert (draft.discord_guild_id, draft.discord_channel_id) == (100, 300)
    assert repository.list_for_conversation(storage_context.conversation.id) == ()
    for guild_id, channel_id, owner_user_id in (
        (101, 300, 401),
        (100, 301, 401),
        (100, 300, 402),
    ):
        with pytest.raises(SecurityError):
            await coordinator.confirm_draft(
                draft_id=draft.id,
                component_nonce="draft-nonce",
                owner_user_id=owner_user_id,
                guild_id=guild_id,
                channel_id=channel_id,
            )

    schedule = await coordinator.confirm_draft(
        draft_id=draft.id,
        component_nonce="draft-nonce",
        owner_user_id=401,
        guild_id=100,
        channel_id=300,
        audit=ScheduleAuditContext.discord_user(
            user_id=401,
            interaction_id="confirm-draft-interaction",
        ),
    )
    repeated = await coordinator.confirm_draft(
        draft_id=draft.id,
        component_nonce="draft-nonce",
        owner_user_id=401,
        guild_id=100,
        channel_id=300,
        audit=ScheduleAuditContext.discord_user(
            user_id=401,
            interaction_id="confirm-draft-interaction",
        ),
    )

    assert repeated.id == schedule.id
    row = storage_context.store.connection.execute(
        "SELECT state, payload_json FROM schedule_drafts WHERE id = ?",
        (draft.id,),
    ).fetchone()
    assert row is not None
    assert row["state"] == "confirmed"
    assert json.loads(row["payload_json"]) == {
        "prompt_hash": schedule.prompt_hash,
        "schedule_id": schedule.id,
    }
    audit_rows = storage_context.store.query_all(
        """
        SELECT action, actor_kind, actor_id_hash, correlation_id
        FROM audit_log
        WHERE schedule_id = ?
          AND action IN ('schedule.confirm', 'schedule.create')
        ORDER BY action
        """,
        (schedule.id,),
    )
    assert [(row["action"], row["correlation_id"]) for row in audit_rows] == [
        ("schedule.confirm", "confirm-draft-interaction"),
        ("schedule.create", "confirm-draft-interaction"),
    ]
    assert all(row["actor_kind"] == "discord_user" for row in audit_rows)
    assert all(
        row["actor_id_hash"] == sha256_text("discord_user:401")
        for row in audit_rows
    )


@pytest.mark.asyncio
async def test_schedule_draft_uses_fixed_full_access(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="permission-thread",
            requested_thread_id=None,
            provider_session_id="permission-session",
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

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=ScheduleRepository(storage_context.store),
        wake_conversation=wake,
    )
    draft = await coordinator.create_draft(
        conversation_id=storage_context.conversation.id,
        name="permission-sensitive",
        kind="cron",
        expression="0 * * * *",
        timezone="UTC",
        misfire_policy="skip",
        prompt_text="Inspect system state",
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        component_nonce="permission-nonce",
    )

    with (
        pytest.raises(sqlite3.IntegrityError, match="fixed to full_access"),
        storage_context.store.transaction() as connection,
    ):
        connection.execute(
            "UPDATE conversations SET sandbox_profile = 'read_only' WHERE id = ?",
            (storage_context.conversation.id,),
        )

    schedule = await coordinator.confirm_draft(
        draft_id=draft.id,
        component_nonce="permission-nonce",
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
    )
    assert schedule.state is ScheduleState.ACTIVE


def test_schedule_draft_component_signature() -> None:
    signer = ComponentSigner(b"x" * 32)
    component_id = signer.schedule_draft_id(
        draft_id="draft-id",
        action="confirm",
        nonce="nonce",
    )

    action = signer.verify_schedule_draft_id(component_id)

    assert (action.draft_id, action.action, action.nonce) == (
        "draft-id",
        "confirm",
        "nonce",
    )
    with pytest.raises(SecurityError):
        signer.verify_schedule_draft_id(component_id.replace("confirm", "cancel"))


def test_modal_intent_signature_and_restart_safe_consumption(
    storage_context: StorageContext,
) -> None:
    signer = ComponentSigner(b"m" * 32)
    expires_at = utc_now_ms() + 60_000
    modal = storage_context.repository.create_modal_intent(
        kind="schedule_create",
        conversation_id=storage_context.conversation.id,
        guild_id=100,
        channel_id=300,
        owner_user_id=400,
        nonce="modal-nonce",
        expires_at=expires_at,
    )
    custom_id = signer.modal_id(
        intent_id=modal.id,
        kind=modal.kind,
        expires_at=modal.expires_at,
        nonce="modal-nonce",
    )

    action = ComponentSigner(b"m" * 32).verify_modal_id(custom_id)
    restarted_repository = Repository(storage_context.store)
    consumed = restarted_repository.consume_modal_intent(
        intent_id=action.intent_id,
        kind=action.kind,
        expires_at=action.expires_at,
        nonce=action.nonce,
        interaction_id="submit-1",
        guild_id=100,
        channel_id=300,
        user_id=400,
    )

    assert consumed.state == "consumed"
    assert consumed.consumed_interaction_id == "submit-1"
    with pytest.raises(ConflictError, match="already consumed"):
        restarted_repository.consume_modal_intent(
            intent_id=action.intent_id,
            kind=action.kind,
            expires_at=action.expires_at,
            nonce=action.nonce,
            interaction_id="submit-2",
            guild_id=100,
            channel_id=300,
            user_id=400,
        )
    with pytest.raises(SecurityError):
        signer.verify_modal_id(custom_id.replace(":sc:", ":st:"))


def test_modal_intent_rejects_changed_discord_scope(
    storage_context: StorageContext,
) -> None:
    modal = storage_context.repository.create_modal_intent(
        kind="schedule_create",
        conversation_id=storage_context.conversation.id,
        guild_id=100,
        channel_id=300,
        owner_user_id=400,
        nonce="scope-nonce",
        expires_at=utc_now_ms() + 60_000,
    )

    with pytest.raises(SecurityError, match="Discord scope changed"):
        storage_context.repository.consume_modal_intent(
            intent_id=modal.id,
            kind=modal.kind,
            expires_at=modal.expires_at,
            nonce="scope-nonce",
            interaction_id="submit-scope",
            guild_id=100,
            channel_id=301,
            user_id=400,
        )
    for guild_id, channel_id, user_id in (
        (101, 300, 400),
        (100, 300, 401),
    ):
        with pytest.raises(SecurityError, match="Discord scope changed"):
            storage_context.repository.consume_modal_intent(
                intent_id=modal.id,
                kind=modal.kind,
                expires_at=modal.expires_at,
                nonce="scope-nonce",
                interaction_id=f"submit-{guild_id}-{user_id}",
                guild_id=guild_id,
                channel_id=channel_id,
                user_id=user_id,
            )


def test_modal_intent_expires_without_consuming(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    modal = storage_context.repository.create_modal_intent(
        kind="schedule_create",
        conversation_id=storage_context.conversation.id,
        guild_id=100,
        channel_id=300,
        owner_user_id=400,
        nonce="expired-nonce",
        expires_at=utc_now_ms() + 60_000,
    )
    monkeypatch.setattr(
        "codexd.storage.repository.utc_now_ms",
        lambda: modal.expires_at,
    )

    with pytest.raises(ConflictError, match="expired"):
        storage_context.repository.consume_modal_intent(
            intent_id=modal.id,
            kind=modal.kind,
            expires_at=modal.expires_at,
            nonce="expired-nonce",
            interaction_id="submit-expired",
            guild_id=100,
            channel_id=300,
            user_id=400,
        )

    row = storage_context.store.query_one(
        "SELECT state, consumed_interaction_id FROM modal_intents WHERE id = ?",
        (modal.id,),
    )
    assert row is not None
    assert (row["state"], row["consumed_interaction_id"]) == ("expired", None)


@pytest.mark.asyncio
async def test_skip_policy_runs_an_on_time_occurrence(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="skip-on-time-thread",
            requested_thread_id=None,
            provider_session_id="skip-on-time-session",
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
    wakeups: list[str] = []

    async def wake(conversation_id: str) -> None:
        wakeups.append(conversation_id)

    now = _milliseconds(datetime(2025, 1, 1, 0, 0, 2, tzinfo=UTC))
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name="skip-on-time",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.SKIP,
        prompt_text="run on time",
        next_due_at=now - 2_000,
        created_by_user_id=400,
    )
    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
        poll_seconds=1,
    )

    assert await coordinator.tick(now_ms=now) == 1
    fire = storage_context.store.query_one(
        "SELECT state, trigger_kind, turn_id FROM schedule_fires WHERE schedule_id = ?",
        (schedule.id,),
    )
    assert fire is not None
    assert (fire["state"], fire["trigger_kind"]) == ("materialized", "timer")
    assert fire["turn_id"] is not None
    assert wakeups == [storage_context.conversation.id]


@pytest.mark.asyncio
async def test_scheduler_blocks_invalid_rule_without_stopping_other_rules(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="isolated-schedule-thread",
            requested_thread_id=None,
            provider_session_id="isolated-schedule-session",
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

    async def wake(_conversation_id: str) -> None:
        return None

    now = _milliseconds(datetime(2025, 1, 1, 0, 0, 2, tzinfo=UTC))
    repository = ScheduleRepository(storage_context.store)
    invalid = repository.create(
        conversation_id=storage_context.conversation.id,
        name="invalid-timezone",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="invalid",
        next_due_at=now,
        created_by_user_id=400,
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE schedules SET timezone = 'Invalid/Timezone' WHERE id = ?",
            (invalid.id,),
        )
    valid = repository.create(
        conversation_id=storage_context.conversation.id,
        name="valid-rule",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="valid",
        next_due_at=now,
        created_by_user_id=400,
    )
    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )

    assert await coordinator.tick(now_ms=now) == 1
    assert repository.get(invalid.id).state is ScheduleState.BLOCKED
    assert repository.get(valid.id).state is ScheduleState.ACTIVE
    incident = storage_context.store.query_one(
        "SELECT code FROM incidents WHERE schedule_id = ?",
        (invalid.id,),
    )
    assert incident is not None
    assert incident["code"] == "schedule_blocked"


@pytest.mark.asyncio
async def test_all_misfire_advances_durable_cursor_one_occurrence_at_a_time(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate_schedule_target(storage_context, "all-misfire")
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name="all-misfire",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.ALL,
        prompt_text="catch up",
        next_due_at=60_000,
        created_by_user_id=400,
    )

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    original_materialize = repository.materialize
    committed = 0

    def crash_after_first_commit(**kwargs: object) -> object:
        nonlocal committed
        result = original_materialize(**kwargs)
        committed += 1
        if committed == 1:
            raise RuntimeError("simulated crash after materialization commit")
        return result

    monkeypatch.setattr(repository, "materialize", crash_after_first_commit)
    with pytest.raises(RuntimeError, match="simulated crash"):
        await coordinator.tick(now_ms=300_000)

    assert repository.get(schedule.id).next_due_at == 120_000
    assert storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM schedule_fires WHERE schedule_id = ?",
        (schedule.id,),
    )["count"] == 1

    monkeypatch.setattr(repository, "materialize", original_materialize)
    assert await coordinator.tick(now_ms=300_000) == 4
    rows = storage_context.store.query_all(
        """
        SELECT t.enqueue_sequence
        FROM schedule_fires sf
        JOIN turns t ON t.id = sf.turn_id
        WHERE sf.schedule_id = ?
        ORDER BY sf.scheduled_for
        """,
        (schedule.id,),
    )
    assert len(rows) == 5
    assert all(row["enqueue_sequence"] is not None for row in rows)
    assert len({int(row["enqueue_sequence"]) for row in rows}) == 5


@pytest.mark.asyncio
async def test_malformed_cron_blocks_only_its_rule(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "malformed-cron")
    repository = ScheduleRepository(storage_context.store)
    invalid = repository.create(
        conversation_id=storage_context.conversation.id,
        name="invalid-cron",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="invalid",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE schedules SET expression = 'x * * * *' WHERE id = ?",
            (invalid.id,),
        )
    valid = repository.create(
        conversation_id=storage_context.conversation.id,
        name="valid-cron",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="valid",
        next_due_at=120_000,
        created_by_user_id=400,
    )

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )

    assert await coordinator.tick(now_ms=120_000) == 1
    assert repository.get(invalid.id).state is ScheduleState.BLOCKED
    assert repository.get(valid.id).state is ScheduleState.ACTIVE


@pytest.mark.asyncio
async def test_startup_reconciliation_validates_future_active_rules(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "startup-validation")
    repository = ScheduleRepository(storage_context.store)
    invalid = repository.create(
        conversation_id=storage_context.conversation.id,
        name="future-invalid",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="invalid",
        next_due_at=9_999_999_999_999,
        created_by_user_id=400,
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE schedules SET timezone = 'Invalid/Timezone' WHERE id = ?",
            (invalid.id,),
        )

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )

    assert await coordinator.reconcile_startup() == 1
    assert repository.get(invalid.id).state is ScheduleState.BLOCKED
    assert storage_context.store.query_one(
        "SELECT 1 FROM incidents WHERE schedule_id = ? AND code = 'schedule_blocked'",
        (invalid.id,),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("assignment", "value"),
    (
        ("prompt_text", ""),
        ("prompt_hash", "wrong"),
        ("next_due_at", None),
    ),
)
async def test_startup_atomically_blocks_malformed_persisted_schedule(
    storage_context: StorageContext,
    assignment: str,
    value: object,
) -> None:
    _activate_schedule_target(storage_context, f"malformed-{assignment}")
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name=f"malformed-{assignment}",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="valid prompt",
        next_due_at=9_999_999_999_999,
        created_by_user_id=400,
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            f"UPDATE schedules SET {assignment} = ? WHERE id = ?",
            (value, schedule.id),
        )

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )

    assert await coordinator.reconcile_startup() == 1
    blocked = repository.get(schedule.id)
    assert blocked.state is ScheduleState.BLOCKED
    assert blocked.next_due_at is None
    assert storage_context.store.query_one(
        "SELECT 1 FROM incidents WHERE schedule_id = ? AND code = 'schedule_blocked'",
        (schedule.id,),
    )


@pytest.mark.asyncio
async def test_schedule_create_rejects_noncanonical_definition(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "invalid-create")

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=ScheduleRepository(storage_context.store),
        wake_conversation=wake,
    )

    with pytest.raises(ConfigurationError, match="name"):
        await coordinator.create(
            conversation_id=storage_context.conversation.id,
            name=" ",
            kind="cron",
            expression="* * * * *",
            timezone="UTC",
            misfire_policy="latest",
            prompt_text="valid",
            owner_user_id=400,
        )
    with pytest.raises(ConfigurationError, match="prompt"):
        await coordinator.create(
            conversation_id=storage_context.conversation.id,
            name="valid",
            kind="cron",
            expression="* * * * *",
            timezone="UTC",
            misfire_policy="latest",
            prompt_text=" ",
            owner_user_id=400,
        )


def test_schedule_block_rejects_stale_scan_snapshot(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "stale-block")
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name="stale-block",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="before repair",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    repaired = repository.update(
        schedule.id,
        expected_version=schedule.version,
        kind=schedule.kind,
        expression="*/2 * * * *",
        timezone=schedule.timezone,
        misfire_policy=schedule.misfire_policy,
        prompt_text="after repair",
        next_due_at=120_000,
    )

    with pytest.raises(ConflictError, match="modified concurrently"):
        repository.block(
            schedule.id,
            expected_version=schedule.version,
            reason="stale-invalid-scan",
        )

    current = repository.get(schedule.id)
    assert current.state is ScheduleState.ACTIVE
    assert current.version == repaired.version
    assert current.prompt_text == "after repair"


def test_once_schedule_accepts_unambiguous_timezone_qualified_local_time() -> None:
    spec = parse_schedule_spec(
        "once",
        "2026-08-07T09:00:00",
        "Asia/Shanghai",
        "latest",
    )

    assert spec.expression == "2026-08-07T01:00:00Z"


@pytest.mark.parametrize(
    ("expression", "message"),
    (
        ("2025-03-09T02:30:00", "nonexistent"),
        ("2025-11-02T01:30:00", "ambiguous"),
    ),
)
def test_once_schedule_rejects_invalid_dst_local_time(
    expression: str,
    message: str,
) -> None:
    with pytest.raises(ConfigurationError, match=message):
        parse_schedule_spec(
            "once",
            expression,
            "America/New_York",
            "latest",
        )


def test_target_unavailable_materialization_blocks_atomically(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "target-unavailable")
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name="target-unavailable",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="run",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    storage_context.repository.block_conversation(
        storage_context.conversation.id,
        reason="test_target_unavailable",
    )

    result = repository.materialize(
        schedule_id=schedule.id,
        occurrence_key="60000",
        trigger_kind="timer",
        scheduled_for=60_000,
        scheduled_local="1970-01-01T00:01:00+00:00",
        next_due_at=120_000,
        expected_version=schedule.version,
    )

    blocked = repository.get(schedule.id)
    assert result.fire_state == "blocked"
    assert blocked.state is ScheduleState.BLOCKED
    assert blocked.next_due_at is None
    assert storage_context.store.query_one(
        "SELECT 1 FROM incidents WHERE schedule_id = ? AND code = 'schedule_blocked'",
        (schedule.id,),
    )
    assert storage_context.store.query_one(
        """
        SELECT 1 FROM discord_outbox
        WHERE json_extract(payload_json, '$.kind') = 'schedule_blocked'
          AND json_extract(payload_json, '$.schedule_id') = ?
        """,
        (schedule.id,),
    )


@pytest.mark.asyncio
async def test_schedule_crud_audit_preserves_actor_and_is_idempotent(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "audit-crud")
    repository = ScheduleRepository(storage_context.store)

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )

    def actor(correlation_id: str) -> ScheduleAuditContext:
        return ScheduleAuditContext.discord_user(
            user_id=400,
            interaction_id=correlation_id,
        )

    schedule = await coordinator.create(
        conversation_id=storage_context.conversation.id,
        name="audited",
        kind="cron",
        expression="* * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="audit me",
        owner_user_id=400,
        now_ms=1,
        audit=actor("audit-create"),
    )
    schedule = await coordinator.pause(
        schedule.id,
        expected_version=schedule.version,
        audit=actor("audit-pause"),
    )
    schedule = await coordinator.resume(
        schedule.id,
        expected_version=schedule.version,
        now_ms=1,
        audit=actor("audit-resume"),
    )
    schedule = await coordinator.update(
        schedule.id,
        expected_version=schedule.version,
        expression="*/2 * * * *",
        prompt_text="updated audit",
        now_ms=1,
        audit=actor("audit-update"),
    )
    first_turn = await coordinator.run_now(
        schedule.id,
        interaction_id="audit-run-now",
        audit=actor("audit-run-now"),
    )
    repeated_turn = await coordinator.run_now(
        schedule.id,
        interaction_id="audit-run-now",
        audit=actor("audit-run-now"),
    )
    assert repeated_turn == first_turn
    await coordinator.delete(
        schedule.id,
        expected_version=schedule.version,
        audit=actor("audit-delete"),
    )

    rows = storage_context.store.query_all(
        """
        SELECT action, actor_kind, actor_id_hash, correlation_id
        FROM audit_log
        WHERE schedule_id = ? AND correlation_id LIKE 'audit-%'
        ORDER BY correlation_id, action
        """,
        (schedule.id,),
    )
    assert {
        (row["action"], row["correlation_id"])
        for row in rows
    } == {
        ("schedule.create", "audit-create"),
        ("schedule.delete", "audit-delete"),
        ("schedule.pause", "audit-pause"),
        ("schedule.resume", "audit-resume"),
        ("schedule.run_now", "audit-run-now"),
        ("schedule.update", "audit-update"),
    }
    assert sum(
        row["action"] == "schedule.run_now"
        and row["correlation_id"] == "audit-run-now"
        for row in rows
    ) == 1
    assert all(row["actor_kind"] == "discord_user" for row in rows)
    assert all(
        row["actor_id_hash"] == sha256_text("discord_user:400")
        for row in rows
    )


@pytest.mark.asyncio
async def test_schedule_skip_misfire_and_block_use_system_audit(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "audit-system")
    repository = ScheduleRepository(storage_context.store)
    catch_up = repository.create(
        conversation_id=storage_context.conversation.id,
        name="audit-misfire",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="catch up",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    invalid = repository.create(
        conversation_id=storage_context.conversation.id,
        name="audit-block",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="block",
        next_due_at=9_999_999_999_999,
        created_by_user_id=400,
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE schedules SET timezone = 'Invalid/Timezone' WHERE id = ?",
            (invalid.id,),
        )

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
        poll_seconds=1,
    )

    assert await coordinator.tick(now_ms=130_000) == 1
    assert await coordinator.reconcile_startup() == 1

    rows = storage_context.store.query_all(
        """
        SELECT action, actor_kind, actor_id_hash, correlation_id
        FROM audit_log
        WHERE schedule_id IN (?, ?)
          AND action IN ('schedule.skip', 'schedule.misfire', 'schedule.block')
        ORDER BY action
        """,
        (catch_up.id, invalid.id),
    )
    assert [row["action"] for row in rows] == [
        "schedule.block",
        "schedule.misfire",
        "schedule.skip",
    ]
    assert all(row["actor_kind"] == "system" for row in rows)
    assert all(row["actor_id_hash"] is None for row in rows)
    assert all(row["correlation_id"] for row in rows)


@pytest.mark.asyncio
async def test_scheduler_cannot_start_before_startup_restore(
    storage_context: StorageContext,
) -> None:
    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=ScheduleRepository(storage_context.store),
        wake_conversation=wake,
    )

    with pytest.raises(InvariantError, match="restored before start"):
        coordinator.start()

    assert await coordinator.restore() == (0, 0)
    assert await coordinator.restore() == (0, 0)
    coordinator.start()
    await coordinator.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", (MisfirePolicy.SKIP, MisfirePolicy.LATEST))
async def test_skip_like_misfire_cursor_survives_crash_after_each_occurrence(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
    policy: MisfirePolicy,
) -> None:
    _activate_schedule_target(storage_context, f"{policy.value}-cursor")
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name=f"{policy.value}-cursor",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=policy,
        prompt_text="catch up safely",
        next_due_at=60_000,
        created_by_user_id=400,
    )

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
        poll_seconds=1,
    )
    original_record_skipped = repository.record_skipped
    committed = 0

    def crash_after_first_skip(**kwargs: object) -> bool:
        nonlocal committed
        result = original_record_skipped(**kwargs)
        committed += 1
        if committed == 1:
            raise RuntimeError("simulated crash after skipped occurrence")
        return result

    monkeypatch.setattr(repository, "record_skipped", crash_after_first_skip)
    with pytest.raises(RuntimeError, match="simulated crash"):
        await coordinator.tick(now_ms=300_000)

    assert repository.get(schedule.id).next_due_at == 120_000
    assert storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM schedule_fires WHERE schedule_id = ?",
        (schedule.id,),
    )["count"] == 1

    monkeypatch.setattr(repository, "record_skipped", original_record_skipped)
    assert await coordinator.tick(now_ms=300_000) == 1
    rows = storage_context.store.query_all(
        """
        SELECT state, scheduled_for
        FROM schedule_fires
        WHERE schedule_id = ?
        ORDER BY scheduled_for
        """,
        (schedule.id,),
    )
    assert [(row["state"], row["scheduled_for"]) for row in rows] == [
        ("skipped", 60_000),
        ("skipped", 120_000),
        ("skipped", 180_000),
        ("skipped", 240_000),
        ("materialized", 300_000),
    ]
    assert repository.get(schedule.id).next_due_at == 360_000


@pytest.mark.asyncio
async def test_schedule_state_machine_rejects_illegal_and_terminal_mutations(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "state-machine")

    async def wake(_conversation_id: str) -> None:
        return None

    repository = ScheduleRepository(storage_context.store)
    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    schedule = await coordinator.create(
        conversation_id=storage_context.conversation.id,
        name="state-machine",
        kind="cron",
        expression="* * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="exercise transitions",
        owner_user_id=400,
        now_ms=1,
    )
    paused = await coordinator.pause(
        schedule.id,
        expected_version=schedule.version,
    )
    with pytest.raises(ConflictError, match="cannot pause"):
        await coordinator.pause(paused.id, expected_version=paused.version)
    resumed = await coordinator.resume(
        paused.id,
        expected_version=paused.version,
        now_ms=1,
    )
    with pytest.raises(ConflictError, match="cannot resume"):
        await coordinator.resume(
            resumed.id,
            expected_version=resumed.version,
            now_ms=1,
        )
    repository.block(
        resumed.id,
        expected_version=resumed.version,
        reason="test block",
    )
    blocked = repository.get(resumed.id)
    with pytest.raises(ConflictError, match="cannot pause"):
        await coordinator.pause(blocked.id, expected_version=blocked.version)
    updated_blocked = await coordinator.update(
        blocked.id,
        expected_version=blocked.version,
        expression="*/2 * * * *",
        now_ms=1,
    )
    assert updated_blocked.state is ScheduleState.BLOCKED
    assert updated_blocked.next_due_at is None
    await coordinator.resume(
        updated_blocked.id,
        expected_version=updated_blocked.version,
        now_ms=1,
    )

    once = repository.create(
        conversation_id=storage_context.conversation.id,
        name="terminal-once",
        kind=ScheduleKind.ONCE,
        expression="1970-01-01T00:01:00Z",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="run once",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    repository.materialize(
        schedule_id=once.id,
        occurrence_key="60000",
        trigger_kind="timer",
        scheduled_for=60_000,
        scheduled_local="1970-01-01T00:01:00+00:00",
        next_due_at=None,
        expected_version=once.version,
    )
    completed = repository.get(once.id)
    assert completed.state is ScheduleState.COMPLETED
    for operation in ("pause", "resume", "update"):
        with pytest.raises(ConflictError):
            if operation == "pause":
                await coordinator.pause(
                    completed.id,
                    expected_version=completed.version,
                )
            elif operation == "resume":
                await coordinator.resume(
                    completed.id,
                    expected_version=completed.version,
                    now_ms=1,
                )
            else:
                await coordinator.update(
                    completed.id,
                    expected_version=completed.version,
                    expression="1970-01-01T00:02:00Z",
                    now_ms=1,
                )
    await coordinator.delete(
        completed.id,
        expected_version=completed.version,
    )
    assert repository.get(completed.id).state is ScheduleState.DELETED


@pytest.mark.asyncio
async def test_schedule_root_availability_is_enforced_for_create_fire_and_resume(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    _activate_schedule_target(storage_context, "root-policy")
    allowed_parent = storage_context.root.parent
    repository = ScheduleRepository(
        storage_context.store,
        allowed_roots=(allowed_parent,),
    )
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name="root-policy",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="check root",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    storage_context.root.rmdir()

    result = repository.materialize(
        schedule_id=schedule.id,
        occurrence_key="60000",
        trigger_kind="timer",
        scheduled_for=60_000,
        scheduled_local="1970-01-01T00:01:00+00:00",
        next_due_at=120_000,
        expected_version=schedule.version,
    )
    blocked = repository.get(schedule.id)
    assert result.fire_state == "blocked"
    assert blocked.state is ScheduleState.BLOCKED
    assert storage_context.store.query_one(
        "SELECT error_code FROM schedule_fires WHERE id = ?",
        (result.fire_id,),
    )["error_code"] == "project_root_unavailable"

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    with pytest.raises(ConflictError, match="project_root_unavailable"):
        await coordinator.resume(
            blocked.id,
            expected_version=blocked.version,
            now_ms=1,
        )
    storage_context.root.mkdir()
    resumed = await coordinator.resume(
        blocked.id,
        expected_version=blocked.version,
        now_ms=1,
    )
    assert resumed.state is ScheduleState.ACTIVE
    storage_context.root.rmdir()
    assert await coordinator.reconcile_startup() == 1
    assert repository.get(schedule.id).state is ScheduleState.BLOCKED
    storage_context.root.mkdir()

    unrelated_root = tmp_path / "unrelated-policy"
    unrelated_root.mkdir()
    unrestricted = ScheduleRepository(
        storage_context.store,
        allowed_roots=(unrelated_root,),
    )
    outside_policy = unrestricted.create(
        conversation_id=storage_context.conversation.id,
        name="outside-policy",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="must succeed",
        next_due_at=60_000,
        created_by_user_id=400,
    )
    assert outside_policy.state is ScheduleState.ACTIVE


@pytest.mark.asyncio
async def test_schedule_update_draft_name_collision_is_stable_conflict(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "name-conflict")

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=ScheduleRepository(storage_context.store),
        wake_conversation=wake,
    )
    first = await coordinator.create(
        conversation_id=storage_context.conversation.id,
        name="first",
        kind="cron",
        expression="* * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="first prompt",
        owner_user_id=400,
        now_ms=1,
    )
    await coordinator.create(
        conversation_id=storage_context.conversation.id,
        name="occupied",
        kind="cron",
        expression="* * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="second prompt",
        owner_user_id=400,
        now_ms=1,
    )
    draft = await coordinator.update_draft(
        schedule_id=first.id,
        expected_version=first.version,
        name="occupied",
        kind="cron",
        expression="*/2 * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="renamed",
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        component_nonce="name-conflict",
    )

    with pytest.raises(
        ConflictError,
        match="schedule name already exists",
    ):
        await coordinator.confirm_draft(
            draft_id=draft.id,
            component_nonce="name-conflict",
            owner_user_id=400,
            guild_id=100,
            channel_id=300,
        )
    assert storage_context.store.query_one(
        "SELECT state FROM schedule_drafts WHERE id = ?",
        (draft.id,),
    )["state"] == "pending"


@pytest.mark.asyncio
async def test_schedule_modal_consume_draft_and_command_effect_are_atomic(
    storage_context: StorageContext,
) -> None:
    _activate_schedule_target(storage_context, "modal-atomic")
    repository = ScheduleRepository(storage_context.store)

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    expires_at = utc_now_ms() + 60_000
    modal = storage_context.repository.create_modal_intent(
        kind="schedule_create",
        conversation_id=storage_context.conversation.id,
        guild_id=100,
        channel_id=300,
        owner_user_id=400,
        nonce="atomic-modal",
        expires_at=expires_at,
    )
    interaction_id = "atomic-modal-submit"
    storage_context.repository.accept_command_intent(
        interaction_id=interaction_id,
        command_name="schedule create submit",
        request={},
        boot_id="old-boot",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
    )
    draft = await coordinator.create_draft(
        conversation_id=storage_context.conversation.id,
        name="atomic-modal",
        kind="cron",
        expression="* * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="persist atomically",
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        component_nonce="draft-component",
        modal_submission=ScheduleModalSubmission(
            intent_id=modal.id,
            kind=modal.kind,
            expires_at=expires_at,
            nonce="atomic-modal",
            interaction_id=interaction_id,
            guild_id=100,
            channel_id=300,
            user_id=400,
        ),
    )

    consumed = storage_context.repository.get_modal_intent(modal.id)
    command = storage_context.repository.get_command_intent(interaction_id)
    assert consumed.state == "consumed"
    assert consumed.consumed_interaction_id == interaction_id
    assert command.state == "effect_in_flight"
    assert (command.effect_kind, command.effect_correlation_id) == (
        "schedule_draft",
        draft.id,
    )

    recovery = storage_context.repository.recover_startup(
        current_boot_id="new-boot"
    )
    assert recovery["reconciled_schedule_intents"] == 1
    assert recovery["unknown_intents"] == 0
    assert (
        storage_context.repository.get_command_intent(interaction_id).state
        == "succeeded"
    )


@pytest.mark.asyncio
async def test_schedule_modal_transaction_rolls_back_all_three_records(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate_schedule_target(storage_context, "modal-rollback")
    repository = ScheduleRepository(storage_context.store)

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    expires_at = utc_now_ms() + 60_000
    modal = storage_context.repository.create_modal_intent(
        kind="schedule_create",
        conversation_id=storage_context.conversation.id,
        guild_id=100,
        channel_id=300,
        owner_user_id=400,
        nonce="rollback-modal",
        expires_at=expires_at,
    )
    interaction_id = "rollback-modal-submit"
    storage_context.repository.accept_command_intent(
        interaction_id=interaction_id,
        command_name="schedule create submit",
        request={},
        boot_id="old-boot",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
    )
    original_insert = repository._insert_draft

    def insert_then_crash(*args: object, **kwargs: object) -> object:
        original_insert(*args, **kwargs)
        raise RuntimeError("simulated transaction crash")

    monkeypatch.setattr(repository, "_insert_draft", insert_then_crash)
    with pytest.raises(RuntimeError, match="transaction crash"):
        await coordinator.create_draft(
            conversation_id=storage_context.conversation.id,
            name="rollback-modal",
            kind="cron",
            expression="* * * * *",
            timezone="UTC",
            misfire_policy="latest",
            prompt_text="must roll back",
            owner_user_id=400,
            guild_id=100,
            channel_id=300,
            component_nonce="rollback-component",
            modal_submission=ScheduleModalSubmission(
                intent_id=modal.id,
                kind=modal.kind,
                expires_at=expires_at,
                nonce="rollback-modal",
                interaction_id=interaction_id,
                guild_id=100,
                channel_id=300,
                user_id=400,
            ),
        )

    assert storage_context.repository.get_modal_intent(modal.id).state == "open"
    assert (
        storage_context.repository.get_command_intent(interaction_id).state
        == "accepted"
    )
    assert storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM schedule_drafts"
    )["count"] == 0


@pytest.mark.asyncio
async def test_schedule_draft_cancel_and_command_effect_are_atomic(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate_schedule_target(storage_context, "draft-cancel")
    repository = ScheduleRepository(storage_context.store)

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=repository,
        wake_conversation=wake,
    )
    draft = await coordinator.create_draft(
        conversation_id=storage_context.conversation.id,
        name="cancel-me",
        kind="cron",
        expression="* * * * *",
        timezone="UTC",
        misfire_policy="latest",
        prompt_text="cancel atomically",
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        component_nonce="cancel-component",
    )
    interaction_id = "draft-cancel-interaction"
    storage_context.repository.accept_command_intent(
        interaction_id=interaction_id,
        command_name="schedule draft cancel",
        request={},
        boot_id="old-boot",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
    )
    original_mark = schedules_module.mark_command_effect_in_transaction

    def mark_then_crash(*args: object, **kwargs: object) -> object:
        original_mark(*args, **kwargs)
        raise RuntimeError("simulated cancel transaction crash")

    monkeypatch.setattr(
        schedules_module,
        "mark_command_effect_in_transaction",
        mark_then_crash,
    )
    audit = ScheduleAuditContext.discord_user(
        user_id=400,
        interaction_id=interaction_id,
    )
    with pytest.raises(RuntimeError, match="cancel transaction crash"):
        await coordinator.cancel_draft(
            draft_id=draft.id,
            component_nonce="cancel-component",
            owner_user_id=400,
            guild_id=100,
            channel_id=300,
            audit=audit,
        )
    assert storage_context.store.query_one(
        "SELECT state FROM schedule_drafts WHERE id = ?",
        (draft.id,),
    )["state"] == "pending"
    assert (
        storage_context.repository.get_command_intent(interaction_id).state
        == "accepted"
    )

    monkeypatch.setattr(
        schedules_module,
        "mark_command_effect_in_transaction",
        original_mark,
    )
    await coordinator.cancel_draft(
        draft_id=draft.id,
        component_nonce="cancel-component",
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        audit=audit,
    )
    command = storage_context.repository.get_command_intent(interaction_id)
    assert command.state == "effect_in_flight"
    assert (command.effect_kind, command.effect_correlation_id) == (
        "schedule_draft_cancel",
        draft.id,
    )
    recovery = storage_context.repository.recover_startup(
        current_boot_id="new-boot"
    )
    assert recovery["reconciled_schedule_intents"] == 1
    assert (
        storage_context.repository.get_command_intent(interaction_id).state
        == "succeeded"
    )
