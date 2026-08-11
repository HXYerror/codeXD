from __future__ import annotations

import json

import pytest
from conftest import StorageContext

from codexd.application.dynamic_tools import DynamicToolDispatcher
from codexd.application.schedule_coordinator import ScheduleCoordinator
from codexd.domain.conversations import SandboxProfile, ThreadConfig, ThreadIdentity
from codexd.domain.schedules import MisfirePolicy, ScheduleKind
from codexd.domain.turns import TurnInput, TurnSource
from codexd.errors import SecurityError
from codexd.runtime.port import DynamicToolCall
from codexd.storage.records import TurnRecord
from codexd.storage.schedules import ScheduleRepository


def test_existing_thread_gets_one_new_session_tool_notice(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    revision = repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="pre-tool-thread",
            requested_thread_id=None,
            provider_session_id="pre-tool-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="0.144.4",
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
        turn_input=TurnInput(text="Can I schedule work?"),
        input_message_id="pre-tool-message",
        requested_by_user_id=400,
    )

    repository.enqueue_dynamic_tool_upgrade_notice(
        conversation_id=storage_context.conversation.id,
        turn_id=turn.id,
    )
    repository.enqueue_dynamic_tool_upgrade_notice(
        conversation_id=storage_context.conversation.id,
        turn_id=turn.id,
    )

    assert revision.dynamic_tools_enabled is False
    rows = storage_context.store.query_all(
        """
        SELECT payload_json FROM discord_outbox
        WHERE dedupe_key = ?
        """,
        (f"conversation:{storage_context.conversation.id}:dynamic-tools-upgrade-v2",),
    )
    assert len(rows) == 1
    payload = json.loads(rows[0]["payload_json"])
    assert "/session new" in payload["content"]


def _activate_runtime_turn(
    storage_context: StorageContext,
    *,
    actor_user_id: int | None,
    provider_thread_id: str = "dynamic-thread",
    provider_turn_id: str = "dynamic-turn",
) -> tuple[TurnRecord, int]:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id=provider_thread_id,
            requested_thread_id=None,
            provider_session_id="dynamic-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="0.144.4",
            dynamic_tools_enabled=True,
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
        environment_hash="dynamic-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="0.144.4",
        runtime_version="0.144.4",
        capability_hash="dynamic-capabilities",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="Create a schedule"),
        input_message_id="9001",
        requested_by_user_id=actor_user_id,
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, provider_turn_id)
    return repository.get_turn(turn.id), lease.generation


def _call(
    turn: TurnRecord,
    generation: int,
    *,
    call_id: str = "call-1",
    thread_id: str = "dynamic-thread",
    turn_id: str = "dynamic-turn",
    arguments: object | None = None,
) -> DynamicToolCall:
    return DynamicToolCall(
        runtime_generation=generation,
        local_turn_id=turn.id,
        provider_thread_id=thread_id,
        provider_turn_id=turn_id,
        provider_call_id=call_id,
        namespace="codexd",
        tool="schedule_create",
        arguments=(
            arguments
            if arguments is not None
            else {
                "name": "daily status",
                "kind": "cron",
                "expression": "0 9 * * *",
                "timezone": "Asia/Shanghai",
                "misfire_policy": "latest",
                "prompt": "Check CI and summarize the project status.",
            }
        ),
    )


def _result(response: dict[str, object]) -> dict[str, object]:
    items = response["contentItems"]
    assert isinstance(items, list) and len(items) == 1
    item = items[0]
    assert isinstance(item, dict) and isinstance(item.get("text"), str)
    result = json.loads(item["text"])
    assert isinstance(result, dict)
    return result


@pytest.mark.asyncio
async def test_dynamic_schedule_tool_is_atomic_replayable_and_message_bound(
    storage_context: StorageContext,
) -> None:
    turn, generation = _activate_runtime_turn(
        storage_context,
        actor_user_id=400,
    )
    schedule_repository = ScheduleRepository(storage_context.store)
    dispatcher = DynamicToolDispatcher(
        schedules=schedule_repository,
        owner_user_id=400,
        guild_id=100,
    )

    first = await dispatcher.handle(_call(turn, generation))
    replay = await dispatcher.handle(_call(turn, generation))

    assert first == replay
    assert first["success"] is True
    result = _result(first)
    assert result["status"] == "confirmation_required"
    assert result["confirmation_required"] is True
    assert result["normalized"] == {
        "name": "daily status",
        "kind": "cron",
        "expression": "0 9 * * *",
        "timezone": "Asia/Shanghai",
        "misfire_policy": "latest",
    }
    assert len(result["next_occurrences"]) == 3
    assert len(storage_context.store.query_all("SELECT id FROM schedule_drafts")) == 1
    assert (
        len(storage_context.store.query_all("SELECT id FROM dynamic_tool_invocations"))
        == 1
    )
    assert (
        len(
            storage_context.store.query_all(
                """
            SELECT id FROM discord_outbox
            WHERE json_extract(payload_json, '$.kind') = 'schedule_draft_card'
              AND operation = 'send'
            """
            )
        )
        == 1
    )
    assert (
        schedule_repository.list_for_conversation(storage_context.conversation.id) == ()
    )

    conflict = await dispatcher.handle(
        _call(
            turn,
            generation,
            arguments={
                "name": "changed",
                "kind": "cron",
                "expression": "0 10 * * *",
                "timezone": "UTC",
                "misfire_policy": "skip",
                "prompt": "Changed input",
            },
        )
    )
    assert conflict["success"] is False
    assert _result(conflict)["code"] == "call_identity_conflict"
    assert len(storage_context.store.query_all("SELECT id FROM schedule_drafts")) == 1

    draft = storage_context.store.query_one("SELECT * FROM schedule_drafts")
    assert draft is not None
    outbox_id = str(draft["confirmation_outbox_id"])
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = 'sending', attempts = 1, lease_owner = 'test-worker',
                lease_expires_at = 9999999999999
            WHERE id = ?
            """,
            (outbox_id,),
        )
    storage_context.repository.ack_outbox(
        outbox_id,
        lease_owner="test-worker",
        lease_attempt=1,
        discord_message_id="7777",
        schedule_draft_id=str(draft["id"]),
    )

    async def wake(_conversation_id: str) -> None:
        return None

    coordinator = ScheduleCoordinator(
        repository=schedule_repository,
        wake_conversation=wake,
    )
    nonce = json.loads(
        str(
            storage_context.store.query_one(
                "SELECT payload_json FROM discord_outbox WHERE id = ?",
                (outbox_id,),
            )["payload_json"]
        )
    )["nonce"]
    with pytest.raises(SecurityError, match="message"):
        await coordinator.confirm_draft(
            draft_id=str(draft["id"]),
            component_nonce=nonce,
            owner_user_id=400,
            guild_id=100,
            channel_id=300,
            message_id="copied-message",
        )
    schedule = await coordinator.confirm_draft(
        draft_id=str(draft["id"]),
        component_nonce=nonce,
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        message_id="7777",
    )
    repeated = await coordinator.confirm_draft(
        draft_id=str(draft["id"]),
        component_nonce=nonce,
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        message_id="7777",
    )
    assert repeated.id == schedule.id
    terminal = storage_context.store.query_one(
        """
        SELECT payload_json FROM discord_outbox
        WHERE dedupe_key = ?
        """,
        (f"schedule-draft:{draft['id']}:terminal:confirmed",),
    )
    assert terminal is not None
    assert json.loads(terminal["payload_json"])["state"] == "confirmed"
    assert len(storage_context.store.query_all("SELECT id FROM schedules")) == 1


@pytest.mark.asyncio
async def test_dynamic_schedule_tool_rejects_non_owner_without_side_effect(
    storage_context: StorageContext,
) -> None:
    turn, generation = _activate_runtime_turn(
        storage_context,
        actor_user_id=401,
    )
    dispatcher = DynamicToolDispatcher(
        schedules=ScheduleRepository(storage_context.store),
        owner_user_id=400,
        guild_id=100,
    )

    response = await dispatcher.handle(
        _call(
            turn,
            generation,
            arguments={"conversation_id": "forged", "prompt": "invalid"},
        )
    )

    assert response["success"] is False
    assert _result(response)["code"] == "owner_required"
    assert storage_context.store.query_all("SELECT id FROM schedule_drafts") == ()
    assert (
        storage_context.store.query_all(
            """
        SELECT id FROM discord_outbox
        WHERE json_extract(payload_json, '$.kind') = 'schedule_draft_card'
        """
        )
        == ()
    )
    invocation = storage_context.store.query_one(
        "SELECT success, result_json FROM dynamic_tool_invocations"
    )
    assert invocation is not None and invocation["success"] == 0


@pytest.mark.asyncio
async def test_dynamic_schedule_tool_rejects_background_turn(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="dynamic-thread",
            requested_thread_id=None,
            provider_session_id="dynamic-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="0.144.4",
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
        name="background",
        kind=ScheduleKind.CRON,
        expression="0 9 * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="Background work",
        next_due_at=9999999999999,
        created_by_user_id=400,
    )
    materialized = schedules.materialize(
        schedule_id=schedule.id,
        occurrence_key="background-tool-test",
        trigger_kind="manual",
        scheduled_for=None,
        scheduled_local="test",
        next_due_at=schedule.next_due_at,
        expected_version=None,
        advance_schedule=False,
    )
    assert materialized.turn_id is not None
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="dynamic-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="0.144.4",
        runtime_version="0.144.4",
        capability_hash="dynamic-capabilities",
    )
    repository.claim_turn(
        materialized.turn_id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(materialized.turn_id, "dynamic-turn")
    turn = repository.get_turn(materialized.turn_id)
    dispatcher = DynamicToolDispatcher(
        schedules=schedules,
        owner_user_id=400,
        guild_id=100,
    )

    response = await dispatcher.handle(_call(turn, lease.generation))

    assert response["success"] is False
    assert _result(response)["code"] == "tool_not_allowed_for_source"
    assert storage_context.store.query_all("SELECT id FROM schedule_drafts") == ()


@pytest.mark.asyncio
async def test_dynamic_schedule_tool_validation_and_provider_scope_fail_closed(
    storage_context: StorageContext,
) -> None:
    turn, generation = _activate_runtime_turn(
        storage_context,
        actor_user_id=400,
    )
    dispatcher = DynamicToolDispatcher(
        schedules=ScheduleRepository(storage_context.store),
        owner_user_id=400,
        guild_id=100,
    )

    invalid = await dispatcher.handle(
        _call(
            turn,
            generation,
            call_id="invalid-call",
            arguments={"name": "missing trusted fields", "conversation_id": "forged"},
        )
    )
    stale = await dispatcher.handle(
        _call(
            turn,
            generation,
            call_id="stale-call",
            thread_id="another-thread",
        )
    )

    assert invalid["success"] is False
    assert _result(invalid)["code"] == "invalid_arguments"
    assert stale["success"] is False
    assert _result(stale)["code"] == "stale_thread"
    assert storage_context.store.query_all("SELECT id FROM schedule_drafts") == ()


@pytest.mark.asyncio
async def test_permanent_confirmation_card_failure_expires_only_the_draft(
    storage_context: StorageContext,
) -> None:
    turn, generation = _activate_runtime_turn(
        storage_context,
        actor_user_id=400,
    )
    dispatcher = DynamicToolDispatcher(
        schedules=ScheduleRepository(storage_context.store),
        owner_user_id=400,
        guild_id=100,
    )
    response = await dispatcher.handle(_call(turn, generation))
    assert response["success"] is True
    draft = storage_context.store.query_one("SELECT * FROM schedule_drafts")
    assert draft is not None
    outbox_id = str(draft["confirmation_outbox_id"])
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = 'sending', attempts = 1, lease_owner = 'test-worker',
                lease_expires_at = 9999999999999
            WHERE id = ?
            """,
            (outbox_id,),
        )

    storage_context.repository.fail_outbox_permanently(
        outbox_id,
        lease_owner="test-worker",
        lease_attempt=1,
        error_code="discord_forbidden",
    )

    failed_draft = storage_context.store.query_one(
        "SELECT state FROM schedule_drafts WHERE id = ?",
        (draft["id"],),
    )
    conversation = storage_context.repository.get_conversation(
        storage_context.conversation.id
    )
    assert failed_draft is not None and failed_draft["state"] == "expired"
    assert conversation.state.value == "active"
    assert (
        storage_context.store.query_one(
            """
        SELECT id FROM incidents
        WHERE code = 'schedule_draft_card_delivery_permanent'
        """
        )
        is not None
    )
