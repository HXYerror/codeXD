from __future__ import annotations

import json
from dataclasses import replace

import pytest
from conftest import StorageContext
from test_dynamic_schedule_tool import _activate_runtime_turn, _call

from codexd.application.dynamic_tools import DynamicToolDispatcher
from codexd.domain.schedules import MisfirePolicy, ScheduleAuditContext, ScheduleKind
from codexd.errors import ConflictError, NotFoundError, SecurityError
from codexd.storage import schedule_management as management
from codexd.storage.schedules import ScheduleRepository


def create(storage_context: StorageContext):
    turn, generation = _activate_runtime_turn(storage_context, actor_user_id=400)
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name="task",
        kind=ScheduleKind.CRON,
        expression="*/2 * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.SKIP,
        prompt_text="private task prompt",
        next_due_at=9_999_999_000_000,
        created_by_user_id=400,
    )
    return repository, schedule, turn, generation


def test_delete_confirmation_ignores_timer_only_updates(storage_context: StorageContext) -> None:
    repository, schedule, _, _ = create(storage_context)
    audit = ScheduleAuditContext.discord_user(user_id=400, interaction_id="delete")
    confirmation, _ = management.prepare_delete(
        repository, schedule.conversation_id, schedule.id[:8], 400
    )
    with repository.store.transaction() as connection:
        connection.execute(
            "UPDATE schedules SET version=version+1,next_due_at=next_due_at+120000 WHERE id=?",
            (schedule.id,),
        )
    management.confirm_delete(repository, schedule.conversation_id, confirmation, 400, audit)
    management.confirm_delete(repository, schedule.conversation_id, confirmation, 400, audit)
    assert repository.get(schedule.id).state.value == "deleted"
    assert repository.get(schedule.id).prompt_text is None
    assert (
        len(repository.store.query_all("SELECT id FROM audit_log WHERE action='schedule.delete'"))
        == 1
    )


def test_delete_confirmation_rejects_edit_and_wrong_scope(storage_context: StorageContext) -> None:
    repository, schedule, _, _ = create(storage_context)
    audit = ScheduleAuditContext.discord_user(user_id=400, interaction_id="delete")
    confirmation, _ = management.prepare_delete(
        repository, schedule.conversation_id, schedule.id, 400
    )
    with pytest.raises(SecurityError):
        management.confirm_delete(repository, "other", confirmation, 400, audit)
    with pytest.raises(SecurityError):
        management.confirm_delete(repository, schedule.conversation_id, confirmation, 401, audit)
    with repository.store.transaction() as connection:
        connection.execute(
            "UPDATE schedules SET expression='*/5 * * * *',version=version+1 WHERE id=?",
            (schedule.id,),
        )
    with pytest.raises(ConflictError, match="changed"):
        management.confirm_delete(repository, schedule.conversation_id, confirmation, 400, audit)
    assert repository.get(schedule.id).state.value == "active"


def test_pause_idempotent_and_references_scoped(storage_context: StorageContext) -> None:
    repository, schedule, _, _ = create(storage_context)
    audit = ScheduleAuditContext.discord_user(user_id=400, interaction_id="pause")
    for reference in (schedule.id, schedule.id[:4]):
        result = management.control(repository, schedule.conversation_id, reference, "pause", audit)
        assert result["state"] == "paused" and result["next_due_at"] is None
    assert not repository.due(9_999_999_999_999)
    assert (
        len(repository.store.query_all("SELECT id FROM audit_log WHERE action='schedule.pause'"))
        == 1
    )
    with pytest.raises(NotFoundError):
        management.control(repository, "other", schedule.id, "pause", audit)
    with pytest.raises(ConflictError):
        management.control(repository, schedule.conversation_id, "%", "pause", audit)


@pytest.mark.asyncio
async def test_background_schedule_management_is_rejected(storage_context: StorageContext) -> None:
    repository, schedule, turn, generation = create(storage_context)
    dispatcher = DynamicToolDispatcher(schedules=repository, owner_user_id=400, guild_id=100)
    call = replace(
        _call(turn, generation), tool="schedule_pause", arguments={"schedule_id": schedule.id}
    )
    from unittest.mock import patch

    original = management._dynamic_tool_scope

    def background(connection, turn_id):
        scope = dict(original(connection, turn_id))
        scope["source_kind"] = "schedule"
        return scope

    with patch.object(management, "_dynamic_tool_scope", side_effect=background):
        result = await dispatcher.handle(call)
    assert result["success"] is False
    assert repository.get(schedule.id).state.value == "active"


@pytest.mark.asyncio
async def test_management_tools_are_durable_owner_scoped_and_no_prompt_leak(
    storage_context: StorageContext,
) -> None:
    repository, schedule, turn, generation = create(storage_context)
    dispatcher = DynamicToolDispatcher(schedules=repository, owner_user_id=400, guild_id=100)
    call = replace(
        _call(turn, generation), tool="schedule_pause", arguments={"schedule_id": schedule.id[:8]}
    )
    first = await dispatcher.handle(call)
    assert first["success"] is True
    assert await dispatcher.handle(call) == first
    assert repository.get(schedule.id).state.value == "paused"
    listed = await dispatcher.handle(
        replace(call, tool="schedule_list", arguments={}, provider_call_id="list")
    )
    assert listed["success"] is True and "private task prompt" not in str(listed)
    assert json.loads(listed["contentItems"][0]["text"])["schedules"][0]["state"] == "paused"
    for bad in (
        replace(call, runtime_generation=generation + 1),
        replace(call, provider_thread_id="other"),
        replace(call, provider_turn_id="other"),
    ):
        assert (await dispatcher.handle(bad))["success"] is False
    unauthorized = DynamicToolDispatcher(schedules=repository, owner_user_id=401, guild_id=100)
    assert (await unauthorized.handle(call))["success"] is False
    changed = replace(call, arguments={"schedule_id": "aaaa"})
    assert (await dispatcher.handle(changed))["success"] is False
    reused_create = replace(call, tool="schedule_create", arguments={
        "name": "duplicate", "kind": "cron", "expression": "*/2 * * * *",
        "timezone": "UTC", "misfire_policy": "skip", "prompt": "do not create",
    })
    assert (await dispatcher.handle(reused_create))["success"] is False
    assert not repository.store.query_all("SELECT id FROM schedule_drafts")
    with repository.store.transaction() as connection:
        connection.execute("UPDATE turns SET requested_by_user_id=NULL WHERE id=?", (turn.id,))
    assert (await dispatcher.handle(replace(call, provider_call_id="nonowner")))["success"] is False


@pytest.mark.asyncio
async def test_schedule_command_schema_hides_version_and_offers_choices(
    storage_context: StorageContext, tmp_path
) -> None:
    from test_discord_bridge import _leaf_commands, _storage_bot

    bot = _storage_bot(storage_context, tmp_path)
    bot._register_commands()
    commands = _leaf_commands(bot)
    for name in ("pause", "resume", "delete", "show"):
        params = commands[f"/schedule/{name}"].parameters
        assert [p.name for p in params] == ["schedule_id"]
        assert params[0].description != "…"
        assert params[0].autocomplete
