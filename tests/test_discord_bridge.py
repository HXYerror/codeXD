from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import discord
import pytest
from conftest import StorageContext
from discord import app_commands

from codexd.application.schedule_coordinator import ScheduleCoordinator
from codexd.application.session_coordinator import ResolvedProject, SessionCoordinator
from codexd.application.session_lifecycle import (
    SessionActivityStatus,
    SessionBehaviorStatus,
    SessionStatus,
    SessionStatusValue,
    SessionStatusView,
)
from codexd.config import AppConfig, DiscordConfig, ScheduleConfig, SecurityConfig
from codexd.domain.conversations import (
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
)
from codexd.domain.ids import sha256_text, utc_now_ms
from codexd.domain.models import (
    AccountStatus,
    ModelCatalogSnapshot,
    ModelDescriptor,
    ServiceTierDescriptor,
)
from codexd.domain.schedules import MisfirePolicy, ScheduleKind
from codexd.domain.turns import TurnInput, TurnSource, TurnState
from codexd.errors import ConflictError, SecurityError
from codexd.paths import AppPaths
from codexd.runtime.codex_sdk import capability_manifest
from codexd.security.signing import ComponentSigner
from codexd.storage.schedules import ScheduleRepository
from codexd.transport.discord.bot import (
    CodexDBot,
    _ScheduleModal,
    _SideQueryModal,
    _timezone_with_offset,
)


def _response() -> Mock:
    done = False

    async def defer(**_kwargs: object) -> None:
        nonlocal done
        done = True

    async def send_message(*_args: object, **_kwargs: object) -> None:
        nonlocal done
        done = True

    async def send_modal(_modal: discord.ui.Modal) -> None:
        nonlocal done
        done = True

    return Mock(
        defer=AsyncMock(side_effect=defer),
        send_message=AsyncMock(side_effect=send_message),
        send_modal=AsyncMock(side_effect=send_modal),
        is_done=Mock(side_effect=lambda: done),
    )


def _interaction(
    interaction_id: int,
    *,
    thread: discord.Thread,
    text_channel: discord.TextChannel,
    in_thread: bool,
) -> Mock:
    channel = thread if in_thread else text_channel
    interaction = Mock(spec=discord.Interaction)
    interaction.id = interaction_id
    interaction.guild_id = 100
    interaction.channel_id = channel.id
    interaction.channel = channel
    interaction.user = Mock(id=400)
    interaction.response = _response()
    interaction.followup = Mock(send=AsyncMock())
    interaction.app_permissions = SimpleNamespace(
        view_channel=True,
        send_messages=True,
        send_messages_in_threads=True,
        embed_links=True,
        attach_files=True,
        create_public_threads=True,
        manage_threads=True,
        read_message_history=True,
    )
    return interaction


def _leaf_commands(bot: CodexDBot) -> dict[str, Any]:
    commands: dict[str, Any] = {}

    def collect(command: Any, prefix: str = "") -> None:
        path = f"{prefix}/{command.name}"
        children = getattr(command, "commands", None)
        if children:
            for child in children:
                collect(child, path)
            return
        commands[path] = command

    guild = discord.Object(id=100)
    for command in bot.tree.get_commands(guild=guild):
        collect(command)
    return commands


def _storage_bot(
    storage_context: StorageContext,
    tmp_path: Path,
) -> CodexDBot:
    conversation = storage_context.repository.get_conversation(
        storage_context.conversation.id
    )
    sessions = Mock()
    sessions.conversation_for_thread = AsyncMock(
        side_effect=lambda channel_id: conversation if channel_id == 300 else None
    )
    sessions.resolve_project_for_channel = AsyncMock(
        return_value=ResolvedProject(storage_context.project, "binding")
    )
    return CodexDBot(
        config=AppConfig(
            paths=AppPaths(tmp_path / "data", tmp_path / "logs"),
            discord=DiscordConfig(
                guild_id=100,
                owner_user_id=400,
                allowed_user_ids=frozenset({400}),
            ),
        ),
        repository=storage_context.repository,
        sessions=sessions,
        session_lifecycle=Mock(),
        turns=Mock(),
        schedules=Mock(),
        schedule_repository=ScheduleRepository(storage_context.store),
        runtimes=Mock(),
        renderer=Mock(),
        media_worker=Mock(),
        signer=ComponentSigner(b"bridge-infrastructure".ljust(32, b"-")),
        capability_manifest=capability_manifest(),
        boot_id="bridge-infrastructure",
    )


def _thread_channels() -> tuple[discord.Thread, discord.TextChannel]:
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.parent_id = 200
    text_channel = Mock(spec=discord.TextChannel)
    text_channel.id = 200
    return thread, text_channel


@pytest.mark.asyncio
async def test_btw_and_side_register_together_and_return_ephemeral_answer(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    side_queries = Mock()
    side_queries.ask = AsyncMock(return_value="A **temporary** answer.")
    bot.side_queries = side_queries
    bot._register_commands()
    commands = _leaf_commands(bot)
    assert "/btw" in commands and "/side" in commands
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        9001,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    interaction.edit_original_response = AsyncMock()

    await bot._apply_side_query(interaction, "Why this approach?")

    interaction.response.defer.assert_awaited_once_with(ephemeral=True)
    side_queries.ask.assert_awaited_once_with(
        interaction_id="9001",
        conversation_id=storage_context.conversation.id,
        requested_by_user_id=400,
        question="Why this approach?",
    )
    assert interaction.edit_original_response.await_count == 2
    final = interaction.edit_original_response.await_args.kwargs["content"]
    assert "A **temporary** answer." in final
    assert "main task unchanged" in final


@pytest.mark.asyncio
async def test_btw_without_question_opens_signed_scoped_modal(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    bot.side_queries = Mock()
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        9002,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )

    await bot._btw(interaction, None)

    modal = interaction.response.send_modal.await_args.args[0]
    assert isinstance(modal, _SideQueryModal)
    action = bot.signer.verify_modal_id(modal.custom_id)
    assert action.kind == "side_query"
    record = storage_context.repository.get_modal_intent(action.intent_id)
    assert record.conversation_id == storage_context.conversation.id
    assert record.owner_user_id == 400


@pytest.mark.asyncio
async def test_btw_long_answer_stays_ephemeral_and_uses_markdown_attachment(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    side_queries = Mock()
    side_queries.ask = AsyncMock(return_value="临时解释" * 5_000)
    bot.side_queries = side_queries
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        9003,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    interaction.edit_original_response = AsyncMock()

    await bot._apply_side_query(interaction, "请详细解释?")

    followup = interaction.followup.send.await_args
    assert followup.kwargs["ephemeral"] is True
    assert followup.kwargs["file"].filename == "btw-answer.md"
    assert "main task unchanged" in (
        interaction.edit_original_response.await_args.kwargs["content"]
    )


@pytest.mark.asyncio
async def test_every_registered_discord_command_executes_through_bridge(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    repository = storage_context.repository
    revision = repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="bridge-provider-thread",
            requested_thread_id=None,
            provider_session_id="bridge-provider-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="bridge-test",
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
        environment_hash="bridge-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk-test",
        runtime_version="runtime-test",
        capability_hash="capability-test",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="exercise every command"),
        input_message_id="bridge-command-message",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    running_turn = repository.mark_turn_running(turn.id, "provider-turn")
    repository.turn_recorded_diff = (  # type: ignore[method-assign]
        lambda turn_id: "+bridge change\n" if turn_id == turn.id else None
    )
    conversation = repository.get_conversation(storage_context.conversation.id)
    schedule_repository = ScheduleRepository(storage_context.store)
    schedule = schedule_repository.create(
        conversation_id=conversation.id,
        name="bridge schedule",
        kind=ScheduleKind.CRON,
        expression="0 9 * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="summarize project status",
        next_due_at=utc_now_ms() + 60_000,
        created_by_user_id=400,
    )

    model = ModelDescriptor(
        id="gpt-test",
        model="gpt-test",
        is_default=True,
        input_modalities=("text", "image"),
        supported_reasoning_efforts=("low", "medium", "high"),
        default_reasoning_effort="medium",
        supports_personality=True,
        service_tiers=(
            ServiceTierDescriptor("fast", "Fast", "Lower-latency service"),
        ),
        default_service_tier="fast",
        upgrade=None,
    )
    catalog = ModelCatalogSnapshot((model,), complete=True, next_cursor=None)
    lifecycle = Mock()
    lifecycle.model_catalog = AsyncMock(return_value=catalog)
    lifecycle.list_revisions = AsyncMock(return_value=(revision,))
    lifecycle.status = AsyncMock(
        return_value=SessionStatus(conversation, revision)
    )
    lifecycle.status_view = AsyncMock(
        return_value=SessionStatusView(
            conversation=conversation,
            active_revision=revision,
            project_name=storage_context.project.name,
            behavior=SessionBehaviorStatus(
                model=SessionStatusValue("gpt-test", "provider default"),
                reasoning_effort=SessionStatusValue("medium", "model default"),
                reasoning_summary=SessionStatusValue(
                    "provider default", "provider default"
                ),
                personality=SessionStatusValue(
                    "provider default", "provider default"
                ),
                service_tier=SessionStatusValue("fast", "model default"),
                web_search_mode="cached",
                input_modalities=("text", "image"),
                resolution="resolved",
            ),
            activity=SessionActivityStatus(
                runtime_state="ready",
                runtime_generation=1,
                queued_turns=0,
                active_turns=1,
                last_completed_at=None,
                active_turn=running_turn,
                active_settings_differ=True,
            ),
            resume_verification="verified by active provider Turn",
            degraded_reason=None,
        )
    )
    for name in (
        "set_model",
        "set_reasoning_effort",
        "set_service_tier",
        "set_reasoning_summary",
        "set_personality",
        "set_web_search",
    ):
        setattr(lifecycle, name, AsyncMock(return_value=conversation))
    for name in ("new", "resume", "fork", "rename"):
        setattr(lifecycle, name, AsyncMock(return_value=revision))
    for name in ("archive", "compact", "clear"):
        setattr(lifecycle, name, AsyncMock())

    sessions = Mock()
    sessions.conversation_for_thread = AsyncMock(
        side_effect=lambda channel_id: conversation if channel_id == 300 else None
    )
    sessions.project_for_channel = AsyncMock(
        return_value=storage_context.project
    )
    sessions.resolve_project_for_channel = AsyncMock(
        return_value=ResolvedProject(storage_context.project, "binding")
    )
    sessions.bind_project = AsyncMock(return_value=storage_context.project)
    sessions.unbind_project = AsyncMock(return_value=storage_context.project)

    turns = Mock()
    turns.cancel = AsyncMock(return_value=running_turn)
    turns.steer = AsyncMock()
    schedules = Mock()
    schedules.pause = AsyncMock(return_value=schedule)
    schedules.resume = AsyncMock(return_value=schedule)
    schedules.delete = AsyncMock()
    schedules.run_now = AsyncMock(return_value=turn.id)

    runtimes = Mock()
    runtimes.project_status = AsyncMock(
        return_value={"state": "ready", "generation": 1, "failures": 0}
    )
    runtimes.account_status_if_loaded = AsyncMock(
        return_value=AccountStatus(False, "api", "test", utc_now_ms())
    )
    runtimes.status = AsyncMock(
        return_value={
            "topology": "project_scoped",
                "ready": 1,
                "starting": 0,
                "unhealthy": 0,
                "capacity_limit": 10,
                "capacity_waiters": 0,
                "idle_ttl_seconds": 900,
                "sqlite_isolated": True,
            }
    )
    bot = CodexDBot(
        config=AppConfig(
            paths=AppPaths(tmp_path / "data", tmp_path / "logs"),
            discord=DiscordConfig(
                guild_id=100,
                owner_user_id=400,
                allowed_user_ids=frozenset({400}),
            ),
        ),
        repository=repository,
        sessions=sessions,
        session_lifecycle=lifecycle,
        turns=turns,
        schedules=schedules,
        schedule_repository=schedule_repository,
        runtimes=runtimes,
        renderer=Mock(),
        media_worker=Mock(),
        signer=ComponentSigner(b"bridge-component-key".ljust(32, b"-")),
        capability_manifest=capability_manifest(),
        boot_id="bridge-test",
    )
    bot._gateway_ready = True
    bot._codex_auth_state = "authenticated"
    bot._register_commands()
    commands = _leaf_commands(bot)
    expected_commands = {
        "/capabilities",
        "/diagnostics",
        "/diff",
        "/model/list",
        "/model/set",
        "/model/show",
        "/model/tier/default",
        "/model/tier/set",
        "/model/tier/show",
        "/personality/set",
        "/personality/show",
        "/project/bind",
        "/project/info",
        "/project/unbind",
        "/reasoning/set",
        "/reasoning/show",
        "/reasoning/summary/default",
        "/reasoning/summary/set",
        "/reasoning/summary/show",
        "/schedule/create",
        "/schedule/delete",
        "/schedule/list",
        "/schedule/pause",
        "/schedule/resume",
        "/schedule/run-now",
        "/schedule/show",
        "/schedule/update",
        "/session/clear",
        "/session/compact",
        "/session/fork",
        "/session/list",
        "/session/new",
        "/session/rename",
        "/session/resume",
        "/session/status",
        "/status",
        "/steer",
        "/turn/cancel",
        "/turn/list",
        "/turn/show",
        "/usage",
        "/websearch/set",
        "/websearch/show",
    }
    assert set(commands) == expected_commands

    arguments: dict[str, dict[str, object]] = {
        "/project/bind": {"path": str(storage_context.root), "name": "bridge"},
        "/project/unbind": {"confirmation_name": storage_context.project.name},
        "/turn/list": {"state": "running"},
        "/turn/show": {"turn": turn.id[:8]},
        "/turn/cancel": {"turn": turn.id[:8]},
        "/model/set": {"model": "default"},
        "/model/tier/set": {"tier": "fast"},
        "/reasoning/set": {"effort": "high"},
        "/reasoning/summary/set": {"summary": "concise"},
        "/personality/set": {"personality": "pragmatic"},
        "/websearch/set": {"mode": "cached", "confirm_live": False},
        "/session/resume": {"revision": revision.id[:8]},
        "/session/rename": {"name": "renamed"},
        "/session/compact": {"confirm": True},
        "/session/clear": {"confirm": True},
        "/schedule/show": {"schedule_id": schedule.id[:8]},
        "/schedule/update": {"schedule_id": schedule.id[:8]},
        "/schedule/pause": {"schedule_id": schedule.id[:8], "version": 1},
        "/schedule/resume": {"schedule_id": schedule.id[:8], "version": 1},
        "/schedule/delete": {"schedule_id": schedule.id[:8], "version": 1},
        "/schedule/run-now": {"schedule_id": schedule.id[:8]},
        "/diff": {"turn": turn.id[:8]},
    }
    text_commands = {"/project/bind", "/project/info", "/project/unbind"}
    direct_modal_commands = {
        "/schedule/create",
        "/schedule/update",
        "/steer",
    }
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.parent_id = 200
    text_channel = Mock(spec=discord.TextChannel)
    text_channel.id = 200
    executed: set[str] = set()
    interactions: dict[str, Mock] = {}

    for index, (path, command) in enumerate(sorted(commands.items()), start=10_000):
        interaction = _interaction(
            index,
            thread=thread,
            text_channel=text_channel,
            in_thread=path not in text_commands,
        )
        interactions[path] = interaction
        callback: Callable[..., Any] = command.callback
        if path in direct_modal_commands:
            await callback(bot, interaction, **arguments.get(path, {}))
        else:
            await callback(interaction, **arguments.get(path, {}))
        executed.add(path)

    assert executed == expected_commands
    assert len(expected_commands) == 43
    for path, interaction in interactions.items():
        if path in direct_modal_commands:
            interaction.response.send_modal.assert_awaited_once()
        else:
            interaction.response.defer.assert_awaited_once()
            interaction.followup.send.assert_awaited()
    rows = storage_context.store.connection.execute(
        """
        SELECT state, json_extract(result_json, '$.delivery') AS delivery
        FROM command_intents
        """
    ).fetchall()
    assert len(rows) == len(commands) - len(direct_modal_commands)
    assert {
        (str(row["state"]), str(row["delivery"])) for row in rows
    } == {("succeeded", "delivered")}
    turn_list = interactions["/turn/list"].followup.send.await_args.args[0]
    assert "summary `[content not retained; 22 bytes]`" in turn_list
    assert "usage `pending`" in turn_list
    assert "input `" not in turn_list
    turn_show = interactions["/turn/show"].followup.send.await_args.args[0]
    assert "Input summary: `[content not retained; 22 bytes]`" in turn_show
    assert "approval: `auto_review`" in turn_show
    assert (
        "https://discord.com/channels/100/300/bridge-command-message"
        in turn_show
    )
    status = interactions["/status"].followup.send.await_args.args[0]
    assert "Schedule: active 1 · paused 0 · blocked 0" in status
    session_status_call = interactions["/session/status"].followup.send.await_args
    session_embed = session_status_call.kwargs["embed"]
    assert session_status_call.kwargs["ephemeral"] is True
    assert session_embed.title == "🟢 Session active"
    assert [field.name for field in session_embed.fields] == [
        "Model & behavior · next Turn",
        "Activity",
        "Session",
        "Execution",
    ]
    assert "Optional capabilities" not in json.dumps(
        session_embed.to_dict(), ensure_ascii=False
    )
    capabilities = interactions["/capabilities"].followup.send.await_args.args[0]
    assert "`bot_mention_input`" in capabilities
    assert "`sdk_mention_input`" in capabilities
    assert "`ordinary_file_materialization`" in capabilities
    assert "`gpt-test` · personality `supported`" in capabilities
    diagnostics = interactions["/diagnostics"].followup.send.await_args.args[0]
    assert "Attachments retained:" in diagnostics
    assert "Runtime lease:" in diagnostics
    assert "SDK `sdk-test` · runtime `runtime-test`" in diagnostics
    assert "Outbox pending:" in diagnostics and "retry:" in diagnostics
    sessions.bind_project.assert_awaited_once()
    sessions.unbind_project.assert_awaited_once()
    turns.cancel.assert_awaited_once()
    assert turns.cancel.await_args.kwargs["interaction_id"] == str(
        interactions["/turn/cancel"].id
    )
    for name in (
        "set_model",
        "set_reasoning_effort",
        "set_personality",
        "set_web_search",
        "new",
        "resume",
        "fork",
        "rename",
        "compact",
        "clear",
    ):
        getattr(lifecycle, name).assert_awaited_once()
    assert lifecycle.set_service_tier.await_count == 2
    assert lifecycle.set_reasoning_summary.await_count == 2
    lifecycle.status_view.assert_awaited_once_with(conversation.id)
    for name in (
        "set_web_search",
        "new",
        "resume",
        "fork",
        "rename",
    ):
        assert getattr(lifecycle, name).await_args.kwargs["interaction_id"]
    lifecycle.archive.assert_not_awaited()
    schedules.pause.assert_awaited_once()
    schedules.resume.assert_awaited_once()
    schedules.delete.assert_awaited_once()
    schedules.run_now.assert_awaited_once()


@pytest.mark.asyncio
async def test_command_acks_before_slow_intent_storage(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_001,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    entered = threading.Event()
    release = threading.Event()
    original = storage_context.repository.accept_command_intent

    def slow_accept(**kwargs: Any) -> Any:
        entered.set()
        release.wait(timeout=2)
        return original(**kwargs)

    storage_context.repository.accept_command_intent = slow_accept  # type: ignore[method-assign]
    task = asyncio.create_task(
        bot._run_intent_action(
            interaction,
            command_name="slow command",
            request={},
            action=AsyncMock(),
        )
    )
    assert await asyncio.to_thread(entered.wait, 1)

    interaction.response.defer.assert_awaited_once()
    assert not task.done()
    release.set()
    assert await task


@pytest.mark.asyncio
async def test_steer_does_not_open_modal_until_turn_is_running(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_040,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    storage_context.repository.active_turn_for_conversation = (  # type: ignore[method-assign]
        lambda _conversation_id: SimpleNamespace(state=TurnState.STARTING)
    )

    with pytest.raises(ConflictError, match="wait until the Turn is running"):
        await bot._steer(interaction)

    interaction.response.send_modal.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oversized", [False, True])
async def test_diff_attaches_only_when_oversized(
    storage_context: StorageContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    oversized: bool,
) -> None:
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="change a file"),
        input_message_id=f"diff-input-{oversized}",
    )
    patch = (
        "diff --git a/example.py b/example.py\n+print('ok')"
        if not oversized
        else "diff --git a/example.py b/example.py\n" + ("+changed\n" * 400)
    )
    monkeypatch.setattr(
        storage_context.repository,
        "turn_recorded_diff",
        lambda turn_id: patch if turn_id == turn.id else None,
    )
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_050 + int(oversized),
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )

    await bot._diff(interaction)

    sent = interaction.followup.send.await_args
    assert "Turn-recorded changes" in sent.args[0]
    if oversized:
        attachment = sent.kwargs["file"]
        assert attachment.filename == f"codexd-turn-{turn.id[:8]}.diff"
        assert "```diff" not in sent.args[0]
    else:
        assert "```diff" in sent.args[0]
        assert "file" not in sent.kwargs
        assert sent.kwargs["allowed_mentions"].everyone is False


@pytest.mark.asyncio
async def test_commands_and_messages_are_not_globally_serialized(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    first_interaction = _interaction(
        20_010,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    second_interaction = _interaction(
        20_011,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    release = asyncio.Event()

    async def first_action(_interaction: discord.Interaction[Any]) -> None:
        first_entered.set()
        await release.wait()

    async def second_action(_interaction: discord.Interaction[Any]) -> None:
        second_entered.set()

    first = asyncio.create_task(
        bot._run_intent_action(
            first_interaction,
            command_name="first",
            request={},
            action=first_action,
        )
    )
    await first_entered.wait()
    second = asyncio.create_task(
        bot._run_intent_action(
            second_interaction,
            command_name="second",
            request={},
            action=second_action,
        )
    )
    await asyncio.wait_for(second_entered.wait(), timeout=1)
    assert await second
    release.set()
    assert await first

    first_message_entered = asyncio.Event()
    second_message_entered = asyncio.Event()
    message_release = asyncio.Event()
    calls = 0

    async def handle_message(_message: discord.Message) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_message_entered.set()
            await message_release.wait()
        else:
            second_message_entered.set()

    bot._handle_message = handle_message  # type: ignore[method-assign]
    first_message = asyncio.create_task(bot.on_message(Mock(spec=discord.Message)))
    await first_message_entered.wait()
    second_message = asyncio.create_task(bot.on_message(Mock(spec=discord.Message)))
    await asyncio.wait_for(second_message_entered.wait(), timeout=1)
    await second_message
    message_release.set()
    await first_message


@pytest.mark.asyncio
async def test_cancelled_commands_have_terminal_durable_states(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()

    async def run_cancelled(
        interaction_id: int,
        *,
        mark_effect: bool,
    ) -> str:
        interaction = _interaction(
            interaction_id,
            thread=thread,
            text_channel=text_channel,
            in_thread=True,
        )
        entered = asyncio.Event()

        async def action(staged: discord.Interaction[Any]) -> None:
            if mark_effect:
                await bot._mark_interaction_effect(
                    staged,
                    effect_kind="provider-test",
                    effect_correlation_id="provider-effect",
                )
            entered.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(
            bot._run_intent_action(
                interaction,
                command_name="cancel test",
                request={"effect": mark_effect},
                action=action,
            )
        )
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return storage_context.repository.get_command_intent(
            str(interaction_id)
        ).state

    assert await run_cancelled(20_020, mark_effect=False) == "rejected"
    assert await run_cancelled(20_021, mark_effect=True) == "unknown"


@pytest.mark.asyncio
async def test_command_error_after_effect_is_unknown_and_diagnostic(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_022,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )

    async def action(staged: discord.Interaction[Any]) -> None:
        await bot._mark_interaction_effect(
            staged,
            effect_kind="provider-test",
            effect_correlation_id="provider-effect",
        )
        raise RuntimeError("provider connection dropped")

    with pytest.raises(RuntimeError, match="provider connection dropped"):
        await bot._run_intent_action(
            interaction,
            command_name="effect failure test",
            request={"effect": True},
            action=action,
        )

    intent = storage_context.repository.get_command_intent("20022")
    assert intent.state == "unknown"
    assert json.loads(intent.result_json or "{}") == {
        "cause_code": "internal_error",
        "code": "command_effect_outcome_unknown",
        "message": (
            "The provider effect may have completed; inspect status before retrying."
        ),
    }
    incidents = storage_context.repository.unresolved_incidents()
    assert any(item["code"] == "command_effect_outcome_unknown" for item in incidents)


@pytest.mark.asyncio
async def test_duplicate_command_replays_specific_persisted_result(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    first = _interaction(
        20_030,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )

    async def action(staged: discord.Interaction[Any]) -> None:
        await staged.followup.send("specific command result", ephemeral=True)

    assert await bot._run_intent_action(
        first,
        command_name="duplicate test",
        request={"value": 1},
        action=action,
    )
    duplicate = _interaction(
        20_030,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    duplicate_action = AsyncMock()

    assert not await bot._run_intent_action(
        duplicate,
        command_name="duplicate test",
        request={"value": 1},
        action=duplicate_action,
    )

    duplicate_action.assert_not_awaited()
    response = duplicate.followup.send.await_args.args[0]
    assert "`ok`" in response
    assert "`/duplicate test` completed." in response
    assert "specific command result" not in response


@pytest.mark.asyncio
async def test_command_delivery_failure_is_durable_and_diagnostic(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_040,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    interaction.followup.send = AsyncMock(side_effect=RuntimeError("Discord down"))

    async def action(staged: discord.Interaction[Any]) -> None:
        await staged.followup.send("completed locally", ephemeral=True)

    assert await bot._run_intent_action(
        interaction,
        command_name="delivery test",
        request={},
        action=action,
    )

    intent = storage_context.repository.get_command_intent("20040")
    result = json.loads(intent.result_json or "{}")
    assert intent.state == "succeeded"
    assert result["delivery"] == "failed"
    assert result["delivery_error_code"] == "RuntimeError"
    incidents = storage_context.repository.unresolved_incidents(limit=10)
    assert any(
        incident["code"] == "discord_command_delivery_failed"
        for incident in incidents
    )


@pytest.mark.asyncio
async def test_wrapped_security_rejection_is_not_silent(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_025,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    interaction.user.id = 401

    async def action(staged: discord.Interaction[Any]) -> None:
        await bot._defer_authorized(staged)

    with pytest.raises(SecurityError) as captured:
        await bot._run_intent_action(
            interaction,
            command_name="security test",
            request={},
            action=action,
        )
    interaction.followup.send.assert_not_awaited()

    await bot._on_command_error(
        interaction,
        cast(app_commands.AppCommandError, captured.value),
    )

    response = interaction.followup.send.await_args.args[0]
    assert response.startswith("`security_error`:")
    intent = storage_context.repository.get_command_intent("20025")
    assert intent.state == "rejected"


@pytest.mark.asyncio
async def test_session_precondition_rejection_is_not_marked_outcome_unknown(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    bot.session_lifecycle.new = AsyncMock(
        side_effect=ConflictError("Conversation has an active Turn")
    )
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_027,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )

    with pytest.raises(ConflictError):
        await bot._run_intent_action(
            interaction,
            command_name="session new",
            request={},
            action=bot._session_new,
        )

    intent = storage_context.repository.get_command_intent("20027")
    assert intent.state == "rejected"
    assert intent.effect_kind is None
    assert json.loads(intent.result_json or "{}")["code"] == "conflict"
    assert not any(
        incident["code"] == "command_effect_outcome_unknown"
        for incident in storage_context.repository.unresolved_incidents(limit=10)
    )


@pytest.mark.asyncio
async def test_direct_owner_rejection_is_not_duplicated(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _storage_bot(storage_context, tmp_path)
    thread, text_channel = _thread_channels()
    interaction = _interaction(
        20_026,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    interaction.user.id = 401

    with pytest.raises(SecurityError) as captured:
        await bot._require_owner(interaction)
    await bot._on_command_error(
        interaction,
        cast(app_commands.AppCommandError, captured.value),
    )

    interaction.response.send_message.assert_awaited_once_with(
        "Owner permission required.",
        ephemeral=True,
    )
    interaction.followup.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbound_channel_command_intent_is_scoped_to_home(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    bot = _storage_bot(storage_context, tmp_path)
    bot.sessions = SessionCoordinator(
        repository=storage_context.repository,
        security=SecurityConfig(allowed_roots=(tmp_path,)),
        home_path=home,
    )
    thread, text_channel = _thread_channels()
    text_channel.id = 999
    interaction = _interaction(
        20_050,
        thread=thread,
        text_channel=text_channel,
        in_thread=False,
    )

    intent = await bot._accept_interaction_intent(
        interaction,
        command_name="status",
        request={},
    )

    assert intent.project_id is not None
    assert storage_context.repository.get_project(intent.project_id).root_path == home


@pytest.mark.asyncio
async def test_schedule_modal_and_component_complete_after_bot_restart(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="schedule-modal-thread",
            requested_thread_id=None,
            provider_session_id="schedule-modal-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="bridge-test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    schedule_repository = ScheduleRepository(storage_context.store)

    async def wake(_conversation_id: str) -> None:
        return

    coordinator = ScheduleCoordinator(
        repository=schedule_repository,
        wake_conversation=wake,
    )
    signer_key = b"schedule-restart-key".ljust(32, b"-")
    first = _storage_bot(storage_context, tmp_path)
    first.signer = ComponentSigner(signer_key)
    thread, text_channel = _thread_channels()
    launch = _interaction(
        20_060,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )

    await first._schedule_create(launch)

    modal = launch.response.send_modal.await_args.args[0]
    restarted = _storage_bot(storage_context, tmp_path)
    restarted.signer = ComponentSigner(signer_key)
    restarted.schedules = coordinator
    submit = _interaction(
        20_061,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    submit.type = discord.InteractionType.modal_submit
    submit.data = {
        "custom_id": modal.custom_id,
        "components": [
            {"components": [{"custom_id": "schedule_name", "value": "daily"}]},
            {
                "components": [
                    {"custom_id": "schedule_when", "value": "0 9 * * *"}
                ]
            },
            {
                "components": [
                    {"custom_id": "schedule_timezone", "value": "UTC"}
                ]
            },
            {
                "components": [
                    {"custom_id": "schedule_misfire", "value": "latest"}
                ]
            },
            {
                "components": [
                    {
                        "custom_id": "schedule_prompt",
                        "value": "summarize project status",
                    }
                ]
            },
        ],
    }

    await restarted.on_interaction(submit)

    preview = submit.followup.send.await_args
    assert preview.kwargs["embed"].title == "Schedule · create"
    confirm_id = preview.kwargs["view"].children[0].custom_id
    assert isinstance(confirm_id, str)
    confirm = _interaction(
        20_062,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    confirm.type = discord.InteractionType.component
    confirm.data = {"custom_id": confirm_id}

    await restarted.on_interaction(confirm)

    schedules = schedule_repository.list_for_conversation(
        storage_context.conversation.id
    )
    assert len(schedules) == 1
    assert schedules[0].name == "daily"
    assert schedules[0].state.value == "active"
    assert repository.get_command_intent("20061").state == "succeeded"
    assert repository.get_command_intent("20062").state == "succeeded"


@pytest.mark.asyncio
async def test_steer_modal_preserves_turn_scope_after_bot_restart(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="steer-modal-thread",
            requested_thread_id=None,
            provider_session_id="steer-modal-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="bridge-test",
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
        environment_hash="steer-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk-test",
        runtime_version="runtime-test",
        capability_hash="capability-test",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="start a long operation"),
        input_message_id="steer-input",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "steer-provider-turn")
    signer_key = b"steer-restart-key".ljust(32, b"-")
    first = _storage_bot(storage_context, tmp_path)
    first.signer = ComponentSigner(signer_key)
    thread, text_channel = _thread_channels()
    launch = _interaction(
        20_070,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )

    await first._steer(launch)

    modal = launch.response.send_modal.await_args.args[0]
    restarted = _storage_bot(storage_context, tmp_path)
    restarted.signer = ComponentSigner(signer_key)

    async def steer(
        turn_id: str,
        _instruction: str,
        *,
        interaction_id: str,
        actor_user_id: int,
    ) -> None:
        instruction_hash = sha256_text(_instruction)
        repository.mark_command_effect(
            interaction_id,
            effect_kind="turn_steer",
            effect_correlation_id=turn_id,
            turn_id=turn_id,
            actor_user_id=actor_user_id,
            audit_action="turn.steer_requested",
            audit_payload={"instruction_hash": instruction_hash},
        )
        repository.record_steer_accepted(
            turn_id=turn_id,
            instruction_hash=instruction_hash,
            actor_user_id=actor_user_id,
            interaction_id=interaction_id,
        )

    restarted.turns.steer = AsyncMock(side_effect=steer)
    submit = _interaction(
        20_071,
        thread=thread,
        text_channel=text_channel,
        in_thread=True,
    )
    submit.type = discord.InteractionType.modal_submit
    submit.data = {
        "custom_id": modal.custom_id,
        "components": [
            {
                "components": [
                    {
                        "custom_id": "steer_instruction",
                        "value": "focus on the failing bridge tests",
                    }
                ]
            }
        ],
    }

    await restarted.on_interaction(submit)

    restarted.turns.steer.assert_awaited_once_with(
        turn.id,
        "focus on the failing bridge tests",
        interaction_id="20071",
        actor_user_id=400,
    )
    action = restarted.signer.verify_modal_id(modal.custom_id)
    row = storage_context.store.connection.execute(
        "SELECT state, turn_id FROM modal_intents WHERE id = ?",
        (action.intent_id,),
    ).fetchone()
    assert row is not None
    assert (row["state"], row["turn_id"]) == ("consumed", turn.id)
    intent = repository.get_command_intent("20071")
    assert intent.state == "succeeded"
    assert intent.effect_kind == "turn_steer"
    assert intent.effect_correlation_id == turn.id
    audit = storage_context.store.query_one(
        """
        SELECT payload_json FROM audit_log
        WHERE action = 'turn.steer_accepted' AND turn_id = ?
        """,
        (turn.id,),
    )
    assert audit is not None
    assert json.loads(str(audit["payload_json"])) == {
        "instruction_hash": sha256_text("focus on the failing bridge tests")
    }
    progress = storage_context.store.query_one(
        """
        SELECT payload_json FROM discord_outbox
        WHERE coalesce_key = ? AND state = 'pending'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (f"turn:{turn.id}:progress",),
    )
    assert progress is not None
    assert "Guidance appended" in str(progress["payload_json"])


def test_schedule_modal_uses_configured_defaults() -> None:
    config = ScheduleConfig(
        default_timezone="Asia/Shanghai",
        default_misfire_policy="skip",
    )
    modal = _ScheduleModal(
        schedule=None,
        custom_id="mi:v1:test",
        default_timezone=config.default_timezone,
        default_misfire_policy=config.default_misfire_policy,
    )
    defaults = {
        item.custom_id: item.default
        for item in modal.children
        if isinstance(item, discord.ui.TextInput)
    }

    assert defaults["schedule_timezone"] == "Asia/Shanghai"
    assert defaults["schedule_misfire"] == "skip"


def test_schedule_timezone_display_includes_dst_specific_numeric_offset() -> None:
    summer = int(
        datetime(2025, 7, 1, 12, tzinfo=UTC).timestamp() * 1000
    )
    winter = int(
        datetime(2025, 1, 1, 12, tzinfo=UTC).timestamp() * 1000
    )

    assert _timezone_with_offset(
        "America/New_York",
        now_ms=summer,
    ) == "America/New_York (UTC-04:00)"
    assert _timezone_with_offset(
        "America/New_York",
        now_ms=winter,
    ) == "America/New_York (UTC-05:00)"


@pytest.mark.asyncio
async def test_schedule_preview_escapes_untrusted_mentions(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="mention-preview-thread",
            requested_thread_id=None,
            provider_session_id="mention-preview-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="bridge-test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    repository = ScheduleRepository(storage_context.store)
    draft = repository.create_draft(
        conversation_id=storage_context.conversation.id,
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        action="create",
        payload={
            "name": "notify <@123456789012345678> @everyone",
            "kind": "cron",
            "expression": "* * * * *",
            "timezone": "UTC",
            "misfire_policy": "latest",
            "prompt_text": "ask <@&223456789012345678> and @here",
            "prompt_hash": "test",
            "next_due_at": 60_000,
        },
        occurrences=(
            {
                "utc_ms": 60_000,
                "local_display": (
                    "1970-01-01T00:01:00+00:00 <@323456789012345678>"
                ),
            },
        ),
        component_nonce="mention-preview",
        expires_at=utc_now_ms() + 60_000,
    )
    bot = _storage_bot(storage_context, tmp_path)
    interaction = Mock()
    interaction.followup = Mock(send=AsyncMock())

    await bot._send_schedule_draft_preview(
        interaction,
        draft=draft,
        full_access=True,
        nonce="mention-preview",
    )

    embed = interaction.followup.send.await_args.kwargs["embed"]
    rendered = "\n".join(str(field.value) for field in embed.fields)
    assert "<@123456789012345678>" not in rendered
    assert "<@&223456789012345678>" not in rendered
    assert "<@323456789012345678>" not in rendered
    assert "@everyone" not in rendered
    assert "@here" not in rendered
    assert "\u200b" in rendered
