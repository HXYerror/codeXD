from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from conftest import StorageContext

from codexd.application.conversation_locks import ConversationLocks
from codexd.application.session_lifecycle import SessionLifecycleCoordinator
from codexd.domain.models import EffectiveModel
from codexd.errors import InvariantError
from codexd.runtime.codex_sdk import CodexSDKRuntime, capability_manifest
from codexd.runtime.fake import FakeCodexRuntime
from codexd.runtime.port import RuntimeSlotConfig


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "override,project,configured,expected,source",
    [
        (None, None, "outside-catalog", "outside-catalog", "Codex effective config"),
        (None, None, "fake-model", "fake-model", "Codex effective config"),
        (None, "project-model", "configured", "project-model", "project default"),
        ("chosen", "project-model", "configured", "chosen", "conversation override"),
        (None, None, None, None, "unknown"),
    ],
)
async def test_effective_identity_never_uses_catalog_default(
    storage_context: StorageContext,
    override: str | None,
    project: str | None,
    configured: str | None,
    expected: str | None,
    source: str,
) -> None:
    runtime = FakeCodexRuntime()
    runtime.configured_model = AsyncMock(
        return_value=EffectiveModel(
            configured,
            "Codex effective config" if configured else "unknown",
        )
    )
    runtimes = Mock()
    runtimes.ensure = AsyncMock(return_value=(runtime, None))
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=runtimes,
        locks=ConversationLocks(),
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE projects SET default_model = ? WHERE id = ?",
            (project, storage_context.project.id),
        )
    before = storage_context.repository.update_conversation_preferences(
        storage_context.conversation.id,
        model_override=override,
    )
    catalog = await runtime.list_models()
    resolved = await lifecycle.effective_model(before.id, catalog)
    assert (resolved.model, resolved.source) == (expected, source)
    assert (resolved.descriptor is not None) == (expected == "fake-model")
    assert storage_context.repository.get_conversation(before.id) == before
    assert storage_context.repository.get_active_revision(before.id) is None
    if override or project:
        runtime.configured_model.assert_not_awaited()
    else:
        runtime.configured_model.assert_awaited_once_with(storage_context.root)
    if expected != "fake-model":
        with pytest.raises(InvariantError, match="capabilities are unknown"):
            await lifecycle.model_descriptor(before.id, catalog)


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True, "invalid-schema"])
async def test_sdk_config_read_is_scoped_read_only_and_degrades_to_unknown(
    tmp_path: Path,
    fails: bool | str,
) -> None:
    request = AsyncMock(return_value=SimpleNamespace(config=SimpleNamespace(model="configured")))
    if fails:
        request.side_effect = TimeoutError("secret path")
    if fails == "invalid-schema":
        from openai_codex.generated.v2_all import ConfigReadResponse
        from pydantic import ValidationError
        with pytest.raises(ValidationError) as invalid:
            ConfigReadResponse.model_validate({"private": "secret path"})
        request.side_effect = invalid.value
    runtime = CodexSDKRuntime(
        client=cast(Any, SimpleNamespace(_client=SimpleNamespace(request=request))),
        slot=RuntimeSlotConfig("project", "project", tmp_path, None, {}, "hash", "project_scoped"),
        generation=1,
        manifest=capability_manifest(),
    )
    result = await runtime.configured_model(tmp_path)
    assert result.model == (None if fails else "configured")
    assert "secret" not in result.source
    assert request.await_args.args == (
        "config/read",
        {"cwd": str(tmp_path), "includeLayers": False},
    )


@pytest.mark.asyncio
async def test_model_show_missing_catalog_model_preserves_identity_and_active_boundary(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    import discord
    from test_discord_bridge import _interaction, _storage_bot

    bot = _storage_bot(storage_context, tmp_path)
    catalog = await FakeCodexRuntime().list_models()
    bot.session_lifecycle.model_catalog = AsyncMock(return_value=catalog)
    bot.session_lifecycle.effective_model = AsyncMock(
        return_value=EffectiveModel("outside-catalog", "Codex effective config"),
    )
    # An active Turn's requested model must not be labeled as an observation.
    from codexd.domain.turns import TurnInput, TurnSource

    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="test"),
        input_message_id="model-active",
    )
    bot.repository.active_turn_for_conversation = Mock(
        return_value=replace(turn, effective_model="old")
    )
    thread = Mock(spec=discord.Thread)
    thread.id, thread.parent_id = 300, 200
    interaction = _interaction(7200, thread=thread, text_channel=Mock(), in_thread=True)
    await bot._model_show(interaction)
    content = interaction.followup.send.await_args.args[0]
    assert "outside-catalog" in content and "unknown / not reported" in content
    assert "Current Turn observed model: `not reported`" in content
    assert "Current Turn requested model: `old`" in content
    assert "fake-model" not in content


@pytest.mark.asyncio
async def test_plain_text_turn_rejects_ambiguous_model_catalog(
    storage_context: StorageContext,
) -> None:
    from codexd.application.turn_coordinator import TurnCoordinator
    from codexd.domain.turns import TurnInput, TurnSource

    runtime = FakeCodexRuntime()
    catalog = await runtime.list_models()
    runtime.list_models = AsyncMock(return_value=replace(
        catalog, models=(catalog.models[0], catalog.models[0]),
    ))
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id, source=TurnSource.DISCORD,
        turn_input=TurnInput(text="hello"), input_message_id="ambiguous-model",
    )
    coordinator = cast(Any, SimpleNamespace(_repository=storage_context.repository))
    with pytest.raises(InvariantError, match="ambiguous"):
        await TurnCoordinator._validate_turn_catalog(
            coordinator, runtime, turn=turn, turn_input=TurnInput(text="hello"),
        )
