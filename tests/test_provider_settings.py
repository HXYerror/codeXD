from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from conftest import StorageContext
from openai_codex import CodexConfig
from test_dynamic_schedule_tool import _activate_runtime_turn

from codexd.application.conversation_locks import ConversationLocks
from codexd.application.session_lifecycle import SessionLifecycleCoordinator
from codexd.domain.observations import ProviderSettings
from codexd.runtime.app_server import DynamicAsyncCodex
from codexd.storage import provider_settings


def test_observations_are_revision_generation_scoped_and_scalar_only(
    storage_context: StorageContext,
) -> None:
    turn, generation = _activate_runtime_turn(storage_context, actor_user_id=400)
    observed = ProviderSettings(
        "dynamic-thread",
        generation,
        {
            "model": "model-b",
            "reasoning_effort": "xhigh",
            "prompt": "secret",
            "personality": "/private/path",
        },
        "thread/settings/updated",
        1000,
        5,
        turn.id,
    )
    provider_settings.save(storage_context.repository, storage_context.project.id, observed)
    stored = provider_settings.load(storage_context.repository, turn.thread_revision_id, turn.id)
    assert stored.values == {"model": "model-b", "reasoning_effort": "xhigh", "personality": None}
    assert not stored.fresh
    for rejected in (
        replace(observed, sequence=4, values={"model": "old"}),
        replace(observed, generation=generation + 1, values={"model": "wrong-runtime"}),
        replace(observed, thread_id="child-thread", values={"model": "child"}),
    ):
        provider_settings.save(storage_context.repository, storage_context.project.id, rejected)
    assert (
        provider_settings.load(storage_context.repository, turn.thread_revision_id).values["model"]
        == "model-b"
    )


@pytest.mark.asyncio
async def test_old_thread_settings_win_over_disk_and_pending_requests(
    storage_context: StorageContext,
) -> None:
    turn, generation = _activate_runtime_turn(storage_context, actor_user_id=400)
    observed = ProviderSettings(
        "dynamic-thread",
        generation,
        {"model": "model-b", "reasoning_effort": "xhigh"},
        "thread/settings/updated",
        123000,
        1,
        turn.id,
        state="active",
    )
    runtimes = Mock()
    runtimes.project_status = AsyncMock(return_value={"state": "ready", "generation": generation})
    runtimes.observed_settings_if_loaded = AsyncMock(return_value=observed)
    runtimes.model_catalog_if_loaded = AsyncMock(return_value=None)
    runtimes.configured_model_if_loaded = AsyncMock(
        side_effect=AssertionError("must not read disk")
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository, runtimes=runtimes, locks=ConversationLocks()
    )
    before = storage_context.repository.get_conversation(storage_context.conversation.id)
    view = await lifecycle.status_view(before.id)
    assert view.behavior.model.value == "model-b"
    assert view.behavior.reasoning_effort.value == "xhigh"
    storage_context.repository.update_conversation_preferences(
        before.id, model_override="next-model"
    )
    assert (await lifecycle.status_view(before.id)).behavior.model.value == "model-b"
    assert (await lifecycle.requested_settings(before.id))["model"] == "next-model"
    assert (await lifecycle.effective_model(before.id)).model == "next-model"
    runtimes.ensure.assert_not_called()
    runtimes.configured_model_if_loaded.assert_not_awaited()
    provider_settings.save(storage_context.repository, storage_context.project.id, observed)
    runtimes.project_status.return_value = {"state": "not_loaded", "generation": generation}
    stale = await lifecycle.provider_settings(before.id)
    assert stale.values["model"] == "model-b" and not stale.fresh
    runtimes.observed_settings_if_loaded.assert_awaited()


@pytest.mark.asyncio
async def test_settings_notification_race_does_not_get_overwritten_by_resume_response() -> None:
    facade = DynamicAsyncCodex(
        CodexConfig(), generation=7, dynamic_tools=(), dynamic_tool_handler=AsyncMock()
    )
    before = facade._settings_sequence
    facade._record_settings(
        "main", SimpleNamespace(model="B", effort="xhigh"), "thread/settings/updated"
    )
    facade._response_settings(
        SimpleNamespace(thread=SimpleNamespace(id="main"), model="A"), "thread/resume", before
    )
    assert facade.settings["main"].values["model"] == "B"
    facade._record_settings("child", SimpleNamespace(model="child"), "thread/settings/updated")
    assert facade.settings["main"].values["model"] == "B"
    assert facade.settings["main"].values["reasoning_effort"] == "xhigh"
    queue = [
        SimpleNamespace(
            method="thread/settings/updated",
            payload=SimpleNamespace(
                thread_id="main", thread_settings=SimpleNamespace(model="C", effort="high")
            ),
        )
    ]

    def next_notification():
        if queue:
            return queue.pop()
        raise OSError("private transport details")

    facade._sync_client = SimpleNamespace(next_notification=next_notification)
    await asyncio.wait_for(facade._observe_settings(), 1)
    assert facade.settings["main"].values["model"] == "C"
    assert not facade.settings["main"].fresh
