from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from conftest import StorageContext
from test_discord_file_integration import _turn_image
from test_runtime_turns import _model, _runtime_supervisor, _turn_coordinator, _wait_for_terminal

from codexd.domain.conversations import (
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    ThreadProviderState,
)
from codexd.domain.models import ModelCatalogSnapshot
from codexd.domain.observations import ProviderSettings
from codexd.domain.turns import TurnInput, TurnSource, TurnState
from codexd.runtime.fake import FakeCodexRuntime
from codexd.transport.discord.outbox import _attachment_failure_guidance


class ImageRuntime(FakeCodexRuntime):
    def __init__(self, generation, *, cold=False, failure=None):
        super().__init__(generation=generation)
        self.cold = cold
        self.fresh = False  # Completed text Turn with no settings notification.
        self.failure = failure
        self.resumes = 0
        self.configs = []
        self.identity = ThreadIdentity("old-thread", None, "old-session", None, None, "fake")
        if not cold:
            self._threads["old-thread"] = self.identity
            self._thread_states["old-thread"] = ThreadProviderState.IDLE

    async def configured_model(self, cwd):
        raise AssertionError("Image execution must not use global model A")

    async def list_models(self):
        models = (
            _model("global-a", is_default=True, modalities=("text",)),
            _model("old-b", is_default=False, modalities=("text", "image")),
        )
        if self.failure == "text_only":
            models = (models[0], replace(models[1], input_modalities=("text",)))
        elif self.failure == "not_in_catalog":
            models = (models[0],)
        elif self.failure == "ambiguous":
            models = (*models, models[1])
        # Keep the runtime's global image-capability preflight satisfied while
        # independently checking the actual thread model.
        models = (*models, _model("other-image", is_default=False, modalities=("text", "image")))
        return ModelCatalogSnapshot(models, self.failure != "incomplete", None)

    async def resume_thread(self, *, thread_id, cwd, config):
        self.resumes += 1
        self.fresh = True
        self._threads[thread_id] = self.identity
        self._thread_states[thread_id] = ThreadProviderState.IDLE
        return replace(self.identity, requested_thread_id=thread_id)

    async def observed_settings(self, thread_id):
        if self.failure == "missing":
            return None
        return ProviderSettings(
            thread_id,
            self.generation + (self.failure == "generation"),
            {"model": None if self.failure == "unconfirmed" else "old-b"},
            "thread/resume",
            1000,
            self.resumes + 1,
            fresh=self.fresh,
            state="idle",
        )

    async def start_turn(self, **kwargs):
        self.configs.append(kwargs["config"])
        self.fresh = False
        return await super().start_turn(**kwargs)


def setup(storage_context, *, cold=False, failure=None):
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity("old-thread", None, "old-session", None, None, "fake"),
        config=ThreadConfig(None, None, SandboxProfile.FULL_ACCESS),
    )
    runtimes = []

    async def factory(slot, generation):
        runtime = ImageRuntime(generation, cold=cold, failure=failure)
        runtimes.append(runtime)
        return runtime

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    image = _turn_image(storage_context.store.path.parent, attachment_id="image48", ordinal=0)
    return coordinator, supervisor, runtimes, image


@pytest.mark.asyncio
@pytest.mark.parametrize("cold", [False, True])
async def test_stale_or_cold_old_thread_image_admission(storage_context: StorageContext, cold):
    coordinator, supervisor, runtimes, image = setup(storage_context, cold=cold)
    try:
        # No provider access is necessary to durably accept an image.
        wake = coordinator._mailboxes.wake
        coordinator._mailboxes.wake = AsyncMock()
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(images=(image,)),
            input_message_id="img48",
        )
        assert runtimes == [] and turn.state is TurnState.QUEUED
        storage_context.repository.update_conversation_preferences(
            turn.conversation_id, model_override="global-a"
        )
        coordinator._mailboxes.wake = wake
        await coordinator.wake(turn.conversation_id)
        terminal = await _wait_for_terminal(storage_context, turn.id)
        assert terminal.state is TurnState.COMPLETED
        assert len(runtimes[0].started_inputs) == 1
        assert runtimes[0].started_inputs[0].images == (image,)
        assert runtimes[0].configs[0].model == "old-b"  # Pin only the verified execution choice.
        assert runtimes[0].resumes >= 1
        assert (
            storage_context.repository.get_conversation(turn.conversation_id).model_override
            == "global-a"
        )
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,code",
    [
        ("text_only", "image_model_text_only"),
        ("not_in_catalog", "image_model_not_in_catalog"),
        ("ambiguous", "image_model_ambiguous"),
        ("incomplete", "image_catalog_incomplete"),
        ("unconfirmed", "image_model_unconfirmed"),
        ("generation", "image_model_unconfirmed"),
        ("missing", "image_model_unconfirmed"),
    ],
)
async def test_image_failure_is_explicit_and_never_sent(
    storage_context: StorageContext, failure, code
):
    coordinator, supervisor, runtimes, image = setup(storage_context, failure=failure)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(images=(image,)),
            input_message_id="img48",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)
        assert terminal.state is TurnState.FAILED and terminal.terminal_code == code
        assert runtimes[0].started_inputs == []
        assert "not sent" in _attachment_failure_guidance(code)
        assert (
            storage_context.repository.get_conversation(turn.conversation_id).recovery_reason
            is None
        )
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_discord_image_reaches_execution_after_stale_observation(storage_context, tmp_path):
    from unittest.mock import Mock

    import discord
    from test_discord_file_integration import _attachment, _bot, _message

    from codexd.transport.discord.attachments import DiscordAttachmentIngestResult

    coordinator, supervisor, runtimes, image = setup(storage_context)
    bot, user = _bot(storage_context, tmp_path)
    bot.turns = coordinator
    bot._attachment_ingestor = Mock(
        ingest=AsyncMock(
            return_value=DiscordAttachmentIngestResult(images=(image,)),
        )
    )
    channel = Mock(spec=discord.Thread)
    channel.id, channel.parent_id = 300, 200
    channel.archived = channel.locked = False
    message = _message(
        message_id=4848,
        channel=channel,
        content="inspect",
        attachments=[_attachment(0, "test.png", content_type="image/png")],
        bot_user=user,
        mentioned=False,
    )
    try:
        await bot._handle_message(message)
        row = storage_context.store.query_one("SELECT id FROM turns WHERE input_message_id='4848'")
        assert row is not None
        terminal = await _wait_for_terminal(storage_context, row["id"])
        assert terminal.state is TurnState.COMPLETED
        assert len(runtimes[0].started_inputs) == 1
        assert runtimes[0].started_inputs[0].images == (image,)
        bot._attachment_ingestor.cleanup.assert_not_called()
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()
