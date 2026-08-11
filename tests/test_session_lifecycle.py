from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from conftest import StorageContext

from codexd.application.conversation_locks import ConversationLocks
from codexd.application.session_lifecycle import SessionLifecycleCoordinator
from codexd.domain.conversations import (
    ConversationState,
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    ThreadProviderState,
    ThreadSnapshot,
)
from codexd.domain.models import ModelCatalogSnapshot, ModelDescriptor
from codexd.domain.turns import InterruptOrigin, TurnInput, TurnSource, TurnState
from codexd.errors import ConflictError, InvariantError
from codexd.runtime.errors import AdapterFailure, ProviderOutcomeUnknown
from codexd.runtime.fake import FakeCodexRuntime
from codexd.runtime.port import CompactStartResult
from codexd.runtime.supervisor import RuntimeFactory, RuntimeSupervisor
from codexd.transport.discord.presentation import session_status_embed


def test_revision_listing_does_not_hide_older_sessions(
    storage_context: StorageContext,
) -> None:
    for index in range(105):
        storage_context.repository.activate_thread_revision(
            conversation_id=storage_context.conversation.id,
            identity=ThreadIdentity(
                thread_id=f"history-thread-{index}",
                requested_thread_id=None,
                provider_session_id=f"history-session-{index}",
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

    revisions = storage_context.repository.list_thread_revisions(
        storage_context.conversation.id
    )

    assert len(revisions) == 105


@pytest.mark.asyncio
async def test_session_status_does_not_cold_start_runtime(
    storage_context: StorageContext,
) -> None:
    factory_calls = 0

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        nonlocal factory_calls
        factory_calls += 1
        return FakeCodexRuntime()

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    try:
        view = await lifecycle.status_view(storage_context.conversation.id)

        assert factory_calls == 0
        assert view.activity.runtime_state == "not_loaded"
        assert view.activity.runtime_generation == 0
        assert view.behavior.model.value == "provider default"
        assert view.behavior.model.source == "provider default"
        assert view.behavior.resolution == "resolves on next Turn"
        assert view.degraded_reason is None
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_session_status_model_source_precedence(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    try:
        await supervisor.ensure(
            storage_context.repository.get_project(storage_context.project.id)
        )
        provider = await lifecycle.status_view(storage_context.conversation.id)
        assert provider.behavior.model.value == "fake-model"
        assert provider.behavior.model.source == "provider default"
        assert provider.behavior.reasoning_effort.value == "medium"
        assert provider.behavior.reasoning_effort.source == "model default"
        assert provider.behavior.service_tier.value == "flex"
        assert provider.behavior.input_modalities == ("text", "image")

        with storage_context.store.transaction() as connection:
            connection.execute(
                "UPDATE projects SET default_model = 'fake-model' WHERE id = ?",
                (storage_context.project.id,),
            )
        project = await lifecycle.status_view(storage_context.conversation.id)
        assert project.behavior.model.value == "fake-model"
        assert project.behavior.model.source == "project default"

        storage_context.repository.update_conversation_preferences(
            storage_context.conversation.id,
            model_override="fake-model",
        )
        conversation = await lifecycle.status_view(
            storage_context.conversation.id
        )
        assert conversation.behavior.model.value == "fake-model"
        assert conversation.behavior.model.source == "conversation override"
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_session_status_resolves_sources_and_active_turn_drift(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            UPDATE projects
            SET default_model = 'old-project-model',
                default_reasoning_effort = 'low',
                default_reasoning_summary = 'project-summary',
                default_personality = 'pragmatic',
                default_service_tier = 'flex'
            WHERE id = ?
            """,
            (storage_context.project.id,),
        )
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="status-thread",
            requested_thread_id=None,
            provider_session_id="status-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="status-runtime",
        ),
        config=ThreadConfig(
            model="old-project-model",
            personality="pragmatic",
            sandbox=SandboxProfile.FULL_ACCESS,
            service_tier="flex",
        ),
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="status snapshot drift"),
        input_message_id="status-snapshot-drift",
    )
    repository.update_conversation_preferences(
        storage_context.conversation.id,
        model_override="fake-model",
        reasoning_effort_override="high",
        reasoning_summary_override="concise",
    )

    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    try:
        _runtime, lease = await supervisor.ensure(
            repository.get_project(storage_context.project.id)
        )
        repository.claim_turn(
            turn.id,
            runtime_lease_id=lease.id,
            runtime_generation=lease.generation,
        )
        running = repository.mark_turn_running(turn.id, "status-provider-turn")

        view = await lifecycle.status_view(storage_context.conversation.id)

        assert view.behavior.model.value == "fake-model"
        assert view.behavior.model.source == "conversation override"
        assert view.behavior.reasoning_effort.value == "high"
        assert view.behavior.reasoning_effort.source == "conversation override"
        assert view.behavior.reasoning_summary.value == "concise"
        assert view.behavior.personality.value == "pragmatic"
        assert view.behavior.personality.source == "project default"
        assert view.behavior.service_tier.value == "flex"
        assert view.behavior.input_modalities == ("text", "image")
        assert view.behavior.resolution == "resolved"
        assert view.activity.active_turn == running
        assert view.activity.active_settings_differ
        assert view.resume_verification == "verified by active provider Turn"

        embed = session_status_embed(view)
        payload = embed.to_dict()
        assert payload["title"] == "🟢 Session active"
        assert [field["name"] for field in payload["fields"]] == [
            "Model & behavior · next Turn",
            "Activity",
            "Session",
            "Execution",
        ]
        rendered = json.dumps(payload, ensure_ascii=False)
        assert "fake-model" in rendered
        assert "old-project-model" in rendered
        assert "differs from next Turn" in rendered
        assert "Optional capabilities" not in rendered
        assert str(storage_context.root) not in rendered
    finally:
        repository.terminal_turn(
            turn.id,
            target=TurnState.INTERRUPTED,
            terminal_code="status_test_cleanup",
        )
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_session_status_catalog_failure_degrades_without_failing(
    storage_context: StorageContext,
) -> None:
    runtimes = Mock()
    runtimes.project_status = AsyncMock(
        return_value={"state": "ready", "generation": 3, "failures": 0}
    )
    runtimes.model_catalog_if_loaded = AsyncMock(
        side_effect=RuntimeError("private path must not escape")
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=runtimes,
        locks=ConversationLocks(),
    )

    view = await lifecycle.status_view(storage_context.conversation.id)

    assert view.activity.runtime_state == "ready"
    assert view.degraded_reason == "model catalog unavailable"
    assert "private path" not in json.dumps(
        session_status_embed(view).to_dict(), ensure_ascii=False
    )

    hostile = replace(view, project_name="@everyone " + ("项目😀" * 200))
    payload = session_status_embed(hostile).to_dict()
    rendered = json.dumps(payload, ensure_ascii=False)
    assert "@everyone" not in rendered
    assert len(str(payload["description"])) <= 4096
    assert all(len(str(field["value"])) <= 1024 for field in payload["fields"])


@pytest.mark.asyncio
async def test_session_lifecycle_uses_provider_threads_and_preserves_history(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    try:
        first = await lifecycle.new(storage_context.conversation.id)
        assert first.provider_thread_id == "fake-thread-1"

        forked = await lifecycle.fork(storage_context.conversation.id)
        assert forked.parent_revision_id == first.id
        assert forked.provider_session_id == first.provider_session_id
        assert storage_context.repository.resolve_thread_revision(
            storage_context.conversation.id, first.id[:8]
        ).state == "superseded"

        archived = await lifecycle.archive(storage_context.conversation.id)
        assert archived.state is ConversationState.ARCHIVED

        resumed = await lifecycle.resume(
            storage_context.conversation.id, forked.id[:8]
        )
        assert resumed.id == forked.id
        assert resumed.state == "active"

        web_search = await lifecycle.set_web_search(
            storage_context.conversation.id, "live"
        )
        assert web_search.web_search_mode == "live"
        active = storage_context.repository.get_active_revision(web_search.id)
        assert active is not None
        assert json.loads(active.thread_config_json)["web_search_mode"] == "live"

        model = await lifecycle.set_model(
            storage_context.conversation.id, "fake-model"
        )
        assert model.model_override == "fake-model"
        reasoning = await lifecycle.set_reasoning_effort(
            storage_context.conversation.id, "high"
        )
        assert reasoning.reasoning_effort_override == "high"
        summary = await lifecycle.set_reasoning_summary(
            storage_context.conversation.id, "concise"
        )
        assert summary.reasoning_summary_override == "concise"
        personality = await lifecycle.set_personality(
            storage_context.conversation.id, "friendly"
        )
        assert personality.personality_override == "friendly"
        service_tier = await lifecycle.set_service_tier(
            storage_context.conversation.id, "flex"
        )
        assert service_tier.service_tier_override == "flex"
        configured_revision = storage_context.repository.get_active_revision(
            storage_context.conversation.id
        )
        assert configured_revision is not None
        configured = json.loads(configured_revision.thread_config_json)
        assert configured["personality"] == "friendly"
        assert configured["service_tier"] == "flex"
        snapshot = storage_context.repository.enqueue_turn(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="snapshot preferences"),
            input_message_id="snapshot-preferences",
        )
        assert snapshot.effective_reasoning_summary == "concise"
        assert snapshot.effective_personality == "friendly"
        assert snapshot.effective_service_tier == "flex"
        storage_context.repository.request_cancel(
            snapshot.id, origin=InterruptOrigin.USER
        )

        renamed = await lifecycle.rename(
            storage_context.conversation.id, "Codex planning"
        )
        assert renamed.name == "Codex planning"
        rename_outbox = None
        while rename_outbox is None:
            candidate = storage_context.repository.claim_outbox(worker_id="test")
            assert candidate is not None
            payload = json.loads(candidate.payload_json)
            if payload.get("kind") == "thread_rename":
                rename_outbox = candidate
                break
            storage_context.repository.ack_outbox(
                candidate.id,
                lease_owner=candidate.lease_owner,
                lease_attempt=candidate.attempts,
                discord_message_id="progress-message",
            )
        assert rename_outbox.operation == "edit"
        assert json.loads(rename_outbox.payload_json) == {
            "kind": "thread_rename",
            "name": "Codex planning",
        }

        cleared = await lifecycle.clear(storage_context.conversation.id)
        assert cleared.state is ConversationState.UNINITIALIZED
        assert cleared.active_revision_id is None
        assert len(await lifecycle.list_revisions(cleared.id)) == 2

        await lifecycle.new(storage_context.conversation.id)
        await lifecycle.compact(storage_context.conversation.id)
        compacting = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        assert compacting.provider_barrier_kind == "compact"
        storage_context.repository.set_provider_barrier(
            storage_context.conversation.id, "external_active"
        )
        assert (
            storage_context.repository.get_conversation(
                storage_context.conversation.id
            ).provider_barrier_kind
            == "compact"
        )
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_session_command_effect_starts_only_after_preconditions(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    try:
        storage_context.repository.accept_command_intent(
            interaction_id="session-new-success",
            command_name="session new",
            request={},
            boot_id="lifecycle-test",
            actor_user_id=400,
            project_id=storage_context.project.id,
            conversation_id=storage_context.conversation.id,
        )
        await lifecycle.new(
            storage_context.conversation.id,
            interaction_id="session-new-success",
        )
        started = storage_context.repository.get_command_intent("session-new-success")
        assert started.state == "succeeded"
        assert started.effect_kind == "session_new"
        assert started.effect_correlation_id == storage_context.conversation.id
        assert (
            storage_context.repository.get_conversation(
                storage_context.conversation.id
            ).provider_barrier_kind
            is None
        )

        queued = storage_context.repository.enqueue_turn(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="keep session busy"),
            input_message_id="session-precondition",
        )
        storage_context.repository.accept_command_intent(
            interaction_id="session-new-rejected",
            command_name="session new",
            request={},
            boot_id="lifecycle-test",
            actor_user_id=400,
            project_id=storage_context.project.id,
            conversation_id=storage_context.conversation.id,
        )
        with pytest.raises(ConflictError):
            await lifecycle.new(
                storage_context.conversation.id,
                interaction_id="session-new-rejected",
            )
        rejected = storage_context.repository.get_command_intent(
            "session-new-rejected"
        )
        assert rejected.state == "accepted"
        assert rejected.effect_kind is None
        storage_context.repository.request_cancel(
            queued.id,
            origin=InterruptOrigin.USER,
        )
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_text_only_catalog_model_can_be_selected_for_text_turns(
    storage_context: StorageContext,
) -> None:
    class MixedModalityRuntime(FakeCodexRuntime):
        async def list_models(self) -> ModelCatalogSnapshot:
            base = await super().list_models()
            return ModelCatalogSnapshot(
                models=(
                    *base.models,
                    ModelDescriptor(
                        id="text-only",
                        model="text-only",
                        is_default=False,
                        input_modalities=("text",),
                        supported_reasoning_efforts=("medium",),
                        default_reasoning_effort="medium",
                        supports_personality=False,
                        service_tiers=(),
                        default_service_tier=None,
                        upgrade=None,
                    ),
                ),
                complete=True,
                next_cursor=None,
            )

    fake = MixedModalityRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    try:
        updated = await lifecycle.set_model(
            storage_context.conversation.id,
            "text-only",
        )
        assert updated.model_override == "text-only"
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_session_mutation_rejects_queued_turn(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="queued"),
        input_message_id="queued-message",
    )
    try:
        with pytest.raises(ConflictError, match="queued or active Turn"):
            await lifecycle.new(storage_context.conversation.id)
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_resume_rejects_same_thread_with_different_provider_session(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
    revision = await lifecycle.new(storage_context.conversation.id)

    async def mismatching_resume(
        *,
        thread_id: str,
        cwd: object,
        config: object,
    ) -> ThreadIdentity:
        del cwd, config
        return ThreadIdentity(
            thread_id=thread_id,
            requested_thread_id=thread_id,
            provider_session_id="different-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="fake",
        )

    monkeypatch.setattr(fake, "resume_thread", mismatching_resume)
    try:
        with pytest.raises(InvariantError, match="different thread identity"):
            await lifecycle.resume(
                storage_context.conversation.id,
                revision.id[:8],
            )
        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        incident = storage_context.store.query_one(
            """
            SELECT details_json
            FROM incidents
            WHERE conversation_id = ? AND code = 'provider_thread_identity_mismatch'
            """,
            (storage_context.conversation.id,),
        )
        assert conversation.state is ConversationState.BLOCKED
        assert incident is not None
        details_json = str(incident["details_json"])
        assert "different-session" not in details_json
        assert revision.provider_session_id not in details_json
        assert set(json.loads(details_json)) == {
            "actual_session_hash",
            "actual_thread_hash",
            "expected_session_hash",
            "expected_thread_hash",
        }
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_fork_rejects_invalid_lineage_without_persisting_provider_ids(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _supervisor(storage_context, factory)
    lifecycle = _lifecycle(storage_context, supervisor)
    source = await lifecycle.new(storage_context.conversation.id)

    async def mismatching_fork(**_kwargs: object) -> ThreadIdentity:
        return ThreadIdentity(
            thread_id="unexpected-fork-thread",
            requested_thread_id=None,
            provider_session_id="unexpected-fork-session",
            forked_from_thread_id=source.provider_thread_id,
            parent_thread_id=None,
            provider_version="fake",
        )

    monkeypatch.setattr(fake, "fork_thread", mismatching_fork)
    try:
        with pytest.raises(InvariantError, match="fork identity validation"):
            await lifecycle.fork(storage_context.conversation.id)

        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        incident = storage_context.store.query_one(
            """
            SELECT details_json
            FROM incidents
            WHERE conversation_id = ? AND code = 'fork_identity_mismatch'
            """,
            (storage_context.conversation.id,),
        )
        assert conversation.state is ConversationState.BLOCKED
        assert conversation.provider_barrier_kind == "unknown_effect"
        assert incident is not None
        details_json = str(incident["details_json"])
        assert source.provider_thread_id not in details_json
        assert "unexpected-fork-thread" not in details_json
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_provider_success_local_commit_failure_blocks_conversation(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    lifecycle = SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )

    def fail_commit(**_kwargs: object) -> None:
        raise OSError("simulated local commit failure")

    monkeypatch.setattr(
        storage_context.repository,
        "activate_thread_revision",
        fail_commit,
    )
    try:
        with pytest.raises(OSError, match="simulated local commit failure"):
            await lifecycle.new(storage_context.conversation.id)
        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        assert conversation.state is ConversationState.BLOCKED
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_new_outcome_unknown_blocks_replay(
    storage_context: StorageContext,
) -> None:
    class UnknownStartRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.start_calls = 0

        async def start_thread(
            self,
            *,
            cwd: Path,
            config: ThreadConfig,
        ) -> ThreadIdentity:
            self.start_calls += 1
            identity = await super().start_thread(cwd=cwd, config=config)
            raise _provider_outcome_unknown(identity.thread_id)

    fake = UnknownStartRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _supervisor(storage_context, factory)
    lifecycle = _lifecycle(storage_context, supervisor)
    try:
        with pytest.raises(ProviderOutcomeUnknown):
            await lifecycle.new(storage_context.conversation.id)
        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        assert conversation.state is ConversationState.BLOCKED
        assert conversation.provider_barrier_kind == "unknown_effect"

        with pytest.raises((ConflictError, InvariantError)):
            await lifecycle.new(storage_context.conversation.id)
        assert fake.start_calls == 1
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["fork", "unarchive", "archive"])
async def test_existing_thread_mutation_outcome_unknown_blocks_replay(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _supervisor(storage_context, factory)
    lifecycle = _lifecycle(storage_context, supervisor)
    revision = await lifecycle.new(storage_context.conversation.id)
    calls = 0
    if operation == "unarchive":
        await lifecycle.archive(storage_context.conversation.id)

        async def fail_unarchive(_thread_id: str) -> ThreadIdentity:
            nonlocal calls
            calls += 1
            raise _provider_outcome_unknown("unarchived-thread")

        monkeypatch.setattr(fake, "unarchive_thread", fail_unarchive)
    elif operation == "fork":

        async def fail_fork(**_kwargs: object) -> ThreadIdentity:
            nonlocal calls
            calls += 1
            raise _provider_outcome_unknown("forked-thread")

        monkeypatch.setattr(fake, "fork_thread", fail_fork)
    else:

        async def fail_archive(_thread_id: str) -> None:
            nonlocal calls
            calls += 1
            raise _provider_outcome_unknown("archived-thread")

        monkeypatch.setattr(fake, "archive_thread", fail_archive)

    try:
        with pytest.raises(ProviderOutcomeUnknown):
            if operation == "fork":
                await lifecycle.fork(storage_context.conversation.id)
            elif operation == "unarchive":
                await lifecycle.resume(
                    storage_context.conversation.id,
                    revision.id[:8],
                )
            else:
                await lifecycle.archive(storage_context.conversation.id)
        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        assert conversation.state is ConversationState.BLOCKED
        assert conversation.provider_barrier_kind == "unknown_effect"

        with pytest.raises((ConflictError, InvariantError)):
            if operation == "fork":
                await lifecycle.fork(storage_context.conversation.id)
            elif operation == "unarchive":
                await lifecycle.resume(
                    storage_context.conversation.id,
                    revision.id[:8],
                )
            else:
                await lifecycle.archive(storage_context.conversation.id)
        assert calls == 1
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_non_archived_resume_commit_failure_is_fenced(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _supervisor(storage_context, factory)
    lifecycle = _lifecycle(storage_context, supervisor)
    revision = await lifecycle.new(storage_context.conversation.id)
    storage_context.repository.accept_command_intent(
        interaction_id="resume-commit-failure",
        command_name="session resume",
        request={"revision": revision.id[:8]},
        boot_id="lifecycle-test",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
    )

    def fail_commit(**_kwargs: object) -> None:
        raise OSError("simulated resume commit failure")

    monkeypatch.setattr(
        storage_context.repository,
        "activate_thread_revision",
        fail_commit,
    )
    try:
        with pytest.raises(OSError, match="simulated resume commit failure"):
            await lifecycle.resume(
                storage_context.conversation.id,
                revision.id[:8],
                interaction_id="resume-commit-failure",
            )

        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        intent = storage_context.repository.get_command_intent(
            "resume-commit-failure"
        )
        assert conversation.state is ConversationState.BLOCKED
        assert conversation.provider_barrier_kind == "unknown_effect"
        assert intent.state == "unknown"
    finally:
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_compact_unknown_keeps_conversation_queueable_until_idle(
    storage_context: StorageContext,
) -> None:
    class UnknownCompactRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.allow_read = asyncio.Event()

        async def compact_thread(self, thread_id: str) -> CompactStartResult:
            self._thread_states[thread_id] = ThreadProviderState.ACTIVE
            raise _provider_outcome_unknown(thread_id)

        async def read_thread(self, thread_id: str) -> ThreadSnapshot:
            await self.allow_read.wait()
            return await super().read_thread(thread_id)

    fake = UnknownCompactRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _supervisor(storage_context, factory)
    lifecycle = _lifecycle(storage_context, supervisor)
    revision = await lifecycle.new(storage_context.conversation.id)
    storage_context.repository.accept_command_intent(
        interaction_id="compact-unknown",
        command_name="session compact",
        request={},
        boot_id="lifecycle-test",
        actor_user_id=400,
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
    )
    try:
        with pytest.raises(ProviderOutcomeUnknown):
            await lifecycle.compact(
                storage_context.conversation.id,
                interaction_id="compact-unknown",
            )

        compacting = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        intent = storage_context.repository.get_command_intent("compact-unknown")
        queued = storage_context.repository.enqueue_turn(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="queue behind compact"),
            input_message_id="compact-queued-input",
        )
        assert compacting.state is ConversationState.ACTIVE
        assert compacting.provider_barrier_kind == "compact"
        assert intent.state == "unknown"
        assert queued.state.value == "queued"

        fake._thread_states[revision.provider_thread_id] = ThreadProviderState.IDLE
        fake.allow_read.set()
        for _ in range(100):
            reconciled = storage_context.repository.get_conversation(
                storage_context.conversation.id
            )
            if reconciled.provider_barrier_kind is None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("compact barrier did not reconcile at provider idle")

        assert reconciled.state is ConversationState.ACTIVE
        assert storage_context.repository.get_turn(queued.id).state.value == "queued"
    finally:
        fake.allow_read.set()
        await lifecycle.close()
        await supervisor.close()


@pytest.mark.asyncio
async def test_unknown_effect_barrier_is_blocked_not_reconciled_as_idle(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _supervisor(storage_context, factory)
    lifecycle = _lifecycle(storage_context, supervisor)
    storage_context.repository.set_provider_barrier(
        storage_context.conversation.id,
        "unknown_effect",
    )
    try:
        await lifecycle.restore_provider_barriers()
        for _ in range(100):
            task = lifecycle._barrier_tasks.get(storage_context.conversation.id)
            if task is None or task.done():
                break
            await asyncio.sleep(0.01)
        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )
        assert conversation.state is ConversationState.BLOCKED
        assert conversation.provider_barrier_kind == "unknown_effect"
    finally:
        await lifecycle.close()
        await supervisor.close()


def _provider_outcome_unknown(thread_id: str) -> ProviderOutcomeUnknown:
    return ProviderOutcomeUnknown(
        AdapterFailure(
            code="provider_effect_outcome_unknown",
            provider_exception="ReadBackFailure",
            message="provider mutation identity read-back failed",
            retryable=False,
            runtime_generation=1,
            thread_id=thread_id,
        )
    )


def _supervisor(
    storage_context: StorageContext,
    factory: RuntimeFactory,
) -> RuntimeSupervisor:
    return RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )


def _lifecycle(
    storage_context: StorageContext,
    supervisor: RuntimeSupervisor,
) -> SessionLifecycleCoordinator:
    return SessionLifecycleCoordinator(
        repository=storage_context.repository,
        runtimes=supervisor,
        locks=ConversationLocks(),
    )
