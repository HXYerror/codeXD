from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path

import pytest
from conftest import StorageContext

from codexd.application import turn_coordinator as turn_coordinator_module
from codexd.application.turn_coordinator import TurnCoordinator
from codexd.domain.conversations import (
    ConversationState,
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    TurnConfig,
)
from codexd.domain.events import NormalizedEvent
from codexd.domain.ids import sha256_file
from codexd.domain.models import AccountStatus, ModelCatalogSnapshot, ModelDescriptor
from codexd.domain.schedules import MisfirePolicy, ScheduleKind
from codexd.domain.turns import (
    InterruptOrigin,
    TurnFile,
    TurnIdentity,
    TurnInput,
    TurnSource,
    TurnState,
)
from codexd.errors import ConflictError, InvariantError
from codexd.runtime.errors import (
    AdapterFailure,
    AdapterInvariantError,
    EventJournalError,
    InterruptFailed,
    ProviderOutcomeUnknown,
    RuntimeUnavailable,
)
from codexd.runtime.fake import FakeCodexRuntime
from codexd.runtime.mailbox import MailboxRegistry
from codexd.runtime.port import StartedTurn
from codexd.runtime.supervisor import RuntimeFactory, RuntimeSupervisor
from codexd.security import private_files
from codexd.storage.projectors import ProjectingEventSink
from codexd.storage.records import TurnRecord
from codexd.storage.schedules import ScheduleRepository


@pytest.mark.asyncio
async def test_fake_runtime_turn_is_durable_end_to_end(
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
    coordinator = TurnCoordinator(
        repository=storage_context.repository,
        runtime_supervisor=supervisor,
        event_sink=ProjectingEventSink(
            storage_context.store, correlation_key=b"x" * 32
        ),
    )
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="hello"),
            input_message_id="discord-1",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)

        assert terminal.state is TurnState.COMPLETED
        events = storage_context.store.query_all(
            "SELECT kind FROM events WHERE turn_id = ? ORDER BY local_event_index",
            (turn.id,),
        )
        assert [row["kind"] for row in events] == [
            "turn.started",
            "assistant.text.completed",
            "turn.completed",
        ]
        outbox = storage_context.store.query_one(
            "SELECT operation, state FROM discord_outbox WHERE dedupe_key = ?",
            (f"turn:{turn.id}:final",),
        )
        assert outbox is not None
        assert outbox["operation"] == "send"
        assert outbox["state"] == "pending"
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_file_only_turn_does_not_require_image_model_modality(
    storage_context: StorageContext,
) -> None:
    class TextDefaultRuntime(FakeCodexRuntime):
        async def list_models(self) -> ModelCatalogSnapshot:
            return ModelCatalogSnapshot(
                models=(
                    _model("text-only", is_default=True, modalities=("text",)),
                    _model("image-capable", is_default=False, modalities=("text", "image")),
                ),
                complete=True,
                next_cursor=None,
            )

    fake = TextDefaultRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    file = _stored_turn_file(storage_context, name="notes.txt")
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(files=(file,)),
            input_message_id="file-only-text-model",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)

        assert terminal.state is TurnState.COMPLETED
        assert len(fake.started_inputs) == 1
        assert fake.started_inputs[0].text is None
        assert fake.started_inputs[0].files == (file,)
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_missing_mention_capability_fails_file_turn_before_runtime_start(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime(mention_input_supported=False)

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    file = _stored_turn_file(storage_context, name="private.txt")
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(files=(file,)),
            input_message_id="unsupported-file-input",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)

        assert terminal.state is TurnState.FAILED
        assert terminal.terminal_code == "file_input_unsupported"
        assert terminal.error_code == "file_input_unsupported"
        assert terminal.error_message_redacted is not None
        assert str(file.canonical_path) not in terminal.error_message_redacted
        assert fake.started_inputs == []
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_changed_file_fails_with_integrity_code_before_provider_turn(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    file = _stored_turn_file(storage_context, name="private.txt")
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id="changed-file-before-provider",
    )
    file.canonical_path.write_bytes(b"x" * file.size_bytes)
    try:
        await coordinator.wake(storage_context.conversation.id)
        terminal = await _wait_for_terminal(storage_context, turn.id)

        assert terminal.state is TurnState.FAILED
        assert terminal.terminal_code == "attachment_integrity_failed"
        assert terminal.error_code == "attachment_integrity_failed"
        assert terminal.provider_turn_id is None
        assert terminal.error_message_redacted is not None
        assert str(file.canonical_path) not in terminal.error_message_redacted
        assert fake.started_inputs == []
        assert file.canonical_path.exists()
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_unexpected_stream_end_interrupts_turn_and_retires_runtime(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime()
    fake.script(
        (
            NormalizedEvent(
                "turn.started",
                {"provider_turn_id": "fake-turn-1", "status": "inProgress"},
            ),
            NormalizedEvent(
                "assistant.text.delta",
                {"item_id": "answer", "text": "partial"},
            ),
        )
    )

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="end unexpectedly"),
            input_message_id="unexpected-stream",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)

        assert terminal.state is TurnState.INTERRUPTED
        assert terminal.terminal_code == "stream_ended_without_terminal"
        for _ in range(100):
            if fake.closed:
                break
            await asyncio.sleep(0.01)
        assert fake.closed
        lease = storage_context.store.query_one(
            """
            SELECT state, failure_code FROM runtime_leases
            WHERE id = (SELECT runtime_lease_id FROM turns WHERE id = ?)
            """,
            (turn.id,),
        )
        assert lease is not None
        assert (lease["state"], lease["failure_code"]) == (
            "unhealthy",
            "stream_ended_without_terminal",
        )
        assert len(storage_context.store.query_all("SELECT id FROM turns")) == 1
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_turn_controls_fence_effects_after_active_handle_validation(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime(event_delay=0.5)

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="control this Turn"),
            input_message_id="turn-controls",
        )
        await _wait_for_state(storage_context, turn.id, TurnState.RUNNING)
        running = storage_context.repository.get_turn(turn.id)
        assert running.provider_turn_id is not None

        storage_context.repository.accept_command_intent(
            interaction_id="steer-active",
            command_name="steer submit",
            request={},
            boot_id="turn-controls",
            actor_user_id=400,
            project_id=storage_context.project.id,
            conversation_id=storage_context.conversation.id,
            turn_id=turn.id,
        )
        await coordinator.steer(
            turn.id,
            "focus on durability",
            interaction_id="steer-active",
            actor_user_id=400,
        )
        steer_intent = storage_context.repository.get_command_intent("steer-active")
        assert steer_intent.state == "succeeded"
        assert steer_intent.effect_kind == "turn_steer"
        assert fake.steers[running.provider_turn_id] == ["focus on durability"]

        storage_context.repository.accept_command_intent(
            interaction_id="cancel-active",
            command_name="turn cancel",
            request={},
            boot_id="turn-controls",
            actor_user_id=400,
            project_id=storage_context.project.id,
            conversation_id=storage_context.conversation.id,
            turn_id=turn.id,
        )
        cancelled = await coordinator.cancel(
            turn.id,
            interaction_id="cancel-active",
        )
        cancel_intent = storage_context.repository.get_command_intent("cancel-active")
        assert cancelled.state is TurnState.CANCELLING
        assert cancel_intent.state == "effect_in_flight"
        assert cancel_intent.effect_kind == "turn_cancel"
        assert running.provider_turn_id in fake._interrupts

        await _wait_for_terminal(storage_context, turn.id)
        storage_context.repository.accept_command_intent(
            interaction_id="steer-terminal",
            command_name="steer submit",
            request={},
            boot_id="turn-controls",
            actor_user_id=400,
            project_id=storage_context.project.id,
            conversation_id=storage_context.conversation.id,
            turn_id=turn.id,
        )
        with pytest.raises(ConflictError, match="not steerable"):
            await coordinator.steer(
                turn.id,
                "too late",
                interaction_id="steer-terminal",
            )
        terminal_intent = storage_context.repository.get_command_intent(
            "steer-terminal"
        )
        assert terminal_intent.state == "accepted"
        assert terminal_intent.effect_kind is None
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_cancel_during_active_handle_publication_is_deferred(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeCodexRuntime(event_delay=0.5)

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    original_mark_running = storage_context.repository.mark_turn_running

    def mark_running_then_cancel(
        turn_id: str,
        provider_turn_id: str,
    ) -> TurnRecord:
        running = original_mark_running(turn_id, provider_turn_id)
        storage_context.repository.request_cancel(
            turn_id,
            origin=InterruptOrigin.USER,
        )
        return running

    monkeypatch.setattr(
        storage_context.repository,
        "mark_turn_running",
        mark_running_then_cancel,
    )
    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="cancel during publication"),
            input_message_id="cancel-publication-race",
        )
        interrupted = await _wait_for_interrupt_reason(
            storage_context,
            turn.id,
            "interrupt_sent",
        )

        assert interrupted.provider_turn_id in fake._interrupts
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_duplicate_cancel_interrupts_provider_once(
    storage_context: StorageContext,
) -> None:
    class BlockingInterruptRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__(event_delay=0.5)
            self.interrupt_calls = 0
            self.interrupt_entered = asyncio.Event()
            self.release_interrupt = asyncio.Event()

        async def interrupt(self, turn: TurnIdentity) -> None:
            self.interrupt_calls += 1
            self.interrupt_entered.set()
            await self.release_interrupt.wait()
            await super().interrupt(turn)

    fake = BlockingInterruptRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="cancel once"),
            input_message_id="duplicate-cancel",
        )
        await _wait_for_state(storage_context, turn.id, TurnState.RUNNING)

        first = asyncio.create_task(coordinator.cancel(turn.id))
        await asyncio.wait_for(fake.interrupt_entered.wait(), timeout=1)
        duplicate = await coordinator.cancel(turn.id)
        fake.release_interrupt.set()
        await first

        resolved = storage_context.repository.get_turn(turn.id)
        assert duplicate.state is TurnState.CANCELLING
        assert fake.interrupt_calls == 1
        assert resolved.interrupt_reason == "interrupt_sent"
    finally:
        fake.release_interrupt.set()
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_deterministic_interrupt_failure_stays_cancelling(
    storage_context: StorageContext,
) -> None:
    class FailingInterruptRuntime(FakeCodexRuntime):
        async def interrupt(self, turn: TurnIdentity) -> None:
            raise InterruptFailed(
                AdapterFailure(
                    code="interrupt_denied",
                    provider_exception="InvalidRequestError",
                    message="provider rejected interrupt",
                    retryable=False,
                    runtime_generation=self.generation,
                    turn_id=turn.provider_turn_id,
                )
            )

    fake = FailingInterruptRuntime(event_delay=0.5)

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="interrupt failure"),
            input_message_id="interrupt-failure",
        )
        await _wait_for_state(storage_context, turn.id, TurnState.RUNNING)

        cancelling = await coordinator.cancel(turn.id)
        persisted = storage_context.repository.get_turn(turn.id)

        assert cancelling.state is TurnState.CANCELLING
        assert persisted.state is TurnState.CANCELLING
        assert persisted.interrupt_reason == "interrupt_failed:interrupt_denied"
        incident = storage_context.store.query_one(
            "SELECT code FROM incidents WHERE turn_id = ? ORDER BY last_seen_at DESC",
            (turn.id,),
        )
        assert incident is not None
        assert incident["code"] == "turn_interrupt_failed"
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_unknown_interrupt_outcome_is_persisted_and_raised(
    storage_context: StorageContext,
) -> None:
    class UnknownInterruptRuntime(FakeCodexRuntime):
        async def interrupt(self, turn: TurnIdentity) -> None:
            raise ProviderOutcomeUnknown(
                AdapterFailure(
                    code="provider_effect_outcome_unknown",
                    provider_exception="TransportClosedError",
                    message="interrupt transport closed",
                    retryable=False,
                    runtime_generation=self.generation,
                    turn_id=turn.provider_turn_id,
                )
            )

    fake = UnknownInterruptRuntime(event_delay=0.5)

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="unknown interrupt"),
            input_message_id="interrupt-unknown",
        )
        await _wait_for_state(storage_context, turn.id, TurnState.RUNNING)

        with pytest.raises(ProviderOutcomeUnknown):
            await coordinator.cancel(turn.id)

        persisted = storage_context.repository.get_turn(turn.id)
        assert persisted.state is TurnState.CANCELLING
        assert (
            persisted.interrupt_reason
            == "interrupt_unknown:provider_effect_outcome_unknown"
        )
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_event_journal_failure_interrupts_provider_and_signals_shutdown(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime(event_delay=0.01)

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    class FailingSink(ProjectingEventSink):
        def record(self, **_kwargs: object) -> int | None:
            raise OSError("simulated durable journal failure")

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
    critical = asyncio.Event()
    failures: list[BaseException] = []

    def fail_daemon(exc: BaseException) -> None:
        failures.append(exc)
        critical.set()

    coordinator = TurnCoordinator(
        repository=storage_context.repository,
        runtime_supervisor=supervisor,
        event_sink=FailingSink(
            storage_context.store,
            correlation_key=b"x" * 32,
        ),
        critical_failure=fail_daemon,
    )
    try:
        await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="journal failure"),
            input_message_id="discord-journal-failure",
        )
        await asyncio.wait_for(critical.wait(), timeout=2)

        assert len(failures) == 1
        assert isinstance(failures[0], EventJournalError)
        assert fake._interrupts == {"fake-turn-1"}
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_runtime_factory_generation_mismatch_fails_closed(
    storage_context: StorageContext,
) -> None:
    fake = FakeCodexRuntime(generation=99)

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    try:
        with pytest.raises(AdapterInvariantError) as error:
            await supervisor.ensure(storage_context.project)
        assert error.value.failure.code == "runtime_generation_mismatch"
        assert fake.closed
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_stale_failure_does_not_close_replacement_generation(
    storage_context: StorageContext,
) -> None:
    runtimes: dict[int, FakeCodexRuntime] = {}

    async def factory(_slot: object, generation: int) -> FakeCodexRuntime:
        runtime = FakeCodexRuntime(generation=generation)
        runtimes[generation] = runtime
        return runtime

    supervisor = _runtime_supervisor(storage_context, factory)
    runtime_one, lease_one = await supervisor.ensure(storage_context.project)
    await supervisor.report_failure(
        storage_context.project,
        expected_lease_id=lease_one.id,
        expected_generation=lease_one.generation,
        failure_code="runtime_unavailable",
    )
    slot = await supervisor._slot(storage_context.project.id)
    slot.retry_at = 0
    runtime_two, lease_two = await supervisor.ensure(storage_context.project)
    try:
        assert lease_two.generation > lease_one.generation

        interrupted = await supervisor.report_failure(
            storage_context.project,
            expected_lease_id=lease_one.id,
            expected_generation=lease_one.generation,
            failure_code="late_generation_one_failure",
        )

        assert interrupted == ()
        assert runtime_two is runtimes[lease_two.generation]
        assert not runtimes[lease_two.generation].closed
        assert runtime_one is runtimes[lease_one.generation]
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_runtime_supervisor_close_has_total_deadline(
    storage_context: StorageContext,
) -> None:
    class HangingCloseRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = asyncio.Event()
            self.never = asyncio.Event()

        async def close(self) -> None:
            self.close_started.set()
            await self.never.wait()

    fake = HangingCloseRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    await supervisor.ensure(storage_context.project)

    with pytest.raises(ExceptionGroup):
        await asyncio.wait_for(
            supervisor.close(timeout_seconds=0.2),
            timeout=1,
        )
    assert fake.close_started.is_set()


@pytest.mark.asyncio
async def test_runtime_supervisor_close_is_durable_and_retryable(
    storage_context: StorageContext,
) -> None:
    class RetryCloseRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.close_attempts = 0

        async def close(self) -> None:
            self.close_attempts += 1
            if self.close_attempts == 1:
                raise OSError("close failed")
            await super().close()

    fake = RetryCloseRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    _runtime, lease = await supervisor.ensure(storage_context.project)

    with pytest.raises(ExceptionGroup):
        await supervisor.close()

    failed = storage_context.store.query_one(
        "SELECT state, failure_code, ended_at FROM runtime_leases WHERE id = ?",
        (lease.id,),
    )
    assert failed is not None
    assert failed["state"] == "failed"
    assert failed["failure_code"] == "runtime_close_error"
    assert failed["ended_at"] is not None
    incident = storage_context.store.query_one(
        """
        SELECT code, occurrence_count
        FROM incidents
        WHERE project_id = ? AND code = 'runtime_close_failed'
        """,
        (storage_context.project.id,),
    )
    assert incident is not None
    assert incident["occurrence_count"] == 1
    slot = await supervisor._slot(storage_context.project.id)
    assert slot.runtime is fake

    await supervisor.close()

    stopped = storage_context.store.query_one(
        "SELECT state, ended_at FROM runtime_leases WHERE id = ?",
        (lease.id,),
    )
    assert stopped is not None
    assert stopped["state"] == "stopped"
    assert stopped["ended_at"] is not None
    assert fake.close_attempts == 2
    assert slot.runtime is None


@pytest.mark.asyncio
async def test_runtime_failure_cleanup_close_is_retained_and_retryable(
    storage_context: StorageContext,
) -> None:
    class RetryCloseRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.close_attempts = 0

        async def close(self) -> None:
            self.close_attempts += 1
            if self.close_attempts == 1:
                raise OSError("close failed")
            await super().close()

    fake = RetryCloseRuntime()

    async def factory(_slot: object, generation: int) -> FakeCodexRuntime:
        fake.generation = generation
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    _runtime, lease = await supervisor.ensure(storage_context.project)

    interrupted = await supervisor.report_failure(
        storage_context.project,
        expected_lease_id=lease.id,
        expected_generation=lease.generation,
        failure_code="runtime_test_failure",
    )

    assert interrupted == ()
    slot = await supervisor._slot(storage_context.project.id)
    assert slot.runtime is fake
    assert slot.lease is not None
    assert slot.lease.state == "failed"
    failed = storage_context.store.query_one(
        "SELECT state, failure_code FROM runtime_leases WHERE id = ?",
        (lease.id,),
    )
    assert failed is not None
    assert (failed["state"], failed["failure_code"]) == (
        "failed",
        "runtime_failure_cleanup_failed",
    )

    await supervisor.close()

    stopped = storage_context.store.query_one(
        "SELECT state, failure_code FROM runtime_leases WHERE id = ?",
        (lease.id,),
    )
    assert stopped is not None
    assert (stopped["state"], stopped["failure_code"]) == (
        "stopped",
        "runtime_failure_cleanup_failed",
    )
    assert (
        storage_context.repository.recent_runtime_failure_count(
            storage_context.project.id,
            since_ms=0,
        )
        == 1
    )
    assert fake.close_attempts == 2
    assert slot.runtime is None


@pytest.mark.asyncio
async def test_runtime_startup_cleanup_close_is_retained_and_retryable(
    storage_context: StorageContext,
) -> None:
    class StartupFailureRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.close_attempts = 0

        async def list_models(self) -> ModelCatalogSnapshot:
            raise OSError("catalog failed")

        async def close(self) -> None:
            self.close_attempts += 1
            if self.close_attempts == 1:
                raise OSError("close failed")
            await super().close()

    fake = StartupFailureRuntime()

    async def factory(_slot: object, generation: int) -> FakeCodexRuntime:
        fake.generation = generation
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)

    with pytest.raises(ExceptionGroup):
        await supervisor.ensure(storage_context.project)

    slot = await supervisor._slot(storage_context.project.id)
    assert slot.runtime is fake
    assert slot.lease is not None
    assert slot.lease.state == "failed"
    lease_id = slot.lease.id
    failed = storage_context.store.query_one(
        "SELECT state, failure_code FROM runtime_leases WHERE id = ?",
        (lease_id,),
    )
    assert failed is not None
    assert (failed["state"], failed["failure_code"]) == (
        "failed",
        "runtime_start_cleanup_failed",
    )

    await supervisor.close()

    stopped = storage_context.store.query_one(
        "SELECT state, failure_code FROM runtime_leases WHERE id = ?",
        (lease_id,),
    )
    assert stopped is not None
    assert (stopped["state"], stopped["failure_code"]) == (
        "stopped",
        "runtime_start_cleanup_failed",
    )
    assert fake.close_attempts == 2
    assert slot.runtime is None


@pytest.mark.asyncio
async def test_runtime_watchdog_interrupts_generation_without_replay(
    storage_context: StorageContext,
) -> None:
    class WatchdogRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.hang = False
            self.never = asyncio.Event()

        async def account_status(self) -> AccountStatus:
            if self.hang:
                await self.never.wait()
            return await super().account_status()

    fake = WatchdogRuntime()

    async def factory(_slot: object, generation: int) -> FakeCodexRuntime:
        fake.generation = generation
        return fake

    supervisor = _runtime_supervisor(
        storage_context,
        factory,
        watchdog_interval_seconds=0.001,
        watchdog_timeout_seconds=0.01,
    )
    repository = storage_context.repository
    runtime, lease = await supervisor.ensure(storage_context.project)
    assert runtime is fake
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="watchdog-thread",
            requested_thread_id=None,
            provider_session_id="watchdog-session",
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
        turn_input=TurnInput(text="watchdog"),
        input_message_id="watchdog-message",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "watchdog-provider-turn")
    fake.hang = True
    slot = await supervisor._slot(storage_context.project.id)
    slot.last_watchdog_at = 0

    await supervisor.watchdog()

    interrupted = repository.get_turn(turn.id)
    assert interrupted.state is TurnState.INTERRUPTED
    assert interrupted.terminal_code == "runtime_lost"
    assert interrupted.error_code == "runtime_watchdog_timeout"
    assert fake.closed
    assert slot.runtime is None
    assert len(storage_context.store.query_all("SELECT id FROM turns")) == 1
    lease_row = storage_context.store.query_one(
        "SELECT state, failure_code FROM runtime_leases WHERE id = ?",
        (lease.id,),
    )
    assert lease_row is not None
    assert (lease_row["state"], lease_row["failure_code"]) == (
        "unhealthy",
        "runtime_watchdog_timeout",
    )
    await supervisor.close()


@pytest.mark.asyncio
async def test_runtime_status_does_not_wait_for_watchdog_cleanup(
    storage_context: StorageContext,
) -> None:
    class HangingWatchdogRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.hang = False
            self.probe_started = asyncio.Event()
            self.never = asyncio.Event()

        async def account_status(self) -> AccountStatus:
            if self.hang:
                self.probe_started.set()
                await self.never.wait()
            return await super().account_status()

    fake = HangingWatchdogRuntime()

    async def factory(_slot: object, generation: int) -> FakeCodexRuntime:
        fake.generation = generation
        return fake

    supervisor = _runtime_supervisor(
        storage_context,
        factory,
        watchdog_interval_seconds=0.001,
        watchdog_timeout_seconds=0.02,
    )
    await supervisor.ensure(storage_context.project)
    fake.hang = True
    slot = await supervisor._slot(storage_context.project.id)
    slot.last_watchdog_at = 0

    status = await asyncio.wait_for(supervisor.status(), timeout=0.01)

    assert status["ready"] == 1
    assert status["watchdog"] == "running"
    await asyncio.wait_for(fake.probe_started.wait(), timeout=0.1)
    watchdog_task = supervisor._watchdog_task
    assert watchdog_task is not None
    await asyncio.wait_for(watchdog_task, timeout=0.2)
    assert fake.closed
    assert slot.runtime is None
    await supervisor.close()


@pytest.mark.asyncio
async def test_runtime_crashes_report_without_exceeding_backoff_cap(
    storage_context: StorageContext,
) -> None:
    async def factory(_slot: object, generation: int) -> FakeCodexRuntime:
        return FakeCodexRuntime(generation=generation)

    supervisor = _runtime_supervisor(storage_context, factory)
    slot = await supervisor._slot(storage_context.project.id)
    for attempt in range(6):
        _runtime, lease = await supervisor.ensure(storage_context.project)
        await supervisor.report_failure(
            storage_context.project,
            expected_lease_id=lease.id,
            expected_generation=lease.generation,
            failure_code=f"runtime_test_crash_{attempt}",
        )
        if attempt < 5:
            slot.retry_at = 0

    remaining = slot.retry_at - time.monotonic()
    assert 50 < remaining <= 60
    incident = storage_context.store.query_one(
        """
        SELECT code, details_json
        FROM incidents
        WHERE project_id = ? AND code = 'runtime_crash_loop'
        """,
        (storage_context.project.id,),
    )
    assert incident is not None
    assert json.loads(str(incident["details_json"]))["failure_count"] == 6
    with pytest.raises(RuntimeUnavailable) as error:
        await supervisor.ensure(storage_context.project)
    assert error.value.failure.code == "runtime_restart_backoff"
    await supervisor.close()


@pytest.mark.asyncio
async def test_runtime_startup_timeout_is_retryable_and_durable(
    storage_context: StorageContext,
) -> None:
    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="project_scoped",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
        startup_timeout_seconds=0.01,
    )

    with pytest.raises(RuntimeUnavailable) as raised:
        await supervisor.ensure(storage_context.project)

    assert raised.value.failure.code == "runtime_start_timeout"
    assert raised.value.failure.retryable
    lease = storage_context.store.query_one(
        """
        SELECT state, failure_code
        FROM runtime_leases
        WHERE project_id = ?
        ORDER BY generation DESC
        LIMIT 1
        """,
        (storage_context.project.id,),
    )
    assert lease is not None
    assert (lease["state"], lease["failure_code"]) == (
        "failed",
        "runtime_start_timeout",
    )
    await supervisor.close()


@pytest.mark.asyncio
async def test_shared_runtime_crash_loop_is_globally_attributed(
    storage_context: StorageContext,
) -> None:
    async def factory(_slot: object, generation: int) -> FakeCodexRuntime:
        return FakeCodexRuntime(generation=generation)

    supervisor = RuntimeSupervisor(
        repository=storage_context.repository,
        factory=factory,
        topology="shared",
        environment={},
        environment_hash="environment",
        codex_home=None,
        neutral_cwd=storage_context.root / ".runtime",
        allowed_roots=(storage_context.root.parent,),
    )
    slot = await supervisor._slot("shared")
    for attempt in range(6):
        _runtime, lease = await supervisor.ensure(storage_context.project)
        await supervisor.report_failure(
            storage_context.project,
            expected_lease_id=lease.id,
            expected_generation=lease.generation,
            failure_code=f"shared_runtime_test_crash_{attempt}",
        )
        if attempt < 5:
            slot.retry_at = 0

    incident = storage_context.store.query_one(
        """
        SELECT project_id, details_json
        FROM incidents
        WHERE code = 'runtime_crash_loop'
        """
    )
    assert incident is not None
    assert incident["project_id"] is None
    assert json.loads(str(incident["details_json"]))["scope_key"] == "shared"
    await supervisor.close()


@pytest.mark.asyncio
async def test_execution_time_catalog_drift_prevents_provider_turn(
    storage_context: StorageContext,
) -> None:
    class DriftingCatalogRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.catalog_reads = 0
            self.start_turn_calls = 0

        async def list_models(self) -> ModelCatalogSnapshot:
            self.catalog_reads += 1
            if self.catalog_reads == 1:
                return await super().list_models()
            return ModelCatalogSnapshot(models=(), complete=True, next_cursor=None)

        async def start_turn(
            self,
            *,
            local_turn_id: str,
            thread: ThreadIdentity,
            input: TurnInput,
            config: TurnConfig,
        ) -> StartedTurn:
            self.start_turn_calls += 1
            return await super().start_turn(
                local_turn_id=local_turn_id,
                thread=thread,
                input=input,
                config=config,
            )

    fake = DriftingCatalogRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="catalog drift"),
            input_message_id="catalog-drift",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)

        assert terminal.state is TurnState.FAILED
        assert fake.start_turn_calls == 0
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_automatic_resume_identity_mismatch_blocks_conversation(
    storage_context: StorageContext,
) -> None:
    config = storage_context.repository.effective_thread_config(
        storage_context.conversation.id
    )
    seed = FakeCodexRuntime()
    identity = await seed.start_thread(cwd=storage_context.root, config=config)
    storage_context.repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=identity,
        config=config,
    )

    class MismatchingRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.start_turn_calls = 0

        async def resume_thread(
            self,
            *,
            thread_id: str,
            cwd: Path,
            config: ThreadConfig,
        ) -> ThreadIdentity:
            del cwd, config
            return ThreadIdentity(
                thread_id="different-thread",
                requested_thread_id=thread_id,
                provider_session_id=identity.provider_session_id,
                forked_from_thread_id=None,
                parent_thread_id=None,
                provider_version="fake",
            )

        async def start_turn(
            self,
            *,
            local_turn_id: str,
            thread: ThreadIdentity,
            input: TurnInput,
            config: TurnConfig,
        ) -> StartedTurn:
            self.start_turn_calls += 1
            return await super().start_turn(
                local_turn_id=local_turn_id,
                thread=thread,
                input=input,
                config=config,
            )

    fake = MismatchingRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="resume"),
            input_message_id="resume-mismatch",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)
        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )

        assert terminal.state is TurnState.INTERRUPTED
        assert conversation.state is ConversationState.BLOCKED
        assert fake.start_turn_calls == 0
    finally:
        await coordinator.close(drain_seconds=1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_shutdown_interrupts_turn_that_was_starting(
    storage_context: StorageContext,
) -> None:
    class BlockingStartRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.start_entered = asyncio.Event()
            self.release_start = asyncio.Event()

        async def start_turn(
            self,
            *,
            local_turn_id: str,
            thread: ThreadIdentity,
            input: TurnInput,
            config: TurnConfig,
        ) -> StartedTurn:
            self.start_entered.set()
            await self.release_start.wait()
            return await super().start_turn(
                local_turn_id=local_turn_id,
                thread=thread,
                input=input,
                config=config,
            )

    fake = BlockingStartRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    await coordinator.enqueue(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="starting during shutdown"),
        input_message_id="shutdown-start-race",
    )
    await asyncio.wait_for(fake.start_entered.wait(), timeout=1)

    close_task = asyncio.create_task(coordinator.close(drain_seconds=1))
    await asyncio.sleep(0)
    fake.release_start.set()
    await asyncio.wait_for(close_task, timeout=2)
    try:
        assert fake._interrupts == {"fake-turn-1"}
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_shutdown_deadline_bounds_hanging_interrupt(
    storage_context: StorageContext,
) -> None:
    class HangingInterruptRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__(event_delay=10)
            self.interrupt_started = asyncio.Event()
            self.never = asyncio.Event()

        async def interrupt(self, turn: TurnIdentity) -> None:
            del turn
            self.interrupt_started.set()
            await self.never.wait()

    fake = HangingInterruptRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    turn = await coordinator.enqueue(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="hang"),
        input_message_id="shutdown-hanging-interrupt",
    )
    await _wait_for_state(storage_context, turn.id, TurnState.RUNNING)

    await asyncio.wait_for(coordinator.close(drain_seconds=0.05), timeout=0.5)
    try:
        assert fake.interrupt_started.is_set()
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_shutdown_force_closes_runtime_when_turn_start_does_not_return(
    storage_context: StorageContext,
) -> None:
    class HangingStartRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.start_entered = asyncio.Event()
            self.never = asyncio.Event()

        async def start_turn(
            self,
            *,
            local_turn_id: str,
            thread: ThreadIdentity,
            input: TurnInput,
            config: TurnConfig,
        ) -> StartedTurn:
            del local_turn_id, thread, input, config
            self.start_entered.set()
            await self.never.wait()
            raise AssertionError("unreachable")

    fake = HangingStartRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    await coordinator.enqueue(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="never returns"),
        input_message_id="shutdown-hanging-start",
    )
    await asyncio.wait_for(fake.start_entered.wait(), timeout=1)

    await asyncio.wait_for(coordinator.close(drain_seconds=1), timeout=2)
    assert fake.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("swallow_cancel", [False, True])
async def test_runtime_startup_cannot_publish_after_supervisor_close(
    storage_context: StorageContext,
    swallow_cancel: bool,
) -> None:
    class BlockingStartupRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.account_entered = asyncio.Event()
            self.never = asyncio.Event()
            self.cancel_observed = False

        async def account_status(self) -> AccountStatus:
            self.account_entered.set()
            try:
                await self.never.wait()
            except asyncio.CancelledError:
                self.cancel_observed = True
                if not swallow_cancel:
                    raise
            return await super().account_status()

    fake = BlockingStartupRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    ensure_task = asyncio.create_task(supervisor.ensure(storage_context.project))
    await asyncio.wait_for(fake.account_entered.wait(), timeout=1)

    await asyncio.wait_for(supervisor.close(timeout_seconds=0.2), timeout=1)
    if swallow_cancel:
        with pytest.raises(InvariantError, match="closed during startup"):
            await ensure_task
    else:
        with pytest.raises(asyncio.CancelledError):
            await ensure_task
    status = await supervisor.status()
    assert fake.cancel_observed
    assert fake.closed
    assert status["ready"] == 0
    assert status["starting"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["local_turn_id", "generation"])
async def test_started_turn_identity_mismatch_is_retired_before_pump(
    storage_context: StorageContext,
    mismatch: str,
) -> None:
    class MismatchedTurnRuntime(FakeCodexRuntime):
        async def start_turn(
            self,
            *,
            local_turn_id: str,
            thread: ThreadIdentity,
            input: TurnInput,
            config: TurnConfig,
        ) -> StartedTurn:
            started = await super().start_turn(
                local_turn_id=local_turn_id,
                thread=thread,
                input=input,
                config=config,
            )
            return StartedTurn(
                identity=TurnIdentity(
                    (
                        "different-local-turn"
                        if mismatch == "local_turn_id"
                        else local_turn_id
                    ),
                    started.identity.provider_turn_id,
                    (
                        self.generation + 1
                        if mismatch == "generation"
                        else self.generation
                    ),
                ),
                stream=started.stream,
            )

    fake = MismatchedTurnRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    critical = asyncio.Event()
    failures: list[BaseException] = []

    def fail_daemon(exc: BaseException) -> None:
        failures.append(exc)
        critical.set()

    coordinator = _turn_coordinator(
        storage_context,
        supervisor,
        critical_failure=fail_daemon,
    )
    turn = await coordinator.enqueue(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="wrong generation"),
        input_message_id="wrong-generation",
    )
    await asyncio.wait_for(critical.wait(), timeout=1)
    try:
        terminal = storage_context.repository.get_turn(turn.id)
        assert terminal.state is TurnState.INTERRUPTED
        assert terminal.terminal_code == "runtime_turn_identity_mismatch"
        assert isinstance(failures[0], InvariantError)
        assert fake.closed
        assert (
            storage_context.store.query_one(
                "SELECT 1 FROM events WHERE turn_id = ? AND kind = 'turn.started'",
                (turn.id,),
            )
            is None
        )
    finally:
        await coordinator.close(drain_seconds=0.1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_provider_identity_persist_failure_always_terminalizes_and_retires(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HangingInterruptRuntime(FakeCodexRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.interrupt_entered = asyncio.Event()
            self.never = asyncio.Event()

        async def interrupt(self, turn: TurnIdentity) -> None:
            del turn
            self.interrupt_entered.set()
            await self.never.wait()

    fake = HangingInterruptRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    def fail_mark_running(_turn_id: str, _provider_turn_id: str) -> TurnRecord:
        raise OSError("identity persistence failed")

    monkeypatch.setattr(
        turn_coordinator_module,
        "_PROVIDER_CLEANUP_TIMEOUT_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        storage_context.repository,
        "mark_turn_running",
        fail_mark_running,
    )
    supervisor = _runtime_supervisor(storage_context, factory)
    critical = asyncio.Event()

    def fail_daemon(_exc: BaseException) -> None:
        critical.set()

    coordinator = _turn_coordinator(
        storage_context,
        supervisor,
        critical_failure=fail_daemon,
    )
    turn = await coordinator.enqueue(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="persist failure"),
        input_message_id="persist-failure",
    )
    await asyncio.wait_for(critical.wait(), timeout=1)
    try:
        terminal = storage_context.repository.get_turn(turn.id)
        assert fake.interrupt_entered.is_set()
        assert fake.closed
        assert terminal.state is TurnState.INTERRUPTED
        assert terminal.terminal_code == "provider_identity_persist_failed"
    finally:
        await coordinator.close(drain_seconds=0.1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_automatic_thread_start_outcome_unknown_blocks_conversation(
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
            raise ProviderOutcomeUnknown(
                AdapterFailure(
                    code="provider_effect_outcome_unknown",
                    provider_exception="ReadBackFailure",
                    message="identity read-back failed",
                    retryable=False,
                    runtime_generation=self.generation,
                    thread_id=identity.thread_id,
                )
            )

    fake = UnknownStartRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        turn = await coordinator.enqueue(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(text="unknown start"),
            input_message_id="unknown-start",
        )
        terminal = await _wait_for_terminal(storage_context, turn.id)
        conversation = storage_context.repository.get_conversation(
            storage_context.conversation.id
        )

        assert terminal.state is TurnState.INTERRUPTED
        assert conversation.state is ConversationState.BLOCKED
        assert conversation.provider_barrier_kind == "unknown_effect"
        assert fake.start_calls == 1
    finally:
        await coordinator.close(drain_seconds=0.1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_mailbox_registry_does_not_reopen_after_close(
    storage_context: StorageContext,
) -> None:
    async def handler(_turn: TurnRecord) -> float | None:
        return None

    async def error_handler(_conversation_id: str, _exc: Exception) -> None:
        return None

    registry = MailboxRegistry(
        repository=storage_context.repository,
        handler=handler,
        error_handler=error_handler,
    )
    await registry.close()
    await registry.wake(storage_context.conversation.id)

    assert registry._mailboxes == {}


@pytest.mark.asyncio
async def test_restore_interrupts_invalid_queued_schedule_snapshot_before_wake(
    storage_context: StorageContext,
) -> None:
    turn_id = _materialize_queued_schedule_turn(
        storage_context,
        suffix="invalid-snapshot",
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE turns SET input_hash = 'corrupt' WHERE id = ?",
            (turn_id,),
        )
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(storage_context, supervisor)
    try:
        assert await coordinator.restore() == 1
        interrupted = storage_context.repository.get_turn(turn_id)
        assert interrupted.state is TurnState.INTERRUPTED
        assert interrupted.terminal_code == "schedule_snapshot_not_replayable"
        incident = storage_context.store.query_one(
            """
            SELECT details_json
            FROM incidents
            WHERE turn_id = ?
              AND code = 'schedule_turn_snapshot_not_replayable'
            """,
            (turn_id,),
        )
        assert incident is not None
        assert json.loads(incident["details_json"]) == {
            "reason_code": "snapshot_invalid"
        }
        assert interrupted.provider_turn_id is None
        assert interrupted.runtime_lease_id is None
    finally:
        await coordinator.close(drain_seconds=0.1)
        await supervisor.close()


@pytest.mark.asyncio
async def test_restore_interrupts_schedule_skill_when_capability_disappeared(
    storage_context: StorageContext,
) -> None:
    skill_path = storage_context.root / "SKILL.md"
    skill_path.write_text("# Test skill\n", encoding="utf-8")
    turn_id = _materialize_queued_schedule_turn(
        storage_context,
        suffix="skill-capability",
        skill_inputs_json=json.dumps(
            [
                {
                    "name": "test-skill",
                    "canonical_path": str(skill_path.resolve()),
                    "content_hash": sha256_file(skill_path),
                }
            ]
        ),
    )
    fake = FakeCodexRuntime()

    async def factory(_slot: object, _generation: int) -> FakeCodexRuntime:
        return fake

    supervisor = _runtime_supervisor(storage_context, factory)
    coordinator = _turn_coordinator(
        storage_context,
        supervisor,
        skill_input_supported=False,
    )
    try:
        assert await coordinator.restore() == 1
        interrupted = storage_context.repository.get_turn(turn_id)
        assert interrupted.state is TurnState.INTERRUPTED
        incident = storage_context.store.query_one(
            """
            SELECT details_json
            FROM incidents
            WHERE turn_id = ?
              AND code = 'schedule_turn_snapshot_not_replayable'
            """,
            (turn_id,),
        )
        assert incident is not None
        assert json.loads(incident["details_json"]) == {
            "reason_code": "skill_capability_missing"
        }
        assert interrupted.provider_turn_id is None
        assert interrupted.runtime_lease_id is None
    finally:
        await coordinator.close(drain_seconds=0.1)
        await supervisor.close()


async def _wait_for_terminal(
    storage_context: StorageContext, turn_id: str
) -> TurnRecord:
    for _ in range(200):
        turn = storage_context.repository.get_turn(turn_id)
        if turn.state.terminal:
            return turn
        await asyncio.sleep(0.01)
    raise AssertionError("Turn did not reach a terminal state")


async def _wait_for_state(
    storage_context: StorageContext,
    turn_id: str,
    expected: TurnState,
) -> None:
    for _ in range(200):
        if storage_context.repository.get_turn(turn_id).state is expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"Turn did not reach {expected.value}")


async def _wait_for_interrupt_reason(
    storage_context: StorageContext,
    turn_id: str,
    expected: str,
) -> TurnRecord:
    for _ in range(200):
        turn = storage_context.repository.get_turn(turn_id)
        if turn.interrupt_reason == expected:
            return turn
        await asyncio.sleep(0.01)
    raise AssertionError(f"Turn interrupt did not resolve to {expected}")


def _materialize_queued_schedule_turn(
    storage_context: StorageContext,
    *,
    suffix: str,
    skill_inputs_json: str | None = None,
) -> str:
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
    repository = ScheduleRepository(storage_context.store)
    schedule = repository.create(
        conversation_id=storage_context.conversation.id,
        name=f"{suffix}-schedule",
        kind=ScheduleKind.CRON,
        expression="* * * * *",
        timezone="UTC",
        misfire_policy=MisfirePolicy.LATEST,
        prompt_text="restore this snapshot",
        next_due_at=60_000,
        created_by_user_id=400,
        skill_inputs_json=skill_inputs_json,
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
    assert result.turn_id is not None
    return result.turn_id


def _model(
    name: str,
    *,
    is_default: bool,
    modalities: tuple[str, ...],
) -> ModelDescriptor:
    return ModelDescriptor(
        id=name,
        model=name,
        is_default=is_default,
        input_modalities=modalities,
        supported_reasoning_efforts=("medium",),
        default_reasoning_effort="medium",
        supports_personality=True,
        service_tiers=(),
        default_service_tier=None,
        upgrade=None,
    )


def _stored_turn_file(
    storage_context: StorageContext,
    *,
    name: str,
) -> TurnFile:
    data_root = storage_context.store.path.parent
    attachments = data_root / "attachments"
    input_root = attachments / "input"
    private_files.ensure_private_directory(data_root)
    private_files.ensure_private_directory(attachments)
    private_files.ensure_private_directory(input_root)
    content = b"opaque file bytes"
    path = input_root / "runtime-test-input.bin"
    path.write_bytes(content)
    private_files.secure_private_file(path)
    return TurnFile(
        attachment_id="runtime-test-file",
        ordinal=0,
        canonical_path=path.resolve(strict=True),
        display_name=name,
        reported_media_type="text/plain",
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        retention_until=9_999_999_999_999,
    )


def _runtime_supervisor(
    storage_context: StorageContext,
    factory: RuntimeFactory,
    *,
    watchdog_interval_seconds: float = 30,
    watchdog_timeout_seconds: float = 5,
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
        watchdog_interval_seconds=watchdog_interval_seconds,
        watchdog_timeout_seconds=watchdog_timeout_seconds,
    )


def _turn_coordinator(
    storage_context: StorageContext,
    supervisor: RuntimeSupervisor,
    *,
    critical_failure: Callable[[BaseException], None] | None = None,
    skill_input_supported: bool = True,
) -> TurnCoordinator:
    return TurnCoordinator(
        repository=storage_context.repository,
        runtime_supervisor=supervisor,
        event_sink=ProjectingEventSink(
            storage_context.store,
            correlation_key=b"x" * 32,
        ),
        critical_failure=critical_failure,
        skill_input_supported=skill_input_supported,
    )
