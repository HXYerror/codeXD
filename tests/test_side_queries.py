from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from conftest import StorageContext

from codexd.application.side_queries import SideQueryCoordinator, SideQueryError
from codexd.domain.conversations import SandboxProfile, ThreadConfig, ThreadIdentity
from codexd.domain.events import NormalizedEvent
from codexd.domain.turns import TurnInput, TurnSource, TurnState
from codexd.runtime.port import (
    SideQueryIdentity,
    StartedSideQuery,
    TurnStream,
)
from codexd.storage.records import RuntimeLeaseRecord
from codexd.storage.side_queries import SideQueryRepository


class _SideRuntime:
    generation = 1

    def __init__(self, *, block: bool = False) -> None:
        self.block = block
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.interrupted = False
        self.closed = False
        self.question: str | None = None
        self.thread_config: object | None = None
        self.turn_config: object | None = None
        self.source_thread: ThreadIdentity | None = None

    async def start_side_query(self, **kwargs: object) -> StartedSideQuery:
        local_query_id = str(kwargs["local_query_id"])
        self.question = str(kwargs["question"])
        self.thread_config = kwargs["thread_config"]
        self.turn_config = kwargs["turn_config"]
        self.source_thread = kwargs["source_thread"]  # type: ignore[assignment]
        identity = SideQueryIdentity(
            local_query_id=local_query_id,
            source_thread_id=self.source_thread.thread_id,
            side_thread_id=f"side-{local_query_id}",
            provider_turn_id=f"side-turn-{local_query_id}",
            runtime_generation=self.generation,
        )

        async def events():
            self.started.set()
            if self.block:
                await self.release.wait()
            yield NormalizedEvent(
                "assistant.text.completed",
                {
                    "item_id": "answer",
                    "text": "The main task chose this approach for isolation.",
                    "phase": "final_answer",
                },
            )
            yield NormalizedEvent(
                "turn.completed",
                {"status": "completed"},
            )

        return StartedSideQuery(identity, TurnStream(events))

    async def interrupt_side_query(self, _query: SideQueryIdentity) -> None:
        self.interrupted = True
        self.release.set()

    async def close_side_query(self, _query: SideQueryIdentity) -> None:
        self.closed = True


def _main_session(storage_context: StorageContext) -> tuple[ThreadIdentity, str]:
    repository = storage_context.repository
    identity = ThreadIdentity(
        thread_id="main-provider-thread",
        requested_thread_id=None,
        provider_session_id="main-provider-session",
        forked_from_thread_id=None,
        parent_thread_id=None,
        provider_version="0.144.4",
        dynamic_tools_enabled=True,
    )
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=identity,
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
        environment_hash="side-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="0.144.4",
        runtime_version="0.144.4",
        capability_hash="side-capability",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="Long-running main task"),
        input_message_id="main-task-message",
        requested_by_user_id=400,
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "main-provider-turn")
    return identity, turn.id


def _coordinator(
    storage_context: StorageContext,
    runtime: _SideRuntime,
    *,
    timeout_seconds: float = 5,
) -> SideQueryCoordinator:
    runtimes = Mock()
    runtimes.ensure = AsyncMock(
        return_value=(
            runtime,
            RuntimeLeaseRecord("side-lease", storage_context.project.id, 1, "ready"),
        )
    )
    return SideQueryCoordinator(
        repository=storage_context.repository,
        side_repository=SideQueryRepository(storage_context.store),
        runtimes=runtimes,
        boot_id="side-boot",
        timeout_seconds=timeout_seconds,
        max_concurrency=2,
    )


@pytest.mark.asyncio
async def test_side_query_returns_hash_only_answer_without_changing_main_turn(
    storage_context: StorageContext,
) -> None:
    source_identity, main_turn_id = _main_session(storage_context)
    runtime = _SideRuntime()
    coordinator = _coordinator(storage_context, runtime)
    question = "Why did the main task choose this approach?"

    answer = await coordinator.ask(
        interaction_id="side-interaction",
        conversation_id=storage_context.conversation.id,
        requested_by_user_id=400,
        question=question,
    )

    assert answer == "The main task chose this approach for isolation."
    assert runtime.question == question
    assert runtime.source_thread is not None
    assert runtime.source_thread.thread_id == source_identity.thread_id
    assert runtime.source_thread.provider_session_id == source_identity.provider_session_id
    assert runtime.thread_config.sandbox is SandboxProfile.READ_ONLY
    assert runtime.turn_config.sandbox is SandboxProfile.READ_ONLY
    assert runtime.closed
    assert not runtime.interrupted
    main_turn = storage_context.repository.get_turn(main_turn_id)
    assert main_turn.state is TurnState.RUNNING
    assert storage_context.repository.get_active_revision(
        storage_context.conversation.id
    ).provider_thread_id == source_identity.thread_id
    rows = storage_context.store.query_all("SELECT * FROM side_queries")
    assert len(rows) == 1
    row = rows[0]
    assert row["state"] == "completed"
    assert row["question_hash"] is not None and row["answer_hash"] is not None
    assert question not in tuple(str(value) for value in row)
    assert answer not in tuple(str(value) for value in row)
    assert len(storage_context.store.query_all("SELECT id FROM turns")) == 1
    assert storage_context.store.query_all("SELECT sequence FROM events") == ()


@pytest.mark.asyncio
async def test_side_query_rejects_second_query_for_same_user(
    storage_context: StorageContext,
) -> None:
    _main_session(storage_context)
    runtime = _SideRuntime(block=True)
    coordinator = _coordinator(storage_context, runtime)
    first = asyncio.create_task(
        coordinator.ask(
            interaction_id="side-first",
            conversation_id=storage_context.conversation.id,
            requested_by_user_id=400,
            question="First question?",
        )
    )
    await asyncio.wait_for(runtime.started.wait(), timeout=1)

    with pytest.raises(SideQueryError) as error:
        await coordinator.ask(
            interaction_id="side-second",
            conversation_id=storage_context.conversation.id,
            requested_by_user_id=400,
            question="Second question?",
        )

    assert error.value.code == "btw_already_running"
    runtime.release.set()
    await first
    assert len(storage_context.store.query_all("SELECT id FROM side_queries")) == 1


@pytest.mark.asyncio
async def test_side_query_timeout_interrupts_and_unsubscribes(
    storage_context: StorageContext,
) -> None:
    _main_session(storage_context)
    runtime = _SideRuntime(block=True)
    coordinator = _coordinator(storage_context, runtime, timeout_seconds=0.01)

    with pytest.raises(SideQueryError) as error:
        await coordinator.ask(
            interaction_id="side-timeout",
            conversation_id=storage_context.conversation.id,
            requested_by_user_id=400,
            question="Will this time out?",
        )

    assert error.value.code == "btw_timeout"
    assert runtime.interrupted and runtime.closed
    row = storage_context.store.query_one(
        "SELECT state, terminal_code FROM side_queries"
    )
    assert row is not None
    assert (row["state"], row["terminal_code"]) == ("failed", "btw_timeout")


@pytest.mark.asyncio
async def test_side_query_shutdown_cancels_and_cleans_provider_handles(
    storage_context: StorageContext,
) -> None:
    _main_session(storage_context)
    runtime = _SideRuntime(block=True)
    coordinator = _coordinator(storage_context, runtime)
    asking = asyncio.create_task(
        coordinator.ask(
            interaction_id="side-shutdown",
            conversation_id=storage_context.conversation.id,
            requested_by_user_id=400,
            question="Will shutdown interrupt this?",
        )
    )
    await asyncio.wait_for(runtime.started.wait(), timeout=1)

    await coordinator.close()

    with pytest.raises(asyncio.CancelledError):
        await asking
    assert runtime.interrupted and runtime.closed
    row = storage_context.store.query_one(
        "SELECT state, terminal_code FROM side_queries"
    )
    assert row is not None
    assert (row["state"], row["terminal_code"]) == (
        "interrupted",
        "daemon_shutdown",
    )


def test_side_query_restart_interrupts_without_replay_or_content(
    storage_context: StorageContext,
) -> None:
    _main_session(storage_context)
    repository = SideQueryRepository(storage_context.store)
    record, created = repository.accept(
        interaction_id="side-before-restart",
        conversation_id=storage_context.conversation.id,
        requested_by_user_id=400,
        question_hash="question-hash",
        question_size=42,
        boot_id="old-boot",
    )
    assert created
    repository.mark_running(record.id)

    result = storage_context.repository.recover_startup(current_boot_id="new-boot")

    recovered = repository.get(record.id)
    assert result["interrupted_side_queries"] == 1
    assert recovered.state == "interrupted"
    assert recovered.terminal_code == "daemon_restarted"
    columns = storage_context.store.query_one(
        "SELECT question_hash, question_size, answer_hash, answer_size FROM side_queries"
    )
    assert columns is not None
    assert dict(columns) == {
        "question_hash": "question-hash",
        "question_size": 42,
        "answer_hash": None,
        "answer_size": None,
    }


@pytest.mark.asyncio
async def test_side_query_rejects_control_characters_before_provider(
    storage_context: StorageContext,
) -> None:
    _main_session(storage_context)
    runtime = _SideRuntime()
    coordinator = _coordinator(storage_context, runtime)

    with pytest.raises(SideQueryError) as error:
        await coordinator.ask(
            interaction_id="side-control",
            conversation_id=storage_context.conversation.id,
            requested_by_user_id=400,
            question="unsafe\x00question",
        )

    assert error.value.code == "btw_control_character"
    assert not runtime.started.is_set()
    assert storage_context.store.query_all("SELECT id FROM side_queries") == ()
