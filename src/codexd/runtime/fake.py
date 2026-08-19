from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Iterable
from dataclasses import replace
from pathlib import Path

from codexd.domain.capabilities import (
    CapabilityManifest,
    CompatibilityInfo,
    EventCapability,
)
from codexd.domain.conversations import (
    ThreadConfig,
    ThreadIdentity,
    ThreadProviderState,
    ThreadSnapshot,
    TurnConfig,
)
from codexd.domain.events import NormalizedEvent
from codexd.domain.ids import utc_now_ms
from codexd.domain.models import (
    AccountStatus,
    ModelCatalogSnapshot,
    ModelDescriptor,
    ServiceTierDescriptor,
)
from codexd.domain.turns import TurnIdentity, TurnInput
from codexd.errors import NotFoundError
from codexd.runtime.errors import file_input_unsupported
from codexd.runtime.port import CompactStartResult, StartedTurn, TurnStream


class FakeCodexRuntime:
    def __init__(
        self,
        *,
        generation: int = 1,
        event_delay: float = 0,
        attachment_materialization_supported: bool = True,
    ) -> None:
        self.generation = generation
        self.event_delay = event_delay
        self.attachment_materialization_supported = attachment_materialization_supported
        self.closed = False
        self._thread_counter = 0
        self._turn_counter = 0
        self._threads: dict[str, ThreadIdentity] = {}
        self._thread_states: dict[str, ThreadProviderState] = {}
        self._scripts: deque[tuple[NormalizedEvent, ...]] = deque()
        self._turn_events: dict[str, tuple[NormalizedEvent, ...]] = {}
        self._interrupts: set[str] = set()
        self.steers: dict[str, list[str]] = defaultdict(list)
        self.started_inputs: list[TurnInput] = []

    def script(self, events: Iterable[NormalizedEvent]) -> None:
        self._scripts.append(tuple(events))

    async def capabilities(self) -> CapabilityManifest:
        return CapabilityManifest(
            adapter="fake",
            sdk_version="fake",
            runtime_version="fake",
            compatibility=CompatibilityInfo("fake", "test", "passed"),
            image_input_modes=("local_path",),
            required={
                "thread.start": True,
                "thread.resume": True,
                "thread.read": True,
                "turn.stream": True,
                "turn.interrupt": True,
                "turn.steer": True,
                "turn.image_input": True,
                "turn.model_override": True,
                "turn.reasoning_effort": True,
                "model.catalog": True,
                "event.turn_lifecycle": True,
                "thread.identity": True,
                "sandbox.configure": True,
                "approval.configure": True,
                "runtime.close": True,
            },
            optional={
                "thread.archive": True,
                "thread.unarchive": True,
                "thread.fork": True,
                "thread.set_name": True,
                "thread.compact": True,
                "turn.personality": True,
                "turn.reasoning_summary": True,
                "turn.service_tier": True,
                "web_search.config": True,
                "mention.input": False,
                "codexd.attachment_materialization": (
                    self.attachment_materialization_supported
                ),
                "collab.item": EventCapability.SUPPORTED_NOT_OBSERVED,
            },
        )

    async def list_models(self) -> ModelCatalogSnapshot:
        return ModelCatalogSnapshot(
            models=(
                ModelDescriptor(
                    id="fake-model",
                    model="fake-model",
                    display_name="Fake Model",
                    description="Fake runtime model",
                    is_default=True,
                    input_modalities=("text", "image"),
                    supported_reasoning_efforts=("low", "medium", "high"),
                    default_reasoning_effort="medium",
                    supports_personality=True,
                    service_tiers=(
                        ServiceTierDescriptor(
                            id="flex",
                            name="Flex",
                            description="Fake flex tier",
                        ),
                    ),
                    default_service_tier="flex",
                    upgrade=None,
                ),
            ),
            complete=True,
            next_cursor=None,
        )

    async def account_status(self) -> AccountStatus:
        return AccountStatus(False, "fake", None, utc_now_ms())

    async def start_thread(self, *, cwd: Path, config: ThreadConfig) -> ThreadIdentity:
        del cwd, config
        self._thread_counter += 1
        thread_id = f"fake-thread-{self._thread_counter}"
        identity = ThreadIdentity(
            thread_id=thread_id,
            requested_thread_id=None,
            provider_session_id=thread_id,
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="fake",
        )
        self._threads[thread_id] = identity
        self._thread_states[thread_id] = ThreadProviderState.IDLE
        return identity

    async def resume_thread(
        self, *, thread_id: str, cwd: Path, config: ThreadConfig
    ) -> ThreadIdentity:
        del cwd, config
        identity = self._threads.get(thread_id)
        if identity is None:
            raise NotFoundError(f"fake thread not found: {thread_id}")
        return ThreadIdentity(
            thread_id=identity.thread_id,
            requested_thread_id=thread_id,
            provider_session_id=identity.provider_session_id,
            forked_from_thread_id=identity.forked_from_thread_id,
            parent_thread_id=identity.parent_thread_id,
            provider_version=identity.provider_version,
        )

    async def fork_thread(
        self, *, thread_id: str, cwd: Path, config: ThreadConfig
    ) -> ThreadIdentity:
        if thread_id not in self._threads:
            raise NotFoundError(f"fake thread not found: {thread_id}")
        identity = await self.start_thread(cwd=cwd, config=config)
        forked = ThreadIdentity(
            thread_id=identity.thread_id,
            requested_thread_id=None,
            provider_session_id=self._threads[thread_id].provider_session_id,
            forked_from_thread_id=thread_id,
            parent_thread_id=None,
            provider_version="fake",
        )
        self._threads[forked.thread_id] = forked
        return forked

    async def read_thread(self, thread_id: str) -> ThreadSnapshot:
        identity = self._threads.get(thread_id)
        if identity is None:
            raise NotFoundError(f"fake thread not found: {thread_id}")
        return ThreadSnapshot(
            replace(identity, requested_thread_id=thread_id),
            self._thread_states[thread_id],
        )

    async def set_thread_name(self, thread_id: str, name: str) -> None:
        del name
        if thread_id not in self._threads:
            raise NotFoundError(f"fake thread not found: {thread_id}")

    async def compact_thread(self, thread_id: str) -> CompactStartResult:
        if thread_id not in self._threads:
            raise NotFoundError(f"fake thread not found: {thread_id}")
        self._thread_states[thread_id] = ThreadProviderState.ACTIVE
        return CompactStartResult(accepted=True)

    async def start_turn(
        self,
        *,
        local_turn_id: str,
        thread: ThreadIdentity,
        input: TurnInput,
        config: TurnConfig,
    ) -> StartedTurn:
        if input.files and (
            not self.attachment_materialization_supported
            or input.attachment_context is None
            or not input.materialized_files
        ):
            raise file_input_unsupported(
                generation=self.generation,
                thread_id=thread.thread_id,
                turn_id=local_turn_id,
            )
        del config
        if thread.thread_id not in self._threads:
            raise NotFoundError(f"fake thread not found: {thread.thread_id}")
        self.started_inputs.append(input)
        self._turn_counter += 1
        provider_turn_id = f"fake-turn-{self._turn_counter}"
        identity = TurnIdentity(local_turn_id, provider_turn_id, self.generation)
        events = self._scripts.popleft() if self._scripts else _default_events(provider_turn_id)
        self._turn_events[provider_turn_id] = events
        self._thread_states[thread.thread_id] = ThreadProviderState.ACTIVE

        async def iterator() -> AsyncIterator[NormalizedEvent]:
            for event in self._turn_events[provider_turn_id]:
                if self.event_delay:
                    await asyncio.sleep(self.event_delay)
                if provider_turn_id in self._interrupts and not event.kind.startswith("turn."):
                    continue
                yield event
            self._thread_states[thread.thread_id] = ThreadProviderState.IDLE

        return StartedTurn(identity=identity, stream=TurnStream(iterator))

    async def steer(self, turn: TurnIdentity, text: str) -> None:
        if not turn.provider_turn_id or turn.provider_turn_id not in self._turn_events:
            raise NotFoundError("fake Turn handle not found")
        self.steers[turn.provider_turn_id].append(text)

    async def interrupt(self, turn: TurnIdentity) -> None:
        if not turn.provider_turn_id or turn.provider_turn_id not in self._turn_events:
            raise NotFoundError("fake Turn handle not found")
        self._interrupts.add(turn.provider_turn_id)

    async def archive_thread(self, thread_id: str) -> None:
        if thread_id not in self._threads:
            raise NotFoundError(f"fake thread not found: {thread_id}")

    async def unarchive_thread(self, thread_id: str) -> ThreadIdentity:
        identity = self._threads.get(thread_id)
        if identity is None:
            raise NotFoundError(f"fake thread not found: {thread_id}")
        return ThreadIdentity(
            thread_id=identity.thread_id,
            requested_thread_id=thread_id,
            provider_session_id=identity.provider_session_id,
            forked_from_thread_id=identity.forked_from_thread_id,
            parent_thread_id=identity.parent_thread_id,
            provider_version=identity.provider_version,
        )

    async def close(self) -> None:
        self.closed = True


def _default_events(provider_turn_id: str) -> tuple[NormalizedEvent, ...]:
    return (
        NormalizedEvent("turn.started", {"provider_turn_id": provider_turn_id}),
        NormalizedEvent(
            "assistant.text.completed",
            {"item_id": "fake-message", "text": "fake response", "phase": "final_answer"},
        ),
        NormalizedEvent(
            "turn.completed",
            {"provider_turn_id": provider_turn_id, "status": "completed"},
        ),
    )
