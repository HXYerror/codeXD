from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from codexd.domain.capabilities import CapabilityManifest
from codexd.domain.conversations import (
    ThreadConfig,
    ThreadIdentity,
    ThreadSnapshot,
    TurnConfig,
)
from codexd.domain.events import NormalizedEvent
from codexd.domain.models import AccountStatus, ModelCatalogSnapshot
from codexd.domain.turns import TurnIdentity, TurnInput
from codexd.errors import InvariantError


@dataclass(frozen=True)
class RuntimeSlotConfig:
    scope_kind: str
    project_id: str | None
    cwd: Path
    codex_home: Path | None
    environment: dict[str, str]
    environment_hash: str
    topology_contract: str
    codex_bin: Path | None = None
    sqlite_home: Path | None = None


@dataclass(frozen=True)
class DynamicToolCall:
    runtime_generation: int
    local_turn_id: str
    provider_thread_id: str
    provider_turn_id: str
    provider_call_id: str
    namespace: str | None
    tool: str
    arguments: object
    observed_image_paths: tuple[str, ...] = ()


DynamicToolHandler = Callable[
    [DynamicToolCall],
    Awaitable[dict[str, object]],
]


class TurnStream:
    def __init__(self, iterator_factory: Callable[[], AsyncIterator[NormalizedEvent]]) -> None:
        self._iterator_factory = iterator_factory
        self._claimed = False

    def __aiter__(self) -> AsyncIterator[NormalizedEvent]:
        if self._claimed:
            raise InvariantError("Turn stream already has a consumer")
        self._claimed = True
        return self._iterator_factory()


@dataclass(frozen=True)
class StartedTurn:
    identity: TurnIdentity
    stream: TurnStream


@dataclass(frozen=True)
class SideQueryIdentity:
    local_query_id: str
    source_thread_id: str
    side_thread_id: str
    provider_turn_id: str
    runtime_generation: int


@dataclass(frozen=True)
class StartedSideQuery:
    identity: SideQueryIdentity
    stream: TurnStream


@dataclass(frozen=True)
class CompactStartResult:
    accepted: bool


class CodexRuntime(Protocol):
    generation: int

    async def capabilities(self) -> CapabilityManifest: ...

    async def list_models(self) -> ModelCatalogSnapshot: ...

    async def account_status(self) -> AccountStatus: ...

    async def start_thread(self, *, cwd: Path, config: ThreadConfig) -> ThreadIdentity: ...

    async def resume_thread(
        self, *, thread_id: str, cwd: Path, config: ThreadConfig
    ) -> ThreadIdentity: ...

    async def fork_thread(
        self, *, thread_id: str, cwd: Path, config: ThreadConfig
    ) -> ThreadIdentity: ...

    async def read_thread(self, thread_id: str) -> ThreadSnapshot: ...

    async def set_thread_name(self, thread_id: str, name: str) -> None: ...

    async def compact_thread(self, thread_id: str) -> CompactStartResult: ...

    async def start_turn(
        self,
        *,
        local_turn_id: str,
        thread: ThreadIdentity,
        input: TurnInput,
        config: TurnConfig,
    ) -> StartedTurn: ...

    async def start_side_query(
        self,
        *,
        local_query_id: str,
        source_thread: ThreadIdentity,
        question: str,
        cwd: Path,
        thread_config: ThreadConfig,
        turn_config: TurnConfig,
    ) -> StartedSideQuery: ...

    async def interrupt_side_query(self, query: SideQueryIdentity) -> None: ...

    async def close_side_query(self, query: SideQueryIdentity) -> None: ...

    async def steer(self, turn: TurnIdentity, text: str) -> None: ...

    async def interrupt(self, turn: TurnIdentity) -> None: ...

    async def archive_thread(self, thread_id: str) -> None: ...

    async def unarchive_thread(self, thread_id: str) -> ThreadIdentity: ...

    async def close(self) -> None: ...
