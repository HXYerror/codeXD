from __future__ import annotations

import asyncio
from dataclasses import dataclass

from codexd.domain.events import NormalizedEvent
from codexd.domain.turns import TurnState
from codexd.runtime.errors import AdapterError, EventJournalError
from codexd.runtime.port import StartedTurn
from codexd.storage.projectors import ProjectingEventSink
from codexd.storage.repository import Repository


@dataclass(frozen=True)
class PumpResult:
    terminal_state: TurnState
    terminal_code: str


class EventPump:
    def __init__(
        self,
        *,
        repository: Repository,
        sink: ProjectingEventSink,
    ) -> None:
        self._repository = repository
        self._sink = sink

    async def run(self, *, local_turn_id: str, started: StartedTurn) -> PumpResult:
        terminal: tuple[TurnState, str] | None = None
        stale_generation = False
        try:
            async for event in started.stream:
                terminal, sequence = await self._record_event(
                    local_turn_id=local_turn_id,
                    started=started,
                    event=event,
                )
                if sequence is None:
                    stale_generation = True
                    terminal = None
                    continue
                if terminal:
                    return PumpResult(*terminal)
        except asyncio.CancelledError:
            raise
        except EventJournalError:
            raise
        except AdapterError as exc:
            code = (
                exc.failure.code
                if exc.failure.code.startswith(("runtime_", "stream_"))
                else f"stream_{exc.failure.code}"
            )
            terminal = (TurnState.INTERRUPTED, code)
            recorded = await self._record_stream_failure(
                local_turn_id, started, code
            )
            return (
                PumpResult(*terminal)
                if recorded
                else PumpResult(TurnState.INTERRUPTED, "stale_generation_ignored")
            )
        except Exception as exc:
            code = f"stream_event_pump_{type(exc).__name__}"
            recorded = await self._record_stream_failure(local_turn_id, started, code)
            return (
                PumpResult(TurnState.INTERRUPTED, code)
                if recorded
                else PumpResult(TurnState.INTERRUPTED, "stale_generation_ignored")
            )
        if terminal is None:
            recorded = await self._record_stream_failure(
                local_turn_id, started, "stream_ended_without_terminal"
            )
            if not recorded or stale_generation:
                return PumpResult(
                    TurnState.INTERRUPTED, "stale_generation_ignored"
                )
            terminal = (TurnState.INTERRUPTED, "stream_ended_without_terminal")
        return PumpResult(*terminal)

    async def _record_event(
        self,
        *,
        local_turn_id: str,
        started: StartedTurn,
        event: NormalizedEvent,
    ) -> tuple[tuple[TurnState, str] | None, int | None]:
        try:
            recorded = await asyncio.to_thread(
                self._sink.record,
                turn_id=local_turn_id,
                runtime_generation=started.identity.runtime_generation,
                event=event,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise EventJournalError(
                f"could not durably record provider event {event.kind}"
            ) from exc
        return recorded.terminal, recorded.sequence

    async def _record_stream_failure(
        self,
        turn_id: str,
        started: StartedTurn,
        code: str,
    ) -> bool:
        event = NormalizedEvent(
            "runtime.stream_interrupted",
            {"code": code},
            raw_type="runtime",
        )
        try:
            recorded = await asyncio.to_thread(
                self._sink.record,
                turn_id=turn_id,
                runtime_generation=started.identity.runtime_generation,
                event=event,
                terminal_state=TurnState.INTERRUPTED,
                terminal_code=code,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise EventJournalError(
                "could not durably record provider stream failure"
            ) from exc
        return recorded.sequence is not None
