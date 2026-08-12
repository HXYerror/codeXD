from __future__ import annotations

import asyncio
from dataclasses import dataclass

from codexd.domain.events import NormalizedEvent
from codexd.domain.turns import TurnState
from codexd.runtime.errors import AdapterError, EventJournalError
from codexd.runtime.port import StartedTurn
from codexd.storage.projectors import ProjectingEventSink
from codexd.storage.repository import Repository

_BUFFERED_EVENT_KINDS = frozenset(
    {
        "assistant.text.delta",
        "plan.delta",
        "reasoning.summary",
        "reasoning.hidden_delta_discarded",
        "command.output.delta",
        "file_change.output.delta",
        "diff.updated",
        "plan.updated",
        "usage.updated",
    }
)
_REPLACE_BUFFERED_KINDS = frozenset({"diff.updated", "plan.updated", "usage.updated"})
_MAX_BUFFERED_TEXT_BYTES = 64 * 1024


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
        stream = started.stream.__aiter__()
        next_event: asyncio.Future[NormalizedEvent] | None = None
        buffered: NormalizedEvent | None = None
        flush_deadline: float | None = None

        async def persist(event: NormalizedEvent) -> PumpResult | None:
            nonlocal stale_generation
            recorded_terminal, sequence = await self._record_event(
                local_turn_id=local_turn_id,
                started=started,
                event=event,
            )
            if sequence is None:
                stale_generation = True
                return None
            if recorded_terminal is not None:
                return PumpResult(*recorded_terminal)
            return None

        try:
            loop = asyncio.get_running_loop()
            flush_seconds = max(0.001, self._sink.stream_update_ms / 1000)
            while True:
                if next_event is None:
                    next_event = asyncio.ensure_future(anext(stream))
                timeout = (
                    max(0.0, flush_deadline - loop.time())
                    if buffered is not None and flush_deadline is not None
                    else None
                )
                done, _pending = await asyncio.wait(
                    {next_event},
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    assert buffered is not None
                    result = await persist(buffered)
                    buffered = None
                    flush_deadline = None
                    if result is not None:
                        return result
                    continue
                completed = next_event
                next_event = None
                try:
                    event = completed.result()
                except StopAsyncIteration:
                    break
                except BaseException:
                    if buffered is not None:
                        result = await persist(buffered)
                        buffered = None
                        flush_deadline = None
                        if result is not None:
                            return result
                    raise
                if _buffer_key(event) is not None:
                    merged = (
                        _merge_buffered(buffered, event)
                        if buffered is not None
                        else None
                    )
                    if merged is None and buffered is not None:
                        result = await persist(buffered)
                        if result is not None:
                            return result
                        buffered = None
                        flush_deadline = None
                    buffered = merged or event
                    if flush_deadline is None:
                        flush_deadline = loop.time() + flush_seconds
                    if (
                        buffered.kind not in _REPLACE_BUFFERED_KINDS
                        and _buffered_size(buffered) >= _MAX_BUFFERED_TEXT_BYTES
                    ):
                        result = await persist(buffered)
                        buffered = None
                        flush_deadline = None
                        if result is not None:
                            return result
                    continue
                if buffered is not None:
                    result = await persist(buffered)
                    buffered = None
                    flush_deadline = None
                    if result is not None:
                        return result
                result = await persist(event)
                if result is not None:
                    return result
            if buffered is not None:
                result = await persist(buffered)
                buffered = None
                flush_deadline = None
                if result is not None:
                    return result
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
        finally:
            if next_event is not None:
                if not next_event.done():
                    next_event.cancel()
                await asyncio.gather(next_event, return_exceptions=True)
            close = getattr(stream, "aclose", None)
            if callable(close):
                await close()
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


def _buffer_key(event: NormalizedEvent) -> tuple[object, ...] | None:
    if event.kind not in _BUFFERED_EVENT_KINDS or event.provider_event_id is not None:
        return None
    if event.kind in _REPLACE_BUFFERED_KINDS:
        return (event.kind,)
    item_id = event.payload.get("item_id")
    if not isinstance(item_id, str):
        return None
    if event.kind == "reasoning.summary":
        return (event.kind, item_id, event.payload.get("summary_index"))
    return (event.kind, item_id)


def _merge_buffered(
    previous: NormalizedEvent | None,
    current: NormalizedEvent,
) -> NormalizedEvent | None:
    if previous is None or _buffer_key(previous) != _buffer_key(current):
        return None
    if current.kind in _REPLACE_BUFFERED_KINDS:
        return current
    if current.kind == "reasoning.hidden_delta_discarded":
        previous_size = int(previous.payload.get("raw_size", previous.raw_size or 0))
        current_size = int(current.payload.get("raw_size", current.raw_size or 0))
        payload = {
            "item_id": current.payload.get("item_id"),
            "chunk_count": int(previous.payload.get("chunk_count", 1)) + 1,
            "raw_size": previous_size + current_size,
        }
    else:
        previous_text = previous.payload.get("text", "")
        current_text = current.payload.get("text", "")
        if not isinstance(previous_text, str) or not isinstance(current_text, str):
            return None
        payload = {**previous.payload, **current.payload, "text": previous_text + current_text}
    raw_size = (
        (previous.raw_size or 0) + (current.raw_size or 0)
        if previous.raw_size is not None or current.raw_size is not None
        else None
    )
    return NormalizedEvent(
        kind=current.kind,
        payload=payload,
        occurred_at=current.occurred_at,
        raw_type=current.raw_type or previous.raw_type,
        raw_hash=None,
        raw_size=raw_size,
        schema_version=current.schema_version,
    )


def _buffered_size(event: NormalizedEvent) -> int:
    text = event.payload.get("text")
    if isinstance(text, str):
        return len(text.encode())
    diff = event.payload.get("diff")
    if isinstance(diff, str):
        return len(diff.encode())
    return int(event.raw_size or event.payload.get("raw_size") or 0)
