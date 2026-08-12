from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field
from typing import Any

from codexd.domain.turns import TurnInput
from codexd.storage.transcript import (
    terminal_assistant_transcript,
    visible_assistant_text,
)

_MAX_VOLATILE_TURNS = 1024


@dataclass(frozen=True)
class VolatileFinalContent:
    visible_text: str
    final_answer_text: str | None


@dataclass
class _VolatileTurn:
    input: TurnInput | None = None
    content_ast: dict[str, Any] = field(
        default_factory=lambda: {"schema_version": 1, "blocks": []}
    )
    latest_usage: dict[str, Any] | None = None
    final: VolatileFinalContent | None = None


class VolatileTurnStore:
    """Process-local conversation content; nothing in this store touches disk."""

    def __init__(self) -> None:
        self._turns: dict[str, _VolatileTurn] = {}
        self._lock = threading.RLock()

    def put_input(self, turn_id: str, turn_input: TurnInput) -> None:
        with self._lock:
            self._entry(turn_id).input = turn_input

    def input(self, turn_id: str) -> TurnInput | None:
        with self._lock:
            entry = self._turns.get(turn_id)
            return entry.input if entry is not None else None

    def drop_input(self, turn_id: str) -> None:
        with self._lock:
            entry = self._turns.get(turn_id)
            if entry is not None:
                entry.input = None

    def content_ast(self, turn_id: str) -> dict[str, Any]:
        with self._lock:
            entry = self._entry(turn_id)
            return copy.deepcopy(entry.content_ast)

    def save_content_ast(self, turn_id: str, ast: dict[str, Any]) -> None:
        with self._lock:
            self._entry(turn_id).content_ast = copy.deepcopy(ast)

    def save_usage(self, turn_id: str, usage: dict[str, Any]) -> None:
        with self._lock:
            self._entry(turn_id).latest_usage = copy.deepcopy(usage)

    def usage(self, turn_id: str) -> dict[str, Any] | None:
        with self._lock:
            entry = self._turns.get(turn_id)
            if entry is None or entry.latest_usage is None:
                return None
            return copy.deepcopy(entry.latest_usage)

    def preview(self, turn_id: str) -> str:
        with self._lock:
            entry = self._turns.get(turn_id)
            if entry is None:
                return ""
            return visible_assistant_text(entry.content_ast, completed_only=False)

    def finalize(self, turn_id: str, *, fallback: str) -> VolatileFinalContent:
        with self._lock:
            entry = self._entry(turn_id)
            if entry.final is not None:
                return entry.final
            transcript = terminal_assistant_transcript(
                entry.content_ast,
                fallback=fallback,
            )
            final = VolatileFinalContent(
                visible_text=transcript.visible_text,
                final_answer_text=transcript.canonical_final_answer,
            )
            entry.final = final
            entry.input = None
            entry.content_ast = {"schema_version": 1, "blocks": []}
            return final

    def final(self, turn_id: str) -> VolatileFinalContent | None:
        with self._lock:
            entry = self._turns.get(turn_id)
            return entry.final if entry is not None else None

    def put_final(
        self,
        turn_id: str,
        *,
        visible_text: str,
        final_answer_text: str | None = None,
    ) -> None:
        with self._lock:
            entry = self._entry(turn_id)
            entry.final = VolatileFinalContent(visible_text, final_answer_text)
            entry.input = None
            entry.content_ast = {"schema_version": 1, "blocks": []}

    def discard(self, turn_id: str) -> None:
        with self._lock:
            self._turns.pop(turn_id, None)

    def _entry(self, turn_id: str) -> _VolatileTurn:
        entry = self._turns.get(turn_id)
        if entry is not None:
            return entry
        if len(self._turns) >= _MAX_VOLATILE_TURNS:
            oldest = next(iter(self._turns))
            self._turns.pop(oldest, None)
        entry = _VolatileTurn()
        self._turns[turn_id] = entry
        return entry
