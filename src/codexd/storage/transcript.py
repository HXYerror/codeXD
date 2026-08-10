from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_VISIBLE_PHASES = frozenset({None, "commentary", "final_answer"})


@dataclass(frozen=True)
class TerminalAssistantTranscript:
    visible_text: str
    canonical_final_answer: str | None


def visible_assistant_text(
    ast: Mapping[str, Any],
    *,
    completed_only: bool,
) -> str:
    """Return visible agent-message text in durable AST order.

    Streaming callers may include the current incomplete agent message. Terminal
    callers must use :func:`terminal_assistant_transcript`, which excludes all
    incomplete blocks and keeps the canonical final answer last.
    """

    return "\n\n".join(
        text
        for _index, _block, text in _visible_text_blocks(
            ast,
            completed_only=completed_only,
        )
    )


def canonical_final_answer(ast: Mapping[str, Any]) -> str | None:
    selected = _canonical_final_block(ast)
    return selected[2] if selected is not None else None


def terminal_assistant_transcript(
    ast: Mapping[str, Any],
    *,
    fallback: str,
) -> TerminalAssistantTranscript:
    """Build the persistent Discord transcript from completed visible messages.

    Commentary, final-answer, and legacy phase-less agent messages are retained.
    The selected canonical final block is moved to the end exactly once. If no
    canonical final exists, the caller-provided terminal fallback is appended;
    incomplete blocks never become durable terminal content.
    """

    blocks = _visible_text_blocks(ast, completed_only=True)
    selected = _canonical_final_block(ast)
    selected_index = selected[0] if selected is not None else None
    parts = [text for index, _block, text in blocks if index != selected_index]
    canonical = selected[2] if selected is not None else None
    if canonical is not None:
        parts.append(canonical)
    elif fallback.strip():
        parts.append(fallback)
    return TerminalAssistantTranscript(
        visible_text="\n\n".join(parts),
        canonical_final_answer=canonical,
    )


def _canonical_final_block(
    ast: Mapping[str, Any],
) -> tuple[int, Mapping[str, Any], str] | None:
    blocks = _visible_text_blocks(ast, completed_only=True)
    final = [entry for entry in blocks if entry[1].get("phase") == "final_answer"]
    compatible = [entry for entry in blocks if entry[1].get("phase") is None]
    return final[-1] if final else (compatible[-1] if compatible else None)


def _visible_text_blocks(
    ast: Mapping[str, Any],
    *,
    completed_only: bool,
) -> list[tuple[int, Mapping[str, Any], str]]:
    raw_blocks = ast.get("blocks", [])
    if not isinstance(raw_blocks, list):
        return []
    visible: list[tuple[int, Mapping[str, Any], str]] = []
    for index, candidate in enumerate(raw_blocks):
        if not isinstance(candidate, dict) or candidate.get("kind") != "text":
            continue
        if completed_only and candidate.get("completed") is not True:
            continue
        if candidate.get("phase") not in _VISIBLE_PHASES:
            continue
        text = candidate.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        visible.append((index, candidate, text))
    return visible
