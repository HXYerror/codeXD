from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from codexd.domain.ids import sha256_text


class BlockKind(StrEnum):
    TEXT = "text"
    CODE = "code"
    TABLE = "table"
    TOOL = "tool"
    FILE_CHANGE = "file_change"
    PLAN = "plan"
    TASK_CARD = "task_card"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class TextBlock:
    text: str
    kind: BlockKind = BlockKind.TEXT


@dataclass(frozen=True)
class CodeBlock:
    code: str
    language: str | None = None
    kind: BlockKind = BlockKind.CODE


@dataclass(frozen=True)
class TableBlock:
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    source_markdown: str
    alignments: tuple[str | None, ...] = ()
    parse_warnings: tuple[str, ...] = ()
    complete: bool = True
    kind: BlockKind = BlockKind.TABLE

    def __post_init__(self) -> None:
        width = len(self.headers)
        if width == 0 or any(len(row) != width for row in self.rows):
            raise ValueError("TableBlock rows must match a non-empty header")
        if self.alignments and len(self.alignments) != width:
            raise ValueError("TableBlock alignments must match the header")

    @property
    def block_id(self) -> str:
        return sha256_text(self.source_markdown)[:20]


@dataclass(frozen=True)
class ToolBlock:
    tool_kind: str
    label: str
    state: str
    summary: Mapping[str, Any] = field(default_factory=dict)
    kind: BlockKind = BlockKind.TOOL


@dataclass(frozen=True)
class FileChangeBlock:
    summary: str
    paths: tuple[str, ...]
    kind: BlockKind = BlockKind.FILE_CHANGE


@dataclass(frozen=True)
class PlanBlock:
    text: str
    kind: BlockKind = BlockKind.PLAN


@dataclass(frozen=True)
class TaskCardBlock:
    view_id: str
    title: str
    state: str
    expanded: bool
    safe_summary: str | None
    revision: int
    kind: BlockKind = BlockKind.TASK_CARD


@dataclass(frozen=True)
class ErrorBlock:
    code: str
    message: str
    kind: BlockKind = BlockKind.ERROR


@dataclass(frozen=True)
class UnknownBlock:
    provider_type: str
    summary: str
    kind: BlockKind = BlockKind.UNKNOWN


ContentBlock = (
    TextBlock
    | CodeBlock
    | TableBlock
    | ToolBlock
    | FileChangeBlock
    | PlanBlock
    | TaskCardBlock
    | ErrorBlock
    | UnknownBlock
)
