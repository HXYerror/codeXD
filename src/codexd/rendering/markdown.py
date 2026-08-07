from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from markdown_it import MarkdownIt
from markdown_it.token import Token

from codexd.domain.content_blocks import CodeBlock, ContentBlock, TableBlock, TextBlock


@dataclass(frozen=True)
class _MappedBlock:
    start: int
    end: int
    block: ContentBlock


class MarkdownContentParser:
    def __init__(self) -> None:
        self._parser = MarkdownIt("commonmark", {"html": False}).enable("table")

    def parse(self, source: str) -> tuple[ContentBlock, ...]:
        if not source:
            return ()
        lines = source.splitlines(keepends=True)
        tokens = self._parser.parse(source)
        mapped: list[_MappedBlock] = []
        index = 0
        while index < len(tokens):
            token = tokens[index]
            if token.type == "table_open" and token.map:
                end_index = _find_close(tokens, index, "table_close")
                table = _table_from_tokens(
                    tokens[index : end_index + 1],
                    "".join(lines[token.map[0] : token.map[1]]),
                )
                if table is not None:
                    mapped.append(_MappedBlock(token.map[0], token.map[1], table))
                index = end_index + 1
                continue
            if token.type in {"fence", "code_block"} and token.map:
                mapped.append(
                    _MappedBlock(
                        token.map[0],
                        token.map[1],
                        CodeBlock(code=token.content, language=(token.info.strip() or None)),
                    )
                )
            index += 1

        if not mapped:
            return (TextBlock(source),)
        mapped.sort(key=lambda item: (item.start, item.end))
        result: list[ContentBlock] = []
        cursor = 0
        for item in mapped:
            if item.start < cursor:
                continue
            prefix = "".join(lines[cursor : item.start])
            if prefix:
                result.append(TextBlock(prefix))
            result.append(item.block)
            cursor = item.end
        suffix = "".join(lines[cursor:])
        if suffix:
            result.append(TextBlock(suffix))
        return tuple(result)


def _find_close(tokens: list[Token], start: int, close_type: str) -> int:
    for index in range(start + 1, len(tokens)):
        if tokens[index].type == close_type:
            return index
    return start


def _table_from_tokens(tokens: list[Token], source: str) -> TableBlock | None:
    headers: list[str] = []
    rows: list[tuple[str, ...]] = []
    alignments: list[str | None] = []
    current: list[str] | None = None
    in_header = False
    cell_alignment: str | None = None

    for token in tokens:
        if token.type == "thead_open":
            in_header = True
        elif token.type == "thead_close":
            in_header = False
        elif token.type == "tr_open":
            current = []
        elif token.type in {"th_open", "td_open"}:
            cell_alignment = _alignment(token.attrs)
        elif token.type == "inline" and current is not None:
            current.append(token.content)
            if in_header:
                alignments.append(cell_alignment)
        elif token.type == "tr_close" and current is not None:
            if in_header and not headers:
                headers = current
            else:
                rows.append(tuple(current))
            current = None
    if not headers or any(len(row) != len(headers) for row in rows):
        return None
    return TableBlock(
        headers=tuple(headers),
        rows=tuple(rows),
        source_markdown=source,
        alignments=tuple(alignments[: len(headers)]),
    )


def _alignment(attrs: dict[str, Any]) -> str | None:
    style = str(attrs.get("style", ""))
    if "text-align:left" in style:
        return "left"
    if "text-align:right" in style:
        return "right"
    if "text-align:center" in style:
        return "center"
    return None

