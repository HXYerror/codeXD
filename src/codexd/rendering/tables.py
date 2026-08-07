from __future__ import annotations

import io
import math
import os
import sys
import textwrap
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from codexd.domain.content_blocks import TableBlock


class TableRenderKind(StrEnum):
    PNG_WITH_SOURCE = "PNG_WITH_SOURCE"
    CODE_BLOCK_WITH_SOURCE = "CODE_BLOCK_WITH_SOURCE"
    SOURCE_ATTACHMENT_ONLY = "SOURCE_ATTACHMENT_ONLY"


@dataclass(frozen=True)
class TableLimits:
    max_columns: int = 20
    max_rows_png: int = 200
    max_cell_chars: int = 120
    max_source_bytes: int = 1024 * 1024
    max_page_width: int = 4096
    max_page_height: int = 4096
    max_pages: int = 8
    memory_mib: int = 128


@dataclass(frozen=True)
class RenderedTable:
    kind: TableRenderKind
    pages: tuple[bytes, ...]
    markdown: bytes
    summary: str
    fallback_code: str | None = None
    reason: str | None = None


class TableFontCoverageError(RuntimeError):
    pass


_Font = ImageFont.FreeTypeFont | ImageFont.ImageFont


class _FontChain:
    def __init__(self, fonts: tuple[_Font, ...]) -> None:
        if not fonts:
            raise TableFontCoverageError("table_font_coverage_missing")
        self.fonts = fonts
        self._resolved: dict[str, _Font] = {}

    def font_for(self, character: str, *, previous: _Font | None = None) -> _Font:
        if _is_joining_character(character):
            return previous or self.fonts[0]
        cached = self._resolved.get(character)
        if cached is not None:
            return cached
        for font in self.fonts:
            if _font_supports(font, character):
                self._resolved[character] = font
                return font
        raise TableFontCoverageError("table_font_coverage_missing")


def render_table(table: TableBlock, limits: TableLimits) -> RenderedTable:
    markdown = table.source_markdown.encode("utf-8")
    summary = f"Table with {len(table.rows)} rows and {len(table.headers)} columns."
    if len(markdown) > limits.max_source_bytes:
        return RenderedTable(
            kind=TableRenderKind.SOURCE_ATTACHMENT_ONLY,
            pages=(),
            markdown=markdown,
            summary=summary,
            reason="table_source_limit",
        )
    if (
        len(table.headers) > limits.max_columns
        or len(table.rows) > limits.max_rows_png
    ):
        return RenderedTable(
            kind=TableRenderKind.SOURCE_ATTACHMENT_ONLY,
            pages=(),
            markdown=markdown,
            summary=summary,
            reason="table_resource_limit",
        )
    try:
        pages = _render_png_pages(table, limits)
    except TableFontCoverageError:
        return RenderedTable(
            kind=TableRenderKind.CODE_BLOCK_WITH_SOURCE,
            pages=(),
            markdown=markdown,
            summary=summary,
            fallback_code=_code_fallback(table),
            reason="table_font_coverage_missing",
        )
    except (OSError, ValueError, RuntimeError) as exc:
        return RenderedTable(
            kind=TableRenderKind.CODE_BLOCK_WITH_SOURCE,
            pages=(),
            markdown=markdown,
            summary=summary,
            fallback_code=_code_fallback(table),
            reason=f"table_render_{type(exc).__name__}",
        )
    return RenderedTable(
        kind=TableRenderKind.PNG_WITH_SOURCE,
        pages=pages,
        markdown=markdown,
        summary=summary,
    )


def _render_png_pages(table: TableBlock, limits: TableLimits) -> tuple[bytes, ...]:
    font = _load_font_chain(22)
    bold = _load_font_chain(22, bold=True)
    _probe_glyphs(font, table.rows)
    _probe_glyphs(bold, (table.headers,))
    padding_x = 16
    padding_y = 12
    line_height = int(max(_text_height(font, "Ag中,😀"), 24) + 4)
    all_rows = (table.headers, *table.rows)
    wrapped_rows: list[list[list[str]]] = []
    column_widths: list[int] = [100] * len(table.headers)
    for row in all_rows:
        wrapped: list[list[str]] = []
        for index, value in enumerate(row):
            display = value[: limits.max_cell_chars]
            if len(value) > limits.max_cell_chars:
                display += "…"
            lines = _wrap_cell(display, 34)
            wrapped.append(lines)
            width = min(
                560,
                max(
                    100,
                    max(_text_width(font, line) for line in lines) + padding_x * 2,
                ),
            )
            column_widths[index] = max(column_widths[index], width)
        wrapped_rows.append(wrapped)
    total_width = sum(column_widths) + 1
    if total_width > limits.max_page_width:
        scale = (limits.max_page_width - 1) / total_width
        column_widths = [max(80, int(width * scale)) for width in column_widths]
        if sum(column_widths) + 1 > limits.max_page_width:
            raise ValueError("table_too_wide")
        wrapped_rows = [
            [
                _wrap_cell(value[: limits.max_cell_chars], max(8, int(width / 13)))
                for value, width in zip(row, column_widths, strict=True)
            ]
            for row in all_rows
        ]
    row_heights: list[int] = [
        int(max(len(cell) for cell in row) * line_height + padding_y * 2)
        for row in wrapped_rows
    ]
    header_height = row_heights[0]
    page_footer = 42
    body_budget = limits.max_page_height - header_height - page_footer
    pages_rows: list[tuple[int, int]] = []
    start = 1
    while start < len(wrapped_rows):
        height = 0
        end = start
        while end < len(wrapped_rows) and height + row_heights[end] <= body_budget:
            height += row_heights[end]
            end += 1
        if end == start:
            raise ValueError("table_row_too_tall")
        pages_rows.append((start, end))
        start = end
    if not pages_rows:
        pages_rows.append((1, 1))
    if len(pages_rows) > limits.max_pages:
        raise ValueError("table_too_many_pages")
    estimated_bytes = (
        sum(column_widths)
        * limits.max_page_height
        * 4
        * max(1, len(pages_rows))
    )
    if estimated_bytes > limits.memory_mib * 1024 * 1024:
        raise ValueError("table_memory_budget")
    return tuple(
        _draw_page(
            wrapped_rows,
            row_heights,
            column_widths,
            body_range,
            font,
            bold,
            line_height,
            padding_x,
            padding_y,
            page_number=index + 1,
            page_count=len(pages_rows),
            page_footer=page_footer,
        )
        for index, body_range in enumerate(pages_rows)
    )


def _draw_page(
    rows: list[list[list[str]]],
    heights: list[int],
    widths: list[int],
    body_range: tuple[int, int],
    font: _FontChain,
    bold: _FontChain,
    line_height: int,
    padding_x: int,
    padding_y: int,
    *,
    page_number: int,
    page_count: int,
    page_footer: int,
) -> bytes:
    selected = [0, *range(*body_range)]
    height = sum(heights[index] for index in selected) + page_footer + 1
    width = sum(widths) + 1
    image = Image.new("RGB", (width, height), "#ffffff")
    draw = ImageDraw.Draw(image)
    y = 0
    for visible_index, row_index in enumerate(selected):
        row_height = heights[row_index]
        background = "#e8edf6" if row_index == 0 else (
            "#f7f8fa" if visible_index % 2 == 0 else "#ffffff"
        )
        draw.rectangle((0, y, width, y + row_height), fill=background)
        x = 0
        for column, cell in enumerate(rows[row_index]):
            draw.rectangle((x, y, x + widths[column], y + row_height), outline="#596273")
            cell_font = bold if row_index == 0 else font
            alignment = "right" if _looks_numeric("\n".join(cell)) and row_index else "left"
            for line_index, line in enumerate(cell):
                text_width = _text_width(cell_font, line)
                text_x = (
                    x + widths[column] - padding_x - text_width
                    if alignment == "right"
                    else x + padding_x
                )
                _draw_text(
                    draw,
                    (text_x, y + padding_y + line_index * line_height),
                    line,
                    fill="#111827",
                    fonts=cell_font,
                )
            x += widths[column]
        y += row_height
    footer = f"page {page_number}/{page_count}"
    _draw_text(
        draw,
        (width - _text_width(font, footer) - 14, y + 10),
        footer,
        fill="#374151",
        fonts=font,
    )
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def _load_font_chain(size: int, *, bold: bool = False) -> _FontChain:
    fonts: list[_Font] = []
    for path in _font_candidates(bold):
        if path.exists():
            try:
                font = _load_candidate_font(path, size)
            except OSError:
                continue
            fonts.append(font)
    try:
        default = ImageFont.load_default(size=size)
    except TypeError:
        default = ImageFont.load_default()
    fonts.append(default)
    return _FontChain(tuple(fonts))


def _load_candidate_font(path: Path, size: int) -> _Font:
    try:
        return ImageFont.truetype(str(path), size=size)
    except OSError:
        if path.name != "Apple Color Emoji.ttc":
            raise
    supported_sizes = (20, 32, 40, 48, 64, 96, 160)
    nearest = min(supported_sizes, key=lambda candidate: abs(candidate - size))
    return ImageFont.truetype(str(path), size=nearest)


def _font_candidates(bold: bool) -> tuple[Path, ...]:
    if sys.platform == "darwin":
        suffix = "Bold" if bold else "Regular"
        return (
            Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
            Path("/System/Library/Fonts/PingFang.ttc"),
            Path(f"/System/Library/Fonts/Supplemental/Arial {suffix}.ttf"),
            Path("/System/Library/Fonts/Apple Color Emoji.ttc"),
        )
    if os.name == "nt":
        windir = Path(os.environ.get("WINDIR", r"C:\Windows"))
        return (
            windir / "Fonts" / ("msyhbd.ttc" if bold else "msyh.ttc"),
            windir / "Fonts" / ("segoeuib.ttf" if bold else "segoeui.ttf"),
            windir / "Fonts" / "seguiemj.ttf",
        )
    return (
        Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
        Path(
            "/usr/share/fonts/truetype/dejavu/"
            + ("DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf")
        ),
        Path("/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf"),
    )


def _probe_glyphs(font: _FontChain, rows: tuple[tuple[str, ...], ...]) -> None:
    values = ["A0中,😀"]
    values.extend(cell for row in rows for cell in row)
    for value in values:
        previous: _Font | None = None
        for character in value:
            previous = font.font_for(character, previous=previous)


def _wrap_cell(value: str, width: int) -> list[str]:
    lines: list[str] = []
    for original in value.splitlines() or [""]:
        lines.extend(textwrap.wrap(original, width=width, replace_whitespace=False) or [""])
    return lines


def _text_width(font: _FontChain, value: str) -> int:
    return math.ceil(
        sum(_font_text_width(run_font, text) for run_font, text in _font_runs(font, value))
    )


def _text_height(font: _FontChain, value: str) -> int:
    return math.ceil(
        max(
            (
                run_font.getbbox(text or " ")[3] - run_font.getbbox(text or " ")[1]
                for run_font, text in _font_runs(font, value)
            ),
            default=0,
        )
    )


def _font_runs(font: _FontChain, value: str) -> tuple[tuple[_Font, str], ...]:
    text = value or " "
    runs: list[tuple[_Font, str]] = []
    previous: _Font | None = None
    for character in text:
        selected = font.font_for(character, previous=previous)
        if runs and runs[-1][0] is selected:
            runs[-1] = (selected, runs[-1][1] + character)
        else:
            runs.append((selected, character))
        previous = selected
    return tuple(runs)


def _draw_text(
    draw: ImageDraw.ImageDraw,
    position: tuple[int, int],
    value: str,
    *,
    fill: str,
    fonts: _FontChain,
) -> None:
    x = float(position[0])
    y = position[1]
    for font, text in _font_runs(fonts, value):
        draw.text((x, y), text, fill=fill, font=font)
        x += _font_text_width(font, text)


def _font_text_width(font: _Font, value: str) -> float:
    getlength = getattr(font, "getlength", None)
    if callable(getlength):
        return float(getlength(value or " "))
    box = font.getbbox(value or " ")
    return float(box[2] - box[0])


def _font_supports(font: _Font, character: str) -> bool:
    if character.isspace() or _is_joining_character(character):
        return True
    signature = _glyph_signature(font, character)
    if signature is None or not signature[1]:
        return False
    if character == "\ufffd":
        return True
    missing = {
        candidate
        for sentinel in ("\u0378", "\u0380", "\uffff", "\ufffd")
        if (candidate := _glyph_signature(font, sentinel)) is not None
    }
    return signature not in missing


def _glyph_signature(font: _Font, character: str) -> tuple[tuple[int, int], bytes] | None:
    try:
        mask = font.getmask(character)
    except (OSError, ValueError):
        return None
    return mask.size, bytes(mask)


def _is_joining_character(character: str) -> bool:
    return unicodedata.category(character) in {"Cf", "Mn", "Me"}


def _looks_numeric(value: str) -> bool:
    candidate = value.replace(",", "").replace(".", "").replace("-", "").strip()
    return bool(candidate) and candidate.isdigit()


def _code_fallback(table: TableBlock) -> str:
    widths = [
        max(len(table.headers[index]), *(len(row[index]) for row in table.rows))
        for index in range(len(table.headers))
    ]
    lines = [
        " | ".join(value.ljust(widths[index]) for index, value in enumerate(table.headers)),
        "-+-".join("-" * width for width in widths),
    ]
    lines.extend(
        " | ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        for row in table.rows
    )
    return "\n".join(lines)
