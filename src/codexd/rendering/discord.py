from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from codexd.domain.content_blocks import CodeBlock, TableBlock, TextBlock
from codexd.errors import InvariantError
from codexd.rendering.markdown import MarkdownContentParser
from codexd.rendering.media_worker import MediaWorker, MediaWorkerError
from codexd.rendering.tables import TableLimits, TableRenderKind

DISCORD_MESSAGE_LIMIT = 1900
DISCORD_ATTACHMENT_LIMIT_BYTES = 8 * 1024 * 1024


class AttachmentKind(StrEnum):
    GENERIC = "generic"
    IMAGE = "image"
    CODE = "code"
    SOURCE = "source"
    TABLE_SOURCE = "table_source"
    TABLE_IMAGE = "table_image"


@dataclass(frozen=True)
class RenderedAttachment:
    filename: str
    content: bytes
    description: str
    kind: AttachmentKind = AttachmentKind.GENERIC
    group_id: str | None = None


@dataclass(frozen=True)
class RenderedDiscordContent:
    messages: tuple[str, ...]
    attachments: tuple[RenderedAttachment, ...]
    incident_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class DurableRenderedAttachment:
    filename: str
    path: Path
    description: str
    sha256: str
    size_bytes: int
    kind: AttachmentKind = AttachmentKind.GENERIC
    group_id: str | None = None


@dataclass(frozen=True)
class DurableDiscordRenderPlan:
    messages: tuple[str, ...]
    attachments: tuple[DurableRenderedAttachment, ...]
    incident_codes: tuple[str, ...] = ()

    def to_payload(self, artifact_root: Path) -> dict[str, Any]:
        root = artifact_root.resolve()
        return {
            "version": 3,
            "messages": list(self.messages),
            "incident_codes": list(self.incident_codes),
            "attachments": [
                {
                    "filename": attachment.filename,
                    "relative_path": attachment.path.resolve().relative_to(root).as_posix(),
                    "description": attachment.description,
                    "sha256": attachment.sha256,
                    "size_bytes": attachment.size_bytes,
                    "kind": attachment.kind.value,
                    "group_id": attachment.group_id,
                }
                for attachment in self.attachments
            ],
        }


class DiscordRenderPlanner:
    def __init__(
        self,
        *,
        media_worker: MediaWorker,
        table_limits: TableLimits,
        artifact_root: Path | None = None,
        retention_days: int = 30,
    ) -> None:
        if retention_days < 1:
            raise ValueError("render plan retention must be positive")
        self._parser = MarkdownContentParser()
        self._media_worker = media_worker
        self._table_limits = table_limits
        self._artifact_root = artifact_root.resolve() if artifact_root else None
        self.retention_days = retention_days

    async def render_markdown(self, source: str) -> RenderedDiscordContent:
        source_bytes = source.encode("utf-8")
        if len(source_bytes) > self._table_limits.max_source_bytes:
            return RenderedDiscordContent(
                messages=(
                    "Final response exceeded rich-rendering limits; Markdown source attached.",
                ),
                attachments=_text_attachments(
                    source,
                    filename="response.md",
                    description="Oversized final response source",
                    max_bytes=min(
                        self._table_limits.max_source_bytes,
                        DISCORD_ATTACHMENT_LIMIT_BYTES,
                    ),
                    kind=AttachmentKind.SOURCE,
                ),
            )
        blocks = self._parser.parse(source)
        messages: list[str] = []
        attachments: list[RenderedAttachment] = []
        incident_codes: set[str] = set()
        for block in blocks:
            if isinstance(block, TextBlock):
                text = _suppress_mentions(block.text)
                try:
                    messages.extend(split_discord_text(text))
                except ValueError:
                    messages.append("Oversized Markdown block attached as source.")
                    attachments.extend(
                        _text_attachments(
                            block.text,
                            filename=f"text-{len(attachments) + 1}.md",
                            description="Oversized Markdown source",
                            max_bytes=DISCORD_ATTACHMENT_LIMIT_BYTES,
                            kind=AttachmentKind.SOURCE,
                        )
                    )
            elif isinstance(block, CodeBlock):
                language = block.language or ""
                code = block.code.removesuffix("\n")
                code_chunks = (
                    split_discord_code(code, language=language)
                    if len(code) + len(language) + 8 <= DISCORD_MESSAGE_LIMIT
                    else ()
                )
                if len(code_chunks) == 1:
                    messages.append(code_chunks[0])
                else:
                    messages.append("Oversized code block attached as `code.txt`.")
                    attachments.extend(
                        _text_attachments(
                            block.code,
                            filename="code.txt",
                            description="Oversized code block",
                            max_bytes=DISCORD_ATTACHMENT_LIMIT_BYTES,
                            kind=AttachmentKind.CODE,
                        )
                    )
            elif isinstance(block, TableBlock):
                try:
                    rendered = await self._media_worker.render_table(
                        block, self._table_limits
                    )
                except MediaWorkerError:
                    incident_codes.add("table_media_worker_failed")
                    base = f"table-{block.block_id}"
                    messages.append("Table rendering failed; Markdown source attached.")
                    table_fenced = f"```\n{block.source_markdown}\n```"
                    if len(table_fenced) <= DISCORD_MESSAGE_LIMIT:
                        messages.append(table_fenced)
                    attachments.extend(
                        _text_attachments(
                            block.source_markdown,
                            filename=f"{base}.md",
                            description="Markdown source for table rendering fallback",
                            max_bytes=DISCORD_ATTACHMENT_LIMIT_BYTES,
                            kind=AttachmentKind.TABLE_SOURCE,
                            group_id=base,
                        )
                    )
                    continue
                base = f"table-{block.block_id}"
                attachments.extend(
                    _byte_attachments(
                        rendered.markdown,
                        filename=f"{base}.md",
                        description=f"Markdown source for {rendered.summary}",
                        max_bytes=DISCORD_ATTACHMENT_LIMIT_BYTES,
                        kind=AttachmentKind.TABLE_SOURCE,
                        group_id=base,
                    )
                )
                if rendered.kind is TableRenderKind.PNG_WITH_SOURCE:
                    for index, page in enumerate(rendered.pages):
                        if len(page) > DISCORD_ATTACHMENT_LIMIT_BYTES:
                            messages.append(
                                "A table image exceeded Discord's attachment limit; "
                                "Markdown source was preserved."
                            )
                            continue
                        attachments.append(
                            RenderedAttachment(
                                f"{base}-{index + 1}.png",
                                page,
                                f"{rendered.summary} Page {index + 1}/{len(rendered.pages)}",
                                AttachmentKind.TABLE_IMAGE,
                                base,
                            )
                        )
                elif rendered.kind is TableRenderKind.CODE_BLOCK_WITH_SOURCE:
                    if rendered.reason is not None:
                        incident_codes.add(rendered.reason)
                    messages.append(
                        f"{rendered.summary} Image rendering was unavailable; "
                        "showing a text fallback."
                    )
                    try:
                        messages.extend(
                            split_discord_code(rendered.fallback_code or "")
                        )
                    except ValueError:
                        messages.append(
                            "Table code fallback exceeded Discord limits; source attached."
                        )
        return RenderedDiscordContent(
            tuple(filter(None, messages)),
            tuple(attachments),
            tuple(sorted(incident_codes)),
        )

    async def create_durable_plan(
        self,
        *,
        turn_id: str,
        source: str,
    ) -> DurableDiscordRenderPlan:
        if self._artifact_root is None:
            raise InvariantError("render artifact root is not configured")
        rendered = await self.render_markdown(source)
        return await asyncio.to_thread(
            self._persist_rendered_plan,
            turn_id,
            rendered,
        )

    async def create_plain_text_fallback_plan(
        self,
        *,
        turn_id: str,
        source: str,
    ) -> DurableDiscordRenderPlan:
        rendered = RenderedDiscordContent(
            messages=(
                "Rich rendering was unavailable; complete Markdown source attached.",
            ),
            attachments=_text_attachments(
                source,
                filename="response.md",
                description="Complete final response after rich-rendering failure",
                max_bytes=DISCORD_ATTACHMENT_LIMIT_BYTES,
                kind=AttachmentKind.SOURCE,
            ),
        )
        return await asyncio.to_thread(
            self._persist_rendered_plan,
            turn_id,
            rendered,
        )

    def _persist_rendered_plan(
        self,
        turn_id: str,
        rendered: RenderedDiscordContent,
    ) -> DurableDiscordRenderPlan:
        if self._artifact_root is None:
            raise InvariantError("render artifact root is not configured")
        target = self._artifact_root / _safe_path_segment(turn_id)
        target.mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.name != "nt":
            target.chmod(0o700)
        attachments: list[DurableRenderedAttachment] = []
        for index, attachment in enumerate(rendered.attachments):
            digest = hashlib.sha256(attachment.content).hexdigest()
            stored_name = (
                f"{index:04d}-{digest[:16]}-{_safe_filename(attachment.filename)}"
            )
            path = target / stored_name
            _write_immutable(path, attachment.content, digest)
            attachments.append(
                DurableRenderedAttachment(
                    filename=attachment.filename,
                    path=path,
                    description=attachment.description,
                    sha256=digest,
                    size_bytes=len(attachment.content),
                    kind=attachment.kind,
                    group_id=attachment.group_id,
                )
            )
        return DurableDiscordRenderPlan(
            rendered.messages,
            tuple(attachments),
            rendered.incident_codes,
        )

    def load_durable_plan(
        self,
        plan_json: str,
    ) -> DurableDiscordRenderPlan:
        if self._artifact_root is None:
            raise InvariantError("render artifact root is not configured")
        try:
            payload = json.loads(plan_json)
        except json.JSONDecodeError as exc:
            raise InvariantError("render plan JSON is invalid") from exc
        if not isinstance(payload, dict) or payload.get("version") not in {1, 2, 3}:
            raise InvariantError("render plan version is unsupported")
        version = int(payload["version"])
        raw_messages = payload.get("messages")
        raw_attachments = payload.get("attachments")
        if not isinstance(raw_messages, list) or not all(
            isinstance(message, str)
            and len(message) <= DISCORD_MESSAGE_LIMIT
            for message in raw_messages
        ):
            raise InvariantError("render plan messages are invalid")
        if not isinstance(raw_attachments, list):
            raise InvariantError("render plan attachments are invalid")
        raw_incident_codes = payload.get("incident_codes", []) if version >= 3 else []
        if not isinstance(raw_incident_codes, list) or not all(
            isinstance(code, str)
            and re.fullmatch(r"[a-z][a-z0-9_]{0,127}", code)
            for code in raw_incident_codes
        ):
            raise InvariantError("render plan incident codes are invalid")
        root = self._artifact_root.resolve()
        attachments: list[DurableRenderedAttachment] = []
        for raw in raw_attachments:
            if not isinstance(raw, dict):
                raise InvariantError("render plan attachment is invalid")
            filename = raw.get("filename")
            relative_path = raw.get("relative_path")
            description = raw.get("description")
            digest = raw.get("sha256")
            size_bytes = raw.get("size_bytes")
            raw_kind = (
                raw.get("kind")
                if version >= 2
                else _infer_attachment_kind(str(filename or ""))
            )
            group_id = (
                raw.get("group_id")
                if version >= 2
                else _infer_table_group(str(filename or ""))
            )
            if (
                not isinstance(filename, str)
                or not filename
                or not isinstance(relative_path, str)
                or not relative_path
                or not isinstance(description, str)
                or not isinstance(digest, str)
                or len(digest) != 64
                or isinstance(size_bytes, bool)
                or not isinstance(size_bytes, int)
                or size_bytes < 0
                or not isinstance(raw_kind, str)
                or (
                    group_id is not None
                    and (
                        not isinstance(group_id, str)
                        or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", group_id)
                    )
                )
            ):
                raise InvariantError("render plan attachment metadata is invalid")
            try:
                kind = AttachmentKind(raw_kind)
            except ValueError as exc:
                raise InvariantError("render plan attachment kind is invalid") from exc
            path = (root / relative_path).resolve()
            if (
                not path.is_relative_to(root)
                or path.is_symlink()
                or not path.is_file()
                or path.stat().st_size != size_bytes
                or _sha256_file(path) != digest
            ):
                raise InvariantError("render plan attachment changed or is missing")
            attachments.append(
                DurableRenderedAttachment(
                    filename=filename,
                    path=path,
                    description=description,
                    sha256=digest,
                    size_bytes=size_bytes,
                    kind=kind,
                    group_id=group_id,
                )
            )
        return DurableDiscordRenderPlan(
            tuple(raw_messages),
            tuple(attachments),
            tuple(dict.fromkeys(raw_incident_codes)),
        )

    @property
    def artifact_root(self) -> Path:
        if self._artifact_root is None:
            raise InvariantError("render artifact root is not configured")
        return self._artifact_root


@dataclass(frozen=True)
class VisualizationMarkerResult:
    text: str
    found: bool
    missing_reason: str | None


def suppress_visualization_markers(
    source: str,
    *,
    has_registered_images: bool,
    has_registered_image_records: bool,
    dynamic_tools_enabled: bool | None,
) -> VisualizationMarkerResult:
    """Remove unsupported private visualize controls without trusting their payload."""

    start_marker = "\ue200"
    end_marker = "\ue201"
    cursor = 0
    output: list[str] = []
    found = False
    placeholder_added = False
    if has_registered_images:
        potential_missing_reason = None
        replacement = ""
    elif has_registered_image_records:
        potential_missing_reason = "artifact_unavailable"
        replacement = (
            "[The registered image artifact was unavailable for Discord delivery.]"
        )
    elif dynamic_tools_enabled is False:
        potential_missing_reason = "legacy_session"
        replacement = (
            "[This session was created before Discord image delivery was enabled. "
            "Run `/session new`, then request the image again.]"
        )
    elif dynamic_tools_enabled is True:
        potential_missing_reason = "publish_tool_not_used"
        replacement = (
            "[This visualization could not be delivered because Codex did not "
            "register an image for Discord delivery.]"
        )
    else:
        potential_missing_reason = "unknown"
        replacement = (
            "[This visualization could not be delivered because no image was "
            "registered for Discord delivery.]"
        )
    while cursor < len(source):
        start = source.find(start_marker, cursor)
        if start < 0:
            output.append(source[cursor:])
            break
        output.append(source[cursor:start])
        end = source.find(end_marker, start + 1)
        prefix = source[start + 1 : min(len(source), start + 32)].casefold()
        if not prefix.startswith("visualize"):
            cursor = start + 1
            continue
        found = True
        if not has_registered_images and not placeholder_added:
            output.append(replacement)
            placeholder_added = True
        cursor = len(source) if end < 0 else end + 1
    sanitized = (
        "".join(output)
        .replace(start_marker, "")
        .replace("\ue202", "")
        .replace(end_marker, "")
    )
    missing_reason = potential_missing_reason if found else None
    return VisualizationMarkerResult(sanitized, found, missing_reason)


def split_discord_text(value: str, limit: int = DISCORD_MESSAGE_LIMIT) -> tuple[str, ...]:
    if limit < 1:
        raise ValueError("Discord message limit must be positive")
    if not value or not value.strip():
        return ()
    if len(value) <= limit:
        return (value,)
    protected = _markdown_link_spans(value)
    if any(end - start > limit for start, end in protected):
        raise ValueError("Markdown link exceeds the Discord message limit")
    grapheme_boundaries = _grapheme_boundaries(value)
    parts: list[str] = []
    start = 0
    while len(value) - start > limit:
        boundary = _split_boundary(
            value,
            start=start,
            hard_limit=start + limit,
            protected=protected,
            grapheme_boundaries=grapheme_boundaries,
        )
        parts.append(value[start:boundary])
        start = boundary
    if start < len(value):
        parts.append(value[start:])
    return tuple(parts)


def split_discord_code(
    value: str,
    *,
    language: str = "",
    limit: int = DISCORD_MESSAGE_LIMIT,
) -> tuple[str, ...]:
    longest_fence = max(
        (len(match.group(0)) for match in re.finditer(r"`+", value)),
        default=0,
    )
    fence = "`" * max(3, longest_fence + 1)
    prefix = f"{fence}{language}\n"
    suffix = f"\n{fence}"
    content_limit = limit - len(prefix) - len(suffix)
    if content_limit < 1:
        raise ValueError("Discord message limit is too small for a code fence")
    chunks = _split_grapheme_chunks(value, content_limit) or ("",)
    return tuple(f"{prefix}{chunk}{suffix}" for chunk in chunks)


def _split_boundary(
    value: str,
    *,
    start: int,
    hard_limit: int,
    protected: tuple[tuple[int, int], ...],
    grapheme_boundaries: frozenset[int],
) -> int:
    preferred: list[int] = []
    for marker in ("\n\n", "\n", ". ", "。", " "):
        position = value.rfind(marker, start, hard_limit + 1)
        if position >= start:
            candidate = position + len(marker)
            if (
                candidate in grapheme_boundaries
                and not _inside_protected(candidate, protected)
            ):
                preferred.append(candidate)
    if preferred and max(preferred) >= start + (hard_limit - start) // 3:
        return max(preferred)

    containing = next(
        (
            span
            for span in protected
            if span[0] < hard_limit < span[1]
        ),
        None,
    )
    if containing is not None:
        if containing[0] > start:
            boundary = max(
                point
                for point in grapheme_boundaries
                if start < point <= containing[0]
            )
            return boundary
        if containing[1] - start <= hard_limit - start:
            return containing[1]

    candidates = [
        point
        for point in grapheme_boundaries
        if start < point <= hard_limit and not _inside_protected(point, protected)
    ]
    if candidates:
        return max(candidates)
    later = [point for point in grapheme_boundaries if point > start]
    if not later:
        return len(value)
    boundary = min(later)
    if boundary > hard_limit:
        raise ValueError("grapheme cluster exceeds the Discord message limit")
    return boundary


def _inside_protected(
    boundary: int,
    protected: tuple[tuple[int, int], ...],
) -> bool:
    return any(start < boundary < end for start, end in protected)


def _markdown_link_spans(value: str) -> tuple[tuple[int, int], ...]:
    spans: list[tuple[int, int]] = []
    index = 0
    while index < len(value):
        if value[index] != "[" or _escaped(value, index):
            index += 1
            continue
        label_end = _matching_delimiter(value, index, "[", "]")
        if label_end is None or label_end + 1 >= len(value) or value[label_end + 1] != "(":
            index += 1
            continue
        destination_end = _matching_delimiter(value, label_end + 1, "(", ")")
        if destination_end is None:
            index += 1
            continue
        start = index - 1 if index and value[index - 1] == "!" else index
        spans.append((start, destination_end + 1))
        index = destination_end + 1
    return tuple(spans)


def _matching_delimiter(
    value: str,
    start: int,
    opening: str,
    closing: str,
) -> int | None:
    depth = 0
    for index in range(start, len(value)):
        if _escaped(value, index):
            continue
        if value[index] == opening:
            depth += 1
        elif value[index] == closing:
            depth -= 1
            if depth == 0:
                return index
    return None


def _escaped(value: str, index: int) -> bool:
    backslashes = 0
    index -= 1
    while index >= 0 and value[index] == "\\":
        backslashes += 1
        index -= 1
    return backslashes % 2 == 1


def _grapheme_boundaries(value: str) -> frozenset[int]:
    boundaries = {0}
    index = 0
    while index < len(value):
        first = value[index]
        index += 1
        if _regional_indicator(first) and index < len(value) and _regional_indicator(value[index]):
            index += 1
        while index < len(value):
            character = value[index]
            previous = value[index - 1]
            if (
                unicodedata.combining(character)
                or _variation_selector(character)
                or _emoji_modifier(character)
                or character == "\u200d"
                or previous == "\u200d"
            ):
                index += 1
                continue
            break
        boundaries.add(index)
    return frozenset(boundaries)


def _split_grapheme_chunks(value: str, limit: int) -> tuple[str, ...]:
    if not value:
        return ()
    boundaries = sorted(_grapheme_boundaries(value))
    chunks: list[str] = []
    start = 0
    while start < len(value):
        candidates = [point for point in boundaries if start < point <= start + limit]
        if not candidates:
            raise ValueError("grapheme cluster exceeds the Discord message limit")
        end = max(candidates)
        chunks.append(value[start:end])
        start = end
    return tuple(chunks)


def _regional_indicator(value: str) -> bool:
    return "\U0001f1e6" <= value <= "\U0001f1ff"


def _variation_selector(value: str) -> bool:
    return "\ufe00" <= value <= "\ufe0f" or "\U000e0100" <= value <= "\U000e01ef"


def _emoji_modifier(value: str) -> bool:
    return "\U0001f3fb" <= value <= "\U0001f3ff"


def _suppress_mentions(value: str) -> str:
    return (
        value.replace("@everyone", "@\u200beveryone")
        .replace("@here", "@\u200bhere")
        .replace("<@", "<@\u200b")
        .replace("<@&", "<@\u200b&")
    )


def _safe_path_segment(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{4,128}", value):
        raise InvariantError("Turn ID is invalid for render artifact storage")
    return value


def _safe_filename(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(value).name).strip(".-")
    return (name or "attachment.bin")[:120]


def _text_attachments(
    value: str,
    *,
    filename: str,
    description: str,
    max_bytes: int,
    kind: AttachmentKind = AttachmentKind.GENERIC,
    group_id: str | None = None,
) -> tuple[RenderedAttachment, ...]:
    if max_bytes < 1:
        raise ValueError("attachment byte limit must be positive")
    content = value.encode("utf-8")
    chunks = (
        (content,)
        if len(content) <= max_bytes
        else _split_utf8_chunks(value, max_bytes)
    )
    return _attachments_from_chunks(
        chunks,
        filename=filename,
        description=description,
        kind=kind,
        group_id=group_id,
    )


def _byte_attachments(
    content: bytes,
    *,
    filename: str,
    description: str,
    max_bytes: int,
    kind: AttachmentKind = AttachmentKind.GENERIC,
    group_id: str | None = None,
) -> tuple[RenderedAttachment, ...]:
    if max_bytes < 1:
        raise ValueError("attachment byte limit must be positive")
    if len(content) <= max_bytes:
        return (RenderedAttachment(filename, content, description, kind, group_id),)
    return _attachments_from_chunks(
        tuple(
            content[index : index + max_bytes]
            for index in range(0, len(content), max_bytes)
        ),
        filename=filename,
        description=description,
        kind=kind,
        group_id=group_id,
    )


def _split_utf8_chunks(value: str, max_bytes: int) -> tuple[bytes, ...]:
    if max_bytes < 1:
        raise ValueError("attachment byte limit must be positive")
    chunks: list[bytes] = []
    current: list[str] = []
    current_bytes = 0
    for character in value:
        encoded = character.encode("utf-8")
        if len(encoded) > max_bytes:
            raise ValueError("attachment byte limit cannot contain one UTF-8 character")
        if current and current_bytes + len(encoded) > max_bytes:
            chunks.append("".join(current).encode("utf-8"))
            current = []
            current_bytes = 0
        current.append(character)
        current_bytes += len(encoded)
    if current:
        chunks.append("".join(current).encode("utf-8"))
    return tuple(chunks)


def _attachments_from_chunks(
    chunks: tuple[bytes, ...],
    *,
    filename: str,
    description: str,
    kind: AttachmentKind,
    group_id: str | None,
) -> tuple[RenderedAttachment, ...]:
    if len(chunks) == 1:
        return (
            RenderedAttachment(filename, chunks[0], description, kind, group_id),
        )
    count = len(chunks)
    path = Path(filename)
    stem = path.stem or "attachment"
    suffix = path.suffix or ".bin"
    return tuple(
        RenderedAttachment(
            filename=f"{stem}.part{index + 1:03d}-of-{count:03d}{suffix}",
            content=chunk,
            description=f"{description} (part {index + 1}/{count})",
            kind=kind,
            group_id=group_id,
        )
        for index, chunk in enumerate(chunks)
    )


def _infer_attachment_kind(filename: str) -> str:
    if re.fullmatch(r"table-[A-Za-z0-9_-]+-\d+\.png", filename):
        return AttachmentKind.TABLE_IMAGE.value
    if re.fullmatch(r"table-[A-Za-z0-9_-]+\.md", filename):
        return AttachmentKind.TABLE_SOURCE.value
    if filename == "code.txt":
        return AttachmentKind.CODE.value
    if filename.endswith((".md", ".txt")):
        return AttachmentKind.SOURCE.value
    return AttachmentKind.GENERIC.value


def _infer_table_group(filename: str) -> str | None:
    match = re.fullmatch(r"(table-[A-Za-z0-9_-]+)(?:-\d+\.png|\.md)", filename)
    return match.group(1) if match else None


def _write_immutable(path: Path, content: bytes, expected_sha256: str) -> None:
    if path.exists():
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size != len(content)
            or _sha256_file(path) != expected_sha256
        ):
            raise InvariantError("existing render artifact does not match its plan")
        return
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name != "nt":
            temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
