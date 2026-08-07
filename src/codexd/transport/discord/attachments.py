from __future__ import annotations

import asyncio
import hashlib
import os
import re
import stat
import unicodedata
import uuid
from collections.abc import Iterable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import aiohttp
import discord

from codexd.domain.ids import utc_now_ms
from codexd.domain.turns import TurnFile, TurnImage
from codexd.errors import CodexDError
from codexd.rendering.media_worker import (
    AttachmentMediaResult,
    MediaWorker,
)

_CDN_HOSTS = frozenset(
    {
        "cdn.discordapp.com",
        "media.discordapp.net",
        "images-ext-1.discordapp.net",
        "images-ext-2.discordapp.net",
    }
)
_IMAGE_EXTENSIONS = frozenset(
    {
        ".apng",
        ".avif",
        ".bmp",
        ".gif",
        ".heic",
        ".heif",
        ".ico",
        ".jfif",
        ".jpe",
        ".jpeg",
        ".jpg",
        ".jxl",
        ".png",
        ".svg",
        ".tif",
        ".tiff",
        ".webp",
    }
)
_DISCORD_MENTION = re.compile(
    r"<(?:@!?|@&|#)\d+>|@(?:everyone|here)\b",
    re.IGNORECASE,
)
_SHORT_EXTENSION = re.compile(r"\.[A-Za-z0-9]{1,16}")
_MAX_DISPLAY_NAME_CHARS = 128
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_REDIRECTS = 4
_DOWNLOAD_TOTAL_SECONDS = 30


class AttachmentError(CodexDError):
    code = "attachment_error"

    def __init__(self, message: str, *, code: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class DiscordAttachmentIngestResult:
    images: tuple[TurnImage, ...] = ()
    files: tuple[TurnFile, ...] = ()


@dataclass(frozen=True)
class _DownloadedAttachment:
    size_bytes: int
    sha256: str


@dataclass
class _DownloadBudget:
    total_bytes: int = 0


class DiscordAttachmentIngestor:
    """Download, classify, and atomically commit Discord message attachments."""

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession,
        media_worker: MediaWorker,
        attachments_dir: Path,
        image_max_bytes: int,
        image_max_pixels: int,
        file_max_bytes: int,
        message_max_bytes: int,
        retention_days: int,
        max_attachment_count: int = 10,
    ) -> None:
        limits = {
            "image_max_bytes": image_max_bytes,
            "image_max_pixels": image_max_pixels,
            "file_max_bytes": file_max_bytes,
            "message_max_bytes": message_max_bytes,
            "retention_days": retention_days,
            "max_attachment_count": max_attachment_count,
        }
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in limits.values()
        ):
            raise ValueError("attachment ingestor limits must be positive integers")
        if file_max_bytes > message_max_bytes:
            raise ValueError("file byte limit may not exceed message byte limit")
        self._session = session
        self._worker = media_worker
        self._attachments_dir = attachments_dir
        self._image_max_bytes = image_max_bytes
        self._image_max_pixels = image_max_pixels
        self._file_max_bytes = file_max_bytes
        self._message_max_bytes = message_max_bytes
        self._max_attachment_count = max_attachment_count
        self._retention_days = retention_days
        self._download_max_bytes = max(image_max_bytes, file_max_bytes)
        self._active_budget: ContextVar[_DownloadBudget | None] = ContextVar(
            f"discord_attachment_budget_{id(self)}",
            default=None,
        )

    async def ingest(
        self,
        attachments: Sequence[discord.Attachment],
    ) -> DiscordAttachmentIngestResult:
        if len(attachments) > self._max_attachment_count:
            raise AttachmentError(
                f"at most {self._max_attachment_count} attachments are accepted",
                code="too_many_attachments",
            )
        token = self._active_budget.set(_DownloadBudget())
        try:
            return await self._ingest_all(attachments)
        finally:
            self._active_budget.reset(token)

    async def _ingest_all(
        self,
        attachments: Sequence[discord.Attachment],
    ) -> DiscordAttachmentIngestResult:
        images: list[TurnImage] = []
        files: list[TurnFile] = []
        try:
            for ordinal, attachment in enumerate(attachments):
                ingested = await self._ingest_one(attachment, ordinal)
                if isinstance(ingested, TurnImage):
                    images.append(ingested)
                elif isinstance(ingested, TurnFile):
                    files.append(ingested)
                else:
                    raise TypeError("attachment ingestor returned an invalid result")
        except BaseException:
            self.cleanup((*images, *files))
            raise
        return DiscordAttachmentIngestResult(images=tuple(images), files=tuple(files))

    @staticmethod
    def cleanup(
        ingested: DiscordAttachmentIngestResult | Iterable[TurnImage | TurnFile],
    ) -> None:
        if isinstance(ingested, DiscordAttachmentIngestResult):
            items: Iterable[TurnImage | TurnFile] = (*ingested.images, *ingested.files)
        else:
            items = ingested
        for item in items:
            item.canonical_path.unlink(missing_ok=True)

    async def _ingest_one(
        self,
        attachment: discord.Attachment,
        ordinal: int,
    ) -> TurnImage | TurnFile:
        reported_size = _reported_size(attachment)
        if reported_size > self._download_max_bytes:
            raise AttachmentError(
                "attachment exceeds the configured byte limit",
                code="attachment_size_limit",
            )

        attachment_id = str(uuid.uuid4())
        display_name = _sanitize_display_name(attachment.filename)
        quarantine = self._attachments_dir / ".quarantine"
        inputs = self._attachments_dir / "input"
        _ensure_private_directory(self._attachments_dir)
        _ensure_private_directory(quarantine)
        _ensure_private_directory(inputs)
        source = quarantine / f"{attachment_id}.download"
        staging = quarantine / f"{attachment_id}.normalized"
        image_final = inputs / f"{attachment_id}.png"
        file_final = inputs / f"{attachment_id}{_validated_extension(display_name)}"
        committed: Path | None = None
        try:
            downloaded_result = await self._download(attachment.url, source)
            if isinstance(downloaded_result, _DownloadedAttachment):
                downloaded = downloaded_result
            else:
                # Compatibility for existing test doubles that only write the destination.
                downloaded = await asyncio.to_thread(_measure_file, source)
            self._record_download(downloaded.size_bytes)
            if downloaded.size_bytes != reported_size:
                raise AttachmentError(
                    "attachment download did not match Discord metadata",
                    code="attachment_integrity_failed",
                )

            try:
                classification = await self._classify_attachment(
                    source=source,
                    output=staging,
                )
            except Exception as exc:
                raise AttachmentError(
                    "image decode or attachment classification failed",
                    code="image_decode_failed",
                ) from exc
            normalized = classification.normalized_image
            if classification.kind == "image":
                if normalized is None:
                    raise AttachmentError(
                        "image decode or normalization failed",
                        code="image_decode_failed",
                    )
                _commit_file(staging, image_final)
                committed = image_final
                normalized = replace(normalized, output_path=image_final)
                return TurnImage(
                    attachment_id=attachment_id,
                    ordinal=ordinal,
                    canonical_path=image_final.resolve(strict=True),
                    media_type=normalized.media_type,
                    source_sha256=normalized.source_sha256,
                    sha256=normalized.normalized_sha256,
                    size_bytes=normalized.size_bytes,
                    width=normalized.width,
                    height=normalized.height,
                    source_name_sanitized=display_name,
                    retention_until=_retention_deadline(self._retention_days),
                )

            if _claims_image(attachment.filename, attachment.content_type):
                raise AttachmentError(
                    "image decode or normalization failed",
                    code="image_decode_failed",
                )
            if downloaded.size_bytes > self._file_max_bytes:
                raise AttachmentError(
                    "file attachment exceeds the configured byte limit",
                    code="attachment_size_limit",
                )
            _commit_file(source, file_final)
            committed = file_final
            return TurnFile(
                attachment_id=attachment_id,
                ordinal=ordinal,
                canonical_path=file_final.resolve(strict=True),
                display_name=display_name,
                reported_media_type=_reported_media_type(attachment.content_type),
                sha256=downloaded.sha256,
                size_bytes=downloaded.size_bytes,
                retention_until=_retention_deadline(self._retention_days),
            )
        except OSError as exc:
            if committed is not None:
                committed.unlink(missing_ok=True)
            raise AttachmentError(
                "attachment could not be stored safely",
                code="attachment_integrity_failed",
            ) from exc
        except BaseException:
            if committed is not None:
                committed.unlink(missing_ok=True)
            raise
        finally:
            source.unlink(missing_ok=True)
            staging.unlink(missing_ok=True)

    async def _classify_attachment(
        self,
        *,
        source: Path,
        output: Path,
    ) -> AttachmentMediaResult:
        return await self._worker.classify_attachment(
            source=source,
            output=output,
            max_image_bytes=self._image_max_bytes,
            max_pixels=self._image_max_pixels,
        )

    async def _download(
        self,
        initial_url: str,
        destination: Path,
    ) -> _DownloadedAttachment:
        timeout = aiohttp.ClientTimeout(
            total=_DOWNLOAD_TOTAL_SECONDS,
            connect=10,
            sock_read=20,
        )
        try:
            async with asyncio.timeout(_DOWNLOAD_TOTAL_SECONDS):
                return await self._download_from_cdn(
                    initial_url=initial_url,
                    destination=destination,
                    request_timeout=timeout,
                )
        except AttachmentError:
            raise
        except TimeoutError as exc:
            raise AttachmentError(
                f"attachment download exceeded {_DOWNLOAD_TOTAL_SECONDS} seconds",
                code="attachment_download_timeout",
            ) from exc
        except aiohttp.ClientPayloadError as exc:
            raise AttachmentError(
                "attachment download was incomplete",
                code="attachment_integrity_failed",
            ) from exc
        except aiohttp.ClientError as exc:
            raise AttachmentError(
                "attachment download failed",
                code="attachment_download_failed",
            ) from exc
        except (OSError, ValueError, TypeError) as exc:
            raise AttachmentError(
                "attachment download failed",
                code="attachment_download_failed",
            ) from exc

    async def _download_from_cdn(
        self,
        *,
        initial_url: str,
        destination: Path,
        request_timeout: aiohttp.ClientTimeout,
    ) -> _DownloadedAttachment:
        url = initial_url
        for redirect_count in range(_MAX_REDIRECTS + 1):
            _validate_cdn_url(url)
            async with self._session.get(
                url,
                allow_redirects=False,
                auto_decompress=False,
                timeout=request_timeout,
                headers={"Accept": "application/octet-stream,*/*;q=0.1"},
            ) as response:
                if response.status in _REDIRECT_STATUSES:
                    if redirect_count == _MAX_REDIRECTS:
                        raise AttachmentError(
                            "attachment download returned too many redirects",
                            code="attachment_download_failed",
                        )
                    location = response.headers.get("Location")
                    if not location:
                        raise AttachmentError(
                            "attachment download returned an invalid redirect",
                            code="attachment_download_failed",
                        )
                    url = urljoin(url, location)
                    _validate_cdn_url(url)
                    continue
                if response.status == 206:
                    raise AttachmentError(
                        "attachment download was partial",
                        code="attachment_integrity_failed",
                    )
                if response.status != 200:
                    raise AttachmentError(
                        f"attachment download returned HTTP {response.status}",
                        code="attachment_download_failed",
                    )
                if response.headers.get("Content-Range") is not None:
                    raise AttachmentError(
                        "attachment download was partial",
                        code="attachment_integrity_failed",
                    )
                expected_length = _content_length(response.headers.get("Content-Length"))
                size = 0
                digest = hashlib.sha256()
                descriptor = os.open(
                    destination,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                with os.fdopen(descriptor, "wb") as output:
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        if not chunk:
                            continue
                        size += len(chunk)
                        self._check_stream_limits(size)
                        output.write(chunk)
                        digest.update(chunk)
                if size == 0 or (
                    expected_length is not None and expected_length != size
                ):
                    raise AttachmentError(
                        "attachment download was empty or incomplete",
                        code="attachment_integrity_failed",
                    )
                if os.name != "nt":
                    await asyncio.to_thread(destination.chmod, 0o600)
                return _DownloadedAttachment(
                    size_bytes=size,
                    sha256=digest.hexdigest(),
                )
        raise AssertionError("redirect loop ended unexpectedly")

    def _check_stream_limits(self, current_size: int) -> None:
        if current_size > self._download_max_bytes:
            raise AttachmentError(
                "attachment exceeds the configured byte limit",
                code="attachment_size_limit",
            )
        budget = self._active_budget.get()
        committed_size = budget.total_bytes if budget is not None else 0
        if committed_size + current_size > self._message_max_bytes:
            raise AttachmentError(
                "message attachments exceed the configured total byte limit",
                code="attachment_total_size_limit",
            )

    def _record_download(self, size_bytes: int) -> None:
        self._check_stream_limits(size_bytes)
        budget = self._active_budget.get()
        if budget is not None:
            budget.total_bytes += size_bytes


class DiscordImageIngestor(DiscordAttachmentIngestor):
    """Compatibility adapter for the existing image-only bot wiring."""

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession,
        media_worker: MediaWorker,
        attachments_dir: Path,
        max_bytes: int,
        max_pixels: int,
        retention_days: int,
        max_images: int = 10,
    ) -> None:
        super().__init__(
            session=session,
            media_worker=media_worker,
            attachments_dir=attachments_dir,
            image_max_bytes=max_bytes,
            image_max_pixels=max_pixels,
            file_max_bytes=max_bytes,
            message_max_bytes=max_bytes * max_images,
            retention_days=retention_days,
            max_attachment_count=max_images,
        )

    async def ingest(  # type: ignore[override]
        self,
        attachments: Sequence[discord.Attachment],
    ) -> tuple[TurnImage, ...]:
        result = await super().ingest(attachments)
        if result.files:
            self.cleanup(result)
            raise AttachmentError(
                "image decode or normalization failed",
                code="image_decode_failed",
            )
        return result.images

    async def _classify_attachment(
        self,
        *,
        source: Path,
        output: Path,
    ) -> AttachmentMediaResult:
        normalized = await self._worker.normalize_image(
            source=source,
            output=output,
            max_bytes=self._image_max_bytes,
            max_pixels=self._image_max_pixels,
        )
        return AttachmentMediaResult(kind="image", normalized_image=normalized)


def _validate_cdn_url(value: str) -> None:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise AttachmentError(
            "attachment URL is not an approved Discord CDN URL",
            code="attachment_download_failed",
        ) from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname not in _CDN_HOSTS
        or parsed.username
        or parsed.password
        or port not in {None, 443}
    ):
        raise AttachmentError(
            "attachment URL is not an approved Discord CDN URL",
            code="attachment_download_failed",
        )


def _reported_size(attachment: discord.Attachment) -> int:
    size = attachment.size
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise AttachmentError(
            "attachment metadata has an invalid size",
            code="attachment_integrity_failed",
        )
    return size


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        length = int(value)
    except (TypeError, ValueError) as exc:
        raise AttachmentError(
            "attachment download returned invalid length metadata",
            code="attachment_integrity_failed",
        ) from exc
    if length < 0:
        raise AttachmentError(
            "attachment download returned invalid length metadata",
            code="attachment_integrity_failed",
        )
    return length


def _reported_media_type(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    media_type = value.strip()
    if (
        not media_type
        or len(media_type) > 255
        or any(_is_control(character) for character in media_type)
    ):
        return None
    return media_type


def _claims_image(filename: str, reported_media_type: str | None) -> bool:
    if isinstance(reported_media_type, str):
        media_type = reported_media_type.partition(";")[0].strip().casefold()
        if media_type.startswith("image/") or media_type == "application/svg+xml":
            return True
    safe_name = _sanitize_display_name(filename).casefold()
    return any(safe_name.endswith(extension) for extension in _IMAGE_EXTENSIONS)


def _sanitize_display_name(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value if isinstance(value, str) else "")
    normalized = normalized.replace("/", "_").replace("\\", "_")
    normalized = "".join(
        "_" if _is_control(character) else character for character in normalized
    )
    normalized = _DISCORD_MENTION.sub(
        lambda match: match.group(0).replace("@", "_"),
        normalized,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip(" .")
    if not normalized:
        return "attachment"
    if len(normalized) > _MAX_DISPLAY_NAME_CHARS:
        extension = _validated_extension(normalized)
        stem_limit = _MAX_DISPLAY_NAME_CHARS - len(extension)
        normalized = normalized[:stem_limit].rstrip(" .") + extension
    if normalized in {".", ".."}:
        return "attachment"
    return normalized or "attachment"


def _validated_extension(display_name: str) -> str:
    extension = Path(display_name).suffix
    if _SHORT_EXTENSION.fullmatch(extension) is None:
        return ""
    return extension.casefold()


def _ensure_private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise OSError("attachment directory is not a regular directory")
        if os.name != "nt":
            path.chmod(0o700)
    except OSError as exc:
        raise AttachmentError(
            "attachment storage directory is unsafe",
            code="attachment_integrity_failed",
        ) from exc


def _commit_file(source: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise OSError("attachment destination already exists")
    os.replace(source, destination)
    try:
        if os.name != "nt":
            destination.chmod(0o600)
    except OSError:
        destination.unlink(missing_ok=True)
        raise


def _measure_file(path: Path) -> _DownloadedAttachment:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return _DownloadedAttachment(size_bytes=size, sha256=digest.hexdigest())


def _retention_deadline(retention_days: int) -> int:
    return utc_now_ms() + retention_days * 24 * 60 * 60 * 1000


def _is_control(value: str) -> bool:
    return unicodedata.category(value).startswith("C")
