from __future__ import annotations

import asyncio
import os
import re
import uuid
from dataclasses import replace
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import aiohttp
import discord

from codexd.domain.ids import utc_now_ms
from codexd.domain.turns import TurnImage
from codexd.errors import CodexDError, SecurityError
from codexd.rendering.media_worker import MediaWorker, MediaWorkerError

_CDN_HOSTS = frozenset(
    {
        "cdn.discordapp.com",
        "media.discordapp.net",
        "images-ext-1.discordapp.net",
        "images-ext-2.discordapp.net",
    }
)
_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")


class AttachmentError(CodexDError):
    code = "attachment_error"

    def __init__(self, message: str, *, code: str) -> None:
        self.code = code
        super().__init__(message)


class DiscordImageIngestor:
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
        self._session = session
        self._worker = media_worker
        self._attachments_dir = attachments_dir
        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._retention_days = retention_days
        self._max_images = max_images

    async def ingest(
        self, attachments: list[discord.Attachment]
    ) -> tuple[TurnImage, ...]:
        if len(attachments) > self._max_images:
            raise AttachmentError(
                f"at most {self._max_images} images are accepted",
                code="too_many_images",
            )
        images: list[TurnImage] = []
        try:
            for ordinal, attachment in enumerate(attachments):
                images.append(await self._ingest_one(attachment, ordinal))
        except BaseException:
            self.cleanup(images)
            raise
        return tuple(images)

    @staticmethod
    def cleanup(images: list[TurnImage] | tuple[TurnImage, ...]) -> None:
        for image in images:
            image.canonical_path.unlink(missing_ok=True)

    async def _ingest_one(
        self, attachment: discord.Attachment, ordinal: int
    ) -> TurnImage:
        if attachment.size <= 0 or attachment.size > self._max_bytes:
            raise AttachmentError(
                "image attachment exceeds the configured byte limit",
                code="image_size_limit",
            )
        source_name = _sanitize_filename(attachment.filename)
        attachment_id = str(uuid.uuid4())
        quarantine = self._attachments_dir / ".quarantine"
        inputs = self._attachments_dir / "input"
        quarantine.mkdir(mode=0o700, parents=True, exist_ok=True)
        inputs.mkdir(mode=0o700, parents=True, exist_ok=True)
        source = quarantine / f"{attachment_id}.download"
        staging = quarantine / f"{attachment_id}.normalized"
        final = inputs / f"{attachment_id}.png"
        try:
            await self._download(attachment.url, source)
            try:
                normalized = await self._worker.normalize_image(
                    source=source,
                    output=staging,
                    max_bytes=self._max_bytes,
                    max_pixels=self._max_pixels,
                )
            except MediaWorkerError as exc:
                raise AttachmentError(
                    "image decode or normalization failed",
                    code="image_decode_failed",
                ) from exc
            os.replace(staging, final)
            if os.name != "nt":
                final.chmod(0o600)
            normalized = replace(normalized, output_path=final)
            return TurnImage(
                attachment_id=attachment_id,
                ordinal=ordinal,
                canonical_path=final.resolve(strict=True),
                media_type=normalized.media_type,
                source_sha256=normalized.source_sha256,
                sha256=normalized.normalized_sha256,
                size_bytes=normalized.size_bytes,
                width=normalized.width,
                height=normalized.height,
                source_name_sanitized=source_name,
                retention_until=utc_now_ms()
                + self._retention_days * 24 * 60 * 60 * 1000,
            )
        finally:
            source.unlink(missing_ok=True)
            staging.unlink(missing_ok=True)

    async def _download(self, initial_url: str, destination: Path) -> None:
        url = initial_url
        timeout = aiohttp.ClientTimeout(connect=10, sock_read=20)
        try:
            async with asyncio.timeout(30):
                for _redirect in range(4):
                    _validate_cdn_url(url)
                    async with self._session.get(
                        url,
                        allow_redirects=False,
                        timeout=timeout,
                        headers={"Accept": "image/*,*/*;q=0.1"},
                    ) as response:
                        if response.status in {301, 302, 303, 307, 308}:
                            location = response.headers.get("Location")
                            if not location:
                                raise AttachmentError(
                                    "Discord CDN returned an empty redirect",
                                    code="image_download_redirect",
                                )
                            url = urljoin(url, location)
                            continue
                        if response.status != 200:
                            raise AttachmentError(
                                f"Discord CDN returned HTTP {response.status}",
                                code="image_download_failed",
                            )
                        size = 0
                        with destination.open("xb") as output:
                            async for chunk in response.content.iter_chunked(64 * 1024):
                                size += len(chunk)
                                if size > self._max_bytes:
                                    raise AttachmentError(
                                        "downloaded image exceeds the byte limit",
                                        code="image_size_limit",
                                    )
                                output.write(chunk)
                        if size == 0:
                            raise AttachmentError(
                                "downloaded image is empty",
                                code="image_download_empty",
                            )
                        if os.name != "nt":
                            await asyncio.to_thread(destination.chmod, 0o600)
                        return
                raise AttachmentError(
                    "too many Discord CDN redirects",
                    code="image_download_redirect",
                )
        except TimeoutError as exc:
            raise AttachmentError(
                "Discord image download exceeded 30 seconds",
                code="image_download_timeout",
            ) from exc


def _validate_cdn_url(value: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in _CDN_HOSTS
        or parsed.username
        or parsed.password
        or parsed.port not in {None, 443}
    ):
        raise SecurityError("attachment URL is not an approved Discord CDN URL")


def _sanitize_filename(value: str) -> str:
    name = Path(value).name.replace("\x00", "")
    safe = _SAFE_FILENAME.sub("_", name).strip("._")
    return (safe or "image")[:128]
