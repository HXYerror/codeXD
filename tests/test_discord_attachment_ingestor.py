from __future__ import annotations

import hashlib
import io
import os
import re
import uuid
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import aiohttp
import discord
import pytest
from PIL import Image

from codexd.rendering.media_worker import MediaWorker, MediaWorkerError
from codexd.security import private_files
from codexd.transport.discord.attachments import (
    AttachmentError,
    DiscordAttachmentIngestor,
    DiscordAttachmentIngestResult,
    _ensure_private_directory,
)


class _FakeContent:
    def __init__(
        self,
        chunks: tuple[bytes, ...],
        error: BaseException | None = None,
    ) -> None:
        self._chunks = chunks
        self._error = error

    async def iter_chunked(self, _size: int) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk
        if self._error is not None:
            raise self._error


class _FakeResponse:
    def __init__(
        self,
        *,
        status: int = 200,
        chunks: tuple[bytes, ...] = (),
        headers: Mapping[str, str] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.status = status
        self.headers = dict(headers or {})
        self.content = _FakeContent(chunks, error)

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: object,
    ) -> None:
        return None


class _FakeSession:
    def __init__(self, *responses: _FakeResponse | BaseException) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.requests.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _attachment(
    content: bytes,
    *,
    filename: str = "notes.txt",
    content_type: str | None = "text/plain",
    size: int | None = None,
    url: str = "https://cdn.discordapp.com/attachments/1/2/notes.txt",
) -> discord.Attachment:
    return cast(
        discord.Attachment,
        SimpleNamespace(
            filename=filename,
            content_type=content_type,
            size=len(content) if size is None else size,
            url=url,
        ),
    )


def _png_bytes() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (5, 3), "purple").save(output, format="PNG")
    return output.getvalue()


def _ingestor(
    tmp_path: Path,
    session: _FakeSession,
    *,
    worker: MediaWorker | None = None,
    image_max_bytes: int = 1024 * 1024,
    file_max_bytes: int = 1024 * 1024,
    message_max_bytes: int = 2 * 1024 * 1024,
    max_attachment_count: int = 10,
) -> DiscordAttachmentIngestor:
    return DiscordAttachmentIngestor(
        session=cast(aiohttp.ClientSession, session),
        media_worker=worker or MediaWorker(),
        attachments_dir=tmp_path,
        image_max_bytes=image_max_bytes,
        image_max_pixels=1_000_000,
        file_max_bytes=file_max_bytes,
        message_max_bytes=message_max_bytes,
        retention_days=7,
        max_attachment_count=max_attachment_count,
    )


@pytest.mark.asyncio
async def test_ingestor_classifies_mixed_content_and_preserves_ordinals(
    tmp_path: Path,
) -> None:
    image_bytes = _png_bytes()
    file_bytes = "opaque 中文 content".encode()
    session = _FakeSession(
        _FakeResponse(chunks=(image_bytes,)),
        _FakeResponse(chunks=(file_bytes,)),
    )
    ingestor = _ingestor(tmp_path, session)
    attachments = [
        _attachment(
            image_bytes,
            filename="actually-text.txt",
            content_type="text/plain",
        ),
        _attachment(
            file_bytes,
            filename="../../资料<@123>.TXT",
            content_type="application/octet-stream",
        ),
    ]

    result = await ingestor.ingest(attachments)

    assert isinstance(result, DiscordAttachmentIngestResult)
    assert [image.ordinal for image in result.images] == [0]
    assert [file.ordinal for file in result.files] == [1]
    image = result.images[0]
    file = result.files[0]
    assert image.media_type == "image/png"
    assert file.canonical_path.read_bytes() == file_bytes
    assert file.sha256 == hashlib.sha256(file_bytes).hexdigest()
    assert file.size_bytes == len(file_bytes)
    assert "资料" in file.display_name
    assert not re.search(r"<(?:@!?|@&|#)\d+>", file.display_name)
    assert "/" not in file.display_name and "\\" not in file.display_name
    assert file.canonical_path.parent == (tmp_path / "input").resolve()
    assert file.canonical_path.suffix == ".txt"
    uuid.UUID(file.canonical_path.stem)
    assert len(session.requests) == 2
    assert all(request[1]["auto_decompress"] is False for request in session.requests)
    if os.name != "nt":
        assert stat_mode(tmp_path / ".quarantine") == 0o700
        assert stat_mode(tmp_path / "input") == 0o700
        assert stat_mode(file.canonical_path) == 0o600
        assert stat_mode(image.canonical_path) == 0o600
    assert list((tmp_path / ".quarantine").iterdir()) == []

    ingestor.cleanup(result)
    assert list((tmp_path / "input").iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filename", "content_type", "content"),
    (
        ("notes.txt", "text/plain", b"plain text"),
        ("config.json", "application/json", b'{"enabled":true}'),
        ("brief.pdf", "application/pdf", b"%PDF-1.7\nopaque test payload"),
        ("payload.bin", "application/octet-stream", b"\x00\x01opaque\xff"),
    ),
)
async def test_ordinary_document_formats_remain_opaque_files(
    tmp_path: Path,
    filename: str,
    content_type: str,
    content: bytes,
) -> None:
    ingestor = _ingestor(
        tmp_path,
        _FakeSession(_FakeResponse(chunks=(content,))),
    )

    result = await ingestor.ingest(
        [_attachment(content, filename=filename, content_type=content_type)]
    )

    assert result.images == ()
    assert len(result.files) == 1
    assert result.files[0].display_name == filename
    assert result.files[0].canonical_path.read_bytes() == content
    ingestor.cleanup(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "filename",
    (
        "payload.exe",
        "tool.COM",
        "run.bat",
        "run.cmd",
        "setup.ps1",
        "installer.msi",
        "saver.scr",
    ),
)
async def test_executable_extensions_are_not_preserved_in_storage_paths(
    tmp_path: Path,
    filename: str,
) -> None:
    content = b"opaque executable-looking bytes"
    ingestor = _ingestor(
        tmp_path,
        _FakeSession(_FakeResponse(chunks=(content,))),
    )

    result = await ingestor.ingest(
        [_attachment(content, filename=filename, content_type="application/octet-stream")]
    )

    assert result.files[0].display_name == filename
    assert result.files[0].canonical_path.suffix == ""
    uuid.UUID(result.files[0].canonical_path.name)
    ingestor.cleanup(result)


@pytest.mark.asyncio
async def test_broadcast_mention_before_cjk_is_sanitized(tmp_path: Path) -> None:
    content = b"ordinary"
    ingestor = _ingestor(
        tmp_path,
        _FakeSession(_FakeResponse(chunks=(content,))),
    )

    result = await ingestor.ingest(
        [_attachment(content, filename="@everyone资料.txt", content_type="text/plain")]
    )

    assert "@everyone" not in result.files[0].display_name.casefold()
    ingestor.cleanup(result)


def test_windows_private_storage_facade_fails_closed_on_this_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(private_files, "_platform_name", lambda: "nt")

    with pytest.raises(AttachmentError) as failure:
        _ensure_private_directory(tmp_path / "attachments")

    assert failure.value.code == "file_input_unsupported"
    assert str(tmp_path) not in str(failure.value)
    assert not (tmp_path / "attachments").exists()


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows semantics")
def test_windows_attachment_storage_is_explicitly_unavailable_without_dacl_support(
    tmp_path: Path,
) -> None:
    with pytest.raises(AttachmentError) as failure:
        _ensure_private_directory(tmp_path / "attachments")

    assert failure.value.code == "file_input_unsupported"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filename", "content_type"),
    (("broken.png", "application/octet-stream"), ("broken.bin", "image/png")),
)
async def test_claimed_image_never_downgrades_to_opaque_file(
    tmp_path: Path,
    filename: str,
    content_type: str,
) -> None:
    content = b"clearly not an image"
    ingestor = _ingestor(tmp_path, _FakeSession(_FakeResponse(chunks=(content,))))

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest(
            [_attachment(content, filename=filename, content_type=content_type)]
        )

    assert failure.value.code == "image_decode_failed"
    assert list((tmp_path / "input").iterdir()) == []
    assert list((tmp_path / ".quarantine").iterdir()) == []


@pytest.mark.asyncio
async def test_broken_image_magic_is_rejected_without_image_metadata(
    tmp_path: Path,
) -> None:
    content = b"\x89PNG\r\n\x1a\ntruncated"
    ingestor = _ingestor(tmp_path, _FakeSession(_FakeResponse(chunks=(content,))))

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest(
            [_attachment(content, filename="payload.bin", content_type=None)]
        )

    assert failure.value.code == "image_decode_failed"


@pytest.mark.asyncio
async def test_worker_failure_is_image_decode_failure_not_opaque_fallback(
    tmp_path: Path,
) -> None:
    content = b"ordinary bytes"
    worker = Mock(spec=MediaWorker)
    worker.classify_attachment = AsyncMock(
        side_effect=MediaWorkerError("worker timed out")
    )
    ingestor = _ingestor(
        tmp_path,
        _FakeSession(_FakeResponse(chunks=(content,))),
        worker=cast(MediaWorker, worker),
    )

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(content)])

    assert failure.value.code == "image_decode_failed"
    assert list((tmp_path / "input").iterdir()) == []


@pytest.mark.asyncio
async def test_ordinary_file_uses_ordinary_per_file_cap(tmp_path: Path) -> None:
    content = b"12345"
    ingestor = _ingestor(
        tmp_path,
        _FakeSession(_FakeResponse(chunks=(content,))),
        image_max_bytes=10,
        file_max_bytes=4,
        message_max_bytes=10,
    )

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(content)])

    assert failure.value.code == "attachment_size_limit"
    assert list((tmp_path / "input").iterdir()) == []


@pytest.mark.asyncio
async def test_actual_aggregate_cap_rolls_back_every_committed_file(
    tmp_path: Path,
) -> None:
    first = b"1234"
    second = b"5678"
    ingestor = _ingestor(
        tmp_path,
        _FakeSession(
            _FakeResponse(chunks=(first,)),
            _FakeResponse(chunks=(second,)),
        ),
        image_max_bytes=5,
        file_max_bytes=5,
        message_max_bytes=6,
    )

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(first), _attachment(second)])

    assert failure.value.code == "attachment_total_size_limit"
    assert list((tmp_path / "input").iterdir()) == []
    assert list((tmp_path / ".quarantine").iterdir()) == []


@pytest.mark.asyncio
async def test_streaming_cap_does_not_trust_content_length(tmp_path: Path) -> None:
    content = b"12345"
    ingestor = _ingestor(
        tmp_path,
        _FakeSession(
            _FakeResponse(chunks=(b"12", b"345"), headers={"Content-Length": "1"})
        ),
        image_max_bytes=4,
        file_max_bytes=4,
        message_max_bytes=8,
    )

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(content, size=1)])

    assert failure.value.code == "attachment_size_limit"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    (
        _FakeResponse(chunks=()),
        _FakeResponse(chunks=(b"abc",), headers={"Content-Length": "4"}),
        _FakeResponse(status=206, chunks=(b"abc",)),
        _FakeResponse(
            chunks=(b"abc",),
            error=aiohttp.ClientPayloadError("truncated body"),
        ),
    ),
)
async def test_empty_and_partial_downloads_are_integrity_failures(
    tmp_path: Path,
    response: _FakeResponse,
) -> None:
    ingestor = _ingestor(tmp_path, _FakeSession(response))

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(b"abc")])

    assert failure.value.code == "attachment_integrity_failed"
    assert list((tmp_path / ".quarantine").iterdir()) == []


@pytest.mark.asyncio
async def test_metadata_size_mismatch_is_integrity_failure(tmp_path: Path) -> None:
    content = b"abc"
    ingestor = _ingestor(tmp_path, _FakeSession(_FakeResponse(chunks=(content,))))

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(content, size=4)])

    assert failure.value.code == "attachment_integrity_failed"


@pytest.mark.asyncio
async def test_redirect_target_is_revalidated_before_second_request(
    tmp_path: Path,
) -> None:
    session = _FakeSession(
        _FakeResponse(status=302, headers={"Location": "https://example.com/file.txt"})
    )
    ingestor = _ingestor(tmp_path, session)

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(b"abc")])

    assert failure.value.code == "attachment_download_failed"
    assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_allowed_redirect_downloads_attachment_once(tmp_path: Path) -> None:
    content = b"abc"
    session = _FakeSession(
        _FakeResponse(status=302, headers={"Location": "/attachments/1/2/final.txt"}),
        _FakeResponse(chunks=(content,)),
    )
    ingestor = _ingestor(tmp_path, session)

    result = await ingestor.ingest([_attachment(content)])

    assert result.files[0].canonical_path.read_bytes() == content
    assert [request[0] for request in session.requests] == [
        "https://cdn.discordapp.com/attachments/1/2/notes.txt",
        "https://cdn.discordapp.com/attachments/1/2/final.txt",
    ]
    ingestor.cleanup(result)


@pytest.mark.asyncio
async def test_download_timeout_has_stable_attachment_code(tmp_path: Path) -> None:
    ingestor = _ingestor(tmp_path, _FakeSession(TimeoutError("connect timed out")))

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(b"abc")])

    assert failure.value.code == "attachment_download_timeout"
    assert "attachment" in str(failure.value)
    assert "image" not in str(failure.value)


@pytest.mark.asyncio
async def test_count_limit_rejects_before_any_download(tmp_path: Path) -> None:
    session = _FakeSession()
    ingestor = _ingestor(tmp_path, session, max_attachment_count=1)

    with pytest.raises(AttachmentError) as failure:
        await ingestor.ingest([_attachment(b"a"), _attachment(b"b")])

    assert failure.value.code == "too_many_attachments"
    assert session.requests == []


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777
