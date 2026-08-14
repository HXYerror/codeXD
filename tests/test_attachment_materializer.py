from __future__ import annotations

import hashlib
import io
import os
import stat
import struct
import zipfile
from pathlib import Path

import pytest
from conftest import StorageContext

from codexd.application import attachment_materializer as materializer_module
from codexd.application.attachment_materializer import (
    ArchiveLimits,
    AttachmentMaterializationError,
    AttachmentMaterializer,
)
from codexd.config import RetentionConfig
from codexd.domain.turns import InterruptOrigin, TurnFile, TurnInput, TurnSource
from codexd.paths import AppPaths
from codexd.security import private_files
from codexd.storage.retention import run_retention

_FUTURE = 9_999_999_999_999


def test_plain_file_materialization_uses_opaque_snapshot_context(
    storage_context: StorageContext,
) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="资料.json",
        content=b'{"marker":"plain-materialized-marker"}',
    )
    materializer = _materializer(storage_context)

    result = materializer.materialize(turn_id=turn.id, files=(source,))

    assert len(result.files) == 1
    materialized = result.files[0]
    assert materialized.canonical_path != source.canonical_path
    assert materialized.canonical_path.read_bytes() == source.canonical_path.read_bytes()
    assert str(materialized.canonical_path.parent) in result.context
    assert str(source.canonical_path) not in result.context
    assert "plain-materialized-marker" not in result.context
    assert "untrusted user data" in result.context
    row = storage_context.store.query_one(
        "SELECT kind, file_count, total_bytes FROM materialized_attachments"
    )
    assert row is not None
    assert (row["kind"], row["file_count"], row["total_bytes"]) == (
        "file",
        1,
        source.size_bytes,
    )


def test_zip_materialization_extracts_marker_without_polluting_project(
    storage_context: StorageContext,
) -> None:
    marker = "zip-materialized-unique-marker"
    archive = _zip_bytes(
        {
            "logs/traces.jsonl": f'{{"marker":"{marker}"}}\n'.encode(),
            "nested/archive.zip": _zip_bytes({"do-not-expand.txt": b"nested"}),
        }
    )
    turn, source = _turn_with_file(
        storage_context,
        name="traces.zip",
        content=archive,
        media_type="application/zip",
    )

    result = _materializer(storage_context).materialize(
        turn_id=turn.id,
        files=(source,),
    )

    names = {path.canonical_path.name for path in result.files}
    assert names == {"traces.jsonl", "archive.zip"}
    extracted = next(
        path.canonical_path
        for path in result.files
        if path.canonical_path.name == "traces.jsonl"
    )
    assert marker in extracted.read_text(encoding="utf-8")
    assert not (storage_context.root / "logs").exists()
    assert "logs/traces.jsonl" in result.context
    assert '"ordinal":0' in result.context
    assert str(source.canonical_path) not in result.context


def test_office_zip_container_is_kept_as_opaque_file(
    storage_context: StorageContext,
) -> None:
    content = _zip_bytes({"word/document.xml": b"<document/>"})
    turn, source = _turn_with_file(
        storage_context,
        name="report.docx",
        content=content,
        media_type=(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ),
    )

    result = _materializer(storage_context).materialize(
        turn_id=turn.id,
        files=(source,),
    )

    row = storage_context.store.query_one(
        "SELECT kind FROM materialized_attachments WHERE attachment_id = ?",
        (source.attachment_id,),
    )
    assert row is not None and row["kind"] == "file"
    assert result.files[0].canonical_path.read_bytes() == content


def test_zip_media_type_parameters_are_recognized(
    storage_context: StorageContext,
) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="payload.bin",
        content=_zip_bytes({"marker.txt": b"marker"}),
        media_type="application/zip; charset=binary",
    )

    result = _materializer(storage_context).materialize(
        turn_id=turn.id,
        files=(source,),
    )

    assert result.files[0].canonical_path.name == "marker.txt"


@pytest.mark.parametrize(
    ("entry_names", "expected_code"),
    (
        (("../escape.txt",), "archive_path_unsafe"),
        (("/absolute.txt",), "archive_path_unsafe"),
        (("C:/drive.txt",), "archive_path_unsafe"),
        (("folder\\escape.txt",), "archive_path_unsafe"),
        (("control\x01.txt",), "archive_path_unsafe"),
        (("invisible\u202e.txt",), "archive_path_unsafe"),
        (("a/./file.txt",), "archive_path_unsafe"),
        (("CON.txt",), "archive_path_unsafe"),
        (("trailing-dot.",), "archive_path_unsafe"),
        (("bad:name.txt",), "archive_path_unsafe"),
        (("e\u0301.txt", "é.txt"), "archive_path_unsafe"),
        (("Name.txt", "name.txt"), "archive_path_unsafe"),
        (("parent", "parent/child.txt"), "archive_path_unsafe"),
    ),
)
def test_zip_unsafe_paths_fail_closed_without_residue(
    storage_context: StorageContext,
    entry_names: tuple[str, ...],
    expected_code: str,
) -> None:
    archive = _zip_bytes({name: b"unsafe" for name in entry_names})
    turn, source = _turn_with_file(
        storage_context,
        name="unsafe.zip",
        content=archive,
    )
    materializer = _materializer(storage_context)

    with pytest.raises(AttachmentMaterializationError) as failure:
        materializer.materialize(turn_id=turn.id, files=(source,))

    assert failure.value.code == expected_code
    _assert_no_materialized_residue(storage_context)


def test_zip_symlink_is_rejected(storage_context: StorageContext) -> None:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        info = zipfile.ZipInfo("link")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "target")
    turn, source = _turn_with_file(
        storage_context,
        name="symlink.zip",
        content=stream.getvalue(),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == "archive_path_unsafe"
    _assert_no_materialized_residue(storage_context)


def test_zip_directory_type_mismatch_is_rejected(
    storage_context: StorageContext,
) -> None:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        info = zipfile.ZipInfo("looks-like-a-file")
        info.create_system = 3
        info.external_attr = (stat.S_IFDIR | 0o700) << 16
        archive.writestr(info, "payload")
    turn, source = _turn_with_file(
        storage_context,
        name="type-mismatch.zip",
        content=stream.getvalue(),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == "archive_path_unsafe"
    _assert_no_materialized_residue(storage_context)


def test_zip_encrypted_flag_is_rejected(storage_context: StorageContext) -> None:
    encrypted = bytearray(_zip_bytes({"secret.txt": b"secret"}))
    _set_encrypted_flags(encrypted)
    turn, source = _turn_with_file(
        storage_context,
        name="encrypted.zip",
        content=bytes(encrypted),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == "archive_encrypted"
    _assert_no_materialized_residue(storage_context)


def test_zip_crc_corruption_is_rejected(storage_context: StorageContext) -> None:
    corrupt = bytearray(_zip_bytes({"payload.txt": b"known-content"}, stored=True))
    filename_length = struct.unpack_from("<H", corrupt, 26)[0]
    extra_length = struct.unpack_from("<H", corrupt, 28)[0]
    data_offset = 30 + filename_length + extra_length
    corrupt[data_offset] ^= 0xFF
    turn, source = _turn_with_file(
        storage_context,
        name="corrupt.zip",
        content=bytes(corrupt),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == "archive_integrity_failed"
    _assert_no_materialized_residue(storage_context)


def test_zip_unknown_compression_method_is_rejected(
    storage_context: StorageContext,
) -> None:
    archive = bytearray(_zip_bytes({"payload.txt": b"payload"}, stored=True))
    local = archive.find(b"PK\x03\x04")
    central = archive.find(b"PK\x01\x02")
    assert local >= 0 and central >= 0
    struct.pack_into("<H", archive, local + 8, 99)
    struct.pack_into("<H", archive, central + 10, 99)
    turn, source = _turn_with_file(
        storage_context,
        name="unknown-method.zip",
        content=bytes(archive),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == "archive_unsupported"
    _assert_no_materialized_residue(storage_context)


def test_zip_empty_file_is_rejected(storage_context: StorageContext) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="empty-entry.zip",
        content=_zip_bytes({"empty.txt": b""}),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == "archive_integrity_failed"
    _assert_no_materialized_residue(storage_context)


def test_zip_extraction_timeout_is_stable_and_leaves_no_residue(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="timeout.zip",
        content=_zip_bytes({"payload.txt": b"payload"}),
    )
    ticks = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(
        materializer_module.time,
        "monotonic",
        lambda: next(ticks, 2.0),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(
            storage_context,
            limits=ArchiveLimits(extract_timeout_seconds=1),
        ).materialize(turn_id=turn.id, files=(source,))

    assert failure.value.code == "attachment_materialization_failed"
    _assert_no_materialized_residue(storage_context)


@pytest.mark.parametrize(
    ("limits", "entries", "expected_code"),
    (
        (
            ArchiveLimits(max_entries=1),
            {"one.txt": b"1", "two.txt": b"2"},
            "archive_entry_limit",
        ),
        (
            ArchiveLimits(max_entry_bytes=4, max_total_bytes=10),
            {"large.txt": b"12345"},
            "archive_uncompressed_size_limit",
        ),
        (
            ArchiveLimits(max_entry_bytes=5, max_total_bytes=5),
            {"one.txt": b"123", "two.txt": b"456"},
            "archive_uncompressed_size_limit",
        ),
        (
            ArchiveLimits(max_compression_ratio=2),
            {"compressible.txt": b"A" * 10_000},
            "archive_compression_ratio_limit",
        ),
        (
            ArchiveLimits(max_path_depth=2),
            {"a/b/c.txt": b"deep"},
            "archive_path_unsafe",
        ),
    ),
)
def test_zip_resource_limits_fail_closed(
    storage_context: StorageContext,
    limits: ArchiveLimits,
    entries: dict[str, bytes],
    expected_code: str,
) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="limited.zip",
        content=_zip_bytes(entries),
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context, limits=limits).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == expected_code
    _assert_no_materialized_residue(storage_context)


def test_unsupported_archive_has_actionable_error(
    storage_context: StorageContext,
) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="bundle.tar",
        content=b"not-a-supported-archive",
    )

    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )

    assert failure.value.code == "archive_unsupported"
    assert "upload a ZIP" in str(failure.value)
    _assert_no_materialized_residue(storage_context)


def test_multi_attachment_failure_rolls_back_prior_materialization(
    storage_context: StorageContext,
) -> None:
    first = _stored_file(storage_context, "first.txt", b"first", ordinal=0)
    second = _stored_file(
        storage_context,
        "unsafe.zip",
        _zip_bytes({"../escape.txt": b"escape"}),
        ordinal=1,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(first, second)),
        input_message_id="multi-materialization-failure",
    )

    with pytest.raises(AttachmentMaterializationError):
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(first, second),
        )

    _assert_no_materialized_residue(storage_context)


def test_materialized_snapshot_revalidates_across_service_replacement(
    storage_context: StorageContext,
) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="restart.txt",
        content=b"restart-marker",
    )
    first = _materializer(storage_context).materialize(
        turn_id=turn.id,
        files=(source,),
    )

    second = _materializer(storage_context).materialize(
        turn_id=turn.id,
        files=(source,),
    )

    assert first.context == second.context
    assert first.files == second.files
    assert storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM materialized_attachments"
    )["count"] == 1
    second.files[0].canonical_path.write_bytes(b"tampered-marker")
    if os.name != "nt":
        second.files[0].canonical_path.chmod(0o600)
    with pytest.raises(AttachmentMaterializationError) as failure:
        _materializer(storage_context).materialize(
            turn_id=turn.id,
            files=(source,),
        )
    assert failure.value.code == "attachment_integrity_failed"
    row = storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM materialized_attachments"
    )
    assert row is not None and row["count"] == 0
    assert not second.files[0].canonical_path.exists()


def test_materialized_retention_is_pinned_until_terminal(
    storage_context: StorageContext,
) -> None:
    turn, source = _turn_with_file(
        storage_context,
        name="retained.txt",
        content=b"retention-marker",
        retention_until=1,
    )
    result = _materializer(storage_context).materialize(
        turn_id=turn.id,
        files=(source,),
    )
    materialized_path = result.files[0].canonical_path
    paths = AppPaths(
        storage_context.store.path.parent,
        storage_context.store.path.parent / "logs",
    )

    active = run_retention(
        storage_context.store,
        paths,
        RetentionConfig(),
        now_ms=1_000_000_000,
    )
    assert active.materialized_attachments == 0
    assert materialized_path.exists()

    storage_context.repository.request_cancel(turn.id, origin=InterruptOrigin.USER)
    terminal = run_retention(
        storage_context.store,
        paths,
        RetentionConfig(),
        now_ms=1_000_000_000,
    )
    assert terminal.materialized_attachments == 1
    assert not materialized_path.exists()
    source_cleanup = run_retention(
        storage_context.store,
        paths,
        RetentionConfig(),
        now_ms=1_000_000_000,
    )
    assert source_cleanup.input_attachments == 1
    assert not source.canonical_path.exists()


def test_materialized_retention_fails_closed_on_symlinked_entry(
    storage_context: StorageContext,
) -> None:
    if os.name == "nt":
        pytest.skip("native Windows reparse-point coverage lives in SDK contract tests")
    turn, source = _turn_with_file(
        storage_context,
        name="retained.txt",
        content=b"retention-marker",
        retention_until=1,
    )
    result = _materializer(storage_context).materialize(
        turn_id=turn.id,
        files=(source,),
    )
    root = result.files[0].canonical_path.parent
    outside = storage_context.root / "outside.txt"
    outside.write_text("must survive", encoding="utf-8")
    (root / "unsafe-link").symlink_to(outside)
    storage_context.repository.request_cancel(turn.id, origin=InterruptOrigin.USER)

    retained = run_retention(
        storage_context.store,
        AppPaths(
            storage_context.store.path.parent,
            storage_context.store.path.parent / "logs",
        ),
        RetentionConfig(),
        now_ms=1_000_000_000,
    )

    assert retained.materialized_attachments == 0
    assert outside.read_text(encoding="utf-8") == "must survive"
    row = storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM materialized_attachments"
    )
    assert row is not None and row["count"] == 1


def _materializer(
    storage_context: StorageContext,
    *,
    limits: ArchiveLimits | None = None,
) -> AttachmentMaterializer:
    return AttachmentMaterializer(
        store=storage_context.store,
        data_root=storage_context.store.path.parent,
        limits=limits,
    )


def _turn_with_file(
    storage_context: StorageContext,
    *,
    name: str,
    content: bytes,
    media_type: str = "application/octet-stream",
    retention_until: int = _FUTURE,
) -> tuple[object, TurnFile]:
    file = _stored_file(
        storage_context,
        name,
        content,
        ordinal=0,
        media_type=media_type,
        retention_until=retention_until,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id=f"materialize-{file.attachment_id}",
    )
    return turn, file


def _stored_file(
    storage_context: StorageContext,
    name: str,
    content: bytes,
    *,
    ordinal: int,
    media_type: str = "application/octet-stream",
    retention_until: int = _FUTURE,
) -> TurnFile:
    attachment_id = hashlib.sha256(
        f"{name}:{ordinal}:{len(content)}".encode()
    ).hexdigest()[:24]
    data_root = storage_context.store.path.parent
    attachment_root = data_root / "attachments"
    input_root = attachment_root / "input"
    private_files.ensure_private_directory(data_root)
    private_files.ensure_private_directory(attachment_root)
    private_files.ensure_private_directory(input_root)
    extension = Path(name).suffix if len(Path(name).suffix) <= 16 else ""
    path = input_root / f"{attachment_id}{extension}"
    path.write_bytes(content)
    private_files.secure_private_file(path)
    return TurnFile(
        attachment_id=attachment_id,
        ordinal=ordinal,
        canonical_path=path.resolve(strict=True),
        display_name=name,
        reported_media_type=media_type,
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        retention_until=retention_until,
    )


def _zip_bytes(
    entries: dict[str, bytes],
    *,
    stored: bool = False,
) -> bytes:
    stream = io.BytesIO()
    compression = zipfile.ZIP_STORED if stored else zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(stream, "w", compression=compression) as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return stream.getvalue()


def _set_encrypted_flags(archive: bytearray) -> None:
    local = archive.find(b"PK\x03\x04")
    central = archive.find(b"PK\x01\x02")
    assert local >= 0 and central >= 0
    local_flags = struct.unpack_from("<H", archive, local + 6)[0] | 0x1
    central_flags = struct.unpack_from("<H", archive, central + 8)[0] | 0x1
    struct.pack_into("<H", archive, local + 6, local_flags)
    struct.pack_into("<H", archive, central + 8, central_flags)


def _assert_no_materialized_residue(storage_context: StorageContext) -> None:
    row = storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM materialized_attachments"
    )
    assert row is not None and row["count"] == 0
    root = storage_context.store.path.parent / "attachments" / "materialized"
    assert not root.exists() or not any(path.is_file() for path in root.rglob("*"))
