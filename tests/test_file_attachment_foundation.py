from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from conftest import StorageContext

import codexd.storage.sqlite as sqlite_module
from codexd.config import RetentionConfig, load_config
from codexd.domain.conversations import SandboxProfile
from codexd.domain.ids import canonical_json, sha256_text
from codexd.domain.turns import (
    InterruptOrigin,
    TurnFile,
    TurnImage,
    TurnInput,
    TurnSkill,
    TurnSource,
)
from codexd.errors import (
    AttachmentIntegrityError,
    ConfigurationError,
    ConflictError,
    InvariantError,
)
from codexd.paths import AppPaths
from codexd.security import private_files
from codexd.storage.repository import Repository
from codexd.storage.retention import run_retention
from codexd.storage.sqlite import SQLiteStore

_FUTURE = 9_999_999_999_999


def _secure_input_directory(data_dir: Path) -> Path:
    attachments = data_dir / "attachments"
    inputs = attachments / "input"
    private_files.ensure_private_directory(data_dir)
    private_files.ensure_private_directory(attachments)
    private_files.ensure_private_directory(inputs)
    return inputs


def _turn_file(
    data_dir: Path,
    *,
    attachment_id: str = "ordinary-file",
    ordinal: int = 0,
    content: bytes = b"ordinary attachment bytes",
    display_name: str = "资料.txt",
    reported_media_type: str | None = "text/plain",
    retention_until: int = _FUTURE,
) -> TurnFile:
    path = _secure_input_directory(data_dir) / f"{attachment_id}.bin"
    path.write_bytes(content)
    private_files.secure_private_file(path)
    return TurnFile(
        attachment_id=attachment_id,
        ordinal=ordinal,
        canonical_path=path.resolve(strict=True),
        display_name=display_name,
        reported_media_type=reported_media_type,
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        retention_until=retention_until,
    )


def test_turn_file_is_immutable_and_file_only_input_is_hashed(tmp_path: Path) -> None:
    first = _turn_file(tmp_path, attachment_id="first", ordinal=2)
    second = _turn_file(
        tmp_path,
        attachment_id="second",
        ordinal=0,
        content=b"second",
        reported_media_type=None,
    )

    turn_input = TurnInput(files=(first, second))

    assert turn_input.text is None
    assert [file.attachment_id for file in turn_input.files] == ["second", "first"]
    assert turn_input.snapshot()["files"] == [
        second.snapshot(),
        first.snapshot(),
    ]
    assert turn_input.input_hash != TurnInput(text="ordinary attachment bytes").input_hash
    assert TurnInput(text="legacy hash compatibility").snapshot()["files"] == []
    with pytest.raises(FrozenInstanceError):
        first.size_bytes = 1  # type: ignore[misc]


def test_turn_input_preserves_positional_skill_input_compatibility(tmp_path: Path) -> None:
    skill = TurnSkill(
        name="review",
        canonical_path=(tmp_path / "SKILL.md").absolute(),
        content_hash="skill-hash",
    )

    turn_input = TurnInput("review this", (), (skill,))

    assert turn_input.skill_inputs == (skill,)
    assert turn_input.files == ()


def test_turn_input_rejects_duplicate_ordinals_across_images_and_files(
    tmp_path: Path,
) -> None:
    file = _turn_file(tmp_path, ordinal=3)
    image = TurnImage(
        attachment_id="image",
        ordinal=3,
        canonical_path=(tmp_path / "image.png").absolute(),
        media_type="image/png",
        source_sha256="source",
        sha256="normalized",
        size_bytes=1,
        width=1,
        height=1,
        source_name_sanitized="image.png",
        retention_until=_FUTURE,
    )

    with pytest.raises(InvariantError, match="attachment ordinals"):
        TurnInput(images=(image,), files=(file,))


@pytest.mark.parametrize(
    "display_name",
    [
        "",
        ".",
        "..",
        "../secret",
        "folder\\secret",
        "bad\x00name",
        "<@123>.txt",
        "@everyone.txt",
        "@everyone资料.txt",
        "@here資料.txt",
        "x" * 129,
    ],
)
def test_turn_file_rejects_unsafe_display_names(
    tmp_path: Path,
    display_name: str,
) -> None:
    with pytest.raises(InvariantError, match="display name"):
        TurnFile(
            attachment_id="unsafe-name",
            ordinal=0,
            canonical_path=(tmp_path / "safe.bin").absolute(),
            display_name=display_name,
            reported_media_type=None,
            sha256="0" * 64,
            size_bytes=1,
            retention_until=_FUTURE,
        )


def test_discord_attachment_limit_defaults_and_validation(tmp_path: Path) -> None:
    default = load_config(tmp_path / "missing.toml", environment={"HOME": str(tmp_path)})

    assert default.discord.max_attachment_count == 10
    assert default.discord.file_max_bytes == 25 * 1024 * 1024
    assert default.discord.message_max_bytes == 50 * 1024 * 1024

    custom_path = tmp_path / "custom.toml"
    custom_path.write_text(
        "\n".join(
            (
                "[discord]",
                "max_attachment_count = 4",
                "file_max_bytes = 1024",
                "message_max_bytes = 4096",
            )
        ),
        encoding="utf-8",
    )
    custom = load_config(custom_path, environment={"HOME": str(tmp_path)})
    assert custom.discord.max_attachment_count == 4
    assert custom.discord.file_max_bytes == 1024
    assert custom.discord.message_max_bytes == 4096

    for name, body, message in (
        ("count", "max_attachment_count = 11", "may not exceed 10"),
        ("zero", "file_max_bytes = 0", "positive integer"),
        (
            "aggregate",
            "file_max_bytes = 4096\nmessage_max_bytes = 1024",
            "may not exceed",
        ),
    ):
        invalid = tmp_path / f"invalid-{name}.toml"
        invalid.write_text(f"[discord]\n{body}\n", encoding="utf-8")
        with pytest.raises(ConfigurationError, match=message):
            load_config(invalid, environment={"HOME": str(tmp_path)})


def test_repository_persists_and_reloads_file_only_snapshot(
    storage_context: StorageContext,
) -> None:
    file = _turn_file(storage_context.store.path.parent, reported_media_type=None)
    original = TurnInput(files=(file,))

    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=original,
        input_message_id="file-only-message",
    )

    row = storage_context.store.query_one(
        "SELECT * FROM attachments WHERE id = ?",
        (file.attachment_id,),
    )
    assert row is not None
    assert row["kind"] == "input_file"
    assert row["relative_path"] == "attachments/input/ordinary-file.bin"
    assert not Path(str(row["relative_path"])).is_absolute()
    assert row["source_sha256"] == file.sha256
    assert row["normalized_sha256"] == file.sha256
    assert row["mime_type"] is None
    assert row["source_name_sanitized"] == "资料.txt"
    assert storage_context.repository.load_turn_input(turn.id) == original
    assert turn.input_hash == original.input_hash


def test_repository_accepts_pre_file_snapshot_hashes_for_upgrade_compatibility(
    storage_context: StorageContext,
) -> None:
    turn_input = TurnInput(text="queued before the file migration")
    legacy_snapshot = turn_input.snapshot()
    legacy_snapshot.pop("files")
    legacy_hash = sha256_text(canonical_json(legacy_snapshot))
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=turn_input,
        input_message_id="legacy-input-hash-message",
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE turns SET input_hash = ? WHERE id = ?",
            (legacy_hash, turn.id),
        )

    assert storage_context.repository.load_turn_input(turn.id) == turn_input
    duplicate = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=turn_input,
        input_message_id="legacy-input-hash-message",
    )
    assert duplicate.id == turn.id


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("outside", "input attachment directory"),
        ("hash", "SHA-256"),
        ("size", "size"),
        ("mode", "mode"),
        ("directory", "regular file"),
    ],
)
def test_repository_rejects_unsafe_file_snapshot_at_enqueue(
    storage_context: StorageContext,
    failure: str,
    message: str,
) -> None:
    data_dir = storage_context.store.path.parent
    file = _turn_file(data_dir, attachment_id=f"unsafe-{failure}")
    values = file.__dict__.copy()
    if failure == "outside":
        outside = data_dir / "outside.bin"
        outside.write_bytes(b"outside")
        if os.name != "nt":
            outside.chmod(0o600)
        values.update(
            canonical_path=outside.resolve(strict=True),
            sha256=hashlib.sha256(b"outside").hexdigest(),
            size_bytes=7,
        )
    elif failure == "hash":
        values["sha256"] = "0" * 64
    elif failure == "size":
        values["size_bytes"] = file.size_bytes + 1
    elif failure == "mode":
        if os.name == "nt":
            pytest.skip("POSIX mode validation")
        file.canonical_path.chmod(0o644)
    else:
        file.canonical_path.unlink()
        file.canonical_path.mkdir(mode=0o700)
    unsafe = TurnFile(**values)

    with pytest.raises(InvariantError, match=message):
        storage_context.repository.enqueue_turn(
            conversation_id=storage_context.conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(files=(unsafe,)),
            input_message_id=f"unsafe-{failure}-message",
        )
    assert storage_context.store.query_one(
        "SELECT 1 FROM turns WHERE input_message_id = ?",
        (f"unsafe-{failure}-message",),
    ) is None


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("content", "SHA-256"),
        ("size", "size"),
        ("leaf_symlink", "symlink"),
        ("parent_symlink", "symlink"),
        ("data_root_symlink", "symlink"),
        ("directory", "regular file"),
        ("mode", "mode"),
        ("absolute_db_path", "relative path"),
        ("traversal_db_path", "relative path"),
    ],
)
def test_repository_fails_closed_when_stored_file_changes(
    storage_context: StorageContext,
    mutation: str,
    message: str,
) -> None:
    file = _turn_file(storage_context.store.path.parent, attachment_id=f"load-{mutation}")
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id=f"load-{mutation}-message",
    )
    path = file.canonical_path
    if mutation == "content":
        path.write_bytes(b"x" * file.size_bytes)
    elif mutation == "size":
        path.write_bytes(path.read_bytes() + b"x")
    elif mutation == "leaf_symlink":
        if os.name == "nt":
            pytest.skip("symlink privileges vary on Windows")
        replacement = path.with_name(f"{path.name}.replacement")
        replacement.write_bytes(path.read_bytes())
        replacement.chmod(0o600)
        path.unlink()
        path.symlink_to(replacement)
    elif mutation == "parent_symlink":
        if os.name == "nt":
            pytest.skip("symlink privileges vary on Windows")
        parent = path.parent
        replacement_parent = parent.with_name("input-replacement")
        parent.rename(replacement_parent)
        parent.symlink_to(replacement_parent, target_is_directory=True)
    elif mutation == "data_root_symlink":
        if os.name == "nt":
            pytest.skip("symlink privileges vary on Windows")
        data_root = storage_context.store.path.parent
        replacement_root = data_root.with_name("data-replacement")
        data_root.rename(replacement_root)
        data_root.symlink_to(replacement_root, target_is_directory=True)
    elif mutation == "directory":
        path.unlink()
        path.mkdir(mode=0o700)
    elif mutation == "mode":
        if os.name == "nt":
            pytest.skip("POSIX mode validation")
        path.chmod(0o644)
    else:
        relative_path = (
            str(path)
            if mutation == "absolute_db_path"
            else "attachments/input/../escape.bin"
        )
        storage_context.store.connection.execute("PRAGMA ignore_check_constraints = ON")
        try:
            with storage_context.store.transaction() as connection:
                connection.execute(
                    "UPDATE attachments SET relative_path = ? WHERE id = ?",
                    (relative_path, file.attachment_id),
                )
        finally:
            storage_context.store.connection.execute(
                "PRAGMA ignore_check_constraints = OFF"
            )

    with pytest.raises(ConflictError, match=message):
        storage_context.repository.load_turn_input(turn.id)


@pytest.mark.parametrize(
    "relative_path",
    [
        "/absolute/file.bin",
        "C:\\absolute\\file.bin",
        "attachments/input/../escape.bin",
        "other/relative.bin",
    ],
)
def test_attachment_schema_rejects_non_relative_paths(
    storage_context: StorageContext,
    relative_path: str,
) -> None:
    file = _turn_file(storage_context.store.path.parent, attachment_id="schema-path")
    storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id="schema-path-message",
    )

    with (
        pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"),
        storage_context.store.transaction() as connection,
    ):
        connection.execute(
            "UPDATE attachments SET relative_path = ? WHERE id = ?",
            (relative_path, file.attachment_id),
        )


def test_input_file_migration_preserves_images_and_enforces_shared_ordinals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migrations = sqlite_module._load_migrations()
    database = tmp_path / "data" / "upgrade.sqlite3"
    project_root = tmp_path / "project"
    project_root.mkdir()
    with SQLiteStore(database) as store:
        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 15),
        )
        assert store.migrate() == 14
        repository = Repository(store)
        project = repository.bind_project(
            name="upgrade",
            root_path=project_root,
            guild_id=100,
            channel_id=200,
            sandbox_profile=SandboxProfile.FULL_ACCESS,
        )
        conversation = repository.create_conversation(
            project_id=project.id,
            discord_thread_id=300,
            discord_guild_id=100,
            discord_parent_channel_id=200,
            owner_user_id=400,
        )
        image_path = database.parent / "legacy-image.png"
        image_path.write_bytes(b"image")
        image_digest = hashlib.sha256(b"image").hexdigest()
        image_turn = repository.enqueue_turn(
            conversation_id=conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(
                images=(
                    TurnImage(
                        attachment_id="legacy-image",
                        ordinal=0,
                        canonical_path=image_path.resolve(strict=True),
                        media_type="image/png",
                        source_sha256=image_digest,
                        sha256=image_digest,
                        size_bytes=5,
                        width=1,
                        height=1,
                        source_name_sanitized="legacy.png",
                        retention_until=_FUTURE,
                    ),
                )
            ),
            input_message_id="legacy-image-message",
        )

        monkeypatch.setattr(sqlite_module, "_load_migrations", lambda: migrations)
        assert store.migrate() == 18
        assert store.integrity_check() == "ok"
        assert store.foreign_key_check() == ()
        assert repository.load_turn_input(image_turn.id).images[0].attachment_id == "legacy-image"

        file = _turn_file(database.parent)
        file_turn = repository.enqueue_turn(
            conversation_id=conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(files=(file,)),
            input_message_id="post-upgrade-file-message",
        )
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            store.connection.execute(
                """
                INSERT INTO attachments(
                    id, turn_id, kind, ordinal, relative_path,
                    source_sha256, normalized_sha256, size_bytes, mime_type,
                    width, height, source_name_sanitized, retention_until, created_at
                )
                SELECT 'duplicate-ordinal', turn_id, kind, ordinal, relative_path,
                       source_sha256, normalized_sha256, size_bytes, mime_type,
                       width, height, source_name_sanitized, retention_until, created_at
                FROM attachments WHERE turn_id = ? AND kind = 'input_file'
                """,
                (file_turn.id,),
            )


def test_file_snapshot_survives_database_reopen(tmp_path: Path) -> None:
    database = tmp_path / "data" / "codexd.sqlite3"
    project_root = tmp_path / "project"
    project_root.mkdir()
    with SQLiteStore(database) as store:
        store.migrate()
        repository = Repository(store)
        project = repository.bind_project(
            name="reopen",
            root_path=project_root,
            guild_id=100,
            channel_id=200,
            sandbox_profile=SandboxProfile.FULL_ACCESS,
        )
        conversation = repository.create_conversation(
            project_id=project.id,
            discord_thread_id=300,
            discord_guild_id=100,
            discord_parent_channel_id=200,
            owner_user_id=400,
        )
        file = _turn_file(database.parent)
        turn = repository.enqueue_turn(
            conversation_id=conversation.id,
            source=TurnSource.DISCORD,
            turn_input=TurnInput(files=(file,)),
            input_message_id="reopen-file-message",
        )

    with SQLiteStore(database) as reopened:
        assert reopened.migrate() == 18
        restored = Repository(reopened).load_turn_input(turn.id)
    assert restored.files == (file,)


@pytest.mark.parametrize("active_state", ["queued", "starting", "running"])
def test_retention_never_deletes_active_file_references(
    storage_context: StorageContext,
    active_state: str,
) -> None:
    file = _turn_file(
        storage_context.store.path.parent,
        attachment_id=f"active-{active_state}",
        retention_until=1,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id=f"active-{active_state}-message",
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE turns SET state = ? WHERE id = ?",
            (active_state, turn.id),
        )
    os.utime(file.canonical_path, (1, 1))

    result = run_retention(
        storage_context.store,
        AppPaths(
            storage_context.store.path.parent,
            storage_context.store.path.parent / "logs",
        ),
        RetentionConfig(),
        now_ms=1_000_000_000,
    )

    assert result.input_attachments == 0
    assert file.canonical_path.exists()


def test_retention_removes_terminal_file_and_unreferenced_file_orphan(
    storage_context: StorageContext,
) -> None:
    file = _turn_file(
        storage_context.store.path.parent,
        attachment_id="expired-file",
        retention_until=1,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id="expired-file-message",
    )
    storage_context.repository.request_cancel(turn.id, origin=InterruptOrigin.USER)
    orphan = _secure_input_directory(storage_context.store.path.parent) / "orphan-file.bin"
    orphan.write_bytes(b"orphan")
    if os.name != "nt":
        orphan.chmod(0o600)
    os.utime(orphan, (1, 1))

    result = run_retention(
        storage_context.store,
        AppPaths(
            storage_context.store.path.parent,
            storage_context.store.path.parent / "logs",
        ),
        RetentionConfig(),
        now_ms=1_000_000_000,
    )

    assert result.input_attachments == 1
    assert result.orphan_artifacts == 1
    assert not file.canonical_path.exists()
    assert not orphan.exists()


def test_repository_windows_permission_facade_never_skips_file_verification(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name == "nt":
        pytest.skip("the native Windows backend is available")
    file = _turn_file(storage_context.store.path.parent)
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id="windows-private-permission-facade",
    )
    monkeypatch.setattr(private_files, "_platform_name", lambda: "nt")

    with pytest.raises(AttachmentIntegrityError) as failure:
        storage_context.repository.load_turn_input(turn.id)

    assert failure.value.code == "attachment_integrity_failed"
    assert str(file.canonical_path) not in str(failure.value)


def test_retention_unlink_failure_log_is_path_safe_and_keeps_metadata(
    storage_context: StorageContext,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    file = _turn_file(
        storage_context.store.path.parent,
        attachment_id="unlink-failure-file",
        retention_until=1,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(file,)),
        input_message_id="unlink-failure-message",
    )
    storage_context.repository.request_cancel(turn.id, origin=InterruptOrigin.USER)
    original_unlink = Path.unlink

    def fail_target_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path == file.canonical_path:
            raise OSError(f"cannot unlink private path {path}")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_target_unlink)
    caplog.set_level(logging.WARNING, logger="codexd.storage.retention")

    result = run_retention(
        storage_context.store,
        AppPaths(
            storage_context.store.path.parent,
            storage_context.store.path.parent / "logs",
        ),
        RetentionConfig(),
        now_ms=1_000_000_000,
    )

    assert result.input_attachments == 0
    assert file.canonical_path.exists()
    assert storage_context.store.query_one(
        "SELECT id FROM attachments WHERE id = ?",
        (file.attachment_id,),
    ) is not None
    records = [
        record
        for record in caplog.records
        if getattr(record, "stable_code", None)
        == "retention_artifact_unlink_failed"
    ]
    assert len(records) == 1
    record = records[0]
    assert record.artifact_id == file.attachment_id
    assert record.exc_info is None
    assert record.exc_text is None
    assert record.args == ()
    assert str(file.canonical_path) not in record.getMessage()
