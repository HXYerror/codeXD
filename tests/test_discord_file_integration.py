from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import discord
import pytest
from conftest import StorageContext

import codexd.transport.discord.bot as bot_module
from codexd.application.session_coordinator import ResolvedProject
from codexd.config import (
    AppConfig,
    DiscordConfig,
    RenderingConfig,
    RetentionConfig,
)
from codexd.domain.ids import utc_now_ms
from codexd.domain.turns import TurnFile, TurnImage
from codexd.paths import AppPaths
from codexd.security import private_files
from codexd.transport.discord.attachments import (
    AttachmentError,
    DiscordAttachmentIngestor,
    DiscordAttachmentIngestResult,
)
from codexd.transport.discord.bot import CodexDBot


def _bot(
    storage_context: StorageContext,
    tmp_path: Path,
) -> tuple[CodexDBot, Mock]:
    sessions = Mock()
    sessions.conversation_for_thread = AsyncMock(return_value=storage_context.conversation)
    sessions.resolve_project_for_channel = AsyncMock(
        return_value=ResolvedProject(storage_context.project, "binding")
    )
    bot = CodexDBot(
        config=AppConfig(
            paths=AppPaths(tmp_path / "data", tmp_path / "logs"),
            discord=DiscordConfig(
                guild_id=100,
                owner_user_id=400,
                allowed_user_ids=frozenset({400}),
            ),
        ),
        repository=storage_context.repository,
        sessions=sessions,
        session_lifecycle=Mock(),
        turns=Mock(),
        schedules=Mock(),
        schedule_repository=Mock(),
        runtimes=Mock(),
        renderer=Mock(),
        media_worker=Mock(),
        signer=Mock(),
        capability_manifest=Mock(
            optional={
                "mention.input": False,
                "codexd.attachment_materialization": True,
            }
        ),
        boot_id="attachment-integration",
    )
    bot_user = Mock(id=999, bot=True)
    bot._connection.user = bot_user
    return bot, bot_user


def _attachment(
    ordinal: int,
    filename: str,
    *,
    content_type: str | None = "application/octet-stream",
) -> discord.Attachment:
    return cast(
        discord.Attachment,
        SimpleNamespace(
            id=10_000 + ordinal,
            filename=filename,
            size=4 + ordinal,
            content_type=content_type,
            url=(f"https://cdn.discordapp.com/attachments/private/{ordinal}/{filename}"),
        ),
    )


def _message(
    *,
    message_id: int,
    channel: discord.abc.Messageable,
    content: str,
    attachments: list[discord.Attachment],
    bot_user: Mock,
    mentioned: bool,
) -> discord.Message:
    message = Mock(spec=discord.Message)
    message.id = message_id
    message.author = Mock(id=400, bot=False)
    message.webhook_id = None
    message.guild = Mock(id=100)
    message.channel = channel
    message.content = content
    message.mentions = [bot_user] if mentioned else []
    message.attachments = attachments
    return cast(discord.Message, message)


def _private_input_directory(data_root: Path) -> Path:
    attachments = data_root / "attachments"
    inputs = data_root / "attachments" / "input"
    private_files.ensure_private_directory(data_root)
    private_files.ensure_private_directory(attachments)
    private_files.ensure_private_directory(inputs)
    return inputs


def _turn_file(
    data_root: Path,
    *,
    attachment_id: str,
    ordinal: int,
    display_name: str,
    content: bytes | None = None,
) -> TurnFile:
    payload = content or f"opaque-{attachment_id}".encode()
    path = _private_input_directory(data_root) / f"{attachment_id}.bin"
    path.write_bytes(payload)
    private_files.secure_private_file(path)
    return TurnFile(
        attachment_id=attachment_id,
        ordinal=ordinal,
        canonical_path=path.resolve(strict=True),
        display_name=display_name,
        reported_media_type="application/octet-stream",
        sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        retention_until=utc_now_ms() + 86_400_000,
    )


def _turn_image(
    data_root: Path,
    *,
    attachment_id: str,
    ordinal: int,
) -> TurnImage:
    payload = f"normalized-{attachment_id}".encode()
    path = _private_input_directory(data_root) / f"{attachment_id}.png"
    path.write_bytes(payload)
    private_files.secure_private_file(path)
    digest = hashlib.sha256(payload).hexdigest()
    return TurnImage(
        attachment_id=attachment_id,
        ordinal=ordinal,
        canonical_path=path.resolve(strict=True),
        media_type="image/png",
        source_sha256=digest,
        sha256=digest,
        size_bytes=len(payload),
        width=2,
        height=1,
        source_name_sanitized=f"{attachment_id}.png",
        retention_until=utc_now_ms() + 86_400_000,
    )


def _repository_enqueue(storage_context: StorageContext) -> AsyncMock:
    async def enqueue(**kwargs: Any) -> object:
        return storage_context.repository.enqueue_turn(**kwargs)

    return AsyncMock(side_effect=enqueue)


@pytest.mark.asyncio
async def test_setup_wires_unified_ingestor_with_all_configured_limits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(
        paths=AppPaths(tmp_path / "data", tmp_path / "logs"),
        discord=DiscordConfig(
            guild_id=100,
            owner_user_id=400,
            allowed_user_ids=frozenset({400}),
            max_attachment_count=4,
            file_max_bytes=12_345,
            message_max_bytes=45_678,
        ),
        rendering=RenderingConfig(
            image_max_bytes=23_456,
            image_max_pixels=34_567,
        ),
        retention=RetentionConfig(input_attachments_days=9),
    )
    bot = CodexDBot(
        config=config,
        repository=Mock(),
        sessions=Mock(),
        session_lifecycle=Mock(),
        turns=Mock(),
        schedules=Mock(),
        schedule_repository=Mock(),
        runtimes=Mock(),
        renderer=Mock(),
        media_worker=Mock(),
        signer=Mock(),
        capability_manifest=Mock(optional={}),
        boot_id="wiring",
    )
    ingestor = Mock(spec=DiscordAttachmentIngestor)
    constructor = Mock(return_value=ingestor)
    worker = Mock(start=Mock())
    monkeypatch.setattr(bot_module, "DiscordAttachmentIngestor", constructor)
    monkeypatch.setattr(bot_module, "OutboxWorker", Mock(return_value=worker))
    monkeypatch.setattr(bot, "_register_commands", Mock())
    monkeypatch.setattr(bot, "_sync_commands_or_degrade", AsyncMock())

    try:
        await bot.setup_hook()

        assert bot._attachment_ingestor is ingestor
        constructor.assert_called_once_with(
            session=bot._http_session,
            media_worker=bot.media_worker,
            attachments_dir=config.paths.attachments,
            image_max_bytes=23_456,
            image_max_pixels=34_567,
            file_max_bytes=12_345,
            message_max_bytes=45_678,
            retention_days=9,
            max_attachment_count=4,
        )
        worker.start.assert_called_once_with()
    finally:
        assert bot._http_session is not None
        await bot._http_session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ("file_only", "text_files", "mixed"))
async def test_existing_thread_uses_one_ingest_and_one_turn_for_file_inputs(
    storage_context: StorageContext,
    tmp_path: Path,
    scenario: str,
) -> None:
    bot, bot_user = _bot(storage_context, tmp_path)
    channel = Mock(spec=discord.Thread)
    channel.id = 300
    channel.parent_id = 200
    channel.send = AsyncMock()
    data_root = storage_context.store.path.parent
    result: DiscordAttachmentIngestResult
    if scenario == "file_only":
        content = ""
        attachments = [_attachment(0, "notes.txt")]
        result = DiscordAttachmentIngestResult(
            files=(
                _turn_file(
                    data_root,
                    attachment_id="thread-file-only",
                    ordinal=0,
                    display_name="notes.txt",
                ),
            )
        )
    elif scenario == "text_files":
        content = "inspect both files"
        attachments = [
            _attachment(0, "config.json"),
            _attachment(1, "brief.pdf"),
        ]
        result = DiscordAttachmentIngestResult(
            files=(
                _turn_file(
                    data_root,
                    attachment_id="thread-json",
                    ordinal=0,
                    display_name="config.json",
                ),
                _turn_file(
                    data_root,
                    attachment_id="thread-pdf",
                    ordinal=1,
                    display_name="brief.pdf",
                ),
            )
        )
    else:
        content = "compare every attachment"
        attachments = [
            _attachment(0, "raw.bin"),
            _attachment(1, "diagram.png"),
            _attachment(2, "notes.txt"),
        ]
        result = DiscordAttachmentIngestResult(
            images=(
                _turn_image(
                    data_root,
                    attachment_id="thread-image",
                    ordinal=1,
                ),
            ),
            files=(
                _turn_file(
                    data_root,
                    attachment_id="thread-late-file",
                    ordinal=2,
                    display_name="notes.txt",
                ),
                _turn_file(
                    data_root,
                    attachment_id="thread-first-file",
                    ordinal=0,
                    display_name="raw.bin",
                ),
            ),
        )
    ingestor = Mock(
        ingest=AsyncMock(return_value=result),
        cleanup=Mock(side_effect=DiscordAttachmentIngestor.cleanup),
    )
    bot._attachment_ingestor = ingestor
    bot.turns.enqueue = _repository_enqueue(storage_context)  # type: ignore[method-assign]
    message = _message(
        message_id=910,
        channel=channel,
        content=content,
        attachments=attachments,
        bot_user=bot_user,
        mentioned=False,
    )

    await bot._handle_message(message)
    await bot._handle_message(message)

    ingress = storage_context.repository.get_ingress_message("910")
    turn_input = storage_context.repository.load_turn_input(
        cast(str, ingress.turn_id),
        volatile_text=content or None,
        use_volatile_text=True,
    )
    assert ingress.state == "ready"
    assert turn_input.text == (content or None)
    combined = sorted(
        [(image.ordinal, "image") for image in turn_input.images]
        + [(file.ordinal, "file") for file in turn_input.files]
    )
    assert (
        combined
        == {
            "file_only": [(0, "file")],
            "text_files": [(0, "file"), (1, "file")],
            "mixed": [(0, "file"), (1, "image"), (2, "file")],
        }[scenario]
    )
    ingestor.ingest.assert_awaited_once_with(attachments)
    ingestor.cleanup.assert_not_called()
    bot.turns.enqueue.assert_awaited_once()
    turn_count = storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM turns WHERE input_message_id = '910'"
    )
    assert turn_count is not None
    assert turn_count["count"] == 1
    assert all(image.canonical_path.exists() for image in result.images)
    assert all(file.canonical_path.exists() for file in result.files)
    channel.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    ("text_only", "image_only", "file_only", "mixed", "misleading_image"),
)
async def test_channel_mention_refetches_then_ingests_attachments_once(
    storage_context: StorageContext,
    tmp_path: Path,
    scenario: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot, bot_user = _bot(storage_context, tmp_path)
    parent = Mock(spec=discord.TextChannel)
    parent.id = 200
    parent.send = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.id = 911
    thread.parent_id = 200
    thread.archived = False
    thread.locked = False
    data_root = storage_context.store.path.parent
    result: DiscordAttachmentIngestResult
    expected_title: str
    expected_text: str | None
    expected_kinds: list[tuple[int, str]]
    if scenario == "text_only":
        content = "<@999> inspect the project"
        attachments = []
        result = DiscordAttachmentIngestResult()
        expected_title = "inspect the project"
        expected_text = "inspect the project"
        expected_kinds = []
    elif scenario == "image_only":
        content = "<@999>"
        attachments = [
            _attachment(0, "capture.bin", content_type="image/png")
        ]
        result = DiscordAttachmentIngestResult(
            images=(
                _turn_image(
                    data_root,
                    attachment_id="channel-image-only",
                    ordinal=0,
                ),
            )
        )
        expected_title = "图片任务"
        expected_text = None
        expected_kinds = [(0, "image")]
    elif scenario == "file_only":
        content = "<@999>"
        attachments = [_attachment(0, "channel.txt")]
        result = DiscordAttachmentIngestResult(
            files=(
                _turn_file(
                    data_root,
                    attachment_id="channel-file-only",
                    ordinal=0,
                    display_name="channel.txt",
                ),
            )
        )
        expected_title = "新任务"
        expected_text = None
        expected_kinds = [(0, "file")]
    elif scenario == "mixed":
        content = "<@999>"
        attachments = [
            _attachment(0, "channel.json"),
            _attachment(1, "channel.png"),
            _attachment(2, "channel.pdf"),
        ]
        result = DiscordAttachmentIngestResult(
            images=(
                _turn_image(
                    data_root,
                    attachment_id="channel-image",
                    ordinal=1,
                ),
            ),
            files=(
                _turn_file(
                    data_root,
                    attachment_id="channel-json",
                    ordinal=0,
                    display_name="channel.json",
                ),
                _turn_file(
                    data_root,
                    attachment_id="channel-pdf",
                    ordinal=2,
                    display_name="channel.pdf",
                ),
            ),
        )
        expected_title = "图片任务"
        expected_text = None
        expected_kinds = [(0, "file"), (1, "image"), (2, "file")]
    else:
        content = "<@999>"
        attachments = [
            _attachment(0, "actually-an-image.txt", content_type="text/plain")
        ]
        result = DiscordAttachmentIngestResult(
            images=(
                _turn_image(
                    data_root,
                    attachment_id="channel-misleading-image",
                    ordinal=0,
                ),
            )
        )
        expected_title = "新任务"
        expected_text = None
        expected_kinds = [(0, "image")]
    message = _message(
        message_id=911,
        channel=parent,
        content=content,
        attachments=attachments,
        bot_user=bot_user,
        mentioned=True,
    )
    parent.fetch_message = AsyncMock(return_value=message)
    ingestor = Mock(
        ingest=AsyncMock(return_value=result),
        cleanup=Mock(side_effect=DiscordAttachmentIngestor.cleanup),
    )
    bot._attachment_ingestor = ingestor
    bot.turns.enqueue = _repository_enqueue(storage_context)  # type: ignore[method-assign]

    await bot._handle_message(message)
    ingestor.ingest.assert_not_awaited()
    ingress = storage_context.repository.get_ingress_message("911")
    outbox_row = storage_context.store.query_one(
        """
        SELECT o.payload_json
        FROM ingress_messages AS i
        JOIN discord_outbox AS o ON o.id = i.thread_creation_outbox_id
        WHERE i.discord_message_id = '911'
        """
    )
    assert outbox_row is not None
    original_payload_json = str(outbox_row["payload_json"])
    creation_payload = json.loads(original_payload_json)
    assert creation_payload["name_strategy"] == "starter_message"
    assert creation_payload["name_suffix"] == ingress.id[:4]
    assert expected_title not in original_payload_json

    await bot._handle_message(message)

    duplicate_rows = storage_context.store.query_all(
        "SELECT payload_json FROM discord_outbox WHERE dedupe_key = 'thread-create:911'"
    )
    assert [str(row["payload_json"]) for row in duplicate_rows] == [
        original_payload_json
    ]
    storage_context.repository.finalize_thread_creation(
        discord_message_id="911",
        discord_thread_id=911,
        owner_user_id=400,
    )
    monkeypatch.setattr(
        bot,
        "get_channel",
        lambda channel_id: parent if channel_id == 200 else thread,
    )

    await bot._process_initial_ingress("911")
    await bot._process_initial_ingress("911")
    await bot._handle_message(message)

    ingress = storage_context.repository.get_ingress_message("911")
    turn_input = storage_context.repository.load_turn_input(
        cast(str, ingress.turn_id),
        volatile_text=expected_text,
        use_volatile_text=True,
    )
    assert ingress.state == "ready"
    assert turn_input.text == expected_text
    assert sorted(
        [(image.ordinal, "image") for image in turn_input.images]
        + [(file.ordinal, "file") for file in turn_input.files]
    ) == expected_kinds
    ingestor.ingest.assert_awaited_once_with(attachments)
    ingestor.cleanup.assert_not_called()
    bot.turns.enqueue.assert_awaited_once()
    parent.fetch_message.assert_awaited_once_with(911)
    turn_count = storage_context.store.query_one(
        "SELECT COUNT(*) AS count FROM turns WHERE input_message_id = '911'"
    )
    assert turn_count is not None
    assert turn_count["count"] == 1


@pytest.mark.asyncio
async def test_initial_ingress_manifest_change_rejects_before_download(
    storage_context: StorageContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot, bot_user = _bot(storage_context, tmp_path)
    parent = Mock(spec=discord.TextChannel)
    parent.id = 200
    parent.send = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.id = 912
    thread.parent_id = 200
    attachment = _attachment(0, "original.txt")
    message = _message(
        message_id=912,
        channel=parent,
        content="<@999>",
        attachments=[attachment],
        bot_user=bot_user,
        mentioned=True,
    )
    parent.fetch_message = AsyncMock(return_value=message)
    ingestor = Mock(ingest=AsyncMock(), cleanup=Mock())
    bot._attachment_ingestor = ingestor
    bot.turns.enqueue = AsyncMock()  # type: ignore[method-assign]

    await bot._handle_message(message)
    storage_context.repository.finalize_thread_creation(
        discord_message_id="912",
        discord_thread_id=912,
        owner_user_id=400,
    )
    attachment.filename = "changed.txt"
    monkeypatch.setattr(
        bot,
        "get_channel",
        lambda channel_id: parent if channel_id == 200 else thread,
    )

    await bot._process_initial_ingress("912")

    ingress = storage_context.repository.get_ingress_message("912")
    assert ingress.state == "rejected"
    assert ingress.error_code == "conflict"
    ingestor.ingest.assert_not_awaited()
    bot.turns.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_repository_attachment_integrity_failure_rolls_back_and_cleans(
    storage_context: StorageContext,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    bot, bot_user = _bot(storage_context, tmp_path)
    channel = Mock(spec=discord.Thread)
    channel.id = 300
    channel.parent_id = 200
    channel.send = AsyncMock()
    secret_content = b"private attachment payload"
    file = _turn_file(
        storage_context.store.path.parent,
        attachment_id="integrity-cleanup",
        ordinal=0,
        display_name="safe.txt",
        content=secret_content,
    )
    result = DiscordAttachmentIngestResult(files=(file,))

    async def ingest(
        _attachments: list[discord.Attachment],
    ) -> DiscordAttachmentIngestResult:
        file.canonical_path.write_bytes(b"x" * len(secret_content))
        return result

    ingestor = Mock(
        ingest=AsyncMock(side_effect=ingest),
        cleanup=Mock(side_effect=DiscordAttachmentIngestor.cleanup),
    )
    bot._attachment_ingestor = ingestor
    bot.turns.enqueue = _repository_enqueue(storage_context)  # type: ignore[method-assign]
    attachment = _attachment(0, "safe.txt")
    message = _message(
        message_id=913,
        channel=channel,
        content="inspect",
        attachments=[attachment],
        bot_user=bot_user,
        mentioned=False,
    )

    with caplog.at_level(logging.ERROR):
        await bot._handle_message(message)

    ingress = storage_context.repository.get_ingress_message("913")
    response = channel.send.await_args.args[0]
    forbidden = (
        str(file.canonical_path),
        attachment.url,
        secret_content.decode(),
    )
    assert ingress.state == "rejected"
    assert ingress.error_code == "attachment_integrity_failed"
    assert "`attachment_integrity_failed`" in response
    assert not any(value in response or value in caplog.text for value in forbidden)
    assert not file.canonical_path.exists()
    ingestor.cleanup.assert_called_once_with(result)
    assert (
        storage_context.store.query_one("SELECT 1 FROM turns WHERE input_message_id = '913'")
        is None
    )


@pytest.mark.asyncio
async def test_post_commit_wake_failure_keeps_durably_owned_artifact(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot, bot_user = _bot(storage_context, tmp_path)
    channel = Mock(spec=discord.Thread)
    channel.id = 300
    channel.parent_id = 200
    channel.send = AsyncMock()
    file = _turn_file(
        storage_context.store.path.parent,
        attachment_id="durably-owned",
        ordinal=0,
        display_name="owned.bin",
    )
    result = DiscordAttachmentIngestResult(files=(file,))
    ingestor = Mock(
        ingest=AsyncMock(return_value=result),
        cleanup=Mock(side_effect=DiscordAttachmentIngestor.cleanup),
    )
    bot._attachment_ingestor = ingestor

    async def enqueue_then_fail(**kwargs: Any) -> None:
        storage_context.repository.enqueue_turn(**kwargs)
        raise RuntimeError("mailbox wake failed")

    bot.turns.enqueue = AsyncMock(  # type: ignore[method-assign]
        side_effect=enqueue_then_fail
    )
    message = _message(
        message_id=914,
        channel=channel,
        content="use the file",
        attachments=[_attachment(0, "owned.bin")],
        bot_user=bot_user,
        mentioned=False,
    )

    await bot._handle_message(message)

    ingress = storage_context.repository.get_ingress_message("914")
    assert ingress.state == "ready"
    assert ingress.turn_id is not None
    assert file.canonical_path.exists()
    ingestor.cleanup.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "message"),
    (
        ("too_many_attachments", "too many attachments"),
        ("attachment_size_limit", "attachment exceeds the byte limit"),
        ("attachment_total_size_limit", "attachments exceed the total byte limit"),
        ("attachment_download_failed", "attachment download failed"),
        ("attachment_download_timeout", "attachment download timed out"),
        ("attachment_integrity_failed", "attachment integrity check failed"),
        ("file_input_unsupported", "attachment storage is unavailable"),
        ("image_decode_failed", "image attachment could not be decoded"),
    ),
)
async def test_attachment_errors_keep_stable_public_codes_and_safe_wording(
    storage_context: StorageContext,
    tmp_path: Path,
    code: str,
    message: str,
) -> None:
    bot, bot_user = _bot(storage_context, tmp_path)
    channel = Mock(spec=discord.Thread)
    channel.id = 300
    channel.parent_id = 200
    channel.send = AsyncMock()
    attachment = _attachment(0, "private.bin")
    ingestor = Mock(
        ingest=AsyncMock(side_effect=AttachmentError(message, code=code)),
        cleanup=Mock(),
    )
    bot._attachment_ingestor = ingestor
    bot.turns.enqueue = AsyncMock()  # type: ignore[method-assign]
    message_object = _message(
        message_id=915,
        channel=channel,
        content="inspect",
        attachments=[attachment],
        bot_user=bot_user,
        mentioned=False,
    )

    await bot._handle_message(message_object)

    response = channel.send.await_args.args[0]
    ingress = storage_context.repository.get_ingress_message("915")
    assert ingress.state == "rejected"
    assert ingress.error_code == code
    assert f"`{code}`" in response
    assert "attachment" in response or code == "image_decode_failed"
    assert attachment.url not in response
    bot.turns.enqueue.assert_not_awaited()
