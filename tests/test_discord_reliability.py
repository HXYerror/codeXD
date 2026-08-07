from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import discord
import pytest
from conftest import StorageContext
from discord import app_commands

from codexd.application.session_coordinator import ResolvedProject, SessionCoordinator
from codexd.config import SecurityConfig, load_config
from codexd.domain.ids import canonical_json, sha256_text
from codexd.domain.turns import TurnImage, TurnInput, TurnSource
from codexd.errors import InvariantError
from codexd.paths import AppPaths
from codexd.rendering.discord import (
    AttachmentKind,
    DurableDiscordRenderPlan,
    DurableRenderedAttachment,
)
from codexd.rendering.media_worker import NormalizedImage
from codexd.runtime.codex_sdk import capability_manifest
from codexd.security.signing import ComponentSigner
from codexd.storage.records import (
    CommandIntentRecord,
    OutboxRecord,
    RenderPlanRecord,
)
from codexd.transport.discord.attachments import (
    AttachmentError,
    DiscordImageIngestor,
)
from codexd.transport.discord.bot import CodexDBot, _remove_bot_mention
from codexd.transport.discord.outbox import (
    DeliveryError,
    DiscordOutboxTransport,
    OutboxWorker,
    _message_has_delivery_marker,
)
from codexd.transport.discord.presentation import TABLE_COPY_CUSTOM_ID, task_card_embed


@pytest.mark.asyncio
async def test_partial_image_ingestion_removes_completed_files(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "first.png"
    canonical.write_bytes(b"normalized")
    image = TurnImage(
        attachment_id="first",
        ordinal=0,
        canonical_path=canonical,
        media_type="image/png",
        source_sha256="source",
        sha256="normalized",
        size_bytes=10,
        width=1,
        height=1,
        source_name_sanitized="first.png",
        retention_until=1,
    )
    ingestor = DiscordImageIngestor(
        session=Mock(),
        media_worker=Mock(),
        attachments_dir=tmp_path,
        max_bytes=1024,
        max_pixels=1024,
        retention_days=1,
    )
    ingestor._ingest_one = AsyncMock(  # type: ignore[method-assign]
        side_effect=(image, AttachmentError("bad second image", code="bad_image"))
    )

    with pytest.raises(AttachmentError, match="bad second image"):
        await ingestor.ingest([Mock(), Mock()])

    assert not canonical.exists()


@pytest.mark.asyncio
async def test_image_ingestor_decodes_webp_despite_png_filename(
    tmp_path: Path,
) -> None:
    worker = Mock()

    async def normalize(
        *,
        source: Path,
        output: Path,
        max_bytes: int,
        max_pixels: int,
    ) -> NormalizedImage:
        assert await asyncio.to_thread(source.read_bytes) == b"webp"
        assert (max_bytes, max_pixels) == (1024, 1024)
        await asyncio.to_thread(output.write_bytes, b"normalized-png")
        return NormalizedImage(
            output_path=output,
            media_type="image/png",
            source_sha256="source",
            normalized_sha256="normalized",
            size_bytes=14,
            width=10,
            height=8,
        )

    worker.normalize_image = AsyncMock(side_effect=normalize)
    ingestor = DiscordImageIngestor(
        session=Mock(),
        media_worker=worker,
        attachments_dir=tmp_path,
        max_bytes=1024,
        max_pixels=1024,
        retention_days=1,
    )

    async def download(_url: str, destination: Path) -> None:
        await asyncio.to_thread(destination.write_bytes, b"webp")

    ingestor._download = AsyncMock(side_effect=download)  # type: ignore[method-assign]
    attachment = cast(
        discord.Attachment,
        SimpleNamespace(
            size=4,
            content_type="image/webp",
            filename="image.png",
            url="https://cdn.discordapp.com/attachments/image.png",
        ),
    )

    (image,) = await ingestor.ingest([attachment])

    assert image.media_type == "image/png"
    assert image.source_name_sanitized == "image.png"
    assert await asyncio.to_thread(image.canonical_path.read_bytes) == b"normalized-png"
    ingestor.cleanup([image])


@pytest.mark.asyncio
async def test_thread_rename_outbox_edits_in_place() -> None:
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.edit = AsyncMock()
    client = Mock(spec=discord.Client)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=Mock(),
        renderer=Mock(),
        signer=Mock(),
    )
    record = OutboxRecord(
        id="rename",
        destination_key="thread:300",
        operation="edit",
        payload_json='{"kind":"thread_rename","name":"New name"}',
        delivery_marker="rename-marker",
        state="pending",
        attempts=0,
        lease_owner="test",
    )

    result = await transport.deliver(record)

    thread.edit.assert_awaited_once_with(
        name="New name", reason="codexD session rename"
    )
    assert result.discord_message_id is None


@pytest.mark.asyncio
async def test_final_outbox_delivers_all_attachment_batches(tmp_path: Path) -> None:
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    sent = [Mock(id=index) for index in range(1, 6)]
    thread.send = AsyncMock(side_effect=sent)
    client = Mock(spec=discord.Client)
    client.get_channel.return_value = thread
    attachments: list[DurableRenderedAttachment] = []
    for index in range(25):
        content = str(index).encode()
        path = tmp_path / f"table-{index}.txt"
        path.write_bytes(content)
        attachments.append(
            DurableRenderedAttachment(
                filename=path.name,
                path=path,
                description=f"attachment {index}",
                sha256=hashlib.sha256(content).hexdigest(),
                size_bytes=len(content),
            )
        )
    plan = DurableDiscordRenderPlan(("Final response",), tuple(attachments))
    renderer = Mock()
    renderer.artifact_root = tmp_path
    renderer.retention_days = 30
    renderer.create_durable_plan = AsyncMock(return_value=plan)
    renderer.load_durable_plan.return_value = plan
    repository = Mock()
    repository.render_plan.return_value = None
    repository.persist_render_plan.return_value = RenderPlanRecord(
        turn_id="turn-final",
        source_sha256=sha256_text("ignored"),
        plan_json=canonical_json(plan.to_payload(tmp_path)),
        retention_until=1,
    )
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=renderer,
        signer=Mock(),
    )
    record = OutboxRecord(
        id="final",
        destination_key="thread:300",
        operation="send",
        payload_json=canonical_json(
            {
                "kind": "turn_final",
                "plain_text": "ignored",
                "turn_id": "turn-final",
                "input_message_id": "777",
                "input_channel_id": "300",
                "discord_guild_id": "100",
            }
        ),
        delivery_marker="final-marker",
        state="pending",
        attempts=0,
        lease_owner="test",
    )

    result = await transport.deliver(record)

    assert result.discord_message_id == "1"
    assert "files" not in thread.send.await_args_list[0].kwargs
    reference = thread.send.await_args_list[0].kwargs["reference"]
    assert (reference.message_id, reference.channel_id, reference.guild_id) == (
        777,
        300,
        100,
    )
    assert thread.send.await_args_list[0].kwargs["mention_author"] is False
    assert [len(call.kwargs["files"]) for call in thread.send.await_args_list[1:4]] == [
        10,
        10,
        5,
    ]
    assert _message_has_delivery_marker(
        thread.send.await_args_list[3].args[0],
        "final-marker-a2",
    )
    assert "codexD:" not in thread.send.await_args_list[3].args[0]
    assert thread.send.await_args_list[4].args[0].startswith(
        "-# 💡 finished | `unknown`"
    )
    assert "embed" not in thread.send.await_args_list[4].kwargs
    for call in thread.send.await_args_list[1:4]:
        for file in call.kwargs.get("files", []):
            file.close()


@pytest.mark.asyncio
async def test_final_outbox_renders_table_embed_with_markdown_copy(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "table-result.md"
    source_content = b"| name | value |\n|---|---|\n| alpha | 1 |\n"
    source_path.write_bytes(source_content)
    image_path = tmp_path / "table-result-1.png"
    image_content = b"\x89PNG\r\n\x1a\nimage"
    image_path.write_bytes(image_content)
    source = DurableRenderedAttachment(
        filename=source_path.name,
        path=source_path,
        description="Markdown source for Table with 1 rows and 2 columns.",
        sha256=hashlib.sha256(source_content).hexdigest(),
        size_bytes=len(source_content),
        kind=AttachmentKind.TABLE_SOURCE,
        group_id="table-result",
    )
    image = DurableRenderedAttachment(
        filename=image_path.name,
        path=image_path,
        description="Table with 1 rows and 2 columns. Page 1/1",
        sha256=hashlib.sha256(image_content).hexdigest(),
        size_bytes=len(image_content),
        kind=AttachmentKind.TABLE_IMAGE,
        group_id="table-result",
    )
    plan = DurableDiscordRenderPlan((), (source, image))
    renderer = Mock(
        artifact_root=tmp_path,
        retention_days=30,
        create_durable_plan=AsyncMock(return_value=plan),
    )
    renderer.load_durable_plan.return_value = plan
    repository = Mock()
    repository.render_plan.return_value = None
    repository.persist_render_plan.return_value = RenderPlanRecord(
        turn_id="turn-final",
        source_sha256=sha256_text("ignored"),
        plan_json=canonical_json(plan.to_payload(tmp_path)),
        retention_until=1,
    )
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(side_effect=[Mock(id=1), Mock(id=2)])
    client = Mock(spec=discord.Client)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=renderer,
        signer=Mock(),
    )

    result = await transport.deliver(
        OutboxRecord(
            id="table-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "plain_text": "ignored",
                    "turn_id": "turn-final",
                    "state": "completed",
                    "terminal_code": "provider_completed",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "high",
                    "sandbox": "full_access",
                    "approval_mode": "auto_review",
                    "started_at": 1_000,
                    "ended_at": 3_500,
                    "usage": {
                        "last": {
                            "input_tokens": 100,
                            "output_tokens": 20,
                            "cached_input_tokens": 10,
                            "reasoning_output_tokens": 5,
                            "total_tokens": 120,
                        },
                        "total": {
                            "input_tokens": 1_000,
                            "output_tokens": 200,
                            "cached_input_tokens": 100,
                            "reasoning_output_tokens": 50,
                            "total_tokens": 1_200,
                        },
                        "model_context_window": 1_000_000,
                    },
                }
            ),
            delivery_marker="table-final",
            state="pending",
            attempts=0,
            lease_owner="test",
        )
    )

    assert result.discord_message_id == "1"
    table_call = thread.send.await_args_list[0]
    assert table_call.kwargs["embed"].title == "📊 Codex table"
    assert table_call.kwargs["embed"].image.url == "attachment://table-result-1.png"
    assert [file.filename for file in table_call.kwargs["files"]] == [
        "table-result-1.png",
        "table-result.md",
    ]
    assert table_call.kwargs["view"].children[0].custom_id == TABLE_COPY_CUSTOM_ID
    assert not any(
        file.filename.endswith(".csv") for file in table_call.kwargs["files"]
    )
    footer = thread.send.await_args_list[1]
    assert footer.args[0].startswith(
        "-# ✅ | 📥 100 | 📤 20 | ⏱️ 2.5s | 🧠 <0.1%\n"
        "-# ⚡ full\\_access · auto\\_review"
    )
    assert "embed" not in footer.kwargs


@pytest.mark.asyncio
async def test_task_card_create_and_edit_survive_transport_restart() -> None:
    key = b"task-card-test-key".ljust(32, b"-")
    signer = ComponentSigner(key)
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False

    async def history(*, limit: int):
        assert limit == 500
        if False:
            yield Mock()

    thread.history = history
    thread.send = AsyncMock(return_value=Mock(id=501))
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    repository = Mock()
    create_payload = canonical_json(
        {
            "kind": "task_card",
            "view_id": "view-test",
            "revision": 1,
            "expanded": False,
            "nonce": "nonce-test",
            "title": "Task",
            "state": "running",
        }
    )
    created = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=signer,
    ).deliver(
        OutboxRecord(
            id="task-card-create",
            destination_key="thread:300",
            operation="send",
            payload_json=create_payload,
            delivery_marker="task-card-create",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert created.discord_message_id == "501"
    assert created.task_card_view_id == "view-test"
    assert thread.send.await_args.kwargs["embed"].title == "⚙️ Task"
    create_view = thread.send.await_args.kwargs["view"]
    create_action = signer.verify_task_card_id(
        create_view.children[0].custom_id
    )
    assert (create_action.view_id, create_action.revision, create_action.action) == (
        "view-test",
        1,
        "expand",
    )

    existing = Mock(spec=discord.Message)
    existing.id = 501
    existing.edit = AsyncMock()
    thread.fetch_message = AsyncMock(return_value=existing)
    repository.task_card_message.return_value = "501"
    restarted_signer = ComponentSigner(key)
    edited = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=restarted_signer,
    ).deliver(
        OutboxRecord(
            id="task-card-edit",
            destination_key="thread:300",
            operation="edit",
            payload_json=canonical_json(
                {
                    "kind": "task_card",
                    "view_id": "view-test",
                    "revision": 2,
                    "expanded": True,
                    "nonce": "nonce-next",
                    "title": "Task",
                    "state": "completed",
                }
            ),
            delivery_marker="task-card-edit",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert edited.discord_message_id == "501"
    assert existing.edit.await_args.kwargs["embed"].title == "✅ Task"
    edit_view = existing.edit.await_args.kwargs["view"]
    edit_action = restarted_signer.verify_task_card_id(
        edit_view.children[0].custom_id
    )
    assert (edit_action.revision, edit_action.action, edit_action.nonce) == (
        2,
        "collapse",
        "nonce-next",
    )


def test_expanded_subagent_card_shows_work_without_activity_noise() -> None:
    embed = task_card_embed(
        {
            "title": "Codex subagent · agent-1",
            "state": "running",
            "status_summary": "started · reviewer · Review storage recovery",
            "operation": "activity",
            "agents": [
                {
                    "label": "agent-1",
                    "state": "running",
                    "message": "reviewer · Review storage recovery",
                }
            ],
        },
        expanded=True,
    )

    assert embed.description == "started · reviewer · Review storage recovery"
    assert [field.name for field in embed.fields] == ["Agents"]
    assert "Review storage recovery" in embed.fields[0].value


@pytest.mark.asyncio
async def test_prompt_reaction_converges_to_terminal_state_idempotently() -> None:
    waiting = Mock(emoji="⏳", me=True)
    user_check = Mock(emoji="✅", me=False)
    message = Mock(spec=discord.Message)
    message.id = 501
    message.reactions = [waiting, user_check]
    message.remove_reaction = AsyncMock()
    message.add_reaction = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(return_value=message)
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    payload = canonical_json(
        {
            "kind": "prompt_reaction",
            "turn_id": "turn",
            "message_id": "501",
            "state": "completed",
        }
    )

    result = await DiscordOutboxTransport(
        client=client,
        repository=Mock(),
        renderer=Mock(),
        signer=ComponentSigner(b"reaction-test".ljust(32, b"-")),
    ).deliver(
        OutboxRecord(
            id="prompt-reaction",
            destination_key="thread:300",
            operation="edit",
            payload_json=payload,
            delivery_marker="prompt-reaction",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert result.discord_message_id == "501"
    message.remove_reaction.assert_awaited_once_with("⏳", client.user)
    message.add_reaction.assert_awaited_once_with("✅")

    terminal_message = Mock(spec=discord.Message)
    terminal_message.id = 501
    terminal_message.reactions = [Mock(emoji="✅", me=True)]
    terminal_message.remove_reaction = AsyncMock()
    terminal_message.add_reaction = AsyncMock()
    thread.fetch_message.return_value = terminal_message
    await DiscordOutboxTransport(
        client=client,
        repository=Mock(),
        renderer=Mock(),
        signer=ComponentSigner(b"reaction-test".ljust(32, b"-")),
    ).deliver(
        OutboxRecord(
            id="prompt-reaction-retry",
            destination_key="thread:300",
            operation="edit",
            payload_json=payload,
            delivery_marker="prompt-reaction",
            state="reconciling",
            attempts=1,
            lease_owner="worker",
        )
    )
    terminal_message.remove_reaction.assert_not_awaited()
    terminal_message.add_reaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_deleted_task_card_is_replaced_without_pending_history_scan() -> None:
    key = b"task-card-delete-key".ljust(32, b"-")
    signer = ComponentSigner(key)
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(
        side_effect=discord.NotFound(
            Mock(status=404, reason="deleted"),
            "deleted",
        )
    )

    async def history(*, limit: int):
        raise AssertionError(f"pending replacement scanned {limit} messages")
        yield Mock()

    thread.history = history
    thread.send = AsyncMock(return_value=Mock(id=777))
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    repository = Mock()
    repository.task_card_message.return_value = "501"

    result = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=signer,
    ).deliver(
        OutboxRecord(
            id="task-card-replacement",
            destination_key="thread:300",
            operation="edit",
            payload_json=canonical_json(
                {
                    "kind": "task_card",
                    "view_id": "view-test",
                    "revision": 3,
                    "expanded": False,
                    "nonce": "nonce-replacement",
                    "title": "Task",
                    "state": "completed",
                }
            ),
            delivery_marker="task-card-replacement",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert result.discord_message_id == "777"
    assert result.task_card_view_id == "view-test"
    assert thread.send.await_args.kwargs["embed"].title == "✅ Task"


@pytest.mark.asyncio
async def test_turn_progress_uses_one_editable_rich_embed() -> None:
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False

    async def history(*, limit: int):
        assert limit == 500
        if False:
            yield Mock()

    thread.history = history
    thread.send = AsyncMock(return_value=Mock(id=601))
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    repository = Mock()
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    )
    created = await transport.deliver(
        OutboxRecord(
            id="progress-create",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_progress",
                    "turn_id": "turn-rich",
                    "content": "Running · command: `pytest -q`",
                    "plain_text": "Ordinary assistant text",
                }
            ),
            delivery_marker="progress-create",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert created.discord_message_id == "601"
    assert created.turn_progress_id == "turn-rich"
    assert thread.send.await_args.args[0].startswith("Ordinary assistant text")
    assert _message_has_delivery_marker(
        thread.send.await_args.args[0],
        "progress-create",
    )
    assert "codexD:" not in thread.send.await_args.args[0]
    assert thread.send.await_args.kwargs["embed"].title == "⚙️ Codex is working"
    assert thread.send.await_args.kwargs["embed"].description == "command: `pytest -q`"
    assert (
        "Ordinary assistant text"
        not in thread.send.await_args.kwargs["embed"].description
    )
    existing = Mock(spec=discord.Message)
    existing.id = 601
    existing.edit = AsyncMock()
    thread.fetch_message = AsyncMock(return_value=existing)
    repository.turn_progress_message.return_value = "601"

    edited = await transport.deliver(
        OutboxRecord(
            id="progress-edit",
            destination_key="thread:300",
            operation="edit",
            payload_json=canonical_json(
                {
                    "kind": "turn_progress",
                    "turn_id": "turn-rich",
                    "content": "Completed · `provider_completed`",
                }
            ),
            delivery_marker="progress-edit",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert edited.discord_message_id == "601"
    assert existing.edit.await_args.kwargs["embed"].title == "✅ Turn completed"
    assert existing.edit.await_args.kwargs["suppress"] is False
    assert "Completed" not in existing.edit.await_args.kwargs["content"]
    assert _message_has_delivery_marker(
        existing.edit.await_args.kwargs["content"],
        "progress-edit",
    )
    assert "Ordinary assistant text" not in existing.edit.await_args.kwargs["content"]


@pytest.mark.asyncio
async def test_table_copy_component_returns_markdown_ephemerally(
    tmp_path: Path,
) -> None:
    bot = _test_bot(tmp_path, repository=Mock())
    attachment = Mock(spec=discord.Attachment)
    attachment.filename = "table-result.md"
    attachment.read = AsyncMock(
        return_value=b"| name | value |\n|---|---|\n| alpha | 1 |\n"
    )
    interaction = Mock(spec=discord.Interaction)
    interaction.type = discord.InteractionType.component
    interaction.data = {"custom_id": TABLE_COPY_CUSTOM_ID}
    interaction.guild_id = 100
    interaction.user = Mock(id=400)
    interaction.message = Mock(attachments=[attachment])
    interaction.response = Mock(
        send_message=AsyncMock(),
        is_done=Mock(return_value=False),
    )

    await bot.on_interaction(interaction)

    attachment.read.assert_awaited_once()
    sent = interaction.response.send_message.await_args
    assert sent.args[0].startswith("```markdown\n| name | value |")
    assert sent.kwargs["ephemeral"] is True
    allowed_mentions = sent.kwargs["allowed_mentions"]
    assert allowed_mentions.everyone is False
    assert allowed_mentions.users is False
    assert allowed_mentions.roles is False


@pytest.mark.asyncio
async def test_signed_task_card_component_updates_matching_view(
    tmp_path: Path,
) -> None:
    repository = Mock()
    repository.accept_command_intent.return_value = CommandIntentRecord(
        interaction_id="700",
        command_name="task card expand",
        request_hash="hash",
        project_id=None,
        conversation_id=None,
        turn_id=None,
        state="accepted",
        result_json=None,
        effect_kind=None,
        effect_correlation_id=None,
        accepted_boot_id="test",
    )
    bot = _test_bot(tmp_path, repository=repository)
    bot.signer = ComponentSigner(b"component-test-key".ljust(32, b"-"))
    bot.sessions.conversation_for_thread = AsyncMock(return_value=None)
    bot.sessions.resolve_project_for_channel = AsyncMock(
        return_value=SimpleNamespace(project=SimpleNamespace(id="component-project"))
    )
    custom_id = bot.signer.task_card_id(
        view_id="view-test",
        revision=3,
        action="expand",
        nonce="nonce-test",
    )
    interaction = Mock(spec=discord.Interaction)
    interaction.id = 700
    interaction.type = discord.InteractionType.component
    interaction.data = {"custom_id": custom_id}
    interaction.guild_id = 100
    interaction.channel_id = 300
    interaction.user = Mock(id=400)
    interaction.message = Mock(id=501)
    interaction.response = Mock(
        defer=AsyncMock(),
        is_done=Mock(return_value=False),
        send_message=AsyncMock(),
    )
    interaction.followup = Mock(send=AsyncMock())

    await bot.on_interaction(interaction)

    repository.update_task_card_display.assert_called_once_with(
        view_id="view-test",
        expected_revision=3,
        action="expand",
        component_nonce="nonce-test",
        interaction_id="700",
        owner_user_id=400,
        guild_id=100,
        channel_id=300,
        message_id=501,
    )
    repository.complete_command_intent.assert_called_once()
    interaction.response.defer.assert_awaited_once()


@pytest.mark.asyncio
async def test_schedule_modal_submit_survives_bot_restart(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    signer_key = b"restart-safe-modal-key".ljust(32, b"-")
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.parent_id = 200
    launch = Mock(spec=discord.Interaction)
    launch.id = 710
    launch.guild_id = 100
    launch.channel_id = 300
    launch.channel = thread
    launch.user = Mock(id=400)
    launch.response = Mock(send_modal=AsyncMock(), is_done=Mock(return_value=False))
    first = _test_bot(tmp_path, repository=storage_context.repository)
    first.signer = ComponentSigner(signer_key)
    first.sessions.conversation_for_thread = AsyncMock(
        return_value=storage_context.conversation
    )

    await first._schedule_create(launch)

    modal = launch.response.send_modal.await_args.args[0]
    custom_id = modal.custom_id
    restarted = _test_bot(tmp_path, repository=storage_context.repository)
    restarted.signer = ComponentSigner(signer_key)
    restarted.sessions.conversation_for_thread = AsyncMock(
        return_value=storage_context.conversation
    )
    restarted._apply_schedule_modal = AsyncMock()  # type: ignore[method-assign]
    submit = Mock(spec=discord.Interaction)
    submit.id = 711
    submit.type = discord.InteractionType.modal_submit
    submit.guild_id = 100
    submit.channel_id = 300
    submit.channel = thread
    submit.user = Mock(id=400)
    submit.data = {
        "custom_id": custom_id,
        "components": [
            {"components": [{"custom_id": "schedule_name", "value": "daily"}]},
            {
                "components": [
                    {"custom_id": "schedule_when", "value": "0 9 * * *"}
                ]
            },
            {
                "components": [
                    {"custom_id": "schedule_timezone", "value": "Asia/Shanghai"}
                ]
            },
            {
                "components": [
                    {"custom_id": "schedule_misfire", "value": "latest"}
                ]
            },
            {
                "components": [
                    {"custom_id": "schedule_prompt", "value": "summarize status"}
                ]
            },
        ],
    }
    submit.response = _interaction_response()
    submit.followup = Mock(send=AsyncMock())

    await restarted.on_interaction(submit)

    restarted._apply_schedule_modal.assert_awaited_once()
    submission = restarted._apply_schedule_modal.await_args.kwargs[
        "modal_submission"
    ]
    assert (submission.intent_id, submission.interaction_id) == (
        restarted.signer.verify_modal_id(custom_id).intent_id,
        "711",
    )
    action = restarted.signer.verify_modal_id(custom_id)
    row = storage_context.store.connection.execute(
        "SELECT state, consumed_interaction_id FROM modal_intents WHERE id = ?",
        (action.intent_id,),
    ).fetchone()
    assert row is not None
    assert (row["state"], row["consumed_interaction_id"]) == ("open", None)


def test_discord_command_schema_exposes_codex_native_controls(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            (
                "[discord]",
                "guild_id = 100",
                "owner_user_id = 400",
                "allowed_user_ids = [400]",
                "[security]",
                f'allowed_roots = ["{tmp_path}"]',
            )
        )
    )
    config = load_config(
        config_path,
        environment={
            "HOME": str(tmp_path),
            "CODEXD_DATA_DIR": str(tmp_path / "data"),
        },
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
        capability_manifest=capability_manifest(),
        boot_id="test",
    )

    bot._register_commands()

    commands = {
        command.name: command
        for command in bot.tree.get_commands(guild=discord.Object(id=100))
    }
    assert "permissions" not in commands
    assert {command.name for command in commands["project"].commands} == {
        "bind",
        "info",
        "unbind",
    }
    assert {"rename", "compact", "fork"} <= {
        command.name for command in commands["session"].commands
    }
    assert "archive" not in {
        command.name for command in commands["session"].commands
    }
    model_tier = next(
        command for command in commands["model"].commands if command.name == "tier"
    )
    assert {command.name for command in model_tier.commands} == {
        "show",
        "set",
        "default",
    }
    reasoning_summary = next(
        command
        for command in commands["reasoning"].commands
        if command.name == "summary"
    )
    assert {command.name for command in reasoning_summary.commands} == {
        "show",
        "set",
        "default",
    }


@pytest.mark.asyncio
async def test_message_ingress_acl_matrix_ignores_untrusted_sources(
    tmp_path: Path,
) -> None:
    repository = Mock()
    bot = _test_bot(tmp_path, repository=repository)
    bot.sessions.resolve_project_for_channel = AsyncMock()
    bot.sessions.conversation_for_thread = AsyncMock(return_value=None)
    bot.turns.enqueue = AsyncMock()
    bot_user = Mock(id=999, bot=True)
    bot._connection.user = bot_user

    def text_message(
        message_id: int,
        *,
        author_id: int = 400,
        author_is_bot: bool = False,
        webhook_id: int | None = None,
        guild_id: int | None = 100,
        mentioned: bool = True,
    ) -> discord.Message:
        channel = Mock(spec=discord.TextChannel)
        channel.id = 200 + message_id
        channel.send = AsyncMock()
        message = Mock(spec=discord.Message)
        message.id = message_id
        message.author = Mock(id=author_id, bot=author_is_bot)
        message.webhook_id = webhook_id
        message.guild = Mock(id=guild_id) if guild_id is not None else None
        message.channel = channel
        message.content = "<@999> ignored"
        message.mentions = [bot_user] if mentioned else []
        message.attachments = []
        return cast(discord.Message, message)

    ignored = (
        text_message(1, author_is_bot=True),
        text_message(2, webhook_id=42),
        text_message(3, guild_id=None),
        text_message(4, author_id=401),
        text_message(5, guild_id=101),
        text_message(6, mentioned=False),
    )
    for message in ignored:
        await bot._handle_message(message)

    thread = Mock(spec=discord.Thread)
    thread.id = 307
    thread.parent_id = 200
    thread.send = AsyncMock()
    unknown_thread_message = text_message(7, mentioned=False)
    unknown_thread_message.channel = thread
    await bot._handle_message(unknown_thread_message)

    bot.sessions.resolve_project_for_channel.assert_not_awaited()
    bot.sessions.conversation_for_thread.assert_awaited_once_with(307)
    bot.turns.enqueue.assert_not_awaited()
    repository.request_thread_creation.assert_not_called()
    for message in ignored:
        message.channel.send.assert_not_awaited()
    thread.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbound_mentions_resolve_to_home_without_implicit_binding(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    sessions = SessionCoordinator(
        repository=storage_context.repository,
        security=SecurityConfig(allowed_roots=(tmp_path,)),
        home_path=home,
    )
    bot = _test_bot(tmp_path, repository=storage_context.repository)
    bot.sessions = sessions
    bot_user = Mock(id=999, bot=True)
    bot._connection.user = bot_user
    channel = Mock(spec=discord.TextChannel)
    channel.id = 201
    channel.send = AsyncMock()
    author = Mock(id=400, bot=False)
    guild = Mock(id=100)
    message = Mock(spec=discord.Message)
    message.id = 901
    message.author = author
    message.webhook_id = None
    message.guild = guild
    message.channel = channel
    message.content = "<@999> inspect HOME"
    message.mentions = [bot_user]
    message.attachments = []

    await bot._handle_message(message)

    ingress = storage_context.repository.get_ingress_message("901")
    project = storage_context.repository.get_project(ingress.project_id)
    assert project.root_path == home.resolve()
    assert ingress.discord_guild_id == 100
    assert ingress.discord_channel_id == 201
    assert storage_context.repository.project_for_channel(100, 201) is None
    outbox = storage_context.repository.claim_outbox(worker_id="home-test")
    assert outbox is not None
    assert outbox.destination_key == "channel:201"
    channel.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_mention_creates_conversation_and_exactly_one_durable_turn(
    storage_context: StorageContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessions = SessionCoordinator(
        repository=storage_context.repository,
        security=SecurityConfig(allowed_roots=(tmp_path,)),
        home_path=tmp_path,
    )
    bot = _test_bot(tmp_path, repository=storage_context.repository)
    bot.sessions = sessions
    bot_user = Mock(id=999, bot=True)
    bot._connection.user = bot_user
    parent = Mock(spec=discord.TextChannel)
    parent.id = 200
    parent.send = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.id = 903
    thread.parent_id = 200
    thread.archived = False
    thread.locked = False
    message = Mock(spec=discord.Message)
    message.id = 903
    message.author = Mock(id=400, bot=False)
    message.webhook_id = None
    message.guild = Mock(id=100)
    message.channel = parent
    message.content = "<@999> inspect the project"
    message.mentions = [bot_user]
    message.attachments = []
    message.thread = None
    message.create_thread = AsyncMock(return_value=thread)
    parent.fetch_message = AsyncMock(return_value=message)
    bot._image_ingestor = Mock(
        ingest=AsyncMock(return_value=()),
        cleanup=Mock(),
    )

    async def enqueue(**kwargs: Any) -> object:
        return storage_context.repository.enqueue_turn(**kwargs)

    bot.turns.enqueue = AsyncMock(side_effect=enqueue)

    await bot._handle_message(message)
    creation = storage_context.repository.claim_outbox(worker_id="mention-test")
    assert creation is not None
    client = Mock(spec=discord.Client)
    client.get_channel.side_effect = lambda channel_id: parent if channel_id == 200 else None
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=Mock(),
        signer=Mock(),
    )
    monkeypatch.setattr(
        transport,
        "_existing_thread",
        AsyncMock(return_value=None),
    )
    delivered = await transport.deliver(creation)
    storage_context.repository.ack_outbox(
        creation.id,
        lease_owner=creation.lease_owner,
        lease_attempt=creation.attempts,
        discord_message_id=delivered.discord_message_id,
    )
    monkeypatch.setattr(
        bot,
        "get_channel",
        lambda channel_id: parent if channel_id == 200 else thread,
    )

    await bot._process_initial_ingress("903")
    await bot._handle_message(message)

    ingress = storage_context.repository.get_ingress_message("903")
    assert ingress.state == "ready"
    assert ingress.turn_id is not None
    conversation = storage_context.repository.conversation_for_thread(903)
    assert conversation is not None
    assert conversation.project_id == storage_context.project.id
    turn = storage_context.repository.get_turn(ingress.turn_id)
    assert turn.input_summary == "inspect the project"
    assert turn.input_message_id == "903"
    assert bot.turns.enqueue.await_count == 1
    message.create_thread.assert_awaited_once()


@pytest.mark.asyncio
async def test_conversation_thread_message_needs_no_mention_and_is_idempotent(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _test_bot(tmp_path, repository=storage_context.repository)
    bot.sessions.conversation_for_thread = AsyncMock(
        return_value=storage_context.conversation
    )
    bot.turns.enqueue = AsyncMock(return_value=Mock(id="turn"))
    bot._image_ingestor = Mock(
        ingest=AsyncMock(return_value=()),
        cleanup=Mock(),
    )
    channel = Mock(spec=discord.Thread)
    channel.id = 300
    channel.parent_id = 200
    channel.send = AsyncMock()
    message = Mock(spec=discord.Message)
    message.id = 902
    message.author = Mock(id=400, bot=False)
    message.webhook_id = None
    message.guild = Mock(id=100)
    message.channel = channel
    message.content = "continue without mentioning the bot"
    message.mentions = []
    message.attachments = []
    bot._connection.user = Mock(id=999, bot=True)

    await bot._handle_message(message)
    await bot._handle_message(message)

    ingress = storage_context.repository.get_ingress_message("902")
    assert ingress.discord_channel_id == 300
    assert ingress.discord_guild_id == 100
    assert ingress.conversation_id == storage_context.conversation.id
    bot.turns.enqueue.assert_awaited_once()
    enqueue = bot.turns.enqueue.await_args.kwargs
    assert enqueue["turn_input"].text == "continue without mentioning the bot"
    assert enqueue["input_message_id"] == "902"
    channel.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_project_info_redacts_local_root(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    bot = _test_bot(tmp_path, repository=storage_context.repository)
    bot.sessions.resolve_project_for_channel = AsyncMock(
        return_value=ResolvedProject(storage_context.project, "binding")
    )
    bot.runtimes.project_status = AsyncMock(
        return_value={"state": "ready", "generation": 2}
    )
    interaction = Mock(spec=discord.Interaction)
    interaction.guild_id = 100
    interaction.channel_id = 200
    interaction.user = Mock(id=400)
    interaction.response = _interaction_response()
    interaction.followup = Mock(send=AsyncMock())

    await bot._project_show(interaction)

    output = interaction.followup.send.await_args.args[0]
    root = str(storage_context.project.root_path)
    assert root not in output
    assert f"project-root#{sha256_text(root)[:12]}" in output


@pytest.mark.asyncio
async def test_bind_is_channel_override_and_unbind_returns_future_work_to_home(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    sessions = SessionCoordinator(
        repository=storage_context.repository,
        security=SecurityConfig(allowed_roots=(tmp_path,)),
        home_path=home,
    )

    first_home = await sessions.resolve_project_for_channel(
        guild_id=100,
        channel_id=201,
    )
    second_home = await sessions.resolve_project_for_channel(
        guild_id=100,
        channel_id=202,
    )
    assert first_home.source == second_home.source == "home"
    assert first_home.project.id == second_home.project.id
    assert first_home.project.root_path == home.resolve()

    override = await sessions.bind_project(
        name="test",
        path=str(storage_context.root),
        guild_id=100,
        channel_id=201,
    )
    assert override.id == storage_context.project.id
    assert (
        await sessions.resolve_project_for_channel(guild_id=100, channel_id=201)
    ).source == "binding"
    assert (
        await sessions.resolve_project_for_channel(guild_id=100, channel_id=202)
    ).project.id == first_home.project.id

    await sessions.unbind_project(
        guild_id=100,
        channel_id=201,
        confirmation_name="test",
    )
    after_unbind = await sessions.resolve_project_for_channel(
        guild_id=100,
        channel_id=201,
    )
    existing = storage_context.repository.get_conversation(
        storage_context.conversation.id
    )
    assert after_unbind.source == "home"
    assert after_unbind.project.id == first_home.project.id
    assert existing.project_id == storage_context.project.id
    assert storage_context.repository.get_project(existing.project_id).root_path == (
        storage_context.root
    )


@pytest.mark.asyncio
async def test_thread_creation_outbox_reconciles_existing_remote_thread(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.request_thread_creation(
        discord_message_id="302",
        content_hash="content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )
    record = storage_context.repository.claim_outbox(worker_id="worker")
    assert record is not None
    channel = Mock(spec=discord.TextChannel)
    channel.id = 200
    thread = Mock(spec=discord.Thread)
    thread.id = 302
    thread.archived = False
    thread.locked = False
    client = Mock(spec=discord.Client)
    client.get_channel.side_effect = (
        lambda channel_id: channel if channel_id == 200 else thread
    )
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=Mock(),
        signer=Mock(),
    )

    result = await transport.deliver(record)

    assert result.discord_message_id == "302"
    assert result.initial_ingress_message_id == "302"
    conversation = storage_context.repository.conversation_for_thread(302)
    assert conversation is not None
    assert conversation.owner_user_id == 400
    assert (
        storage_context.repository.get_ingress_message("302").state
        == "pending_preflight"
    )


@pytest.mark.asyncio
async def test_outbox_reconciliation_ignores_user_spoofed_marker() -> None:
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    spoof = Mock(spec=discord.Message)
    spoof.content = "spoof\n-# codexD:spoof-marker"
    spoof.author = Mock(id=123)

    async def history(*, limit: int):
        assert limit == 500
        yield spoof

    thread.history = history
    thread.send = AsyncMock(return_value=Mock(id=999))
    client = Mock(spec=discord.Client)
    client.user = Mock(id=456)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=Mock(),
        renderer=Mock(),
        signer=Mock(),
    )
    record = OutboxRecord(
        id="spoof",
        destination_key="thread:300",
        operation="send",
        payload_json='{"content":"real update"}',
        delivery_marker="spoof-marker",
        state="reconciling",
        attempts=1,
        lease_owner="test",
    )

    result = await transport.deliver(record)

    assert result.discord_message_id == "999"
    thread.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconciliation_history_window_retries_with_incident(
    storage_context: StorageContext,
) -> None:
    outbox_id = storage_context.repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "must not be duplicated"},
        dedupe_key="history-window",
        delivery_marker="history-window",
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE discord_outbox SET state = 'reconciling' WHERE id = ?",
            (outbox_id,),
        )
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False

    async def history(*, limit: int):
        assert limit == 500
        for index in range(limit):
            yield Mock(
                spec=discord.Message,
                author=Mock(id=999),
                content=f"unrelated-{index}",
            )

    thread.history = history
    thread.send = AsyncMock()
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    worker = OutboxWorker(
        repository=storage_context.repository,
        transport=DiscordOutboxTransport(
            client=client,
            repository=storage_context.repository,
            renderer=Mock(),
            signer=Mock(),
        ),
        worker_id="history-worker",
    )

    assert await worker.drain_once()

    row = storage_context.store.query_one(
        "SELECT state, last_error_code FROM discord_outbox WHERE id = ?",
        (outbox_id,),
    )
    assert row is not None
    assert (row["state"], row["last_error_code"]) == (
        "retry",
        "discord_history_window_exhausted",
    )
    incident = storage_context.store.query_one(
        "SELECT severity, code, details_json FROM incidents WHERE code = ?",
        ("discord_reconciliation_uncertain",),
    )
    assert incident is not None
    assert (incident["severity"], incident["code"]) == (
        "warning",
        "discord_reconciliation_uncertain",
    )
    assert json.loads(incident["details_json"])["outbox_id"] == outbox_id
    thread.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciliation_rate_limit_preserves_retry_after() -> None:
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    response = Mock(status=429, reason="rate limited")
    error = discord.HTTPException(response, "rate limited")
    error.retry_after = 7.25

    async def history(*, limit: int):
        assert limit == 500
        raise error
        yield Mock()

    thread.history = history
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=Mock(),
        renderer=Mock(),
        signer=Mock(),
    )

    with pytest.raises(DeliveryError) as raised:
        await transport.deliver(
            OutboxRecord(
                id="rate-limit",
                destination_key="thread:300",
                operation="send",
                payload_json='{"content":"safe"}',
                delivery_marker="rate-limit",
                state="reconciling",
                attempts=1,
                lease_owner="worker",
            )
        )

    assert raised.value.permanent is False
    assert raised.value.retry_after == 7.25
    assert raised.value.incident_code == "discord_reconciliation_uncertain"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_type", "status"),
    ((discord.Forbidden, 403), (discord.NotFound, 404)),
)
async def test_reconciliation_history_unavailable_is_not_permanent(
    error_type: type[discord.HTTPException],
    status: int,
) -> None:
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    error = error_type(Mock(status=status, reason="unavailable"), "unavailable")

    async def history(*, limit: int):
        assert limit == 500
        raise error
        yield Mock()

    thread.history = history
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=Mock(),
        renderer=Mock(),
        signer=Mock(),
    )

    with pytest.raises(DeliveryError) as raised:
        await transport.deliver(
            OutboxRecord(
                id=f"history-{status}",
                destination_key="thread:300",
                operation="send",
                payload_json='{"content":"safe"}',
                delivery_marker=f"history-{status}",
                state="reconciling",
                attempts=1,
                lease_owner="worker",
            )
        )

    assert raised.value.permanent is False
    assert raised.value.incident_code == "discord_reconciliation_uncertain"


@pytest.mark.asyncio
async def test_destination_lookup_rate_limit_preserves_retry_after() -> None:
    response = Mock(status=429, reason="rate limited")
    error = discord.HTTPException(response, "rate limited")
    error.retry_after = 4.5
    client = Mock(spec=discord.Client)
    client.get_channel.return_value = None
    client.fetch_channel = AsyncMock(side_effect=error)
    transport = DiscordOutboxTransport(
        client=client,
        repository=Mock(),
        renderer=Mock(),
        signer=Mock(),
    )

    with pytest.raises(DeliveryError) as raised:
        await transport.deliver(
            OutboxRecord(
                id="lookup-rate-limit",
                destination_key="thread:300",
                operation="send",
                payload_json='{"content":"safe"}',
                delivery_marker="lookup-rate-limit",
                state="pending",
                attempts=1,
                lease_owner="worker",
            )
        )

    assert raised.value.permanent is False
    assert raised.value.retry_after == 4.5


@pytest.mark.asyncio
async def test_malformed_outbox_payload_dead_letters_and_notifies_parent(
    storage_context: StorageContext,
) -> None:
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="malformed delivery"),
        input_message_id="malformed-delivery",
    )
    row = storage_context.store.query_one(
        "SELECT id FROM discord_outbox WHERE dedupe_key = ?",
        (f"turn:{turn.id}:progress:1",),
    )
    assert row is not None
    outbox_id = str(row["id"])
    connection = storage_context.store.connection
    connection.execute("PRAGMA ignore_check_constraints = ON")
    connection.execute(
        "UPDATE discord_outbox SET payload_json = '{' WHERE id = ?",
        (outbox_id,),
    )
    connection.commit()
    connection.execute("PRAGMA ignore_check_constraints = OFF")
    worker = OutboxWorker(
        repository=storage_context.repository,
        transport=DiscordOutboxTransport(
            client=Mock(spec=discord.Client),
            repository=storage_context.repository,
            renderer=Mock(),
            signer=Mock(),
        ),
        worker_id="malformed-worker",
    )

    assert await worker.drain_once()

    failed = storage_context.store.query_one(
        "SELECT state, last_error_code FROM discord_outbox WHERE id = ?",
        (outbox_id,),
    )
    assert failed is not None
    assert (failed["state"], failed["last_error_code"]) == (
        "dead_letter",
        "payload_invalid",
    )
    notice = storage_context.store.query_one(
        """
        SELECT destination_key, payload_json
        FROM discord_outbox
        WHERE dedupe_key = ?
        """,
        (f"conversation:{turn.conversation_id}:delivery-blocked",),
    )
    assert notice is not None
    assert notice["destination_key"] == "channel:200"
    assert "payload_invalid" in notice["payload_json"]


@pytest.mark.asyncio
async def test_recovered_initial_ingress_never_replays_old_prompt(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    storage_context.repository.request_thread_creation(
        discord_message_id="304",
        content_hash="content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="old-boot",
    )
    creation = storage_context.repository.claim_outbox(worker_id="worker")
    assert creation is not None
    storage_context.repository.finalize_thread_creation(
        discord_message_id="304",
        discord_thread_id=304,
        owner_user_id=400,
    )
    storage_context.repository.ack_outbox(
        creation.id,
        lease_owner=creation.lease_owner,
        lease_attempt=creation.attempts,
        discord_message_id="304",
    )
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            (
                "[discord]",
                "guild_id = 100",
                "owner_user_id = 400",
                "allowed_user_ids = [400]",
                "[security]",
                f'allowed_roots = ["{storage_context.root}"]',
            )
        )
    )
    config = load_config(
        config_path,
        environment={
            "HOME": str(tmp_path),
            "CODEXD_DATA_DIR": str(tmp_path / "data"),
        },
    )
    turns = Mock()
    bot = CodexDBot(
        config=config,
        repository=storage_context.repository,
        sessions=Mock(),
        session_lifecycle=Mock(),
        turns=turns,
        schedules=Mock(),
        schedule_repository=Mock(),
        runtimes=Mock(),
        renderer=Mock(),
        media_worker=Mock(),
        signer=Mock(),
        capability_manifest=capability_manifest(),
        boot_id="new-boot",
    )

    await bot._process_initial_ingress("304")

    ingress = storage_context.repository.get_ingress_message("304")
    assert ingress.state == "rejected"
    assert ingress.error_code == "daemon_restarted_before_preflight"
    turns.enqueue.assert_not_called()


def _test_bot(
    tmp_path: Path,
    *,
    repository: Mock | None = None,
    discord_status: object | None = None,
    codex_auth_status: object | None = None,
) -> CodexDBot:
    from codexd.config import AppConfig, DiscordConfig

    return CodexDBot(
        config=AppConfig(
            paths=AppPaths(tmp_path / "data", tmp_path / "logs"),
            discord=DiscordConfig(
                guild_id=100,
                owner_user_id=400,
                allowed_user_ids=frozenset({400}),
            ),
        ),
        repository=repository or Mock(),
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
        boot_id="test",
        discord_status=discord_status,  # type: ignore[arg-type]
        codex_auth_status=codex_auth_status,  # type: ignore[arg-type]
    )


def _interaction_response() -> Mock:
    state = False

    async def defer(**_kwargs: object) -> None:
        nonlocal state
        state = True

    async def send_message(*_args: object, **_kwargs: object) -> None:
        nonlocal state
        state = True

    return Mock(
        defer=AsyncMock(side_effect=defer),
        is_done=Mock(side_effect=lambda: state),
        send_message=AsyncMock(side_effect=send_message),
    )


@pytest.mark.asyncio
async def test_shutdown_gate_drains_active_ingress_and_rejects_new_work(
    tmp_path: Path,
) -> None:
    repository = Mock()
    bot = _test_bot(tmp_path, repository=repository)
    accepted = CommandIntentRecord(
        interaction_id="899",
        command_name="test",
        request_hash="hash",
        project_id=None,
        conversation_id=None,
        turn_id=None,
        state="accepted",
        result_json=None,
        effect_kind=None,
        effect_correlation_id=None,
        accepted_boot_id="test",
    )
    bot._accept_interaction_intent = AsyncMock(return_value=accepted)  # type: ignore[method-assign]
    interaction = Mock(spec=discord.Interaction)
    interaction.id = 899
    interaction.user = Mock(id=400)
    interaction.followup = Mock(send=AsyncMock())
    interaction.response = _interaction_response()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def action(_staged: discord.Interaction[object]) -> None:
        entered.set()
        await release.wait()

    active = asyncio.create_task(
        bot._run_intent_action(
            interaction,
            command_name="test",
            request={},
            action=action,
        )
    )
    await entered.wait()
    shutdown = asyncio.create_task(bot.begin_shutdown())
    await asyncio.sleep(0)
    assert not shutdown.done()

    release.set()
    assert await active
    await shutdown

    rejected_action = AsyncMock()
    assert not await bot._run_intent_action(
        interaction,
        command_name="test",
        request={},
        action=rejected_action,
    )
    rejected_action.assert_not_awaited()
    interaction.followup.send.assert_awaited_once()

    await bot.on_message(Mock(spec=discord.Message))
    bot.sessions.conversation_for_thread.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_command_interaction_is_single_flight(tmp_path: Path) -> None:
    repository = Mock()
    bot = _test_bot(tmp_path, repository=repository)
    accepted = CommandIntentRecord(
        interaction_id="900",
        command_name="test",
        request_hash="hash",
        project_id=None,
        conversation_id=None,
        turn_id=None,
        state="accepted",
        result_json=None,
        effect_kind=None,
        effect_correlation_id=None,
        accepted_boot_id="test",
    )
    bot._accept_interaction_intent = AsyncMock(return_value=accepted)  # type: ignore[method-assign]
    first = Mock(spec=discord.Interaction)
    first.id = 900
    first.user = Mock(id=400)
    first.followup = Mock(send=AsyncMock())
    first.response = _interaction_response()
    duplicate = Mock(spec=discord.Interaction)
    duplicate.id = 900
    duplicate.user = Mock(id=400)
    duplicate.followup = Mock(send=AsyncMock())
    duplicate.response = _interaction_response()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def action(_staged: discord.Interaction[object]) -> None:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()

    active = asyncio.create_task(
        bot._run_intent_action(
            first,
            command_name="test",
            request={},
            action=action,
        )
    )
    await entered.wait()

    assert not await bot._run_intent_action(
        duplicate,
        command_name="test",
        request={},
        action=action,
    )
    assert calls == 1
    duplicate.followup.send.assert_awaited_once_with(
        "`command_pending`: this interaction is already being processed.",
        ephemeral=True,
    )

    release.set()
    assert await active
    assert not bot._active_command_intents


@pytest.mark.asyncio
async def test_shutdown_quiesces_initial_ingress_and_reconnect_callbacks(
    tmp_path: Path,
) -> None:
    bot = _test_bot(tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def initial_callback(_message_id: str) -> None:
        entered.set()
        await release.wait()

    bot._process_initial_ingress_locked = AsyncMock(  # type: ignore[method-assign]
        side_effect=initial_callback
    )
    callback = asyncio.create_task(bot._process_initial_ingress("message-1"))
    await entered.wait()
    shutdown = asyncio.create_task(
        bot.begin_shutdown(deadline_seconds=1)
    )
    await asyncio.sleep(0)
    assert not shutdown.done()

    release.set()
    await callback
    assert await shutdown

    await bot._process_initial_ingress("message-2")
    assert bot._process_initial_ingress_locked.await_count == 1
    bot.repository.list_enabled_projects.reset_mock()
    await bot.on_ready()
    bot.repository.list_enabled_projects.assert_not_called()


@pytest.mark.asyncio
async def test_shutdown_timeout_waits_for_active_ingress_before_returning(
    tmp_path: Path,
) -> None:
    repository = Mock()
    bot = _test_bot(tmp_path, repository=repository)
    bot._accept_interaction_intent = AsyncMock(  # type: ignore[method-assign]
        return_value=CommandIntentRecord(
            interaction_id="902",
            command_name="test",
            request_hash="hash",
            project_id=None,
            conversation_id=None,
            turn_id=None,
            state="accepted",
            result_json=None,
            effect_kind=None,
            effect_correlation_id=None,
            accepted_boot_id="test",
        )
    )
    interaction = Mock(spec=discord.Interaction)
    interaction.id = 902
    interaction.user = Mock(id=400)
    interaction.followup = Mock(send=AsyncMock())
    interaction.response = _interaction_response()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def action(_staged: discord.Interaction[object]) -> None:
        entered.set()
        await release.wait()

    active = asyncio.create_task(
        bot._run_intent_action(
            interaction,
            command_name="test",
            request={},
            action=action,
        )
    )
    await entered.wait()

    shutdown = asyncio.create_task(
        bot.begin_shutdown(deadline_seconds=0.001)
    )
    await asyncio.sleep(0.01)
    assert shutdown.done()
    assert not await shutdown
    release.set()
    assert await active
    assert not bot._ingress_tasks
    repository.complete_command_intent.assert_called_once()


@pytest.mark.asyncio
async def test_command_success_reply_waits_for_intent_commit(tmp_path: Path) -> None:
    events: list[str] = []
    repository = Mock()
    repository.complete_command_intent.side_effect = lambda *_args, **_kwargs: events.append(
        "committed"
    )
    bot = _test_bot(tmp_path, repository=repository)
    bot._accept_interaction_intent = AsyncMock(  # type: ignore[method-assign]
        return_value=CommandIntentRecord(
            interaction_id="900",
            command_name="test",
            request_hash="hash",
            project_id=None,
            conversation_id=None,
            turn_id=None,
            state="accepted",
            result_json=None,
            effect_kind=None,
            effect_correlation_id=None,
            accepted_boot_id="test",
        )
    )
    interaction = Mock(spec=discord.Interaction)
    interaction.id = 900
    interaction.user = Mock(id=400)
    interaction.followup = Mock()
    interaction.response = _interaction_response()

    async def deliver(*_args: object, **_kwargs: object) -> None:
        events.append("replied")

    interaction.followup.send = AsyncMock(side_effect=deliver)

    async def action(staged: discord.Interaction[object]) -> None:
        events.append("mutated")
        await staged.followup.send("done", ephemeral=True)

    await bot._run_intent_action(
        interaction,
        command_name="test",
        request={},
        action=action,
    )

    assert events == ["mutated", "committed", "replied"]


@pytest.mark.asyncio
async def test_command_response_is_chunked_before_intent_commit(
    tmp_path: Path,
) -> None:
    repository = Mock()
    bot = _test_bot(tmp_path, repository=repository)
    bot._accept_interaction_intent = AsyncMock(  # type: ignore[method-assign]
        return_value=CommandIntentRecord(
            interaction_id="903",
            command_name="test",
            request_hash="hash",
            project_id=None,
            conversation_id=None,
            turn_id=None,
            state="accepted",
            result_json=None,
            effect_kind=None,
            effect_correlation_id=None,
            accepted_boot_id="test",
        )
    )
    interaction = Mock(spec=discord.Interaction)
    interaction.id = 903
    interaction.user = Mock(id=400)
    interaction.followup = Mock(send=AsyncMock())
    interaction.response = _interaction_response()
    content = "x" * 5000

    async def action(staged: discord.Interaction[object]) -> None:
        await staged.followup.send(content, ephemeral=True)

    await bot._run_intent_action(
        interaction,
        command_name="test",
        request={},
        action=action,
    )

    chunks = [call.args[0] for call in interaction.followup.send.await_args_list]
    assert "".join(chunks) == content
    assert all(len(chunk) <= 1900 for chunk in chunks)
    repository.complete_command_intent.assert_called_once()


@pytest.mark.asyncio
async def test_command_commit_failure_does_not_send_success(tmp_path: Path) -> None:
    repository = Mock()
    repository.complete_command_intent.side_effect = RuntimeError("write failed")
    bot = _test_bot(tmp_path, repository=repository)
    bot._accept_interaction_intent = AsyncMock(  # type: ignore[method-assign]
        return_value=CommandIntentRecord(
            interaction_id="901",
            command_name="test",
            request_hash="hash",
            project_id=None,
            conversation_id=None,
            turn_id=None,
            state="accepted",
            result_json=None,
            effect_kind=None,
            effect_correlation_id=None,
            accepted_boot_id="test",
        )
    )
    interaction = Mock(spec=discord.Interaction)
    interaction.id = 901
    interaction.user = Mock(id=400)
    interaction.followup = Mock(send=AsyncMock())
    interaction.response = _interaction_response()

    async def action(staged: discord.Interaction[object]) -> None:
        await staged.followup.send("done", ephemeral=True)

    with pytest.raises(RuntimeError, match="write failed"):
        await bot._run_intent_action(
            interaction,
            command_name="test",
            request={},
            action=action,
        )

    interaction.followup.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_command_sync_removes_global_commands_before_guild_sync(
    tmp_path: Path,
) -> None:
    bot = _test_bot(tmp_path)
    bot.tree.clear_commands = Mock()
    bot.tree.sync = AsyncMock(side_effect=[[], []])
    guild = discord.Object(id=100)

    await bot._sync_commands_or_degrade(guild)

    bot.tree.clear_commands.assert_called_once_with(guild=None)
    assert bot.tree.sync.await_count == 2
    assert bot.tree.sync.await_args_list[0].args == ()
    assert bot.tree.sync.await_args_list[0].kwargs == {}
    assert bot.tree.sync.await_args_list[1].kwargs["guild"].id == 100


@pytest.mark.asyncio
async def test_command_sync_failure_degrades_and_retries(tmp_path: Path) -> None:
    statuses: list[str] = []
    repository = Mock()
    bot = _test_bot(
        tmp_path,
        repository=repository,
        discord_status=statuses.append,
    )
    response = Mock(status=503, reason="unavailable")
    bot.tree.sync = AsyncMock(
        side_effect=[discord.HTTPException(response, "down"), [], []]
    )
    bot._command_sync_retry_seconds = 0.001
    bot._gateway_ready = True

    await bot._sync_commands_or_degrade(discord.Object(id=100))
    retry = bot._command_sync_task
    assert retry is not None
    await retry

    assert bot.tree.sync.await_count == 3
    assert bot.tree.sync.await_args_list[-1].kwargs["guild"].id == 100
    assert statuses == ["degraded", "ready"]
    repository.record_incident.assert_called()


@pytest.mark.asyncio
async def test_stale_global_command_returns_actionable_error(tmp_path: Path) -> None:
    bot = _test_bot(tmp_path)
    interaction = Mock(spec=discord.Interaction)
    interaction.response = Mock(
        is_done=Mock(return_value=False),
        send_message=AsyncMock(),
    )
    interaction.followup = Mock(send=AsyncMock())

    await bot._on_command_error(
        interaction,
        app_commands.CommandNotFound("project", []),
    )

    interaction.response.send_message.assert_awaited_once_with(
        "`stale_command`: refresh Discord and retry the guild-scoped codexD command.",
        ephemeral=True,
    )
    interaction.followup.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_command_sync_recovery_does_not_override_disconnect(
    tmp_path: Path,
) -> None:
    statuses: list[str] = []
    bot = _test_bot(tmp_path, discord_status=statuses.append)
    bot._gateway_ready = True
    bot._command_sync_degraded = True

    await bot.on_disconnect()
    bot._command_sync_degraded = False
    bot._update_discord_ready_status()

    assert statuses == ["disconnected"]


@pytest.mark.asyncio
async def test_ready_preflight_publishes_sanitized_codex_auth_state(
    tmp_path: Path,
) -> None:
    auth_states: list[str] = []
    repository = Mock()
    repository.list_enabled_projects.return_value = [
        SimpleNamespace(id="project-1")
    ]
    bot = _test_bot(
        tmp_path,
        repository=repository,
        codex_auth_status=auth_states.append,
    )
    runtime = Mock()
    runtime.account_status = AsyncMock(
        return_value=SimpleNamespace(
            auth_required=False,
            account_type="chatgpt",
            plan_type="pro",
            email="must-not-be-published@example.com",
        )
    )
    bot.runtimes.ensure = AsyncMock(
        return_value=(runtime, SimpleNamespace())
    )
    bot.session_lifecycle.restore_provider_barriers = AsyncMock()
    bot.turns.restore = AsyncMock()

    await bot.on_ready()

    assert auth_states == ["authenticated"]


@pytest.mark.asyncio
async def test_final_attachment_failure_falls_back_to_actual_content(
    tmp_path: Path,
) -> None:
    attachment_path = tmp_path / "oversized.bin"
    attachment_path.write_bytes(b"content")
    attachment = DurableRenderedAttachment(
        filename="oversized.bin",
        path=attachment_path,
        description="oversized",
        sha256=hashlib.sha256(b"content").hexdigest(),
        size_bytes=7,
    )
    plan = DurableDiscordRenderPlan(("Final response",), (attachment,))
    renderer = Mock(
        artifact_root=tmp_path,
        retention_days=30,
        create_durable_plan=AsyncMock(return_value=plan),
    )
    renderer.load_durable_plan.return_value = plan
    repository = Mock()
    repository.render_plan.return_value = None
    repository.persist_render_plan.return_value = RenderPlanRecord(
        turn_id="turn-final",
        source_sha256=sha256_text("ignored"),
        plan_json=canonical_json(plan.to_payload(tmp_path)),
        retention_until=1,
    )
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    response = Mock(status=413, reason="too large")
    thread.send = AsyncMock(
        side_effect=[
            Mock(id=1),
            discord.HTTPException(response, "too large"),
            Mock(id=2),
            Mock(id=3),
        ]
    )
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=renderer,
        signer=Mock(),
    )
    record = OutboxRecord(
        id="final",
        destination_key="thread:300",
        operation="send",
        payload_json=canonical_json(
            {
                "kind": "turn_final",
                "plain_text": "ignored",
                "turn_id": "turn-final",
                "input_message_id": "888",
                "input_channel_id": "200",
                "discord_guild_id": "100",
            }
        ),
        delivery_marker="final-marker",
        state="pending",
        attempts=0,
        lease_owner="test",
    )

    result = await transport.deliver(record)

    assert result.discord_message_id == "1"
    assert "Final response" in thread.send.await_args_list[0].args[0]
    assert (
        "https://discord.com/channels/100/200/888"
        in thread.send.await_args_list[0].args[0]
    )
    assert "reference" not in thread.send.await_args_list[0].kwargs
    assert "files" not in thread.send.await_args_list[0].kwargs
    assert "content" in thread.send.await_args_list[2].args[0]
    assert _message_has_delivery_marker(
        thread.send.await_args_list[2].args[0],
        "final-marker-at0-0",
    )
    assert "codexD:" not in thread.send.await_args_list[2].args[0]
    assert thread.send.await_args_list[3].args[0].startswith(
        "-# 💡 finished | `unknown`"
    )
    assert "embed" not in thread.send.await_args_list[3].kwargs

    delivered_messages = []
    for message_id, call in (
        (1, thread.send.await_args_list[0]),
        (2, thread.send.await_args_list[2]),
        (3, thread.send.await_args_list[3]),
    ):
        delivered = Mock(spec=discord.Message)
        delivered.id = message_id
        delivered.author = Mock(id=999)
        delivered.content = call.args[0]
        delivered_messages.append(delivered)

    async def history(*, limit: int):
        assert limit == 500
        for delivered in delivered_messages:
            yield delivered

    thread.history = history
    thread.send = AsyncMock(
        side_effect=[discord.HTTPException(response, "too large")]
    )
    reconciled = await transport.deliver(
        OutboxRecord(
            id="final-reconcile",
            destination_key="thread:300",
            operation="send",
            payload_json=record.payload_json,
            delivery_marker="final-marker",
            state="reconciling",
            attempts=1,
            lease_owner="test",
        )
    )

    assert reconciled.discord_message_id == "1"
    assert thread.send.await_count == 1


@pytest.mark.asyncio
async def test_corrupt_render_plan_uses_bounded_plain_text_fallback(
    tmp_path: Path,
) -> None:
    plain_text = "The durable answer remains available."
    renderer = Mock(artifact_root=tmp_path, retention_days=30)
    renderer.load_durable_plan.side_effect = InvariantError(
        "render plan attachment changed or is missing"
    )
    repository = Mock()
    repository.render_plan.return_value = RenderPlanRecord(
        turn_id="turn-corrupt",
        source_sha256=sha256_text(plain_text),
        plan_json='{"version":2,"messages":[],"attachments":[]}',
        retention_until=1,
    )
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(side_effect=[Mock(id=1), Mock(id=2)])
    client = Mock(spec=discord.Client)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=renderer,
        signer=Mock(),
    )

    result = await transport.deliver(
        OutboxRecord(
            id="corrupt-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "turn_id": "turn-corrupt",
                    "plain_text": plain_text,
                    "state": "completed",
                    "terminal_code": "provider_completed",
                }
            ),
            delivery_marker="corrupt-final",
            state="pending",
            attempts=1,
            lease_owner="worker",
        )
    )

    assert result.discord_message_id == "1"
    assert "Rich rendering was unavailable" in thread.send.await_args_list[0].args[0]
    assert plain_text in thread.send.await_args_list[0].args[0]
    assert "files" not in thread.send.await_args_list[0].kwargs
    assert thread.send.await_args_list[1].args[0].startswith("-# ✅")
    assert "embed" not in thread.send.await_args_list[1].kwargs
    repository.record_incident.assert_called_once_with(
        severity="error",
        code="discord_render_fallback",
        summary="Discord rich rendering failed; bounded plain text was used",
        turn_id="turn-corrupt",
        details={"stable_code": "render_plan_invalid"},
    )


def test_remove_bot_trigger_preserves_other_mention_text() -> None:
    message = Mock(spec=discord.Message)
    message.content = "<@42> run this and preserve `<@42>`"
    message.mentions = [Mock(id=42)]

    assert _remove_bot_mention(message, 42) == "run this and preserve `<@42>`"

    message.mentions = []
    assert _remove_bot_mention(message, 42) == message.content
