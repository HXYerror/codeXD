from __future__ import annotations

import asyncio
import hashlib
import json
import queue
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import discord
import pytest
from conftest import StorageContext
from discord import app_commands

from codexd.application.session_coordinator import ResolvedProject, SessionCoordinator
from codexd.application.volatile_turns import VolatileTurnStore
from codexd.config import AppConfig, DiscordConfig, SecurityConfig, load_config
from codexd.domain.conversations import SandboxProfile, ThreadConfig, ThreadIdentity
from codexd.domain.ids import canonical_json, sha256_text
from codexd.domain.turns import InterruptOrigin, TurnInput, TurnSource, TurnState
from codexd.errors import InvariantError
from codexd.paths import AppPaths
from codexd.rendering.discord import (
    AttachmentKind,
    DurableDiscordRenderPlan,
    DurableRenderedAttachment,
    RenderedAttachment,
    RenderedDiscordContent,
)
from codexd.runtime.codex_sdk import capability_manifest
from codexd.security.signing import ComponentSigner
from codexd.storage.progress import insert_progress_update
from codexd.storage.records import (
    CommandIntentRecord,
    OutboxRecord,
    RenderPlanRecord,
    TurnProgressDeleteTarget,
)
from codexd.transport.discord.attachments import DiscordAttachmentIngestResult
from codexd.transport.discord.bot import (
    CodexDBot,
    _bounded_response,
    _remove_bot_mention,
)
from codexd.transport.discord.outbox import (
    DeliveryError,
    DeliveryResult,
    DiscordOutboxTransport,
    OutboxWorker,
    _attachment_failure_guidance,
    _bounded_plain_text_fallback,
    _message_has_delivery_marker,
)
from codexd.transport.discord.presentation import TABLE_COPY_CUSTOM_ID, task_card_embed


def test_attachment_failure_guidance_is_code_based_and_actionable() -> None:
    unsupported = _attachment_failure_guidance("file_input_unsupported")
    assert unsupported is not None
    assert "bound project workspace" in unsupported
    assert "relative path" in unsupported
    assert "ZIP" in (_attachment_failure_guidance("archive_unsupported") or "")
    assert _attachment_failure_guidance("provider_completed") is None


def _volatile_final(
    turn_id: str,
    visible_text: str,
    *,
    final_answer_text: str | None = None,
) -> VolatileTurnStore:
    store = VolatileTurnStore()
    store.put_final(
        turn_id,
        visible_text=visible_text,
        final_answer_text=final_answer_text,
    )
    return store


def _volatile_preview(turn_id: str, text: str) -> VolatileTurnStore:
    store = VolatileTurnStore()
    store.save_content_ast(
        turn_id,
        {
            "schema_version": 1,
            "blocks": [
                {
                    "kind": "text",
                    "item_id": "preview",
                    "text": text,
                    "phase": "commentary",
                    "completed": False,
                }
            ],
        },
    )
    return store


@pytest.mark.asyncio
async def test_outbox_workers_deliver_independent_destinations_concurrently() -> None:
    records: queue.Queue[OutboxRecord] = queue.Queue()
    records.put(
        OutboxRecord(
            id="destination-a",
            destination_key="thread:100",
            operation="send",
            payload_json='{"content":"a"}',
            delivery_marker="a",
            state="pending",
            attempts=1,
            lease_owner="worker",
        )
    )
    records.put(
        OutboxRecord(
            id="destination-b",
            destination_key="thread:200",
            operation="send",
            payload_json='{"content":"b"}',
            delivery_marker="b",
            state="pending",
            attempts=1,
            lease_owner="worker",
        )
    )
    repository = Mock()

    def claim_outbox(*, worker_id: str, lease_ms: int) -> OutboxRecord | None:
        assert worker_id == "parallel-worker"
        assert lease_ms == 30_000
        try:
            return records.get_nowait()
        except queue.Empty:
            return None

    repository.claim_outbox.side_effect = claim_outbox
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_delivered = asyncio.Event()

    async def deliver(record: OutboxRecord) -> DeliveryResult:
        if record.id == "destination-a":
            first_started.set()
            await release_first.wait()
        else:
            second_delivered.set()
        return DeliveryResult(discord_message_id=record.id)

    worker = OutboxWorker(
        repository=repository,
        transport=SimpleNamespace(deliver=deliver),
        worker_id="parallel-worker",
        poll_seconds=0.01,
        concurrency=2,
    )
    worker.start()
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await asyncio.wait_for(second_delivered.wait(), timeout=1)
    release_first.set()
    await asyncio.sleep(0.05)
    await worker.close()

    acknowledged = {
        call.args[0] for call in repository.ack_outbox.call_args_list
    }
    assert acknowledged == {"destination-a", "destination-b"}


@pytest.mark.asyncio
async def test_outbox_worker_renews_lease_during_slow_delivery(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "slow"},
        dedupe_key="slow-delivery",
        delivery_marker="slow-delivery",
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    async def deliver(_record: OutboxRecord) -> DeliveryResult:
        entered.set()
        await release.wait()
        return DeliveryResult(discord_message_id="delivered")

    worker = OutboxWorker(
        repository=storage_context.repository,
        transport=SimpleNamespace(deliver=deliver),
        worker_id="slow-worker",
        lease_ms=30,
        lease_renew_seconds=0.01,
    )
    draining = asyncio.create_task(worker.drain_once())
    await entered.wait()
    await asyncio.sleep(0.05)

    assert storage_context.repository.claim_outbox(
        worker_id="competing-worker",
        lease_ms=30,
    ) is None
    release.set()
    assert await draining


@pytest.mark.asyncio
async def test_outbox_worker_cancels_delivery_after_lease_loss(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "slow"},
        dedupe_key="lost-lease",
        delivery_marker="lost-lease",
    )
    cancelled = asyncio.Event()

    async def deliver(_record: OutboxRecord) -> DeliveryResult:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    storage_context.repository.renew_outbox_lease = (  # type: ignore[method-assign]
        lambda *_args, **_kwargs: False
    )
    worker = OutboxWorker(
        repository=storage_context.repository,
        transport=SimpleNamespace(deliver=deliver),
        worker_id="lost-lease-worker",
        lease_ms=30,
        lease_renew_seconds=0.01,
    )

    with pytest.raises(RuntimeError, match="lease was lost"):
        await worker.drain_once()

    assert cancelled.is_set()
    incident = storage_context.store.query_one(
        "SELECT occurrence_count FROM incidents WHERE code = ?",
        ("outbox_delivery_lease_lost",),
    )
    assert incident is not None and incident["occurrence_count"] == 1
    assert storage_context.repository.health_counts()["outbox_lease_losses"] == 1


@pytest.mark.asyncio
async def test_outbox_worker_cancels_delivery_when_another_worker_finishes_record() -> None:
    record = OutboxRecord(
        id="externally-finished",
        destination_key="thread:300",
        operation="send",
        payload_json='{"content":"slow"}',
        delivery_marker="externally-finished",
        state="pending",
        attempts=1,
        lease_owner="first-worker",
    )
    repository = Mock()
    repository.claim_outbox.return_value = record
    repository.renew_outbox_lease.return_value = False
    cancelled = asyncio.Event()

    async def deliver(_record: OutboxRecord) -> DeliveryResult:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    worker = OutboxWorker(
        repository=repository,
        transport=SimpleNamespace(deliver=deliver),
        worker_id="first-worker",
        lease_ms=30,
        lease_renew_seconds=0.01,
    )

    with pytest.raises(RuntimeError, match="lease was lost"):
        await worker.drain_once()

    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_outbox_stops_lease_renewal_before_slow_post_ack_callback(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.enqueue_outbox(
        destination_key="thread:300",
        operation="send",
        payload={"content": "created"},
        dedupe_key="post-ack-callback",
        delivery_marker="post-ack-callback",
    )
    callback_completed = asyncio.Event()

    async def deliver(_record: OutboxRecord) -> DeliveryResult:
        return DeliveryResult(
            discord_message_id="300",
            initial_ingress_message_id="starter-message",
        )

    async def callback(_message_id: str) -> None:
        await asyncio.sleep(0.05)
        callback_completed.set()

    original_ack = storage_context.repository.ack_outbox

    def slow_return_after_ack(*args: Any, **kwargs: Any) -> None:
        original_ack(*args, **kwargs)
        time.sleep(0.05)

    storage_context.repository.ack_outbox = slow_return_after_ack  # type: ignore[method-assign]
    worker = OutboxWorker(
        repository=storage_context.repository,
        transport=SimpleNamespace(deliver=deliver),
        worker_id="post-ack-worker",
        lease_ms=30,
        lease_renew_seconds=0.01,
        initial_ingress_ready=callback,
    )

    assert await worker.drain_once()
    assert callback_completed.is_set()


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
async def test_outbox_delivers_different_conversations_concurrently(
    storage_context: StorageContext,
) -> None:
    for channel_id in (301, 302):
        storage_context.repository.enqueue_outbox(
            destination_key=f"thread:{channel_id}",
            operation="send",
            payload={"content": f"message-{channel_id}"},
            dedupe_key=f"parallel-{channel_id}",
            delivery_marker=f"parallel-{channel_id}",
        )
    started: set[str] = set()
    both_started = asyncio.Event()
    release = asyncio.Event()

    class ParallelTransport:
        async def deliver(self, record: OutboxRecord) -> DeliveryResult:
            started.add(record.destination_key)
            if len(started) == 2:
                both_started.set()
            await release.wait()
            return DeliveryResult(record.id)

    worker = OutboxWorker(
        repository=storage_context.repository,
        transport=ParallelTransport(),
        worker_id="parallel-worker",
        poll_seconds=0.01,
        concurrency=2,
    )
    worker.start()
    try:
        await asyncio.wait_for(both_started.wait(), timeout=1)
        assert started == {"thread:301", "thread:302"}
    finally:
        release.set()
        await worker.close()


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
    attachments: list[RenderedAttachment] = []
    for index in range(25):
        content = str(index).encode()
        path = tmp_path / f"table-{index}.txt"
        path.write_bytes(content)
        attachments.append(
            RenderedAttachment(
                filename=path.name,
                content=content,
                description=f"attachment {index}",
            )
        )
    plan = RenderedDiscordContent(("Final response",), tuple(attachments))
    renderer = Mock()
    renderer.artifact_root = tmp_path
    renderer.retention_days = 30
    renderer.render_markdown = AsyncMock(return_value=plan)
    repository = Mock()
    repository.render_plan.return_value = None
    repository.persist_render_plan.return_value = RenderPlanRecord(
        turn_id="turn-final",
        source_sha256=sha256_text("ignored"),
        plan_json="{}",
        retention_until=1,
    )
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=renderer,
        signer=Mock(),
        volatile_turns=_volatile_final("turn-final", "Final response"),
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
async def test_final_outbox_renders_visible_transcript_not_only_canonical_final(
    tmp_path: Path,
) -> None:
    visible_text = "Commentary one\n\nCommentary two\n\nCanonical final"
    plan = RenderedDiscordContent((visible_text,), ())
    renderer = Mock(
        artifact_root=tmp_path,
        retention_days=30,
        render_markdown=AsyncMock(return_value=plan),
    )
    repository = Mock()
    repository.render_plan.return_value = None
    repository.persist_render_plan.return_value = RenderPlanRecord(
        turn_id="turn-visible",
        source_sha256=sha256_text(visible_text),
        plan_json="{}",
        retention_until=1,
    )
    thread = Mock(spec=discord.Thread)
    thread.id = 300
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
        volatile_turns=_volatile_final(
            "turn-visible",
            visible_text,
            final_answer_text="Canonical final",
        ),
    )

    await transport.deliver(
        OutboxRecord(
            id="visible-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "visible_text": visible_text,
                    "final_answer_text": "Canonical final",
                    "turn_id": "turn-visible",
                    "state": "completed",
                }
            ),
            delivery_marker="visible-final",
            state="pending",
            attempts=0,
            lease_owner="test",
        )
    )

    renderer.render_markdown.assert_awaited_once_with(visible_text)
    repository.persist_render_plan.assert_not_called()
    assert thread.send.await_args_list[0].args[0].startswith(visible_text)


@pytest.mark.asyncio
async def test_final_delivery_after_restart_reports_that_content_was_not_retained(
    tmp_path: Path,
) -> None:
    renderer = Mock(artifact_root=tmp_path)

    async def render(source: str) -> RenderedDiscordContent:
        return RenderedDiscordContent((source,), ())

    renderer.render_markdown = AsyncMock(side_effect=render)
    repository = Mock()
    thread = Mock(spec=discord.Thread)
    thread.id = 300
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
        volatile_turns=VolatileTurnStore(),
    )

    await transport.deliver(
        OutboxRecord(
            id="restart-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "turn_id": "turn-after-restart",
                    "state": "completed",
                    "terminal_code": "provider_completed",
                    "content_storage": "volatile",
                }
            ),
            delivery_marker="restart-final",
            state="pending",
            attempts=1,
            lease_owner="worker",
        )
    )

    rendered_source = renderer.render_markdown.await_args.args[0]
    assert "not retained in SQLite" in rendered_source
    repository.render_plan.assert_not_called()


@pytest.mark.asyncio
async def test_outbox_worker_waits_for_full_final_retry_before_progress_cleanup(
    storage_context: StorageContext,
    tmp_path: Path,
) -> None:
    repository = storage_context.repository
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="deliver every final part before cleanup"),
        input_message_id="final-retry-cleanup",
    )
    repository.request_cancel(turn.id, origin=InterruptOrigin.USER)
    final_row = storage_context.store.query_one(
        "SELECT id FROM discord_outbox WHERE dedupe_key = ?",
        (f"turn:{turn.id}:final",),
    )
    assert final_row is not None
    final_id = str(final_row["id"])

    while storage_context.store.query_one(
        """
        SELECT 1 FROM discord_outbox
        WHERE id <> ?
          AND state IN ('pending', 'retry', 'reconciling', 'sending')
        LIMIT 1
        """,
        (final_id,),
    ) is not None:
        setup_record = repository.claim_outbox(worker_id="final-retry-setup")
        assert setup_record is not None
        assert setup_record.id != final_id
        setup_payload = json.loads(setup_record.payload_json)
        is_progress = setup_payload.get("kind") == "turn_progress"
        repository.ack_outbox(
            setup_record.id,
            lease_owner=setup_record.lease_owner,
            lease_attempt=setup_record.attempts,
            discord_message_id="901" if is_progress else None,
            turn_progress_id=turn.id if is_progress else None,
        )

    attachment_content = b"attachment"
    attachment_path = tmp_path / "final-attachment.txt"
    attachment_path.write_bytes(attachment_content)
    plan = DurableDiscordRenderPlan(
        ("Final part one", "Final part two"),
        (
            DurableRenderedAttachment(
                filename=attachment_path.name,
                path=attachment_path,
                description="Final attachment",
                sha256=hashlib.sha256(attachment_content).hexdigest(),
                size_bytes=len(attachment_content),
            ),
        ),
    )
    renderer = Mock(
        artifact_root=tmp_path,
        retention_days=30,
        render_markdown=AsyncMock(return_value=plan),
    )
    volatile_turns = _volatile_final(
        turn.id,
        "Final part one\n\nFinal part two",
    )

    bot_user = Mock(id=999)
    delivered_messages: list[Mock] = []
    response = Mock(status=503, reason="unavailable")
    footer_failure = discord.HTTPException(response, "unavailable")
    footer_failure.retry_after = 0.0
    send_attempt = 0

    async def send(content: str, **_kwargs: Any) -> Mock:
        nonlocal send_attempt
        send_attempt += 1
        if send_attempt == 4:
            raise footer_failure
        message = Mock(spec=discord.Message)
        message.id = 1000 + send_attempt
        message.author = bot_user
        message.content = content
        message.attachments = []
        delivered_messages.append(message)
        return message

    async def history(*, limit: int):
        assert limit == 500
        for message in delivered_messages:
            yield message

    progress_message = Mock(spec=discord.Message)
    progress_message.id = 901
    progress_message.author = bot_user
    progress_message.delete = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(side_effect=send)
    thread.history = history
    thread.fetch_message = AsyncMock(return_value=progress_message)
    client = Mock(spec=discord.Client)
    client.user = bot_user
    client.get_channel.return_value = thread
    worker = OutboxWorker(
        repository=repository,
        transport=DiscordOutboxTransport(
            client=client,
            repository=repository,
            renderer=renderer,
            signer=Mock(),
            volatile_turns=volatile_turns,
        ),
        worker_id="final-retry-worker",
    )

    assert await worker.drain_once()

    failed_final = storage_context.store.query_one(
        "SELECT state, attempts, last_error_code FROM discord_outbox WHERE id = ?",
        (final_id,),
    )
    assert failed_final is not None
    assert dict(failed_final) == {
        "state": "retry",
        "attempts": 1,
        "last_error_code": "discord_http_503",
    }
    assert thread.send.await_count == 4
    assert "files" in thread.send.await_args_list[2].kwargs
    assert thread.send.await_args_list[3].args[0].startswith("-# ")
    assert storage_context.store.query_one(
        "SELECT 1 FROM discord_outbox WHERE dedupe_key = ?",
        (f"turn:{turn.id}:progress:delete",),
    ) is None
    assert storage_context.store.query_one(
        "SELECT cleanup_state FROM turn_progress_views WHERE turn_id = ?",
        (turn.id,),
    )["cleanup_state"] == "active"

    assert await worker.drain_once()

    cleanup_rows = storage_context.store.query_all(
        """
        SELECT id, operation, state FROM discord_outbox
        WHERE dedupe_key = ?
        """,
        (f"turn:{turn.id}:progress:delete",),
    )
    assert len(cleanup_rows) == 1
    assert cleanup_rows[0]["operation"] == "delete"
    assert cleanup_rows[0]["state"] == "pending"
    sent_final = storage_context.store.query_one(
        "SELECT state, attempts FROM discord_outbox WHERE id = ?",
        (final_id,),
    )
    assert sent_final is not None
    assert dict(sent_final) == {"state": "sent", "attempts": 2}
    assert thread.send.await_count == 5
    assert renderer.render_markdown.await_count == 2

    assert await worker.drain_once()

    progress_message.delete.assert_awaited_once()
    deleted_view = storage_context.store.query_one(
        """
        SELECT discord_message_id, cleanup_state, deleted_at
        FROM turn_progress_views WHERE turn_id = ?
        """,
        (turn.id,),
    )
    assert deleted_view is not None
    assert deleted_view["discord_message_id"] is None
    assert deleted_view["cleanup_state"] == "deleted"
    assert deleted_view["deleted_at"] is not None
    assert storage_context.store.query_one(
        "SELECT state FROM discord_outbox WHERE id = ?",
        (cleanup_rows[0]["id"],),
    )["state"] == "sent"
    assert not await worker.drain_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("progress_state", ("retry", "reconciling"))
async def test_progress_fallback_send_reconciles_before_terminal_cleanup(
    storage_context: StorageContext,
    tmp_path: Path,
    progress_state: str,
) -> None:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="fallback-progress-provider-thread",
            requested_thread_id=None,
            provider_session_id="fallback-progress-provider-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="test",
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="recover fallback progress replacement"),
        input_message_id="fallback-progress-recovery",
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="fallback-progress-recovery-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="sdk-test",
        runtime_version="runtime-test",
        capability_hash="fallback-progress-recovery-capabilities",
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(
        turn.id,
        "fallback-progress-provider-turn",
    )
    initial = repository.claim_outbox(worker_id="initial-progress-worker")
    assert initial is not None
    repository.ack_outbox(
        initial.id,
        lease_owner=initial.lease_owner,
        lease_attempt=initial.attempts,
        discord_message_id="601",
        turn_progress_id=turn.id,
    )
    with storage_context.store.transaction() as connection:
        running_id = insert_progress_update(
            connection,
            turn_id=turn.id,
            state="running",
            content="Running · recovering replacement",
            now=1,
        )
    assert running_id is not None
    running = repository.claim_outbox(worker_id="crashed-progress-worker")
    assert running is not None
    assert running.id == running_id

    bot_user = Mock(id=999)
    delivered_messages: list[Mock] = []

    async def send(content: str, **_kwargs: Any) -> Mock:
        message = Mock(spec=discord.Message)
        message.id = 602 + len(delivered_messages)
        message.author = bot_user
        message.content = content
        message.attachments = []
        message.edit = AsyncMock()
        message.delete = AsyncMock()
        delivered_messages.append(message)
        return message

    async def fetch_message(message_id: int) -> Mock:
        if message_id == 601:
            raise discord.NotFound(
                Mock(status=404, reason="missing"),
                "missing",
            )
        for delivered in delivered_messages:
            if delivered.id == message_id:
                return delivered
        raise AssertionError(f"unexpected Discord message ID: {message_id}")

    async def history(*, limit: int):
        assert limit == 500
        for delivered in delivered_messages:
            yield delivered

    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(side_effect=send)
    thread.fetch_message = AsyncMock(side_effect=fetch_message)
    thread.history = history
    client = Mock(spec=discord.Client)
    client.user = bot_user
    client.get_channel.return_value = thread
    plan = DurableDiscordRenderPlan(("Recovered final response",), ())
    renderer = Mock(
        artifact_root=tmp_path,
        retention_days=30,
        render_markdown=AsyncMock(return_value=plan),
    )
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=renderer,
        signer=Mock(),
    )

    fallback = await transport.deliver(running)
    assert fallback.discord_message_id == "602"
    assert thread.send.await_count == 1
    replacement = delivered_messages[0]
    assert _message_has_delivery_marker(
        replacement.content,
        running.delivery_marker,
    )

    repository.retry_outbox(
        running.id,
        lease_owner=running.lease_owner,
        lease_attempt=running.attempts,
        error_code="fallback_send_ack_lost",
        next_attempt_at=0,
    )
    if progress_state == "reconciling":
        with storage_context.store.transaction() as connection:
            connection.execute(
                "UPDATE discord_outbox SET state = 'reconciling' WHERE id = ?",
                (running.id,),
            )
    repository.terminal_turn(
        turn.id,
        target=TurnState.INTERRUPTED,
        terminal_code="fallback_progress_ack_lost",
    )
    preserved = storage_context.store.query_one(
        "SELECT state FROM discord_outbox WHERE id = ?",
        (running.id,),
    )
    assert preserved is not None
    assert preserved["state"] == progress_state

    worker = OutboxWorker(
        repository=repository,
        transport=transport,
        worker_id="recovered-progress-worker",
    )

    assert await worker.drain_once()
    running_row = storage_context.store.query_one(
        "SELECT state FROM discord_outbox WHERE id = ?",
        (running.id,),
    )
    assert running_row is not None
    assert running_row["state"] == "sent"
    assert repository.turn_progress_message(turn.id) == "602"
    assert thread.send.await_count == 1

    assert await worker.drain_once()
    replacement.edit.assert_awaited_once()
    assert thread.send.await_count == 1

    assert await worker.drain_once()
    assert thread.send.await_count == 3

    assert await worker.drain_once()
    replacement.delete.assert_awaited_once_with()
    assert not await worker.drain_once()
    assert sum(
        1 for call in thread.send.await_args_list if "embed" in call.kwargs
    ) == 1
    view = storage_context.store.query_one(
        "SELECT discord_message_id, cleanup_state, deleted_at "
        "FROM turn_progress_views WHERE turn_id = ?",
        (turn.id,),
    )
    assert view is not None
    assert view["discord_message_id"] is None
    assert view["cleanup_state"] == "deleted"
    assert view["deleted_at"] is not None


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
        render_markdown=AsyncMock(return_value=plan),
    )
    repository = Mock()
    repository.render_plan.return_value = None
    repository.persist_render_plan.return_value = RenderPlanRecord(
        turn_id="turn-final",
        source_sha256=sha256_text("ignored"),
        plan_json=canonical_json(plan.to_payload(tmp_path)),
        retention_until=1,
    )
    thread = Mock(spec=discord.Thread)
    thread.id = 300
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
        volatile_turns=_volatile_final("turn-final", "ignored"),
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
        volatile_turns=_volatile_preview("turn-rich", "Ordinary assistant text"),
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
    existing.content = thread.send.await_args.args[0]
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
    assert "Ordinary assistant text" in existing.edit.await_args.kwargs["content"]


def _claim_terminal_progress_cleanup(
    storage_context: StorageContext,
    suffix: str,
) -> tuple[str, OutboxRecord]:
    repository = storage_context.repository
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text=f"cleanup validation {suffix}"),
        input_message_id=f"cleanup-validation-{suffix}",
    )
    repository.request_cancel(turn.id, origin=InterruptOrigin.USER)
    while True:
        record = repository.claim_outbox(worker_id=f"cleanup-{suffix}-worker")
        assert record is not None
        payload = json.loads(record.payload_json)
        if record.operation == "delete":
            return turn.id, record
        if payload.get("kind") == "turn_progress":
            repository.ack_outbox(
                record.id,
                lease_owner=record.lease_owner,
                lease_attempt=record.attempts,
                discord_message_id="601",
                turn_progress_id=turn.id,
            )
            continue
        assert payload.get("kind") == "turn_final"
        repository.ack_outbox(
            record.id,
            lease_owner=record.lease_owner,
            lease_attempt=record.attempts,
        )


@pytest.mark.asyncio
async def test_turn_progress_delete_uses_only_the_trusted_bot_message_id() -> None:
    message = Mock(spec=discord.Message)
    message.id = 601
    message.author = Mock(id=999)
    message.delete = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(return_value=message)
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )
    payload = {
        "kind": "turn_progress_delete",
        "turn_id": "turn-delete",
    }

    result = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    ).deliver(
        OutboxRecord(
            id="progress-delete",
            destination_key="thread:777",
            operation="delete",
            payload_json=canonical_json(payload),
            delivery_marker="progress-delete",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert result == DeliveryResult()
    repository.turn_progress_delete_target.assert_called_once_with(
        "progress-delete"
    )
    client.get_channel.assert_called_once_with(300)
    thread.fetch_message.assert_awaited_once_with(601)
    message.delete.assert_awaited_once_with()
    assert "message_id" not in payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper",
    ("turn", "destination", "dependency", "state"),
)
async def test_turn_progress_delete_rejects_tampered_cleanup_identity_without_side_effect(
    storage_context: StorageContext,
    tamper: str,
) -> None:
    turn_id, cleanup = _claim_terminal_progress_cleanup(
        storage_context,
        tamper,
    )
    with storage_context.store.transaction() as connection:
        if tamper == "turn":
            connection.execute(
                "UPDATE discord_outbox SET payload_json = ? WHERE id = ?",
                (
                    canonical_json(
                        {
                            "kind": "turn_progress_delete",
                            "turn_id": "tampered-turn",
                        }
                    ),
                    cleanup.id,
                ),
            )
        elif tamper == "destination":
            connection.execute(
                "UPDATE discord_outbox SET destination_key = ? WHERE id = ?",
                ("thread:999", cleanup.id),
            )
        elif tamper == "dependency":
            connection.execute(
                "UPDATE discord_outbox SET depends_on_outbox_id = NULL WHERE id = ?",
                (cleanup.id,),
            )
        else:
            connection.execute(
                "UPDATE discord_outbox SET state = 'retry' WHERE id = ?",
                (cleanup.id,),
            )

    message = Mock(spec=discord.Message)
    message.author = Mock(id=999)
    message.delete = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(return_value=message)
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread

    with pytest.raises(DeliveryError) as raised:
        await DiscordOutboxTransport(
            client=client,
            repository=storage_context.repository,
            renderer=Mock(),
            signer=Mock(),
        ).deliver(cleanup)

    assert raised.value.code == "turn_progress_delete_target_invalid"
    assert raised.value.permanent is True
    client.get_channel.assert_not_called()
    client.fetch_channel.assert_not_called()
    thread.fetch_message.assert_not_awaited()
    message.delete.assert_not_awaited()
    view = storage_context.store.query_one(
        "SELECT discord_message_id, cleanup_state FROM turn_progress_views "
        "WHERE turn_id = ?",
        (turn_id,),
    )
    assert view is not None
    assert dict(view) == {
        "discord_message_id": "601",
        "cleanup_state": "delete_pending",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("record_state", ("pending", "reconciling"))
async def test_turn_progress_delete_not_found_is_idempotent_success(
    record_state: str,
) -> None:
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(
        side_effect=discord.NotFound(
            Mock(status=404, reason="deleted"),
            "deleted",
        )
    )
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )

    result = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    ).deliver(
        OutboxRecord(
            id=f"progress-delete-{record_state}",
            destination_key="thread:300",
            operation="delete",
            payload_json=canonical_json(
                {
                    "kind": "turn_progress_delete",
                    "turn_id": "turn-delete",
                }
            ),
            delivery_marker="progress-delete",
            state=record_state,
            attempts=1,
            lease_owner="worker",
        )
    )

    assert result == DeliveryResult()
    thread.fetch_message.assert_awaited_once_with(601)


@pytest.mark.asyncio
async def test_turn_progress_delete_missing_destination_is_idempotent_success() -> None:
    client = Mock(spec=discord.Client)
    client.get_channel.return_value = None
    client.fetch_channel = AsyncMock(
        side_effect=discord.NotFound(
            Mock(status=404, reason="deleted"),
            "deleted",
        )
    )
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )

    result = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    ).deliver(
        OutboxRecord(
            id="progress-delete-missing-destination",
            destination_key="thread:300",
            operation="delete",
            payload_json=canonical_json(
                {
                    "kind": "turn_progress_delete",
                    "turn_id": "turn-delete",
                }
            ),
            delivery_marker="progress-delete-missing-destination",
            state="pending",
            attempts=1,
            lease_owner="worker",
        )
    )

    assert result == DeliveryResult()
    client.fetch_channel.assert_awaited_once_with(300)


@pytest.mark.asyncio
async def test_turn_progress_delete_deleted_archived_thread_is_idempotent_success() -> None:
    thread = Mock(spec=discord.Thread)
    thread.archived = True
    thread.locked = False
    thread.edit = AsyncMock(
        side_effect=discord.NotFound(
            Mock(status=404, reason="deleted"),
            "deleted",
        )
    )
    thread.fetch_message = AsyncMock()
    client = Mock(spec=discord.Client)
    client.get_channel.return_value = thread
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )

    result = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    ).deliver(
        OutboxRecord(
            id="progress-delete-deleted-thread",
            destination_key="thread:300",
            operation="delete",
            payload_json=canonical_json(
                {
                    "kind": "turn_progress_delete",
                    "turn_id": "turn-delete",
                }
            ),
            delivery_marker="progress-delete-deleted-thread",
            state="pending",
            attempts=1,
            lease_owner="worker",
        )
    )

    assert result == DeliveryResult()
    thread.edit.assert_awaited_once_with(archived=False)
    thread.fetch_message.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("resolution", ("lookup", "unarchive"))
@pytest.mark.parametrize(
    ("status", "permanent"),
    ((403, True), (429, False), (503, False)),
)
async def test_turn_progress_delete_destination_resolution_classifies_failures(
    resolution: str,
    status: int,
    permanent: bool,
) -> None:
    response = Mock(status=status, reason="resolution failed")
    error: discord.HTTPException
    if status == 403:
        error = discord.Forbidden(response, "resolution forbidden")
    else:
        error = discord.HTTPException(response, "resolution failed")
    if status == 429:
        error.retry_after = 3.5

    client = Mock(spec=discord.Client)
    if resolution == "lookup":
        client.get_channel.return_value = None
        client.fetch_channel = AsyncMock(side_effect=error)
    else:
        thread = Mock(spec=discord.Thread)
        thread.archived = True
        thread.locked = False
        thread.edit = AsyncMock(side_effect=error)
        thread.fetch_message = AsyncMock()
        client.get_channel.return_value = thread
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )

    with pytest.raises(DeliveryError) as raised:
        await DiscordOutboxTransport(
            client=client,
            repository=repository,
            renderer=Mock(),
            signer=Mock(),
        ).deliver(
            OutboxRecord(
                id=f"progress-delete-{resolution}-{status}",
                destination_key="thread:300",
                operation="delete",
                payload_json=canonical_json(
                    {
                        "kind": "turn_progress_delete",
                        "turn_id": "turn-delete",
                    }
                ),
                delivery_marker="progress-delete-resolution-failure",
                state="pending",
                attempts=1,
                lease_owner="worker",
            )
        )

    assert raised.value.permanent is permanent
    if status == 429:
        assert raised.value.retry_after == 3.5


@pytest.mark.asyncio
async def test_turn_progress_delete_racing_not_found_is_idempotent_success() -> None:
    message = Mock(spec=discord.Message)
    message.author = Mock(id=999)
    message.delete = AsyncMock(
        side_effect=discord.NotFound(
            Mock(status=404, reason="deleted"),
            "deleted",
        )
    )
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(return_value=message)
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )

    result = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    ).deliver(
        OutboxRecord(
            id="progress-delete-race",
            destination_key="thread:300",
            operation="delete",
            payload_json=canonical_json(
                {
                    "kind": "turn_progress_delete",
                    "turn_id": "turn-delete",
                }
            ),
            delivery_marker="progress-delete-race",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert result == DeliveryResult()
    message.delete.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_turn_progress_delete_without_message_id_skips_discord() -> None:
    client = Mock(spec=discord.Client)
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", None)
    )

    result = await DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    ).deliver(
        OutboxRecord(
            id="progress-delete-empty",
            destination_key="thread:300",
            operation="delete",
            payload_json=canonical_json(
                {
                    "kind": "turn_progress_delete",
                    "turn_id": "turn-delete",
                }
            ),
            delivery_marker="progress-delete-empty",
            state="pending",
            attempts=0,
            lease_owner="worker",
        )
    )

    assert result == DeliveryResult()
    client.get_channel.assert_not_called()
    client.fetch_channel.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "permanent"),
    ((403, True), (429, False), (503, False)),
)
async def test_turn_progress_delete_classifies_discord_failures(
    status: int,
    permanent: bool,
) -> None:
    response = Mock(status=status, reason="delete failed")
    error: discord.HTTPException
    if status == 403:
        error = discord.Forbidden(response, "delete forbidden")
    else:
        error = discord.HTTPException(response, "delete failed")
    if status == 429:
        error.retry_after = 3.5
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(side_effect=error)
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    repository = Mock()
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )

    with pytest.raises(DeliveryError) as raised:
        await DiscordOutboxTransport(
            client=client,
            repository=repository,
            renderer=Mock(),
            signer=Mock(),
        ).deliver(
            OutboxRecord(
                id=f"progress-delete-{status}",
                destination_key="thread:300",
                operation="delete",
                payload_json=canonical_json(
                    {
                        "kind": "turn_progress_delete",
                        "turn_id": "turn-delete",
                    }
                ),
                delivery_marker="progress-delete-failure",
                state="pending",
                attempts=1,
                lease_owner="worker",
            )
        )

    assert raised.value.permanent is permanent
    if status == 429:
        assert raised.value.retry_after == 3.5


@pytest.mark.asyncio
async def test_turn_progress_delete_rejects_payload_message_id_and_non_bot_target() -> None:
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    repository = Mock()
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=Mock(),
    )
    repository.turn_progress_delete_target.side_effect = InvariantError(
        "invalid cleanup identity"
    )
    with pytest.raises(DeliveryError) as arbitrary_id:
        await transport.deliver(
            OutboxRecord(
                id="progress-delete-untrusted",
                destination_key="thread:300",
                operation="delete",
                payload_json=canonical_json(
                    {
                        "kind": "turn_progress_delete",
                        "turn_id": "turn-delete",
                        "message_id": "777",
                    }
                ),
                delivery_marker="progress-delete-untrusted",
                state="pending",
                attempts=0,
                lease_owner="worker",
            )
        )
    assert arbitrary_id.value.permanent is True
    repository.turn_progress_delete_target.assert_called_once_with(
        "progress-delete-untrusted"
    )

    message = Mock(spec=discord.Message)
    message.author = Mock(id=123)
    message.delete = AsyncMock()
    thread = Mock(spec=discord.Thread)
    thread.archived = False
    thread.locked = False
    thread.fetch_message = AsyncMock(return_value=message)
    client.get_channel.return_value = thread
    repository.turn_progress_delete_target.side_effect = None
    repository.turn_progress_delete_target.return_value = (
        TurnProgressDeleteTarget("thread:300", "601")
    )
    with pytest.raises(DeliveryError) as author_mismatch:
        await transport.deliver(
            OutboxRecord(
                id="progress-delete-wrong-author",
                destination_key="thread:300",
                operation="delete",
                payload_json=canonical_json(
                    {
                        "kind": "turn_progress_delete",
                        "turn_id": "turn-delete",
                    }
                ),
                delivery_marker="progress-delete-wrong-author",
                state="pending",
                attempts=0,
                lease_owner="worker",
            )
        )
    assert author_mismatch.value.permanent is True
    message.delete.assert_not_awaited()


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
        defer=AsyncMock(),
        send_message=AsyncMock(),
        is_done=Mock(return_value=True),
    )
    interaction.followup = Mock(send=AsyncMock())

    await bot.on_interaction(interaction)

    attachment.read.assert_awaited_once()
    interaction.response.defer.assert_awaited_once_with(ephemeral=True)
    sent = interaction.followup.send.await_args
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


def test_archive_command_requires_archive_and_unarchive_capabilities(
    tmp_path: Path,
) -> None:
    manifest = capability_manifest()
    optional = dict(manifest.optional)
    optional["thread.archive"] = True
    optional["thread.unarchive"] = False
    bot = CodexDBot(
        config=AppConfig(
            paths=AppPaths(tmp_path / "data", tmp_path / "logs"),
            discord=DiscordConfig(
                guild_id=100,
                owner_user_id=400,
                allowed_user_ids=frozenset({400}),
            ),
        ),
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
        capability_manifest=replace(manifest, optional=optional),
        boot_id="archive-gate",
    )

    bot._register_commands()

    session = next(
        command
        for command in bot.tree.get_commands(guild=discord.Object(id=100))
        if command.name == "session"
    )
    assert "archive" not in {command.name for command in session.commands}


def test_long_error_response_preserves_unicode_graphemes() -> None:
    family = "👩‍👩‍👧‍👦"

    response = _bounded_response("error: " + ("界" * 1890) + family + ("x" * 100))

    assert len(response) <= 1900
    assert not response.endswith("\u200d")
    assert response.endswith("…")


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
@pytest.mark.parametrize(
    ("raw_content", "attachment_metadata", "expected_text", "expected_image_hint"),
    (
        ("<@999> fix login", (), "fix login", False),
        (
            "<@999> inspect files",
            (("notes.txt", "text/plain"),),
            "inspect files",
            False,
        ),
        ("<@999>", (("capture.bin", "image/png"),), "", True),
        ("<@999>", (("capture.PNG", "application/octet-stream"),), "", True),
        ("<@999>", (("notes.txt", "application/octet-stream"),), "", False),
        (
            "<@999>",
            (
                ("notes.txt", "text/plain"),
                ("capture.webp", "application/octet-stream"),
                ("brief.pdf", "application/pdf"),
            ),
            "",
            True,
        ),
    ),
)
async def test_channel_mention_passes_title_inputs_to_repository(
    tmp_path: Path,
    raw_content: str,
    attachment_metadata: tuple[tuple[str, str | None], ...],
    expected_text: str,
    expected_image_hint: bool,
) -> None:
    repository = Mock()
    bot = _test_bot(tmp_path, repository=repository)
    bot.sessions.resolve_project_for_channel = AsyncMock(
        return_value=SimpleNamespace(project=SimpleNamespace(id="project"))
    )
    bot_user = Mock(id=999, bot=True)
    bot._connection.user = bot_user
    channel = Mock(spec=discord.TextChannel)
    channel.id = 200
    channel.send = AsyncMock()
    message = Mock(spec=discord.Message)
    message.id = 910
    message.author = Mock(id=400, bot=False)
    message.webhook_id = None
    message.guild = Mock(id=100)
    message.channel = channel
    message.content = raw_content
    message.mentions = [bot_user]
    message.attachments = [
        SimpleNamespace(
            id=501 + index,
            filename=filename,
            size=42,
            content_type=content_type,
        )
        for index, (filename, content_type) in enumerate(attachment_metadata)
    ]

    await bot._handle_message(message)

    request = repository.request_thread_creation.call_args.kwargs
    assert request["first_request_text"] == expected_text
    assert request["has_image_attachment"] is expected_image_hint
    assert request["content_hash"] == sha256_text(expected_text)
    channel.send.assert_not_awaited()


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
    bot._attachment_ingestor = Mock(
        ingest=AsyncMock(return_value=DiscordAttachmentIngestResult()),
        cleanup=Mock(),
    )

    async def enqueue(**kwargs: Any) -> object:
        return storage_context.repository.enqueue_turn(**kwargs)

    bot.turns.enqueue = AsyncMock(side_effect=enqueue)

    await bot._handle_message(message, backfill=True)
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
    message.content = "<@999> edited after durable acceptance"
    await bot._handle_message(message, backfill=True)

    ingress = storage_context.repository.get_ingress_message("903")
    assert ingress.state == "ready"
    assert ingress.discovery_kind == "backfill"
    assert ingress.turn_id is not None
    conversation = storage_context.repository.conversation_for_thread(903)
    assert conversation is not None
    assert conversation.project_id == storage_context.project.id
    turn = storage_context.repository.get_turn(ingress.turn_id)
    assert turn.input_summary == "[content not retained; 19 bytes]"
    assert turn.input_message_id == "903"
    assert bot.turns.enqueue.await_count == 1
    message.create_thread.assert_awaited_once()
    created_name = message.create_thread.await_args.kwargs["name"]
    assert created_name.startswith("inspect the project · ")
    assert len(created_name.rsplit(" · ", 1)[1]) == 4
    assert 1 <= len(created_name) <= 100


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
    bot._attachment_ingestor = Mock(
        ingest=AsyncMock(return_value=DiscordAttachmentIngestResult()),
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
        first_request_text="reconcile existing thread",
        has_image_attachment=False,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        boot_id="boot",
    )
    record = storage_context.repository.claim_outbox(worker_id="worker")
    assert record is not None
    ingress = storage_context.repository.get_ingress_message("302")
    payload = json.loads(record.payload_json)
    assert payload["name_strategy"] == "starter_message"
    assert payload["name_suffix"] == ingress.id[:4]
    assert "reconcile existing thread" not in record.payload_json
    channel = Mock(spec=discord.TextChannel)
    channel.id = 200
    starter = Mock(spec=discord.Message)
    starter.create_thread = AsyncMock()
    channel.fetch_message = AsyncMock(return_value=starter)
    thread = Mock(spec=discord.Thread)
    thread.id = 302
    thread.archived = False
    thread.locked = False
    thread.edit = AsyncMock()
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
    channel.fetch_message.assert_not_awaited()
    starter.create_thread.assert_not_awaited()
    thread.edit.assert_not_awaited()


@pytest.mark.asyncio
async def test_thread_creation_outbox_fetches_uncached_existing_remote_thread(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.request_thread_creation(
        discord_message_id="307",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text="reconcile uncached existing thread",
        has_image_attachment=False,
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
    starter = Mock(spec=discord.Message)
    starter.create_thread = AsyncMock()
    channel.fetch_message = AsyncMock(return_value=starter)
    thread = Mock(spec=discord.Thread)
    thread.id = 307
    thread.archived = False
    thread.locked = False
    thread.edit = AsyncMock()
    client = Mock(spec=discord.Client)
    client.get_channel.side_effect = (
        lambda channel_id: channel if channel_id == 200 else None
    )
    client.fetch_channel = AsyncMock(return_value=thread)
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=Mock(),
        signer=Mock(),
    )
    finalize = storage_context.repository.finalize_thread_creation

    with patch.object(
        storage_context.repository,
        "finalize_thread_creation",
        wraps=finalize,
    ) as finalize_spy:
        result = await transport.deliver(record)

    assert result.discord_message_id == "307"
    assert result.initial_ingress_message_id == "307"
    finalize_spy.assert_called_once_with(
        discord_message_id="307",
        discord_thread_id=307,
        owner_user_id=400,
    )
    assert storage_context.repository.conversation_for_thread(307) is not None
    assert (
        storage_context.repository.get_ingress_message("307").state
        == "pending_preflight"
    )
    client.fetch_channel.assert_awaited_once_with(307)
    channel.fetch_message.assert_not_awaited()
    starter.create_thread.assert_not_awaited()
    thread.edit.assert_not_awaited()


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
async def test_reconciliation_history_window_resends_with_incident(
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
    thread.id = 300
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
    assert (row["state"], row["last_error_code"]) == ("sent", None)
    incident = storage_context.store.query_one(
        "SELECT severity, code, details_json FROM incidents WHERE code = ?",
        ("delivery_duplicate_possible",),
    )
    assert incident is not None
    assert (incident["severity"], incident["code"]) == (
        "warning",
        "delivery_duplicate_possible",
    )
    assert json.loads(incident["details_json"])["destination_channel_id"] == "300"
    thread.send.assert_awaited_once()


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
        first_request_text="recovered request",
        has_image_attachment=False,
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
async def test_modal_preparation_timeout_responds_before_opening_modal(
    tmp_path: Path,
) -> None:
    bot = _test_bot(tmp_path)
    interaction = SimpleNamespace(
        response=SimpleNamespace(
            is_done=lambda: False,
            send_message=AsyncMock(),
            send_modal=AsyncMock(),
        ),
        followup=SimpleNamespace(send=AsyncMock()),
    )

    async def prepare() -> discord.ui.Modal:
        raise TimeoutError

    await bot._open_modal(cast(Any, interaction), prepare)

    interaction.response.send_modal.assert_not_awaited()
    sent = interaction.response.send_message.await_args.args[0]
    assert "interaction_timeout" in sent
    assert not bot._ingress_tasks


@pytest.mark.asyncio
async def test_modal_preparation_reserves_network_response_budget(
    tmp_path: Path,
) -> None:
    bot = _test_bot(tmp_path)
    prepare = AsyncMock(return_value=Mock(spec=discord.ui.Modal))
    interaction = SimpleNamespace(
        created_at=datetime.now(UTC) - timedelta(seconds=2.1),
        response=SimpleNamespace(
            is_done=lambda: False,
            send_message=AsyncMock(),
            send_modal=AsyncMock(),
        ),
        followup=SimpleNamespace(send=AsyncMock()),
    )

    await bot._open_modal(cast(Any, interaction), prepare)

    prepare.assert_not_awaited()
    interaction.response.send_modal.assert_not_awaited()
    sent = interaction.response.send_message.await_args.args[0]
    assert "interaction_timeout" in sent


@pytest.mark.asyncio
async def test_raw_thread_delete_cleans_uncached_conversation(
    tmp_path: Path,
) -> None:
    repository = Mock()
    bot = _test_bot(tmp_path, repository=repository)

    await bot.on_raw_thread_delete(cast(Any, SimpleNamespace(thread_id=9876)))

    repository.mark_conversation_deleted.assert_called_once_with(9876)


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
async def test_command_sync_timeout_degrades_without_blocking_login(
    tmp_path: Path,
) -> None:
    statuses: list[str] = []
    repository = Mock()
    bot = _test_bot(
        tmp_path,
        repository=repository,
        discord_status=statuses.append,
    )
    blocked = asyncio.Event()

    async def sync_scopes(_guild: discord.Object) -> None:
        await blocked.wait()

    bot._sync_command_scopes = AsyncMock(side_effect=sync_scopes)
    bot._command_sync_initial_timeout_seconds = 0.001

    await asyncio.wait_for(
        bot._sync_commands_or_degrade(discord.Object(id=100)),
        timeout=0.1,
    )

    retry = bot._command_sync_task
    assert retry is not None
    assert statuses == ["degraded"]
    repository.record_incident.assert_called()
    bot._command_sync_stop.set()
    await retry


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
    bot.runtimes.account_status_if_loaded = AsyncMock(
        return_value=SimpleNamespace(
            auth_required=False,
            account_type="chatgpt",
            plan_type="pro",
            email="must-not-be-published@example.com",
        )
    )
    bot.session_lifecycle.restore_provider_barriers = AsyncMock()
    bot.turns.restore = AsyncMock()

    await bot.on_ready()

    assert auth_states == ["authenticated"]


@pytest.mark.asyncio
async def test_ready_preflight_does_not_start_unloaded_project_runtimes(
    tmp_path: Path,
) -> None:
    auth_states: list[str] = []
    repository = Mock()
    repository.list_enabled_projects.return_value = [
        SimpleNamespace(id=f"project-{index}") for index in range(20)
    ]
    bot = _test_bot(
        tmp_path,
        repository=repository,
        codex_auth_status=auth_states.append,
    )
    bot.runtimes.account_status_if_loaded = AsyncMock(return_value=None)
    bot.runtimes.ensure = AsyncMock()
    bot.session_lifecycle.restore_provider_barriers = AsyncMock()
    bot.turns.restore = AsyncMock()

    await bot.on_ready()

    assert bot._startup_preflight_complete
    assert auth_states == ["unknown"]
    bot.runtimes.ensure.assert_not_awaited()
    assert bot.runtimes.account_status_if_loaded.await_count == 20


@pytest.mark.asyncio
async def test_ready_preflight_retries_failed_startup_recovery(
    tmp_path: Path,
) -> None:
    repository = Mock()
    repository.list_enabled_projects.return_value = ()
    bot = _test_bot(tmp_path, repository=repository)
    bot._gateway_ready = True
    bot._startup_recovery_retry_seconds = 0.01
    bot.session_lifecycle.restore_provider_barriers = AsyncMock(
        side_effect=[RuntimeError("temporary recovery failure"), None]
    )
    bot.turns.restore = AsyncMock()

    complete = await bot._ready_preflight()
    retry = bot._startup_recovery_task
    assert not complete
    assert retry is not None

    await asyncio.wait_for(retry, timeout=1)

    assert bot._startup_preflight_complete
    assert not bot._ready_preflight_degraded
    bot.turns.restore.assert_awaited_once()


@pytest.mark.asyncio
async def test_ready_preflight_keeps_runtime_degraded_until_runtime_retry(
    tmp_path: Path,
) -> None:
    repository = Mock()
    repository.list_enabled_projects.return_value = [
        SimpleNamespace(id="project-1")
    ]
    bot = _test_bot(tmp_path, repository=repository)
    bot._gateway_ready = True
    bot._startup_recovery_retry_seconds = 0.01
    bot.runtimes.account_status_if_loaded = AsyncMock(
        side_effect=[
            RuntimeError("temporary runtime failure"),
            SimpleNamespace(auth_required=False),
        ]
    )
    bot.session_lifecycle.restore_provider_barriers = AsyncMock()
    bot.turns.restore = AsyncMock()

    complete = await bot._ready_preflight()
    retry = bot._startup_recovery_task
    assert not complete
    assert retry is not None
    assert bot._provider_recovery_complete
    assert not bot._runtime_preflight_complete

    await asyncio.wait_for(retry, timeout=1)

    assert bot._startup_preflight_complete
    assert not bot._ready_preflight_degraded
    assert bot.runtimes.account_status_if_loaded.await_count == 2
    bot.turns.restore.assert_awaited_once()


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
        render_markdown=AsyncMock(return_value=plan),
    )
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
        volatile_turns=_volatile_final("turn-final", "Final response"),
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
async def test_parent_source_link_does_not_overflow_full_final_chunk(
    tmp_path: Path,
) -> None:
    source = "界" * 1900
    plan = DurableDiscordRenderPlan((source,), ())
    renderer = Mock(
        artifact_root=tmp_path,
        retention_days=30,
        render_markdown=AsyncMock(return_value=plan),
    )
    repository = Mock()
    repository.render_plan.return_value = None
    repository.persist_render_plan.return_value = RenderPlanRecord(
        turn_id="turn-long-parent",
        source_sha256=sha256_text(source),
        plan_json=canonical_json(plan.to_payload(tmp_path)),
        retention_until=1,
    )
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(
        side_effect=[Mock(id=1), Mock(id=2), Mock(id=3)]
    )
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=renderer,
        signer=Mock(),
        volatile_turns=_volatile_final("turn-long-parent", source),
    )
    record = OutboxRecord(
        id="long-parent",
        destination_key="thread:300",
        operation="send",
        payload_json=canonical_json(
            {
                "kind": "turn_final",
                "plain_text": source,
                "turn_id": "turn-long-parent",
                "state": "completed",
                "input_message_id": "1234567890123456789",
                "input_channel_id": "2234567890123456789",
                "discord_guild_id": "3234567890123456789",
            }
        ),
        delivery_marker="long-parent",
        state="pending",
        attempts=0,
        lease_owner="test",
    )

    await transport.deliver(record)

    assert len(thread.send.await_args_list) == 3
    sent = [call.args[0] for call in thread.send.await_args_list]
    assert all(len(content) <= 2000 for content in sent)
    assert "Original request" in sent[0]
    assert source in sent[1]


@pytest.mark.asyncio
async def test_in_memory_render_failure_uses_bounded_plain_text_fallback(
    tmp_path: Path,
) -> None:
    plain_text = "The durable answer remains available."
    renderer = Mock(
        artifact_root=tmp_path,
        retention_days=30,
        render_markdown=AsyncMock(
            side_effect=InvariantError("in-memory render failed")
        ),
    )
    repository = Mock()
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
        volatile_turns=_volatile_final("turn-corrupt", plain_text),
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
        summary="Discord in-memory rich rendering failed; bounded text was used",
        turn_id="turn-corrupt",
        details={"stable_code": "volatile_render_failed"},
    )


def test_render_fallback_for_five_chunks_is_bounded_in_memory() -> None:
    source = "\n".join("x" * 600 for _ in range(10))
    chunks = _bounded_plain_text_fallback(source)

    assert len(chunks) == 4
    assert all(len(chunk) <= 1900 for chunk in chunks)
    assert "fallback truncated" in chunks[-1]


def test_plain_text_render_fallback_is_bounded_when_attachment_storage_fails() -> None:
    source = ("持续输出 👩‍👩‍👧‍👦\n" * 2000).strip()

    chunks = _bounded_plain_text_fallback(source)

    assert len(chunks) == 4
    assert all(len(chunk) <= 1900 for chunk in chunks)
    assert "fallback truncated" in chunks[-1]


def test_plain_text_render_fallback_handles_whitespace_only_output() -> None:
    assert _bounded_plain_text_fallback("  \n\t") == (
        "Codex completed, but rich rendering was unavailable.",
    )


def test_remove_bot_trigger_preserves_other_mention_text() -> None:
    message = Mock(spec=discord.Message)
    message.content = "<@42> run this and preserve `<@42>`"
    message.mentions = [Mock(id=42)]

    assert _remove_bot_mention(message, 42) == "run this and preserve `<@42>`"

    message.mentions = []
    assert _remove_bot_mention(message, 42) == message.content
