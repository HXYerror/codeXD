from __future__ import annotations

import asyncio
from unittest.mock import Mock

import discord
import pytest
from conftest import StorageContext

from codexd.domain.turns import TurnInput, TurnSource, TurnState
from codexd.errors import SecurityError
from codexd.storage.ingress_reconciliation import IngressCheckpointRepository
from codexd.storage.records import DiscordIngressTargetRecord
from codexd.transport.discord.reconciliation import DiscordInboundReconciler

_DISCORD_EPOCH_MS = 1_420_070_400_000


def _target(storage_context: StorageContext) -> DiscordIngressTargetRecord:
    return DiscordIngressTargetRecord(
        discord_guild_id=100,
        discord_channel_id=300,
        scope_kind="conversation_thread",
        conversation_id=storage_context.conversation.id,
        discord_parent_channel_id=200,
    )


def _seed_activation(storage_context: StorageContext) -> None:
    with storage_context.store.transaction() as connection:
        connection.execute(
            "UPDATE discord_ingress_feature_state SET activated_at = ? WHERE singleton = 1",
            (_DISCORD_EPOCH_MS,),
        )


def test_checkpoint_progress_is_not_a_completed_barrier(
    storage_context: StorageContext,
) -> None:
    _seed_activation(storage_context)
    repository = IngressCheckpointRepository(storage_context.store)
    checkpoint = repository.ensure(
        target=_target(storage_context),
        remote_barrier_id=102,
    )
    assert checkpoint.last_scanned_message_id == 1
    scan = repository.begin_scan(checkpoint.id, remote_barrier_id=102)
    assert (scan.in_progress_after_id, scan.in_progress_barrier_id) == (1, 102)

    repository.record_progress(
        checkpoint.id,
        barrier_id=102,
        after_message_id=101,
    )
    resumed = repository.begin_scan(checkpoint.id, remote_barrier_id=103)

    assert resumed.last_scanned_message_id == 1
    assert (resumed.in_progress_after_id, resumed.in_progress_barrier_id) == (101, 102)
    completed = repository.complete(checkpoint.id, barrier_id=102)
    assert completed.last_scanned_message_id == 102
    assert completed.in_progress_barrier_id is None
    next_scan = repository.begin_scan(checkpoint.id, remote_barrier_id=103)
    assert (next_scan.in_progress_after_id, next_scan.in_progress_barrier_id) == (
        102,
        103,
    )


def test_checkpoint_scope_and_existing_ingress_fail_closed(
    storage_context: StorageContext,
) -> None:
    _seed_activation(storage_context)
    repository = IngressCheckpointRepository(storage_context.store)
    repository.ensure(target=_target(storage_context), remote_barrier_id=101)
    with pytest.raises(SecurityError):
        repository.ensure(
            target=DiscordIngressTargetRecord(
                discord_guild_id=100,
                discord_channel_id=300,
                scope_kind="parent_channel",
                conversation_id=None,
                discord_parent_channel_id=None,
            ),
            remote_barrier_id=101,
        )
    storage_context.repository.claim_ingress_message(
        discord_message_id="101",
        content_hash="content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        discord_guild_id=100,
        discord_channel_id=300,
        requested_by_user_id=400,
        boot_id="boot",
    )
    assert repository.known_ingress(
        discord_message_id="101",
        guild_id=100,
        channel_id=300,
    )
    with pytest.raises(SecurityError):
        repository.known_ingress(
            discord_message_id="101",
            guild_id=100,
            channel_id=301,
        )


def test_backfilled_initial_preflight_survives_restart_for_rest_revalidation(
    storage_context: StorageContext,
) -> None:
    storage_context.repository.request_thread_creation(
        discord_message_id="501",
        content_hash="content",
        attachment_manifest_hash="attachments",
        first_request_text="backfilled mention",
        has_image_attachment=False,
        project_id=storage_context.project.id,
        discord_guild_id=100,
        discord_channel_id=200,
        owner_user_id=400,
        discovery_kind="backfill",
        boot_id="old-boot",
    )
    creation = storage_context.repository.claim_outbox(worker_id="worker")
    assert creation is not None
    storage_context.repository.finalize_thread_creation(
        discord_message_id="501",
        discord_thread_id=501,
        owner_user_id=400,
    )
    storage_context.repository.ack_outbox(
        creation.id,
        lease_owner=creation.lease_owner,
        lease_attempt=creation.attempts,
        discord_message_id="501",
    )

    recovery = storage_context.repository.recover_startup(
        current_boot_id="new-boot"
    )

    ingress = storage_context.repository.get_ingress_message("501")
    assert recovery["rejected_ingress"] == 0
    assert ingress.state == "pending_preflight"
    assert ingress.discovery_kind == "backfill"
    assert storage_context.repository.pending_backfill_preflight_ids() == ("501",)


def test_queued_backfill_turn_survives_restart_before_provider_start(
    storage_context: StorageContext,
) -> None:
    repository = storage_context.repository
    repository.claim_ingress_message(
        discord_message_id="601",
        content_hash="backfill-content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        discord_guild_id=100,
        discord_channel_id=300,
        requested_by_user_id=400,
        discovery_kind="backfill",
        boot_id="old-boot",
    )
    backfill = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="backfilled queued work"),
        input_message_id="601",
        ingress_message_id="601",
        requested_by_user_id=400,
    )
    repository.claim_ingress_message(
        discord_message_id="602",
        content_hash="live-content",
        attachment_manifest_hash="attachments",
        project_id=storage_context.project.id,
        conversation_id=storage_context.conversation.id,
        discord_guild_id=100,
        discord_channel_id=300,
        requested_by_user_id=400,
        discovery_kind="live",
        boot_id="old-boot",
    )
    live = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="live queued work"),
        input_message_id="602",
        ingress_message_id="602",
        requested_by_user_id=400,
    )

    repository.recover_startup(current_boot_id="new-boot")

    assert repository.get_turn(backfill.id).state is TurnState.QUEUED
    assert repository.get_turn(live.id).state is TurnState.INTERRUPTED


def _message(
    message_id: int,
    channel: discord.Thread,
) -> discord.Message:
    message = Mock(spec=discord.Message)
    message.id = message_id
    message.channel = channel
    message.guild = channel.guild
    message.author = Mock(id=400, bot=False)
    message.webhook_id = None
    message.content = f"message {message_id}"
    message.attachments = []
    message.mentions = []
    return message


def _discord_scope(
    storage_context: StorageContext,
    *,
    last_message_id: int,
    history_messages: list[discord.Message],
) -> tuple[Mock, discord.Thread]:
    guild = Mock(spec=discord.Guild)
    guild.id = 100
    guild.text_channels = []
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.guild = guild
    thread.parent_id = 200
    thread.last_message_id = last_message_id

    async def history(**kwargs: object):
        after = kwargs["after"]
        before = kwargs["before"]
        limit = int(kwargs["limit"])
        selected = [
            message
            for message in history_messages
            if after.id < message.id < before.id
        ][:limit]
        for message in selected:
            yield message

    thread.history = history
    for message in history_messages:
        message.channel = thread
        message.guild = guild
    client = Mock(spec=discord.Client)
    client.get_guild.return_value = guild
    client.get_channel.side_effect = lambda channel_id: thread if channel_id == 300 else None
    client.fetch_channel = Mock()
    client.is_ready.return_value = True
    return client, thread


@pytest.mark.asyncio
async def test_live_message_waits_behind_history_and_preserves_snowflake_order(
    storage_context: StorageContext,
) -> None:
    _seed_activation(storage_context)
    placeholder_thread = Mock(spec=discord.Thread)
    history_messages = [
        _message(101, placeholder_thread),
        _message(102, placeholder_thread),
    ]
    client, thread = _discord_scope(
        storage_context,
        last_message_id=102,
        history_messages=history_messages,
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    accepted: list[tuple[int, bool]] = []

    async def handle(message: discord.Message, backfill: bool) -> None:
        accepted.append((message.id, backfill))
        if message.id == 101:
            entered.set()
            await release.wait()

    reconciler = DiscordInboundReconciler(
        repository=IngressCheckpointRepository(storage_context.store),
        guild_id=100,
        handler=handle,
        periodic_seconds=3600,
    )
    trigger = asyncio.create_task(reconciler.trigger(client, reason="ready"))
    await asyncio.wait_for(entered.wait(), timeout=1)
    live = _message(103, thread)
    thread.last_message_id = 103
    live_task = asyncio.create_task(reconciler.process_live(live))
    await asyncio.sleep(0)
    assert not live_task.done()
    release.set()

    assert await trigger
    await live_task
    assert accepted == [(101, True), (102, True), (103, False)]
    checkpoint = storage_context.store.query_one(
        "SELECT last_scanned_message_id, scan_state FROM discord_ingress_checkpoints"
    )
    assert checkpoint is not None
    assert (int(checkpoint["last_scanned_message_id"]), checkpoint["scan_state"]) == (
        102,
        "idle",
    )
    await reconciler.close()


@pytest.mark.asyncio
async def test_multi_page_history_is_oldest_first_and_checkpointed(
    storage_context: StorageContext,
) -> None:
    _seed_activation(storage_context)
    placeholder_thread = Mock(spec=discord.Thread)
    messages = [_message(message_id, placeholder_thread) for message_id in range(2, 207)]
    client, _thread = _discord_scope(
        storage_context,
        last_message_id=206,
        history_messages=messages,
    )
    accepted: list[int] = []

    async def handle(message: discord.Message, _backfill: bool) -> None:
        accepted.append(message.id)

    reconciler = DiscordInboundReconciler(
        repository=IngressCheckpointRepository(storage_context.store),
        guild_id=100,
        handler=handle,
        periodic_seconds=3600,
    )

    assert await reconciler.trigger(client, reason="startup")
    assert accepted == list(range(2, 207))
    checkpoint = storage_context.store.query_one(
        "SELECT last_scanned_message_id FROM discord_ingress_checkpoints"
    )
    assert checkpoint is not None
    assert int(checkpoint["last_scanned_message_id"]) == 206
    await reconciler.close()


@pytest.mark.asyncio
async def test_failed_page_does_not_advance_barrier_and_retry_rediscovers_safely(
    storage_context: StorageContext,
) -> None:
    _seed_activation(storage_context)
    placeholder_thread = Mock(spec=discord.Thread)
    messages = [_message(101, placeholder_thread), _message(102, placeholder_thread)]
    client, _thread = _discord_scope(
        storage_context,
        last_message_id=102,
        history_messages=messages,
    )
    attempts = 0
    accepted: list[int] = []

    async def handle(message: discord.Message, _backfill: bool) -> None:
        nonlocal attempts
        accepted.append(message.id)
        if message.id == 102 and attempts == 0:
            attempts += 1
            raise RuntimeError("simulated crash after partial page acceptance")

    reconciler = DiscordInboundReconciler(
        repository=IngressCheckpointRepository(storage_context.store),
        guild_id=100,
        handler=handle,
        periodic_seconds=3600,
    )

    assert not await reconciler.trigger(client, reason="first")
    failed = storage_context.store.query_one(
        """
        SELECT last_scanned_message_id, in_progress_after_id,
               in_progress_barrier_id, scan_state
        FROM discord_ingress_checkpoints
        """
    )
    assert failed is not None
    assert (
        int(failed["last_scanned_message_id"]),
        int(failed["in_progress_after_id"]),
        int(failed["in_progress_barrier_id"]),
        failed["scan_state"],
    ) == (1, 1, 102, "retry")

    assert await reconciler.trigger(client, reason="retry")
    assert accepted == [101, 102, 101, 102]
    completed = storage_context.store.query_one(
        "SELECT last_scanned_message_id, scan_state FROM discord_ingress_checkpoints"
    )
    assert completed is not None
    assert (int(completed["last_scanned_message_id"]), completed["scan_state"]) == (
        102,
        "idle",
    )
    await reconciler.close()


@pytest.mark.asyncio
async def test_parent_channel_history_discovers_messages_without_requiring_binding(
    storage_context: StorageContext,
) -> None:
    _seed_activation(storage_context)
    guild = Mock(spec=discord.Guild)
    guild.id = 100
    parent = Mock(spec=discord.TextChannel)
    parent.id = 200
    parent.last_message_id = 2
    parent_message = Mock(spec=discord.Message)
    parent_message.id = 2
    parent_message.channel = parent
    parent_message.guild = guild

    async def parent_history(**_kwargs: object):
        yield parent_message

    parent.history = parent_history
    guild.text_channels = [parent]
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.guild = guild
    thread.parent_id = 200
    thread.last_message_id = None

    async def thread_history(**_kwargs: object):
        if False:
            yield parent_message

    thread.history = thread_history
    client = Mock(spec=discord.Client)
    client.get_guild.return_value = guild
    client.get_channel.side_effect = lambda channel_id: thread if channel_id == 300 else parent
    client.is_ready.return_value = True
    discovered: list[tuple[int, int]] = []

    async def handle(message: discord.Message, _backfill: bool) -> None:
        discovered.append((message.channel.id, message.id))

    reconciler = DiscordInboundReconciler(
        repository=IngressCheckpointRepository(storage_context.store),
        guild_id=100,
        handler=handle,
        periodic_seconds=3600,
    )

    assert await reconciler.trigger(client, reason="ready")
    assert discovered == [(200, 2)]
    checkpoints = storage_context.store.query_all(
        """
        SELECT discord_channel_id, scope_kind, last_scanned_message_id
        FROM discord_ingress_checkpoints ORDER BY discord_channel_id
        """
    )
    assert [
        (int(row["discord_channel_id"]), row["scope_kind"])
        for row in checkpoints
    ] == [(200, "parent_channel"), (300, "conversation_thread")]
    await reconciler.close()
