from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field

import discord

from codexd.errors import CodexDError, SecurityError
from codexd.storage.ingress_reconciliation import IngressCheckpointRepository
from codexd.storage.records import DiscordIngressTargetRecord

logger = logging.getLogger(__name__)

MessageHandler = Callable[[discord.Message, bool], Awaitable[None]]
StatusObserver = Callable[[str], None]

_PAGE_SIZE = 100
_MAX_MESSAGES_PER_SCAN = 500
_MAX_BUFFERED_LIVE = 1_000


@dataclass
class _BufferedMessage:
    message: discord.Message
    completed: asyncio.Future[None]


@dataclass
class _ChannelState:
    ingest_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    gate: asyncio.Lock = field(default_factory=asyncio.Lock)
    scanning: bool = False
    buffered: dict[int, _BufferedMessage] = field(default_factory=dict)


@dataclass(frozen=True)
class _ScanTarget:
    scope: DiscordIngressTargetRecord
    channel: discord.TextChannel | discord.Thread


class DiscordInboundReconciler:
    def __init__(
        self,
        *,
        repository: IngressCheckpointRepository,
        guild_id: int,
        handler: MessageHandler,
        status_observer: StatusObserver | None = None,
        concurrency: int = 2,
        periodic_seconds: float = 60.0,
    ) -> None:
        if concurrency < 1 or periodic_seconds <= 0:
            raise ValueError("Discord inbound reconciliation limits must be positive")
        self._repository = repository
        self._guild_id = guild_id
        self._handler = handler
        self._status_observer = status_observer or (lambda _status: None)
        self._concurrency = concurrency
        self._periodic_seconds = periodic_seconds
        self._states: dict[int, _ChannelState] = {}
        self._trigger_lock = asyncio.Lock()
        self._discovery_ready = asyncio.Event()
        self._discovery_ready.set()
        self._stop = asyncio.Event()
        self._periodic_task: asyncio.Task[None] | None = None
        self._continuation_task: asyncio.Task[None] | None = None
        self._client: discord.Client | None = None

    def start(self, client: discord.Client) -> None:
        self._client = client
        if self._periodic_task is None:
            self._periodic_task = asyncio.create_task(
                self._periodic_loop(),
                name="codexd-discord-inbound-reconciliation",
            )

    async def close(self) -> None:
        self._stop.set()
        self._discovery_ready.set()
        if self._periodic_task is not None:
            self._periodic_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._periodic_task
            self._periodic_task = None
        if self._continuation_task is not None:
            self._continuation_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._continuation_task
            self._continuation_task = None
        for state in self._states.values():
            await self._fail_buffered(
                state,
                RuntimeError("Discord inbound reconciliation stopped"),
            )

    async def process_live(self, message: discord.Message) -> None:
        await self._discovery_ready.wait()
        state = self._states.setdefault(message.channel.id, _ChannelState())
        async with state.gate:
            if state.scanning:
                existing = state.buffered.get(message.id)
                if existing is None:
                    if len(state.buffered) >= _MAX_BUFFERED_LIVE:
                        raise RuntimeError(
                            "Discord live ingress buffer reached its safe limit"
                        )
                    future = asyncio.get_running_loop().create_future()
                    existing = _BufferedMessage(message, future)
                    state.buffered[message.id] = existing
                completion = existing.completed
            else:
                completion = None
        if completion is not None:
            await completion
            return
        async with state.ingest_lock:
            await self._handler(message, False)

    async def known_ingress(
        self,
        message: discord.Message,
    ) -> tuple[str, str] | None:
        if message.guild is None:
            return None
        return await asyncio.to_thread(
            self._repository.known_ingress,
            discord_message_id=str(message.id),
            guild_id=message.guild.id,
            channel_id=message.channel.id,
        )

    async def trigger(self, client: discord.Client, *, reason: str) -> bool:
        del reason
        async with self._trigger_lock:
            if self._stop.is_set():
                return False
            self._client = client
            self._discovery_ready.clear()
            self._status_observer("catching_up")
            targets: tuple[_ScanTarget, ...] = ()
            try:
                targets = await self._discover_targets(client)
                for target in targets:
                    state = self._states.setdefault(
                        target.scope.discord_channel_id,
                        _ChannelState(),
                    )
                    async with state.gate:
                        state.scanning = True
            except Exception:
                self._status_observer("degraded")
                return False
            finally:
                self._discovery_ready.set()
            semaphore = asyncio.Semaphore(self._concurrency)

            async def scan(target: _ScanTarget) -> bool:
                async with semaphore:
                    return await self._scan_target(target)

            results = await asyncio.gather(
                *(scan(target) for target in targets),
                return_exceptions=True,
            )
            healthy = all(result is True for result in results)
            pending_retry = any(state.scanning for state in self._states.values())
            self._status_observer(
                "ready" if healthy else "catching_up" if pending_retry else "degraded"
            )
            if pending_retry and (
                self._continuation_task is None
                or self._continuation_task.done()
            ):
                self._continuation_task = asyncio.create_task(
                    self._continue_after_delay(client),
                    name="codexd-discord-inbound-continuation",
                )
            return healthy

    async def _continue_after_delay(self, client: discord.Client) -> None:
        try:
            while not self._stop.is_set() and client.is_ready():
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=1.0)
                if self._stop.is_set():
                    return
                healthy = await self.trigger(client, reason="continuation")
                if healthy or not any(
                    state.scanning for state in self._states.values()
                ):
                    return
        finally:
            self._continuation_task = None

    async def _discover_targets(self, client: discord.Client) -> tuple[_ScanTarget, ...]:
        guild = client.get_guild(self._guild_id)
        if guild is None:
            raise RuntimeError("configured Discord guild is unavailable")
        targets: dict[int, _ScanTarget] = {}
        for channel in guild.text_channels:
            targets[channel.id] = _ScanTarget(
                DiscordIngressTargetRecord(
                    discord_guild_id=self._guild_id,
                    discord_channel_id=channel.id,
                    scope_kind="parent_channel",
                    conversation_id=None,
                    discord_parent_channel_id=None,
                ),
                channel,
            )
        conversations = await asyncio.to_thread(
            self._repository.conversation_targets,
            guild_id=self._guild_id,
        )
        for scope in conversations:
            resolved_channel = client.get_channel(scope.discord_channel_id)
            if resolved_channel is None:
                resolved_channel = await client.fetch_channel(
                    scope.discord_channel_id
                )
            if (
                not isinstance(resolved_channel, discord.Thread)
                or resolved_channel.guild.id != self._guild_id
                or resolved_channel.parent_id != scope.discord_parent_channel_id
            ):
                raise SecurityError("Discord reconciliation Thread origin changed")
            targets[resolved_channel.id] = _ScanTarget(scope, resolved_channel)
        return tuple(targets[channel_id] for channel_id in sorted(targets))

    async def _scan_target(self, target: _ScanTarget, *, attempt: int = 0) -> bool:
        state = self._states[target.scope.discord_channel_id]
        checkpoint_id: str | None = None
        try:
            async with state.ingest_lock:
                remote_barrier = _last_message_id(target.channel)
                checkpoint = await asyncio.to_thread(
                    self._repository.ensure,
                    target=target.scope,
                    remote_barrier_id=remote_barrier,
                )
                checkpoint_id = checkpoint.id
                scan = await asyncio.to_thread(
                    self._repository.begin_scan,
                    checkpoint.id,
                    remote_barrier_id=remote_barrier,
                )
                if scan.scan_state == "blocked":
                    await self._fail_buffered(
                        state,
                        RuntimeError("Discord history checkpoint is blocked"),
                    )
                    return False
                barrier = scan.in_progress_barrier_id
                after = scan.in_progress_after_id
                if barrier is None or after is None:
                    raise RuntimeError("Discord history scan has no durable barrier")
                caught_up = await self._scan_history(
                    target.channel,
                    checkpoint_id=checkpoint.id,
                    after=after,
                    barrier=barrier,
                )
                if self._stop.is_set():
                    raise asyncio.CancelledError
                if not caught_up:
                    return False
                await asyncio.to_thread(
                    self._repository.record_progress,
                    checkpoint.id,
                    barrier_id=barrier,
                    after_message_id=barrier,
                )
                await asyncio.to_thread(
                    self._repository.complete,
                    checkpoint.id,
                    barrier_id=barrier,
                )
                await self._drain_buffered(state)
                return True
        except asyncio.CancelledError:
            raise
        except SecurityError as exc:
            if checkpoint_id is not None:
                await asyncio.to_thread(
                    self._repository.fail,
                    checkpoint_id,
                    error_code="reconciliation_scope_mismatch",
                    blocked=True,
                )
            await self._fail_buffered(state, exc)
            return False
        except (discord.Forbidden, discord.NotFound) as exc:
            if checkpoint_id is not None:
                await asyncio.to_thread(
                    self._repository.fail,
                    checkpoint_id,
                    error_code=f"discord_http_{exc.status}",
                    blocked=True,
                )
            await self._fail_buffered(state, exc)
            return False
        except (discord.HTTPException, OSError, CodexDError) as exc:
            if checkpoint_id is not None:
                await asyncio.to_thread(
                    self._repository.fail,
                    checkpoint_id,
                    error_code=_error_code(exc),
                    blocked=False,
                )
            logger.warning(
                "Discord inbound reconciliation will retry",
                extra={
                    "stable_code": "discord_inbound_reconciliation_retry",
                    "channel_id": target.scope.discord_channel_id,
                    "error_code": _error_code(exc),
                },
            )
            if attempt < 2 and not self._stop.is_set():
                delay = (
                    float(getattr(exc, "retry_after", 1.0) or 1.0)
                    if isinstance(exc, discord.HTTPException) and exc.status == 429
                    else min(30.0, float(2**attempt))
                )
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, delay))
                if not self._stop.is_set():
                    return await self._scan_target(target, attempt=attempt + 1)
            return False
        except Exception:
            if checkpoint_id is not None:
                await asyncio.to_thread(
                    self._repository.fail,
                    checkpoint_id,
                    error_code="reconciliation_internal_error",
                    blocked=False,
                )
            logger.exception(
                "Discord inbound reconciliation failed unexpectedly",
                extra={"stable_code": "discord_inbound_reconciliation_internal"},
            )
            return False

    async def _scan_history(
        self,
        channel: discord.TextChannel | discord.Thread,
        *,
        checkpoint_id: str,
        after: int,
        barrier: int,
    ) -> bool:
        cursor = after
        processed = 0
        while cursor < barrier and not self._stop.is_set():
            messages = [
                message
                async for message in channel.history(
                    limit=_PAGE_SIZE,
                    after=discord.Object(id=cursor),
                    before=discord.Object(id=barrier + 1),
                    oldest_first=True,
                )
                if cursor < message.id <= barrier
            ]
            messages.sort(key=lambda message: message.id)
            if not messages:
                return True
            for message in messages:
                await self._handler(message, True)
                cursor = message.id
                processed += 1
            await asyncio.to_thread(
                self._repository.record_progress,
                checkpoint_id,
                barrier_id=barrier,
                after_message_id=cursor,
            )
            if len(messages) < _PAGE_SIZE:
                return True
            if processed >= _MAX_MESSAGES_PER_SCAN:
                return cursor >= barrier
        if self._stop.is_set():
            raise asyncio.CancelledError
        return True

    async def _drain_buffered(self, state: _ChannelState) -> None:
        while True:
            async with state.gate:
                pending = tuple(
                    state.buffered[key] for key in sorted(state.buffered)
                )
                state.buffered.clear()
                if not pending:
                    state.scanning = False
                    return
            for buffered in pending:
                try:
                    await self._handler(buffered.message, False)
                except Exception as exc:
                    if not buffered.completed.done():
                        buffered.completed.set_exception(exc)
                else:
                    if not buffered.completed.done():
                        buffered.completed.set_result(None)

    async def _fail_buffered(
        self,
        state: _ChannelState,
        error: BaseException,
    ) -> None:
        async with state.gate:
            pending = tuple(state.buffered.values())
            state.buffered.clear()
            state.scanning = False
        for buffered in pending:
            if not buffered.completed.done():
                buffered.completed.set_exception(error)

    async def _periodic_loop(self) -> None:
        while not self._stop.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._periodic_seconds,
                )
            if self._stop.is_set():
                return
            client = self._client
            if client is None or not client.is_ready():
                continue
            try:
                await self.trigger(client, reason="periodic")
            except Exception:
                logger.exception(
                    "Periodic Discord inbound reconciliation failed",
                    extra={"stable_code": "discord_inbound_periodic_failed"},
                )


def _last_message_id(channel: discord.TextChannel | discord.Thread) -> int | None:
    value = channel.last_message_id
    return int(value) if value is not None else None


def _error_code(exc: BaseException) -> str:
    if isinstance(exc, discord.HTTPException):
        return f"discord_http_{exc.status}"
    return getattr(exc, "code", type(exc).__name__)[:128]
