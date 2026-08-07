from __future__ import annotations

import asyncio
import importlib.metadata
import json
import logging
import os
import signal
import uuid
from collections.abc import Awaitable
from contextlib import suppress
from pathlib import Path

import aiohttp
import discord

from codexd.config import AppConfig
from codexd.domain.ids import utc_now_ms
from codexd.errors import ConfigurationError, SecurityError
from codexd.observability.health import HealthReporter
from codexd.observability.logging import configure_logging
from codexd.rendering.discord import DiscordRenderPlanner
from codexd.rendering.media_worker import MediaWorker
from codexd.rendering.tables import TableLimits
from codexd.runtime.codex_sdk import CodexSDKRuntime, capability_manifest
from codexd.runtime.port import CodexRuntime, RuntimeSlotConfig
from codexd.runtime.supervisor import RuntimeSupervisor
from codexd.security.secrets import SecretStore
from codexd.security.signing import ComponentSigner
from codexd.service.containment import create_process_containment
from codexd.service.locking import InstanceLock
from codexd.service.process import current_process_identity
from codexd.storage.projectors import ProjectingEventSink
from codexd.storage.repository import Repository
from codexd.storage.schedules import ScheduleRepository
from codexd.storage.sqlite import SQLiteStore
from codexd.transport.discord.bot import CodexDBot

_DISCORD_INITIAL_RETRY_DELAYS_SECONDS = (1.0, 2.0, 5.0, 10.0, 30.0)
_DISCORD_INITIAL_LOGIN_TIMEOUT_SECONDS = 30.0


def run_daemon(config: AppConfig, bootstrap_token: str | None) -> int:
    return asyncio.run(_run_daemon(config, bootstrap_token))


async def _run_daemon(config: AppConfig, bootstrap_token: str | None) -> int:
    if not config.daemon_ready_for_discord:
        raise ConfigurationError(
            "discord.guild_id, discord.owner_user_id, and one or more "
            "discord.allowed_user_ids are required"
        )
    secrets = SecretStore()
    token = bootstrap_token or secrets.discord_token()
    if not token:
        raise SecurityError(
            "Discord token is not configured; use `codexd auth discord set`"
        )
    existing_database = config.paths.database.exists()
    with InstanceLock(config.paths.data_dir / "durable-keys.lock"):
        projection_key = secrets.projection_key(
            allow_create=not existing_database
        )
        component_key = secrets.component_key(
            allow_create=not existing_database
        )
    configure_logging(config.paths.log_file)
    logger = logging.getLogger(__name__)
    boot_id = str(uuid.uuid4())
    started_at = utc_now_ms()
    process_identity = current_process_identity()
    process_token = process_identity.start_token
    logger.info("daemon starting", extra={"boot_id": boot_id})

    with (
        create_process_containment(),
        InstanceLock(config.paths.instance_lock),
        SQLiteStore(config.paths.database) as store,
    ):
        if existing_database:
            integrity = store.integrity_check()
            foreign_keys = store.foreign_key_check()
            if integrity != "ok" or foreign_keys:
                raise ConfigurationError(
                    "database startup checks failed: "
                    f"integrity={integrity}, foreign_keys={len(foreign_keys)}"
                )
        store.migrate()
        integrity = store.integrity_check()
        foreign_keys = store.foreign_key_check()
        if integrity != "ok" or foreign_keys:
            raise ConfigurationError(
                "database post-migration checks failed: "
                f"integrity={integrity}, foreign_keys={len(foreign_keys)}"
            )
        repository = Repository(store)
        manifest = capability_manifest()
        await asyncio.to_thread(
            repository.acquire_daemon_lease,
            boot_id=boot_id,
            pid=os.getpid(),
            process_start_token=process_token,
            stale_before=started_at - 60_000,
        )
        await asyncio.to_thread(repository.recover_startup, current_boot_id=boot_id)
        neutral_cwd = config.paths.data_dir / "runtime" / "shared"

        async def runtime_factory(
            slot: RuntimeSlotConfig, generation: int
        ) -> CodexRuntime:
            return await CodexSDKRuntime.create(slot=slot, generation=generation)

        home_root = Path.home().resolve(strict=True)
        runtime_allowed_roots = tuple(
            dict.fromkeys((*config.security.allowed_roots, home_root))
        )
        runtimes = RuntimeSupervisor(
            repository=repository,
            factory=runtime_factory,
            topology=config.runtime.topology,
            environment=dict(os.environ),
            environment_hash=_environment_hash(os.environ),
            codex_home=Path(os.environ["CODEX_HOME"])
            if os.environ.get("CODEX_HOME")
            else None,
            neutral_cwd=neutral_cwd,
            allowed_roots=runtime_allowed_roots,
            codex_bin=config.runtime.codex_bin,
        )
        sink = ProjectingEventSink(
            store,
            correlation_key=projection_key,
            stream_update_ms=config.rendering.stream_update_ms,
        )
        from codexd.application.conversation_locks import ConversationLocks
        from codexd.application.schedule_coordinator import ScheduleCoordinator
        from codexd.application.session_coordinator import SessionCoordinator
        from codexd.application.session_lifecycle import SessionLifecycleCoordinator
        from codexd.application.turn_coordinator import TurnCoordinator
        from codexd.storage.retention import RetentionWorker

        conversation_locks = ConversationLocks()
        stop = asyncio.Event()

        def critical_failure(exc: BaseException) -> None:
            logger.critical(
                "daemon critical persistence failure",
                exc_info=(type(exc), exc, exc.__traceback__),
                extra={
                    "boot_id": boot_id,
                    "stable_code": getattr(exc, "code", "critical_failure"),
                },
            )
            stop.set()

        sessions = SessionCoordinator(
            repository=repository,
            security=config.security,
            home_path=home_root,
        )
        await sessions.ensure_home_project()
        session_lifecycle = SessionLifecycleCoordinator(
            repository=repository,
            runtimes=runtimes,
            locks=conversation_locks,
        )
        turns = TurnCoordinator(
            repository=repository,
            runtime_supervisor=runtimes,
            event_sink=sink,
            conversation_locks=conversation_locks,
            critical_failure=critical_failure,
            provider_barrier_observer=(
                session_lifecycle.monitor_provider_barrier
            ),
            skill_input_supported=manifest.optional.get("skill.input") is True,
        )
        schedule_repository = ScheduleRepository(
            store,
            allowed_roots=runtime_allowed_roots,
        )
        schedules = ScheduleCoordinator(
            repository=schedule_repository,
            wake_conversation=turns.wake,
            poll_seconds=config.schedule.poll_seconds,
            conversation_locks=conversation_locks,
            critical_failure=critical_failure,
        )
        media = MediaWorker(
            environment={
                name: os.environ[name]
                for name in config.runtime.nonsecret_env_allowlist
                if name in os.environ
            }
        )
        renderer = DiscordRenderPlanner(
            media_worker=media,
            table_limits=TableLimits(
                max_columns=config.rendering.table_max_columns,
                max_rows_png=config.rendering.table_max_rows_png,
                memory_mib=config.rendering.table_memory_mib,
            ),
            artifact_root=config.paths.attachments / "render",
            retention_days=config.retention.render_attachments_days,
        )
        health = HealthReporter(
            path=config.paths.health,
            repository=repository,
            runtime_status=runtimes.status,
            boot_id=boot_id,
            process_start_token=process_token,
            started_at=started_at,
            sdk_version=importlib.metadata.version("openai-codex"),
            runtime_version=importlib.metadata.version("openai-codex-cli-bin"),
            critical_failure=critical_failure,
        )
        retention = RetentionWorker(
            store=store,
            paths=config.paths,
            config=config.retention,
        )
        bot = CodexDBot(
            config=config,
            repository=repository,
            sessions=sessions,
            session_lifecycle=session_lifecycle,
            turns=turns,
            schedules=schedules,
            schedule_repository=schedule_repository,
            runtimes=runtimes,
            renderer=renderer,
            media_worker=media,
            signer=ComponentSigner(component_key),
            capability_manifest=manifest,
            boot_id=boot_id,
            discord_status=health.observe_discord,
            codex_auth_status=health.observe_codex_auth,
        )
        await session_lifecycle.restore_provider_barriers()
        await schedules.restore()
        await turns.restore()
        schedules.start()
        loop = asyncio.get_running_loop()
        for signal_name in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(signal_name, stop.set)
        health.service = "healthy"
        health.discord = "connecting"
        health.start()
        retention.start()
        bot_task = asyncio.create_task(
            _start_discord_with_initial_retries(
                bot=bot,
                token=token,
                stop=stop,
                health=health,
                logger=logger,
            ),
            name="codexd-discord",
        )
        stop_task = asyncio.create_task(stop.wait(), name="codexd-stop")
        shutdown_request_task = asyncio.create_task(
            _watch_shutdown_request(
                config.paths.data_dir / "service" / "shutdown.request",
                boot_id=boot_id,
                stop=stop,
            ),
            name="codexd-shutdown-request",
        )
        shutdown_clean = True
        try:
            done, _pending = await asyncio.wait(
                {bot_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if bot_task in done:
                exception = bot_task.exception()
                if exception is not None:
                    raise exception
            health.discord = "stopping"
            health.service = "stopping"
            await health.write()
        finally:
            stop_task.cancel()
            shutdown_request_task.cancel()
            with suppress(asyncio.CancelledError):
                await shutdown_request_task
            shutdown_clean = await bot.begin_shutdown(deadline_seconds=10)
            if not shutdown_clean:
                logger.critical(
                    "Discord ingress exceeded shutdown deadline and was cancelled",
                    extra={
                        "boot_id": boot_id,
                        "stable_code": "discord_ingress_drain_timeout",
                    },
                )
            await _cleanup_step(
                "retention",
                retention.close(),
                deadline_seconds=10,
                logger=logger,
            )
            await _cleanup_step(
                "schedules",
                schedules.close(),
                deadline_seconds=10,
                logger=logger,
            )
            await _cleanup_step(
                "turns",
                turns.close(
                    drain_seconds=config.runtime.shutdown_drain_seconds
                ),
                deadline_seconds=config.runtime.shutdown_drain_seconds + 10,
                logger=logger,
            )
            await _cleanup_step(
                "session lifecycle",
                session_lifecycle.close(),
                deadline_seconds=10,
                logger=logger,
            )
            await _cleanup_step(
                "runtimes",
                runtimes.close(),
                deadline_seconds=35,
                logger=logger,
            )
            if not bot.is_closed():
                await _cleanup_step(
                    "Discord client",
                    bot.close(),
                    deadline_seconds=15,
                    logger=logger,
                )
            if not bot_task.done():
                await _cleanup_step(
                    "Discord task",
                    bot_task,
                    deadline_seconds=10,
                    logger=logger,
                )
            health.discord = "disconnected"
            health.service = "stopped"
            await _cleanup_step(
                "health reporter",
                health.close(),
                deadline_seconds=10,
                logger=logger,
            )
            await _cleanup_step(
                "daemon lease",
                asyncio.to_thread(repository.release_daemon_lease, boot_id),
                deadline_seconds=10,
                logger=logger,
            )
            try:
                checkpoint = await asyncio.wait_for(
                    asyncio.to_thread(store.checkpoint, "TRUNCATE"),
                    timeout=10,
                )
                if checkpoint[0] != 0:
                    logger.error(
                        "daemon shutdown WAL checkpoint remained busy",
                        extra={
                            "boot_id": boot_id,
                            "stable_code": "wal_checkpoint_busy",
                        },
                    )
            except Exception:
                logger.exception(
                    "daemon shutdown WAL checkpoint failed",
                    extra={
                        "boot_id": boot_id,
                        "stable_code": "wal_checkpoint_failed",
                    },
                )
            logger.info("daemon stopped", extra={"boot_id": boot_id})
        return 0


async def _start_discord_with_initial_retries(
    *,
    bot: CodexDBot,
    token: str,
    stop: asyncio.Event,
    health: HealthReporter,
    logger: logging.Logger,
) -> None:
    attempt = 0
    while not stop.is_set():
        try:
            await asyncio.wait_for(
                bot.login(token),
                timeout=_DISCORD_INITIAL_LOGIN_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except (
            TimeoutError,
            aiohttp.ClientError,
            OSError,
            discord.ConnectionClosed,
            discord.GatewayNotFound,
            discord.HTTPException,
        ) as exc:
            if bot.transport_initialized or not _retryable_discord_start_error(exc):
                raise
            delay = _DISCORD_INITIAL_RETRY_DELAYS_SECONDS[
                min(attempt, len(_DISCORD_INITIAL_RETRY_DELAYS_SECONDS) - 1)
            ]
            attempt += 1
            health.observe_discord("connecting")
            logger.warning(
                "Discord initial connection failed; retrying",
                extra={
                    "stable_code": "discord_initial_connection_retry",
                    "attempt": attempt,
                    "retry_delay_seconds": delay,
                    "exception_type": type(exc).__name__,
                },
            )
            await _reset_discord_client(bot)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                continue
        else:
            break
    if not stop.is_set():
        await bot.connect(reconnect=True)


def _retryable_discord_start_error(exc: BaseException) -> bool:
    if isinstance(exc, discord.LoginFailure):
        return False
    if isinstance(exc, discord.HTTPException):
        return exc.status == 429 or exc.status >= 500
    return True


async def _reset_discord_client(bot: CodexDBot) -> None:
    await bot.http.close()
    bot.http.connector = discord.utils.MISSING
    bot.clear()


def _environment_hash(environment: os._Environ[str]) -> str:
    import hashlib

    digest = hashlib.sha256()
    for name, value in sorted(environment.items()):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(value.encode())
        digest.update(b"\0")
    return digest.hexdigest()


async def _watch_shutdown_request(
    path: Path,
    *,
    boot_id: str,
    stop: asyncio.Event,
) -> None:
    logger = logging.getLogger(__name__)
    while not stop.is_set():
        try:
            requested = await asyncio.to_thread(
                _consume_shutdown_request,
                path,
                boot_id,
            )
        except (OSError, json.JSONDecodeError, SecurityError) as exc:
            logger.warning(
                "invalid service shutdown request",
                extra={
                    "stable_code": "invalid_shutdown_request",
                    "exception": type(exc).__name__,
                },
            )
        else:
            if requested:
                stop.set()
                return
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=0.5)


async def _cleanup_step(
    name: str,
    operation: Awaitable[object],
    *,
    deadline_seconds: float,
    logger: logging.Logger,
) -> None:
    try:
        await asyncio.wait_for(operation, timeout=deadline_seconds)
    except Exception:
        logger.exception(
            "daemon cleanup step failed",
            extra={
                "stable_code": "daemon_cleanup_failed",
                "cleanup_step": name,
            },
        )


def _consume_shutdown_request(path: Path, boot_id: str) -> bool:
    if not path.exists():
        return False
    try:
        if path.is_symlink() or not path.is_file():
            raise SecurityError("shutdown request is not a regular file")
        payload = json.loads(path.read_text(encoding="utf-8"))
        target = payload.get("boot_id") if isinstance(payload, dict) else None
        return target == boot_id
    finally:
        path.unlink(missing_ok=True)
