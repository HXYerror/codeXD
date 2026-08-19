from __future__ import annotations

import asyncio
import functools
import io
import json
import logging
import re
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from zoneinfo import ZoneInfo

import aiohttp
import discord
from discord import app_commands

from codexd.application.schedule_coordinator import ScheduleCoordinator
from codexd.application.session_coordinator import SessionCoordinator
from codexd.application.session_lifecycle import SessionLifecycleCoordinator
from codexd.application.side_queries import SideQueryCoordinator
from codexd.application.turn_coordinator import TurnCoordinator
from codexd.config import AppConfig
from codexd.domain.capabilities import CapabilityManifest, EventCapability
from codexd.domain.ids import canonical_json, sha256_text, utc_now_ms
from codexd.domain.models import ModelDescriptor
from codexd.domain.schedules import ScheduleAuditContext, ScheduleModalSubmission
from codexd.domain.turns import TurnInput, TurnSource
from codexd.errors import CodexDError, ConflictError, InvariantError, SecurityError
from codexd.rendering.discord import (
    DiscordRenderPlanner,
    split_discord_code,
    split_discord_text,
)
from codexd.rendering.media_worker import MediaWorker
from codexd.runtime.supervisor import RuntimeSupervisor
from codexd.security.redaction import redact_diff
from codexd.security.signing import ComponentSigner
from codexd.storage.ingress_reconciliation import IngressCheckpointRepository
from codexd.storage.records import (
    CommandIntentRecord,
    ConversationRecord,
    ModalIntentRecord,
    ProjectRecord,
    ScheduleDraftRecord,
    ScheduleRecord,
    TurnRecord,
)
from codexd.storage.repository import Repository
from codexd.storage.schedules import ScheduleRepository
from codexd.transport.discord.attachments import (
    AttachmentError,
    DiscordAttachmentIngestor,
    DiscordAttachmentIngestResult,
    attachment_metadata_hints_image,
)
from codexd.transport.discord.outbox import (
    DiscordOutboxTransport,
    OutboxWorker,
)
from codexd.transport.discord.presentation import (
    COLOR_FAILURE,
    COLOR_RUNNING,
    TABLE_COPY_CUSTOM_ID,
    format_usage,
    notice_embed,
    schedule_draft_embed,
    session_status_embed,
)
from codexd.transport.discord.reconciliation import DiscordInboundReconciler

logger = logging.getLogger(__name__)
_DISCORD_INTERACTION_ACK_DEADLINE_SECONDS = 3.0
_MODAL_RESPONSE_NETWORK_BUDGET_SECONDS = 1.0


@dataclass(frozen=True)
class _DeferredFollowupCall:
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class _DeferredFollowup:
    def __init__(self, followup: Any) -> None:
        self._followup = followup
        self._calls: list[_DeferredFollowupCall] = []

    async def send(self, *args: Any, **kwargs: Any) -> None:
        content: str | None = None
        positional = bool(args) and isinstance(args[0], str)
        if positional:
            content = str(args[0])
        elif isinstance(kwargs.get("content"), str):
            content = str(kwargs["content"])
        if content is None or len(content) <= 1900:
            self._calls.append(_DeferredFollowupCall(args=args, kwargs=kwargs))
            return
        try:
            chunks = split_discord_text(content)
        except ValueError:
            chunks = split_discord_code(content, limit=1800)
        single_send_options = {
            "embed",
            "embeds",
            "file",
            "files",
            "poll",
            "view",
        }
        for index, chunk in enumerate(chunks):
            call_args = args
            call_kwargs = dict(kwargs)
            if positional:
                call_args = (chunk, *args[1:])
            else:
                call_kwargs["content"] = chunk
            if index:
                for name in single_send_options:
                    call_kwargs.pop(name, None)
            self._calls.append(
                _DeferredFollowupCall(args=call_args, kwargs=call_kwargs)
            )

    async def flush(self) -> None:
        for call in self._calls:
            await self._followup.send(*call.args, **call.kwargs)
        self._calls.clear()

    def result_message(self, command_name: str) -> str:
        return _bounded_result(f"`/{command_name}` completed.")


class _DeferredInteraction:
    def __init__(self, interaction: discord.Interaction[Any]) -> None:
        self._interaction = interaction
        self.followup = _DeferredFollowup(interaction.followup)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._interaction, name)

    async def flush(self) -> None:
        await self.followup.flush()


class CodexDBot(discord.Client):
    def __init__(
        self,
        *,
        config: AppConfig,
        repository: Repository,
        sessions: SessionCoordinator,
        session_lifecycle: SessionLifecycleCoordinator,
        turns: TurnCoordinator,
        schedules: ScheduleCoordinator,
        schedule_repository: ScheduleRepository,
        runtimes: RuntimeSupervisor,
        renderer: DiscordRenderPlanner,
        media_worker: MediaWorker,
        signer: ComponentSigner,
        capability_manifest: CapabilityManifest,
        boot_id: str,
        side_queries: SideQueryCoordinator | None = None,
        discord_status: Callable[[str], None] | None = None,
        codex_auth_status: Callable[[str], None] | None = None,
    ) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.message_content = True
        super().__init__(
            intents=intents,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self.tree = app_commands.CommandTree(self)
        self.config = config
        self.repository = repository
        self.sessions = sessions
        self.session_lifecycle = session_lifecycle
        self.turns = turns
        self.schedules = schedules
        self.schedule_repository = schedule_repository
        self.runtimes = runtimes
        self.renderer = renderer
        self.media_worker = media_worker
        self.signer = signer
        self.capability_manifest = capability_manifest
        self.boot_id = boot_id
        self.side_queries = side_queries
        self._discord_status = discord_status or (lambda _status: None)
        self._codex_auth_status = codex_auth_status or (lambda _status: None)
        self._http_session: aiohttp.ClientSession | None = None
        self._attachment_ingestor: DiscordAttachmentIngestor | None = None
        self._outbox: OutboxWorker | None = None
        self._command_sync_task: asyncio.Task[None] | None = None
        self._startup_recovery_task: asyncio.Task[None] | None = None
        self._command_sync_stop = asyncio.Event()
        self._command_sync_degraded = False
        self._command_sync_initial_timeout_seconds = 10.0
        self._command_sync_retry_seconds = 5.0
        self._startup_recovery_retry_seconds = 5.0
        self._ready_preflight_degraded = False
        self._startup_preflight_complete = False
        self._runtime_preflight_complete = False
        self._provider_recovery_complete = False
        self._startup_preflight_lock = asyncio.Lock()
        self._commands_registered = False
        self._accepting_ingress = True
        self._ingress_lock = asyncio.Lock()
        self._ingress_tasks: set[asyncio.Task[Any]] = set()
        self._gateway_ready = False
        self._started_at = utc_now_ms()
        self._codex_auth_state = "unknown"
        self._security_responses_sent: set[str] = set()
        self._active_command_intents: set[str] = set()
        self._inbound_catching_up = False
        self._inbound_reconciliation_degraded = False
        self._inbound_reconciler = (
            DiscordInboundReconciler(
                repository=IngressCheckpointRepository(repository.store),
                guild_id=config.discord.guild_id,
                handler=self._handle_reconciled_message,
                status_observer=self._observe_reconciliation_status,
            )
            if config.discord.guild_id is not None
            else None
        )
        self._inbound_reconciler_started = False
        self.tree.error(self._on_command_error)

    @property
    def transport_initialized(self) -> bool:
        return self._outbox is not None

    async def setup_hook(self) -> None:
        self._http_session = aiohttp.ClientSession()
        self._attachment_ingestor = DiscordAttachmentIngestor(
            session=self._http_session,
            media_worker=self.media_worker,
            attachments_dir=self.config.paths.attachments,
            image_max_bytes=self.config.rendering.image_max_bytes,
            image_max_pixels=self.config.rendering.image_max_pixels,
            file_max_bytes=self.config.discord.file_max_bytes,
            message_max_bytes=self.config.discord.message_max_bytes,
            retention_days=self.config.retention.input_attachments_days,
            max_attachment_count=self.config.discord.max_attachment_count,
        )
        transport = DiscordOutboxTransport(
            client=self,
            repository=self.repository,
            renderer=self.renderer,
            signer=self.signer,
            volatile_turns=self.turns.volatile_turns,
        )
        self._outbox = OutboxWorker(
            repository=self.repository,
            transport=transport,
            worker_id=f"discord:{self.boot_id}",
            initial_ingress_ready=self._process_initial_ingress,
            acknowledged=transport.acknowledged,
        )
        self._outbox.start()
        if self._inbound_reconciler is not None:
            self._inbound_reconciler.start(self)
            self._inbound_reconciler_started = True
        self._register_commands()
        guild_id = self.config.discord.guild_id
        if guild_id is None:
            raise RuntimeError("Discord guild_id is not configured")
        guild = discord.Object(id=guild_id)
        await self._sync_commands_or_degrade(guild)

    async def close(self) -> None:
        await self.begin_shutdown()
        self._discord_status("stopping")
        self._command_sync_stop.set()
        if self._command_sync_task is not None:
            self._command_sync_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._command_sync_task
            self._command_sync_task = None
        if self._startup_recovery_task is not None:
            self._startup_recovery_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._startup_recovery_task
            self._startup_recovery_task = None
        if self._outbox is not None:
            await self._outbox.close()
        await self.schedules.close()
        if self._http_session is not None:
            await self._http_session.close()
        await super().close()

    async def begin_shutdown(
        self,
        *,
        deadline_seconds: float | None = None,
    ) -> bool:
        self._accepting_ingress = False
        self._gateway_ready = False
        if self._inbound_reconciler is not None:
            await self._inbound_reconciler.close()
        started = time.monotonic()
        try:
            if deadline_seconds is None:
                async with self._ingress_lock:
                    pass
            else:
                await asyncio.wait_for(
                    self._ingress_lock.acquire(),
                    timeout=deadline_seconds,
                )
        except TimeoutError:
            return False
        else:
            if self._ingress_lock.locked():
                self._ingress_lock.release()

        current = asyncio.current_task()
        tasks = tuple(
            task for task in self._ingress_tasks if task is not current and not task.done()
        )
        if not tasks:
            return True
        if deadline_seconds is None:
            await asyncio.gather(*tasks, return_exceptions=True)
            return True
        remaining = deadline_seconds - (time.monotonic() - started)
        if remaining <= 0:
            return False
        _done, pending = await asyncio.wait(tasks, timeout=remaining)
        return not pending

    async def on_ready(self) -> None:
        task = self._track_ingress()
        try:
            async with self._ingress_lock:
                if not self._accepting_ingress:
                    return
                self._gateway_ready = True
                if not self._startup_preflight_complete:
                    await self._ready_preflight()
                else:
                    self._update_discord_ready_status()
            if (
                self._inbound_reconciler is not None
                and self._inbound_reconciler_started
                and self._accepting_ingress
            ):
                await self._recover_pending_backfill_preflights()
                await self._inbound_reconciler.trigger(self, reason="ready")
        finally:
            self._untrack_ingress(task)

    async def _recover_pending_backfill_preflights(self) -> None:
        message_ids = await asyncio.to_thread(
            self.repository.pending_backfill_preflight_ids
        )
        for message_id in message_ids:
            await self._process_initial_ingress_locked(message_id)

    async def on_resumed(self) -> None:
        task = self._track_ingress()
        try:
            if (
                self._inbound_reconciler is not None
                and self._inbound_reconciler_started
                and self._accepting_ingress
            ):
                await self._inbound_reconciler.trigger(self, reason="resumed")
        finally:
            self._untrack_ingress(task)

    async def _ready_preflight(self) -> bool:
        async with self._startup_preflight_lock:
            self._runtime_preflight_complete = await self._runtime_ready_preflight()
            self._provider_recovery_complete = await self._restore_startup_state(
                retry=False
            )
            complete = self._commit_startup_preflight_state()
            if not complete:
                self._schedule_startup_recovery()
            return complete

    async def _runtime_ready_preflight(self) -> bool:
        degraded = False
        auth_states: set[str] = set()
        projects = await asyncio.to_thread(self.repository.list_enabled_projects)
        for project in projects:
            try:
                account = await self.runtimes.account_status_if_loaded(project.id)
                if account is None:
                    continue
                auth_states.add(
                    "required" if account.auth_required else "authenticated"
                )
            except Exception as exc:
                failure = getattr(exc, "failure", None)
                if getattr(failure, "code", None) == "codex_auth_required":
                    auth_states.add("required")
                degraded = True
                logger.exception(
                    "Runtime preflight failed for project",
                    extra={"project_id": project.id},
                )
                await asyncio.to_thread(
                    self.repository.record_incident,
                    severity="error",
                    code="runtime_preflight_failed",
                    summary="Codex runtime capability preflight failed",
                    project_id=project.id,
                )
        self._codex_auth_state = (
            "required"
            if "required" in auth_states
            else "authenticated"
            if "authenticated" in auth_states
            else "unknown"
        )
        self._codex_auth_status(self._codex_auth_state)
        return not degraded

    async def _restore_startup_state(self, *, retry: bool) -> bool:
        try:
            await self.session_lifecycle.restore_provider_barriers()
            await self.turns.restore()
        except Exception:
            logger.exception(
                "Discord startup recovery retry failed"
                if retry
                else "Discord ready preflight failed"
            )
            return False
        return True

    def _commit_startup_preflight_state(self) -> bool:
        complete = (
            self._runtime_preflight_complete and self._provider_recovery_complete
        )
        self._startup_preflight_complete = complete
        self._ready_preflight_degraded = not complete
        self._update_discord_ready_status()
        return complete

    def _schedule_startup_recovery(self) -> None:
        if (
            self._command_sync_stop.is_set()
            or self._startup_recovery_task is not None
        ):
            return
        self._startup_recovery_task = asyncio.create_task(
            self._retry_startup_recovery(),
            name="codexd-startup-recovery",
        )

    async def _retry_startup_recovery(self) -> None:
        delay = self._startup_recovery_retry_seconds
        try:
            while not self._command_sync_stop.is_set():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._command_sync_stop.wait(),
                        timeout=delay,
                    )
                if self._command_sync_stop.is_set():
                    return
                async with self._startup_preflight_lock:
                    if not self._runtime_preflight_complete:
                        self._runtime_preflight_complete = (
                            await self._runtime_ready_preflight()
                        )
                    if not self._provider_recovery_complete:
                        self._provider_recovery_complete = (
                            await self._restore_startup_state(retry=True)
                        )
                    complete = self._commit_startup_preflight_state()
                if not complete:
                    delay = min(delay * 2, 300.0)
                    continue
                return
        finally:
            self._startup_recovery_task = None

    async def on_disconnect(self) -> None:
        self._gateway_ready = False
        self._discord_status("disconnected")

    async def _sync_commands_or_degrade(self, guild: discord.Object) -> None:
        try:
            await asyncio.wait_for(
                self._sync_command_scopes(guild),
                timeout=self._command_sync_initial_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except (
            TimeoutError,
            discord.HTTPException,
            app_commands.CommandSyncFailure,
        ) as exc:
            await self._record_command_sync_failure(exc)
            if self._command_sync_task is None:
                self._command_sync_task = asyncio.create_task(
                    self._retry_command_sync(guild),
                    name="codexd-command-sync",
                )

    async def _retry_command_sync(self, guild: discord.Object) -> None:
        delay = self._command_sync_retry_seconds
        try:
            while not self._command_sync_stop.is_set():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._command_sync_stop.wait(),
                        timeout=delay,
                    )
                if self._command_sync_stop.is_set():
                    return
                try:
                    await self._sync_command_scopes(guild)
                except asyncio.CancelledError:
                    raise
                except (discord.HTTPException, app_commands.CommandSyncFailure) as exc:
                    await self._record_command_sync_failure(exc)
                    delay = min(delay * 2, 300.0)
                    continue
                self._command_sync_degraded = False
                self._update_discord_ready_status()
                return
        finally:
            self._command_sync_task = None

    async def _sync_command_scopes(self, guild: discord.Object) -> None:
        self.tree.clear_commands(guild=None)
        await self.tree.sync()
        await self.tree.sync(guild=guild)

    async def _record_command_sync_failure(self, exc: BaseException) -> None:
        self._command_sync_degraded = True
        self._discord_status("degraded")
        logger.warning(
            "Discord command synchronization failed; retrying",
            extra={
                "stable_code": "discord_command_sync_failed",
                "exception": type(exc).__name__,
            },
        )
        try:
            await asyncio.to_thread(
                self.repository.record_incident,
                severity="warning",
                code="discord_command_sync_failed",
                summary="Discord application-command synchronization failed",
                details={"exception": type(exc).__name__},
            )
        except Exception:
            logger.exception("Failed to persist Discord command-sync incident")

    def _update_discord_ready_status(self) -> None:
        if not self._gateway_ready:
            return
        if self._inbound_catching_up:
            self._discord_status("catching_up")
            return
        self._discord_status(
            "degraded"
            if self._ready_preflight_degraded
            or self._command_sync_degraded
            or self._inbound_reconciliation_degraded
            else "ready"
        )

    def _observe_reconciliation_status(self, status: str) -> None:
        self._inbound_catching_up = status == "catching_up"
        self._inbound_reconciliation_degraded = status == "degraded"
        self._update_discord_ready_status()

    def _track_ingress(self) -> asyncio.Task[Any] | None:
        task = asyncio.current_task()
        if task is not None:
            self._ingress_tasks.add(task)
        return task

    def _untrack_ingress(self, task: asyncio.Task[Any] | None) -> None:
        if task is not None:
            self._ingress_tasks.discard(task)

    async def _handle_schedule_draft_component(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        try:
            action = self.signer.verify_schedule_draft_id(custom_id)
            await self._run_intent_action(
                interaction,
                command_name=f"schedule draft {action.action}",
                request={
                    "draft_id": action.draft_id,
                    "action": action.action,
                    "component_hash": sha256_text(custom_id),
                    "message_id": (
                        str(interaction.message.id)
                        if interaction.message is not None
                        else None
                    ),
                },
                action=lambda staged: self._apply_schedule_draft_action(
                    staged,
                    draft_id=action.draft_id,
                    action=action.action,
                    nonce=action.nonce,
                ),
            )
        except (CodexDError, ValueError) as exc:
            message = _bounded_response(
                f"`{getattr(exc, 'code', 'invalid_input')}`: {exc}"
            )
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Schedule draft component failed")
            message = "`internal_error`: Schedule confirmation failed; see diagnostics."
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)

    async def _handle_table_copy_component(
        self,
        interaction: discord.Interaction[Any],
    ) -> None:
        task = self._track_ingress()
        try:
            if not self._accepting_ingress:
                await self._respond_shutting_down(interaction)
                return
            if not self._authorized_interaction(interaction):
                await interaction.response.send_message(
                    embed=notice_embed(
                        "This table control is restricted to the configured codexD user.",
                        level="error",
                        title="Not authorized",
                    ),
                    ephemeral=True,
                )
                return
            message = interaction.message
            if message is None:
                raise ConflictError("table copy interaction has no source message")
            source = next(
                (
                    attachment
                    for attachment in message.attachments
                    if attachment.filename.startswith("table-")
                    and attachment.filename.endswith(".md")
                ),
                None,
            )
            if source is None:
                raise ConflictError("table Markdown source is missing")
            await interaction.response.defer(ephemeral=True)
            try:
                content = await source.read()
                markdown = content.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise InvariantError("table Markdown source is not UTF-8") from exc
            chunks = split_discord_code(markdown, language="markdown", limit=1800)
            if len(chunks) == 1:
                await interaction.followup.send(
                    chunks[0],
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                return
            file = discord.File(
                io.BytesIO(content),
                filename=source.filename,
                description="Markdown source for the rendered Codex table",
            )
            try:
                await interaction.followup.send(
                    "The table source is too large for an ephemeral code block.",
                    file=file,
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            finally:
                file.close()
        finally:
            self._untrack_ingress(task)

    async def _apply_schedule_draft_action(
        self,
        interaction: discord.Interaction[Any],
        *,
        draft_id: str,
        action: str,
        nonce: str,
    ) -> None:
        await self._defer_owner(interaction)
        if interaction.guild_id is None or interaction.channel_id is None:
            raise SecurityError("Schedule draft interaction has no Discord scope")
        if interaction.message is None:
            raise SecurityError("Schedule draft interaction has no source message")
        message_id = str(interaction.message.id)
        draft = await asyncio.to_thread(
            self.schedule_repository.get_draft,
            draft_id,
        )
        was_pending = draft.state == "pending"
        draft_payload = json.loads(draft.payload_json)
        draft_occurrences = json.loads(draft.occurrences_json)
        if not isinstance(draft_payload, dict) or not isinstance(
            draft_occurrences, list
        ):
            raise InvariantError("Schedule draft card payload is invalid")
        card_payload: dict[str, object] = {
            "kind": "schedule_draft_card",
            "draft_id": draft.id,
            "action": draft.action,
            "name": draft_payload.get("name"),
            "schedule_kind": draft_payload.get("kind"),
            "expression": draft_payload.get("expression"),
            "timezone": draft_payload.get("timezone"),
            "misfire_policy": draft_payload.get("misfire_policy"),
            "prompt_text": draft_payload.get("prompt_text"),
            "occurrences": draft_occurrences,
        }
        if action == "confirm":
            schedule = await self.schedules.confirm_draft(
                draft_id=draft_id,
                component_nonce=nonce,
                owner_user_id=interaction.user.id,
                guild_id=interaction.guild_id,
                channel_id=interaction.channel_id,
                message_id=message_id,
                audit=_schedule_audit_context(interaction),
            )
            card_payload.update(
                state="confirmed",
                schedule_ref=schedule.id[:8],
                next_due_at=schedule.next_due_at,
            )
            if was_pending:
                with suppress(discord.HTTPException):
                    await interaction.message.edit(
                        embed=schedule_draft_embed(card_payload),
                        view=None,
                    )
            await interaction.followup.send(
                f"Schedule `{schedule.id[:8]}` confirmed; next "
                f"<t:{(schedule.next_due_at or 0) // 1000}:F>.",
                ephemeral=True,
            )
            return
        await self.schedules.cancel_draft(
            draft_id=draft_id,
            component_nonce=nonce,
            owner_user_id=interaction.user.id,
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
            message_id=message_id,
            audit=_schedule_audit_context(interaction),
        )
        card_payload["state"] = "cancelled"
        if was_pending:
            with suppress(discord.HTTPException):
                await interaction.message.edit(
                    embed=schedule_draft_embed(card_payload),
                    view=None,
                )
        await interaction.followup.send("Schedule draft cancelled.", ephemeral=True)

    async def _on_command_error(
        self,
        interaction: discord.Interaction[Any],
        error: app_commands.AppCommandError,
    ) -> None:
        original: BaseException = (
            error.original
            if isinstance(error, app_commands.CommandInvokeError)
            else error
        )
        try:
            intent = await asyncio.to_thread(
                self.repository.get_command_intent,
                str(interaction.id),
            )
        except CodexDError:
            intent = None
        if intent is not None and intent.state == "unknown" and intent.result_json:
            result = json.loads(intent.result_json)
            message = _bounded_response(
                f"`{result.get('code', 'command_effect_outcome_unknown')}`: "
                f"{result.get('message', 'The command outcome is unknown.')}"
            )
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
            return
        if isinstance(original, SecurityError):
            interaction_id = str(interaction.id)
            if interaction_id in self._security_responses_sent:
                self._security_responses_sent.discard(interaction_id)
                return
        if isinstance(original, app_commands.CommandNotFound):
            logger.warning(
                "Stale Discord application command rejected",
                extra={"stable_code": "discord_stale_global_command"},
            )
            message = (
                "`stale_command`: refresh Discord and retry the guild-scoped "
                "codexD command."
            )
        elif isinstance(original, (CodexDError, ValueError)):
            code = getattr(original, "code", "invalid_input")
            message = _bounded_response(f"`{code}`: {original}")
        else:
            logger.exception(
                "Discord command failed",
                exc_info=(type(original), original, original.__traceback__),
            )
            message = "`internal_error`: command failed; see codexD diagnostics."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    def _intentful(
        self,
        command_name: str,
        callback: Callable[..., Any],
    ) -> Callable[..., Any]:
        @functools.wraps(callback)
        async def wrapped(
            interaction: discord.Interaction[Any],
            *args: object,
            **kwargs: object,
        ) -> None:
            await self._run_intent_action(
                interaction,
                command_name=command_name,
                request={
                    "args": [_command_value(value) for value in args],
                    "kwargs": {
                        name: _command_value(value)
                        for name, value in sorted(kwargs.items())
                    },
                },
                action=lambda staged: callback(staged, *args, **kwargs),
            )

        wrapped.__qualname__ = wrapped.__name__
        return wrapped

    async def _run_intent_action(
        self,
        interaction: discord.Interaction[Any],
        *,
        command_name: str,
        request: Mapping[str, Any],
        action: Callable[[discord.Interaction[Any]], Awaitable[None]],
    ) -> bool:
        task = self._track_ingress()
        claimed = False
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            async with self._ingress_lock:
                intent = await self._accept_intent_locked(
                    interaction,
                    command_name=command_name,
                    request=request,
                )
            if intent is None:
                return False
            claimed = True
            return await self._run_accepted_intent(
                interaction,
                intent=intent,
                command_name=command_name,
                action=action,
            )
        finally:
            if claimed:
                self._active_command_intents.discard(str(interaction.id))
            self._untrack_ingress(task)

    async def _accept_intent_locked(
        self,
        interaction: discord.Interaction[Any],
        *,
        command_name: str,
        request: Mapping[str, Any],
    ) -> CommandIntentRecord | None:
        if not self._accepting_ingress:
            await self._respond_shutting_down(interaction)
            return None
        intent = await self._accept_interaction_intent(
            interaction,
            command_name=command_name,
            request=request,
        )
        if intent.state != "accepted":
            await self._respond_existing_intent(interaction, intent.result_json)
            return None
        interaction_id = str(interaction.id)
        if interaction_id in self._active_command_intents:
            await self._respond_existing_intent(interaction, None)
            return None
        self._active_command_intents.add(interaction_id)
        if not self._accepting_ingress:
            await asyncio.to_thread(
                self.repository.complete_command_intent,
                intent.interaction_id,
                state="rejected",
                result={
                    "code": "daemon_stopping",
                    "message": "codexD is shutting down.",
                },
                actor_user_id=interaction.user.id,
            )
            self._active_command_intents.discard(interaction_id)
            await self._respond_shutting_down(interaction)
            return None
        return intent

    async def _run_accepted_intent(
        self,
        interaction: discord.Interaction[Any],
        *,
        intent: CommandIntentRecord,
        command_name: str,
        action: Callable[[discord.Interaction[Any]], Awaitable[None]],
    ) -> bool:
        deferred = _DeferredInteraction(interaction)
        staged_interaction = cast(discord.Interaction[Any], deferred)
        try:
            await action(staged_interaction)
        except asyncio.CancelledError:
            await asyncio.shield(
                self._complete_failed_action(
                    interaction,
                    intent=intent,
                    default_state="rejected",
                    code="command_cancelled",
                    message="The command was cancelled before its effect started.",
                )
            )
            raise
        except (CodexDError, ValueError) as exc:
            await self._complete_failed_action(
                interaction,
                intent=intent,
                default_state="rejected",
                code=getattr(exc, "code", "invalid_input"),
                message=str(exc)[:512],
            )
            raise
        except Exception as exc:
            await self._complete_failed_action(
                interaction,
                intent=intent,
                default_state="failed",
                code="internal_error",
                message=type(exc).__name__,
            )
            raise
        result = {
            "code": "ok",
            "message": deferred.followup.result_message(command_name),
            "delivery": "pending",
        }
        current = await asyncio.to_thread(
            self.repository.get_command_intent,
            intent.interaction_id,
        )
        if current.state not in {"succeeded", "rejected", "failed", "unknown"}:
            await asyncio.to_thread(
                self.repository.complete_command_intent,
                intent.interaction_id,
                state="succeeded",
                result=result,
                actor_user_id=interaction.user.id,
            )
        try:
            await deferred.flush()
        except Exception as exc:
            logger.warning(
                "Discord command result delivery failed",
                exc_info=(type(exc), exc, exc.__traceback__),
                extra={"stable_code": "discord_command_delivery_failed"},
            )
            await asyncio.to_thread(
                self.repository.mark_command_delivery_failed,
                intent.interaction_id,
                error_code=type(exc).__name__,
            )
            return True
        await asyncio.to_thread(
            self.repository.mark_command_delivered,
            intent.interaction_id,
        )
        return True

    async def _complete_failed_action(
        self,
        interaction: discord.Interaction[Any],
        *,
        intent: CommandIntentRecord,
        default_state: str,
        code: str,
        message: str,
    ) -> None:
        current = await asyncio.to_thread(
            self.repository.get_command_intent,
            intent.interaction_id,
        )
        if current.state in {"succeeded", "rejected", "failed", "unknown"}:
            return
        state = "unknown" if current.state == "effect_in_flight" else default_state
        result = (
            {
                "code": "command_effect_outcome_unknown",
                "message": (
                    "The provider effect may have completed; inspect status before "
                    "retrying."
                ),
                "cause_code": code,
            }
            if state == "unknown"
            else {"code": code, "message": message}
        )
        await asyncio.to_thread(
            self.repository.complete_command_intent,
            intent.interaction_id,
            state=state,
            result=result,
            actor_user_id=interaction.user.id,
        )
        if state == "unknown":
            await asyncio.to_thread(
                self.repository.record_incident,
                severity="warning",
                code="command_effect_outcome_unknown",
                summary="A Discord command provider effect has an unknown outcome",
                project_id=current.project_id,
                conversation_id=current.conversation_id,
                turn_id=current.turn_id,
                details={
                    "command_name": current.command_name,
                    "cause_code": code,
                },
            )

    @staticmethod
    async def _respond_shutting_down(interaction: discord.Interaction[Any]) -> None:
        message = "codexD is shutting down; retry after the service restarts."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    async def _accept_interaction_intent(
        self,
        interaction: discord.Interaction[Any],
        *,
        command_name: str,
        request: Mapping[str, Any],
        turn_id: str | None = None,
    ) -> CommandIntentRecord:
        conversation = (
            await self.sessions.conversation_for_thread(interaction.channel_id)
            if interaction.channel_id is not None
            else None
        )
        project_id = conversation.project_id if conversation is not None else None
        if project_id is None and interaction.guild_id and interaction.channel_id:
            resolved = await self.sessions.resolve_project_for_channel(
                guild_id=interaction.guild_id,
                channel_id=interaction.channel_id,
            )
            project_id = resolved.project.id
        return await asyncio.to_thread(
            self.repository.accept_command_intent,
            interaction_id=str(interaction.id),
            command_name=command_name,
            request=request,
            boot_id=self.boot_id,
            actor_user_id=interaction.user.id,
            project_id=project_id,
            conversation_id=conversation.id if conversation is not None else None,
            turn_id=turn_id,
        )

    @staticmethod
    async def _respond_existing_intent(
        interaction: discord.Interaction[Any],
        result_json: str | None,
    ) -> None:
        message = "`command_pending`: this interaction is already being processed."
        if result_json:
            result = json.loads(result_json)
            code = str(result.get("code", "command_result"))
            detail = str(result.get("message", "The command was already processed."))
            message = _bounded_response(f"`{code}`: {detail}")
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    def _optional_available(self, name: str) -> bool:
        value = self.capability_manifest.optional.get(name)
        return value is True or (
            isinstance(value, EventCapability)
            and value is not EventCapability.UNSUPPORTED
        )

    async def _mark_interaction_effect(
        self,
        interaction: discord.Interaction[Any],
        *,
        effect_kind: str,
        effect_correlation_id: str | None,
    ) -> None:
        await asyncio.to_thread(
            self.repository.mark_command_effect,
            str(interaction.id),
            effect_kind=effect_kind,
            effect_correlation_id=effect_correlation_id,
        )

    async def on_message(self, message: discord.Message) -> None:
        task = self._track_ingress()
        try:
            async with self._ingress_lock:
                if not self._accepting_ingress:
                    return
            if (
                self._inbound_reconciler is not None
                and self._inbound_reconciler_started
            ):
                await self._inbound_reconciler.process_live(message)
            else:
                await self._handle_message(message)
        finally:
            self._untrack_ingress(task)

    async def _handle_reconciled_message(
        self,
        message: discord.Message,
        backfill: bool,
    ) -> None:
        await self._handle_message(message, backfill=backfill)

    async def _handle_message(
        self,
        message: discord.Message,
        *,
        backfill: bool = False,
    ) -> None:
        if (
            message.author.bot
            or message.webhook_id is not None
            or message.guild is None
            or self.user is None
        ):
            return
        if not self._authorized(message.author.id, message.guild.id):
            return
        conversation: ConversationRecord | None = None
        try:
            if backfill and self._inbound_reconciler is not None:
                known = await self._inbound_reconciler.known_ingress(message)
                if known:
                    if known == ("pending_preflight", "backfill"):
                        await self._process_initial_ingress_locked(str(message.id))
                    return
            content = message.content
            if isinstance(message.channel, discord.Thread):
                conversation = await self.sessions.conversation_for_thread(message.channel.id)
                if conversation is None:
                    return
                if (
                    message.guild.id != conversation.discord_guild_id
                    or message.channel.parent_id
                    != conversation.discord_parent_channel_id
                ):
                    raise SecurityError("Conversation thread scope changed")
                await self._ingest_message(
                    message=message,
                    conversation=conversation,
                    content=content,
                    preclaimed=False,
                    backfill=backfill,
                )
            elif (
                isinstance(message.channel, discord.TextChannel)
                and self.user in message.mentions
            ):
                resolved = await self.sessions.resolve_project_for_channel(
                    guild_id=message.guild.id,
                    channel_id=message.channel.id,
                )
                project = resolved.project
                content = _remove_bot_mention(message, self.user.id)
                attachments = _message_attachments(message)
                if not content.strip() and not attachments:
                    raise AttachmentError(
                        "A prompt or attachment is required.",
                        code="empty_input",
                    )
                has_image_attachment = any(
                    attachment_metadata_hints_image(
                        attachment.filename,
                        attachment.content_type,
                    )
                    for attachment in attachments
                )
                await asyncio.to_thread(
                    self.repository.request_thread_creation,
                    discord_message_id=str(message.id),
                    content_hash=sha256_text(content),
                    attachment_manifest_hash=_attachment_manifest_hash(
                        attachments
                    ),
                    first_request_text=content,
                    has_image_attachment=has_image_attachment,
                    project_id=project.id,
                    discord_guild_id=message.guild.id,
                    discord_channel_id=message.channel.id,
                    owner_user_id=message.author.id,
                    discovery_kind="backfill" if backfill else "live",
                    boot_id=self.boot_id,
                )
            else:
                return
        except (CodexDError, ValueError) as exc:
            if backfill and isinstance(exc, SecurityError):
                raise
            await message.channel.send(
                _bounded_response(
                    f"codexD `{getattr(exc, 'code', 'invalid_input')}`: {exc}"
                ),
                allowed_mentions=discord.AllowedMentions.none(),
                suppress_embeds=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Discord message ingestion failed")
            await asyncio.to_thread(
                self.repository.record_incident,
                severity="error",
                code="discord_ingress_internal_error",
                summary="Discord message ingestion failed unexpectedly",
                conversation_id=(
                    conversation.id if conversation is not None else None
                ),
                details={"exception": type(exc).__name__},
            )
            if backfill:
                raise
            await message.channel.send(
                "codexD `internal_error`: message ingestion failed; see diagnostics.",
                allowed_mentions=discord.AllowedMentions.none(),
                suppress_embeds=True,
            )

    async def _process_initial_ingress(self, discord_message_id: str) -> None:
        task = self._track_ingress()
        try:
            async with self._ingress_lock:
                if not self._accepting_ingress:
                    return
            await self._process_initial_ingress_locked(discord_message_id)
        finally:
            self._untrack_ingress(task)

    async def _process_initial_ingress_locked(
        self,
        discord_message_id: str,
    ) -> None:
        ingress = await asyncio.to_thread(
            self.repository.get_ingress_message, discord_message_id
        )
        if ingress.state in {"ready", "rejected"}:
            return
        if ingress.state != "pending_preflight" or ingress.conversation_id is None:
            raise InvariantError(
                f"initial ingress is not ready for preflight: {ingress.state}"
            )
        conversation = await asyncio.to_thread(
            self.repository.get_conversation, ingress.conversation_id
        )
        if (
            ingress.accepted_boot_id != self.boot_id
            and ingress.discovery_kind != "backfill"
        ):
            await self._reject_initial_ingress(
                ingress.discord_message_id,
                conversation,
                code="daemon_restarted_before_preflight",
                message="Conversation recovered after restart; resend the original prompt.",
            )
            return
        try:
            channel = self.get_channel(ingress.discord_channel_id)
            if channel is None:
                channel = await self.fetch_channel(ingress.discord_channel_id)
            if not isinstance(channel, discord.TextChannel):
                raise SecurityError("initial message channel is no longer a text channel")
            target_thread = self.get_channel(conversation.discord_thread_id)
            if target_thread is None:
                target_thread = await self.fetch_channel(
                    conversation.discord_thread_id
                )
            if not isinstance(target_thread, discord.Thread):
                raise SecurityError("Conversation thread is no longer available")
            message = await channel.fetch_message(int(discord_message_id))
            if (
                message.guild is None
                or message.guild.id != ingress.discord_guild_id
                or message.channel.id != ingress.discord_channel_id
                or conversation.discord_guild_id != ingress.discord_guild_id
                or conversation.discord_parent_channel_id
                != ingress.discord_channel_id
                or message.author.id != conversation.owner_user_id
                or message.author.bot
                or message.webhook_id is not None
                or self.user is None
                or self.user not in message.mentions
                or not self._authorized(message.author.id, message.guild.id)
            ):
                raise SecurityError("initial Discord message scope changed before preflight")
            content = _remove_bot_mention(message, self.user.id)
            attachments = _message_attachments(message)
            if (
                sha256_text(content) != ingress.accepted_content_hash
                or _attachment_manifest_hash(attachments)
                != ingress.accepted_attachment_manifest_hash
            ):
                raise ConflictError(
                    "initial Discord message changed before preflight"
                )
            await self._ingest_message(
                message=message,
                conversation=conversation,
                content=content,
                preclaimed=True,
                backfill=ingress.discovery_kind == "backfill",
            )
        except asyncio.CancelledError:
            raise
        except discord.NotFound:
            await self._reject_initial_ingress(
                discord_message_id,
                conversation,
                code="initial_message_not_found",
                message="The original message was deleted; resend the prompt.",
            )
        except discord.Forbidden:
            await self._reject_initial_ingress(
                discord_message_id,
                conversation,
                code="initial_message_forbidden",
                message="codexD cannot re-read the original message.",
            )
        except (CodexDError, ValueError) as exc:
            await self._reject_initial_ingress(
                discord_message_id,
                conversation,
                code=getattr(exc, "code", "invalid_input"),
                message=str(exc),
            )
        except Exception as exc:
            logger.exception("Initial Discord message preflight failed")
            await asyncio.to_thread(
                self.repository.record_incident,
                severity="error",
                code="initial_ingress_internal_error",
                summary="Initial Conversation message preflight failed unexpectedly",
                project_id=conversation.project_id,
                conversation_id=conversation.id,
                details={"exception": type(exc).__name__},
            )
            await self._reject_initial_ingress(
                discord_message_id,
                conversation,
                code="initial_ingress_internal_error",
                message="Initial message preflight failed; see diagnostics.",
            )

    async def _ingest_message(
        self,
        *,
        message: discord.Message,
        conversation: ConversationRecord,
        content: str,
        preclaimed: bool,
        backfill: bool = False,
    ) -> None:
        attachments = _message_attachments(message)
        if not content.strip() and not attachments:
            raise AttachmentError(
                "A prompt or attachment is required.",
                code="empty_input",
            )
        if not preclaimed:
            claimed, _existing_turn_id = await asyncio.to_thread(
                self.repository.claim_ingress_message,
                discord_message_id=str(message.id),
                content_hash=sha256_text(content),
                attachment_manifest_hash=_attachment_manifest_hash(
                    attachments
                ),
                project_id=conversation.project_id,
                conversation_id=conversation.id,
                discord_guild_id=conversation.discord_guild_id,
                discord_channel_id=message.channel.id,
                requested_by_user_id=message.author.id,
                discovery_kind="backfill" if backfill else "live",
                boot_id=self.boot_id,
            )
            if not claimed:
                return
        assert self._attachment_ingestor is not None
        ingested = DiscordAttachmentIngestResult()
        try:
            ingested = await self._attachment_ingestor.ingest(attachments)
            await self.turns.enqueue(
                conversation_id=conversation.id,
                source=TurnSource.DISCORD,
                turn_input=TurnInput(
                    text=content,
                    images=ingested.images,
                    files=ingested.files,
                ),
                input_message_id=str(message.id),
                ingress_message_id=str(message.id),
                requested_by_user_id=message.author.id,
            )
        except BaseException as exc:
            ingress = await asyncio.to_thread(
                self.repository.get_ingress_message, str(message.id)
            )
            if ingress.state != "ready":
                self._attachment_ingestor.cleanup(ingested)
                await asyncio.to_thread(
                    self.repository.reject_ingress_message,
                    discord_message_id=str(message.id),
                    error_code=getattr(exc, "code", "turn_enqueue_failed"),
                )
            raise

    async def _reject_initial_ingress(
        self,
        discord_message_id: str,
        conversation: ConversationRecord,
        *,
        code: str,
        message: str,
    ) -> None:
        await asyncio.to_thread(
            self.repository.reject_ingress_message,
            discord_message_id=discord_message_id,
            error_code=code,
        )
        await asyncio.to_thread(
            self.repository.enqueue_outbox,
            destination_key=f"thread:{conversation.discord_thread_id}",
            operation="send",
            payload={
                "kind": "notice",
                "level": "error",
                "title": "Turn input rejected",
                "content": f"`{code}`: {message}",
            },
            dedupe_key=f"initial-ingress:{discord_message_id}:error",
            delivery_marker=f"initial-{discord_message_id}-error",
        )

    async def on_thread_delete(self, thread: discord.Thread) -> None:
        await self._mark_thread_deleted(thread.id)

    async def on_raw_thread_delete(self, payload: discord.RawThreadDeleteEvent) -> None:
        await self._mark_thread_deleted(payload.thread_id)

    async def _mark_thread_deleted(self, thread_id: int) -> None:
        task = self._track_ingress()
        try:
            async with self._ingress_lock:
                if not self._accepting_ingress:
                    return
                await asyncio.to_thread(
                    self.repository.mark_conversation_deleted, thread_id
                )
        finally:
            self._untrack_ingress(task)

    async def on_interaction(self, interaction: discord.Interaction[Any]) -> None:
        if interaction.type is discord.InteractionType.modal_submit:
            data = cast(Mapping[str, object], interaction.data or {})
            custom_id = str(data.get("custom_id", ""))
            if custom_id.startswith("mi:v1:"):
                await self._handle_modal_submit(interaction, custom_id)
            return
        if interaction.type is not discord.InteractionType.component:
            return
        data = cast(Mapping[str, object], interaction.data or {})
        custom_id = str(data.get("custom_id", ""))
        if custom_id == TABLE_COPY_CUSTOM_ID:
            try:
                await self._handle_table_copy_component(interaction)
            except asyncio.CancelledError:
                raise
            except CodexDError as exc:
                await self._send_component_error(
                    interaction,
                    message=f"`{exc.code}`: {exc}",
                    title="Table source unavailable",
                )
            except Exception:
                logger.exception("Table copy component failed")
                await self._send_component_error(
                    interaction,
                    message="The table source could not be returned; see diagnostics.",
                    title="Table copy failed",
                )
            return
        if custom_id.startswith("sd:v1:"):
            await self._handle_schedule_draft_component(interaction, custom_id)
            return
        if not custom_id.startswith("tc:"):
            return
        try:
            action = self.signer.verify_task_card_id(custom_id)
            await self._run_intent_action(
                interaction,
                command_name=f"task card {action.action}",
                request={
                    "view_id": action.view_id,
                    "revision": action.revision,
                    "action": action.action,
                    "component_hash": sha256_text(custom_id),
                },
                action=lambda staged: self._apply_task_card_action(
                    staged,
                    view_id=action.view_id,
                    revision=action.revision,
                    action=action.action,
                    nonce=action.nonce,
                ),
            )
        except asyncio.CancelledError:
            raise
        except CodexDError as exc:
            message = _bounded_response(f"`{exc.code}`: {exc}")
            if interaction.response.is_done():
                await interaction.followup.send(
                    message, ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    message, ephemeral=True
                )
        except Exception:
            logger.exception("Task-card interaction failed")
            if interaction.response.is_done():
                await interaction.followup.send(
                    "`internal_error`: task-card update failed.",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    "`internal_error`: task-card update failed.",
                    ephemeral=True,
                )

    @staticmethod
    async def _send_component_error(
        interaction: discord.Interaction[Any],
        *,
        message: str,
        title: str,
    ) -> None:
        embed = notice_embed(message, level="error", title=title)
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)

    async def _handle_modal_submit(
        self,
        interaction: discord.Interaction[Any],
        custom_id: str,
    ) -> None:
        try:
            modal = self.signer.verify_modal_id(custom_id)
            values = _modal_values(interaction.data)
            request: dict[str, object] = {
                "modal_intent_id": modal.intent_id,
                "modal_kind": modal.kind,
                "modal_hash": sha256_text(custom_id),
            }
            if modal.kind in {"schedule_create", "schedule_update"}:
                request.update(
                    {
                        "name": values.get("schedule_name", ""),
                        "expression": values.get("schedule_when", ""),
                        "timezone": values.get("schedule_timezone", ""),
                        "misfire_policy": values.get("schedule_misfire", ""),
                        "prompt_hash": sha256_text(
                            values.get("schedule_prompt", "")
                        ),
                    }
                )
            elif modal.kind == "side_query":
                question = values.get("side_query_question", "")
                request.update(
                    {
                        "question_hash": sha256_text(question),
                        "question_size": len(question.encode()),
                    }
                )
            else:
                request["instruction_hash"] = sha256_text(
                    values.get("steer_instruction", "")
                )
            await self._run_intent_action(
                interaction,
                command_name=f"{modal.kind.replace('_', ' ')} submit",
                request=request,
                action=lambda staged: self._apply_modal_submit(
                    staged,
                    modal_intent_id=modal.intent_id,
                    modal_kind=modal.kind,
                    modal_expires_at=modal.expires_at,
                    modal_nonce=modal.nonce,
                    values=values,
                ),
            )
        except asyncio.CancelledError:
            raise
        except (CodexDError, ValueError) as exc:
            await _respond_modal_error(interaction, exc)
        except Exception:
            logger.exception("Durable modal submission failed")
            await _respond_modal_error(
                interaction,
                InvariantError("Modal submission failed; see diagnostics"),
            )

    async def _apply_modal_submit(
        self,
        interaction: discord.Interaction[Any],
        *,
        modal_intent_id: str,
        modal_kind: str,
        modal_expires_at: int,
        modal_nonce: str,
        values: Mapping[str, str],
    ) -> None:
        if interaction.guild_id is None or interaction.channel_id is None:
            raise SecurityError("modal submission has no Discord scope")
        if modal_kind in {"schedule_create", "schedule_update"}:
            modal = await asyncio.to_thread(
                self.repository.get_modal_intent,
                modal_intent_id,
            )
        else:
            modal = await asyncio.to_thread(
                self.repository.consume_modal_intent,
                intent_id=modal_intent_id,
                kind=modal_kind,
                expires_at=modal_expires_at,
                nonce=modal_nonce,
                interaction_id=str(interaction.id),
                guild_id=interaction.guild_id,
                channel_id=interaction.channel_id,
                user_id=interaction.user.id,
            )
        conversation = await self._conversation(interaction)
        if (
            conversation.id != modal.conversation_id
            or conversation.project_id != modal.project_id
        ):
            raise SecurityError("modal Conversation scope changed")
        if modal_kind == "steer":
            await self._apply_steer_modal(interaction, modal, values)
            return
        if modal_kind == "side_query":
            await self._apply_side_query(
                interaction,
                values.get("side_query_question", ""),
            )
            return
        await self._require_owner(interaction)
        await self._apply_schedule_modal(
            interaction,
            modal,
            conversation,
            values,
            modal_submission=ScheduleModalSubmission(
                intent_id=modal_intent_id,
                kind=modal_kind,
                expires_at=modal_expires_at,
                nonce=modal_nonce,
                interaction_id=str(interaction.id),
                guild_id=interaction.guild_id,
                channel_id=interaction.channel_id,
                user_id=interaction.user.id,
            ),
        )

    async def _apply_steer_modal(
        self,
        interaction: discord.Interaction[Any],
        modal: ModalIntentRecord,
        values: Mapping[str, str],
    ) -> None:
        instruction = values.get("steer_instruction", "").strip()
        if not instruction:
            raise ConflictError("steer instruction is required")
        if modal.turn_id is None:
            raise InvariantError("steer modal is missing its Turn")
        await self.turns.steer(
            modal.turn_id,
            instruction,
            interaction_id=str(interaction.id),
            actor_user_id=interaction.user.id,
        )
        await interaction.followup.send("Steer accepted.", ephemeral=True)

    async def _apply_schedule_modal(
        self,
        interaction: discord.Interaction[Any],
        modal: ModalIntentRecord,
        conversation: ConversationRecord,
        values: Mapping[str, str],
        *,
        modal_submission: ScheduleModalSubmission,
    ) -> None:
        name = values.get("schedule_name", "").strip()
        expression = values.get("schedule_when", "").strip()
        timezone = values.get("schedule_timezone", "").strip()
        misfire_policy = values.get("schedule_misfire", "").strip().lower()
        prompt = values.get("schedule_prompt", "").strip()
        if not all((name, expression, timezone, misfire_policy, prompt)):
            raise ConflictError("all Schedule modal fields are required")
        if interaction.guild_id is None or interaction.channel_id is None:
            raise SecurityError("Schedule modal has no Discord scope")
        kind = "cron" if len(expression.split()) == 5 else "once"
        component_nonce = secrets.token_urlsafe(9)
        common: dict[str, Any] = {
            "name": name,
            "kind": kind,
            "expression": expression,
            "timezone": timezone,
            "misfire_policy": misfire_policy,
            "prompt_text": prompt,
            "owner_user_id": interaction.user.id,
            "guild_id": interaction.guild_id,
            "channel_id": interaction.channel_id,
            "component_nonce": component_nonce,
            "modal_submission": modal_submission,
        }
        if modal.kind == "schedule_create":
            draft = await self.schedules.create_draft(
                conversation_id=conversation.id,
                **common,
            )
        else:
            if modal.schedule_id is None or modal.expected_version is None:
                raise InvariantError("Schedule update modal lost its target version")
            draft = await self.schedules.update_draft(
                schedule_id=modal.schedule_id,
                expected_version=modal.expected_version,
                **common,
            )
        await self._send_schedule_draft_preview(
            interaction,
            draft=draft,
            full_access=True,
            nonce=component_nonce,
        )

    async def _apply_task_card_action(
        self,
        interaction: discord.Interaction[Any],
        *,
        view_id: str,
        revision: int,
        action: str,
        nonce: str,
    ) -> None:
        if (
            interaction.guild_id is None
            or interaction.channel_id is None
            or interaction.message is None
        ):
            raise SecurityError("task-card interaction has no guild/channel/message scope")
        if not self._authorized(interaction.user.id, interaction.guild_id):
            raise SecurityError("user is not authorized")
        await asyncio.to_thread(
            self.repository.update_task_card_display,
            view_id=view_id,
            expected_revision=revision,
            action=action,
            component_nonce=nonce,
            interaction_id=str(interaction.id),
            owner_user_id=interaction.user.id,
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
            message_id=interaction.message.id,
        )

    def _register_commands(self) -> None:
        if self._commands_registered:
            return
        self._commands_registered = True
        guild_id = self.config.discord.guild_id
        if guild_id is None:
            raise InvariantError("Discord guild is not configured")
        guild = discord.Object(id=guild_id)
        intent = self._intentful

        project = app_commands.Group(name="project", description="Project binding")
        project.command(name="bind", description="Bind this channel to a local project")(
            intent("project bind", self._project_bind)
        )
        project.command(name="info", description="Show this channel's project")(
            intent("project info", self._project_show)
        )
        project.command(name="unbind", description="Disable this channel's project binding")(
            intent("project unbind", self._project_unbind)
        )
        self.tree.add_command(project, guild=guild)

        turn = app_commands.Group(name="turn", description="Current Codex Turn")
        turn.command(name="list", description="List recent Turns")(
            intent("turn list", self._turn_list)
        )
        turn.command(name="show", description="Show a Turn")(
            intent("turn show", self._turn_show)
        )
        turn.command(name="cancel", description="Cancel the active Turn")(
            intent("turn cancel", self._turn_cancel)
        )
        self.tree.add_command(turn, guild=guild)

        model = app_commands.Group(name="model", description="Codex model")
        model.command(name="list", description="List available Codex models")(
            intent("model list", self._model_list)
        )
        model.command(name="show", description="Show current model")(
            intent("model show", self._model_show)
        )
        model.command(name="set", description="Set model for future Turns")(
            intent("model set", self._model_set)
        )
        if self._optional_available("turn.service_tier"):
            model_tier = app_commands.Group(
                name="tier",
                description="Codex model service tier",
                parent=model,
            )
            model_tier.command(name="show", description="Show service tiers")(
                intent("model tier show", self._model_tier_show)
            )
            model_tier.command(
                name="set", description="Set service tier for future Turns"
            )(intent("model tier set", self._model_tier_set))
            model_tier.command(
                name="default", description="Use the model's default tier"
            )(intent("model tier default", self._model_tier_default))
        self.tree.add_command(model, guild=guild)

        reasoning = app_commands.Group(name="reasoning", description="Reasoning effort")
        reasoning.command(name="show", description="Show reasoning effort")(
            intent("reasoning show", self._reasoning_show)
        )
        reasoning.command(name="set", description="Set reasoning effort")(
            intent("reasoning set", self._reasoning_set)
        )
        if self._optional_available("turn.reasoning_summary"):
            reasoning_summary = app_commands.Group(
                name="summary",
                description="Reasoning summary detail",
                parent=reasoning,
            )
            reasoning_summary.command(
                name="show", description="Show reasoning summary detail"
            )(intent("reasoning summary show", self._reasoning_summary_show))
            reasoning_summary.command(
                name="set", description="Set reasoning summary detail"
            )(intent("reasoning summary set", self._reasoning_summary_set))
            reasoning_summary.command(
                name="default",
                description="Use the provider's reasoning summary default",
            )(intent("reasoning summary default", self._reasoning_summary_default))
        self.tree.add_command(reasoning, guild=guild)

        if self._optional_available("turn.personality"):
            personality = app_commands.Group(
                name="personality", description="Codex response personality"
            )
            personality.command(
                name="show", description="Show response personality"
            )(intent("personality show", self._personality_show))
            personality.command(
                name="set", description="Set response personality"
            )(intent("personality set", self._personality_set))
            self.tree.add_command(personality, guild=guild)

        if self._optional_available("web_search.config"):
            websearch = app_commands.Group(
                name="websearch", description="Codex web search mode"
            )
            websearch.command(name="show", description="Show web search mode")(
                intent("websearch show", self._websearch_show)
            )
            websearch.command(name="set", description="Set web search mode")(
                intent("websearch set", self._websearch_set)
            )
            self.tree.add_command(websearch, guild=guild)

        session = app_commands.Group(name="session", description="Codex Thread lifecycle")
        session.command(name="list", description="List known Thread revisions")(
            intent("session list", self._session_list)
        )
        session.command(name="status", description="Show active Thread revision")(
            intent("session status", self._session_status)
        )
        session.command(name="new", description="Create and activate a new Codex Thread")(
            intent("session new", self._session_new)
        )
        session.command(name="resume", description="Resume a known Thread revision")(
            intent("session resume", self._session_resume)
        )
        if self._optional_available("thread.fork"):
            session.command(name="fork", description="Fork the active Codex Thread")(
                intent("session fork", self._session_fork)
            )
        if (
            self._optional_available("thread.archive")
            and self._optional_available("thread.unarchive")
        ):
            session.command(
                name="archive", description="Archive the active Codex Thread"
            )(intent("session archive", self._session_archive))
        if self._optional_available("thread.set_name"):
            session.command(name="rename", description="Rename the active Codex Thread")(
                intent("session rename", self._session_rename)
            )
        if self._optional_available("thread.compact"):
            session.command(
                name="compact", description="Compact the active Codex Thread"
            )(intent("session compact", self._session_compact))
        session.command(name="clear", description="Detach the active Thread revision")(
            intent("session clear", self._session_clear)
        )
        self.tree.add_command(session, guild=guild)

        schedule = app_commands.Group(name="schedule", description="Persistent local schedules")
        schedule.command(name="create", description="Create a once/cron schedule")(
            self._schedule_create
        )
        schedule.command(name="list", description="List Conversation schedules")(
            intent("schedule list", self._schedule_list)
        )
        schedule.command(name="show", description="Show a schedule")(
            intent("schedule show", self._schedule_show)
        )
        schedule.command(name="update", description="Update a schedule")(
            self._schedule_update
        )
        schedule.command(name="pause", description="Pause a schedule")(
            intent("schedule pause", self._schedule_pause)
        )
        schedule.command(name="resume", description="Resume a schedule")(
            intent("schedule resume", self._schedule_resume)
        )
        schedule.command(name="delete", description="Delete a schedule")(
            intent("schedule delete", self._schedule_delete)
        )
        schedule.command(name="run-now", description="Run a schedule now")(
            intent("schedule run-now", self._schedule_run_now)
        )
        self.tree.add_command(schedule, guild=guild)

        self.tree.add_command(
            app_commands.Command(
                name="status",
                description="Show codexD status",
                callback=intent("status", self._status),
            ),
            guild=guild,
        )
        if (
            self.side_queries is not None
            and self._optional_available("thread.side_query")
        ):
            self.tree.add_command(
                app_commands.Command(
                    name="btw",
                    description="Ask a temporary question without changing the main task",
                    callback=self._btw,
                ),
                guild=guild,
            )
            self.tree.add_command(
                app_commands.Command(
                    name="side",
                    description="Alias for /btw temporary Side Query",
                    callback=self._side,
                ),
                guild=guild,
            )
        self.tree.add_command(
            app_commands.Command(
                name="usage",
                description="Show the latest provider-reported token usage",
                callback=intent("usage", self._usage),
            ),
            guild=guild,
        )
        if self._optional_available("turn.diff.updated"):
            self.tree.add_command(
                app_commands.Command(
                    name="diff",
                    description="Download the latest Codex diff",
                    callback=intent("diff", self._diff),
                ),
                guild=guild,
            )
        self.tree.add_command(
            app_commands.Command(
                name="diagnostics",
                description="Show codexD incident and queue diagnostics",
                callback=intent("diagnostics", self._diagnostics),
            ),
            guild=guild,
        )
        self.tree.add_command(
            app_commands.Command(
                name="capabilities",
                description="Show Codex SDK capabilities",
                callback=intent("capabilities", self._capabilities),
            ),
            guild=guild,
        )
        self.tree.add_command(
            app_commands.Command(
                name="steer",
                description="Steer the active Turn",
                callback=self._steer,
            ),
            guild=guild,
        )

    @app_commands.describe(
        path="Absolute path, ~/path, or path relative to the service user's home"
    )
    async def _project_bind(
        self, interaction: discord.Interaction[Any], path: str, name: str = "project"
    ) -> None:
        await self._defer_owner(interaction)
        if not isinstance(interaction.channel, discord.TextChannel) or interaction.guild_id is None:
            await interaction.followup.send("Run this in a guild text channel.", ephemeral=True)
            return
        permissions = interaction.app_permissions
        if not all(
            (
                permissions.view_channel,
                permissions.send_messages,
                permissions.send_messages_in_threads,
                permissions.embed_links,
                permissions.attach_files,
                permissions.create_public_threads,
                permissions.manage_threads,
                permissions.read_message_history,
            )
        ):
            raise ConflictError(
                "bot needs view/send/embed/attach/create-thread/send-in-thread/"
                "manage-thread/read-history permissions"
            )
        project = await self.sessions.bind_project(
            name=name,
            path=path,
            guild_id=interaction.guild_id,
            channel_id=interaction.channel.id,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Bound project **{project.name}**. Mention me here to create a Conversation.",
            ephemeral=True,
        )

    async def _project_show(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        project, conversation, routing = await self._command_scope(interaction)
        conversations, runtime = await asyncio.gather(
            asyncio.to_thread(
                self.repository.count_conversations_for_project,
                project.id,
            ),
            self.runtimes.project_status(project.id),
        )
        await interaction.followup.send(
            "\n".join(
                (
                    f"Project: **{discord.utils.escape_markdown(project.name)}**",
                    f"Root: `{_redacted_project_root(project.root_path)}`",
                    f"Routing: `{routing}`"
                    + (
                        " (explicit channel override)"
                        if routing == "binding"
                        else " (immutable Conversation origin)"
                        if conversation is not None
                        else " (default `$HOME`)"
                    ),
                    f"Conversations: {conversations}",
                    f"Runtime: `{runtime['state']}` · generation "
                    f"`{runtime['generation']}`",
                    "Execution: **FULL ACCESS** / auto_review (fixed)",
                )
            ),
            ephemeral=True,
        )

    async def _project_unbind(
        self, interaction: discord.Interaction[Any], confirmation_name: str
    ) -> None:
        await self._defer_owner(interaction)
        if not isinstance(interaction.channel, discord.TextChannel):
            raise ConflictError("run project unbind in the bound text channel")
        if interaction.guild_id is None:
            raise ConflictError("project unbind requires a guild")
        project = await self.sessions.unbind_project(
            guild_id=interaction.guild_id,
            channel_id=interaction.channel.id,
            confirmation_name=confirmation_name,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Removed the **{project.name}** override. Future Conversations use "
            "`$HOME`; existing Conversations keep their original working directory.",
            ephemeral=True,
        )

    async def _status(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        project, conversation, routing = await self._command_scope(interaction)
        counts, runtime, account, inbound = await asyncio.gather(
            asyncio.to_thread(self.repository.health_counts),
            self.runtimes.project_status(project.id),
            self.runtimes.account_status_if_loaded(project.id),
            asyncio.to_thread(self.repository.ingress_reconciliation_counts),
        )
        discord_state = (
            "disconnected"
            if not self._gateway_ready
            else "catching_up"
            if self._inbound_catching_up
            else "degraded"
            if self._ready_preflight_degraded
            or self._command_sync_degraded
            or self._inbound_reconciliation_degraded
            else "ready"
        )
        service_state = (
            "stopping"
            if not self._accepting_ingress
            else "degraded"
            if discord_state != "ready" or counts["outbox_dead_letter"]
            else "healthy"
        )
        auth = self._codex_auth_state
        if account is not None:
            auth = "required" if account.auth_required else "authenticated"
            if not account.auth_required:
                detail = "/".join(
                    value
                    for value in (account.account_type, account.plan_type)
                    if value
                )
                if detail:
                    auth = f"{auth} ({detail})"
        if conversation is None:
            conversation_line = "Conversation: none (run inside a codexD thread for details)"
            provider_line = "Provider: no Conversation selected"
            schedule_line = "Schedule: no Conversation selected"
            turn_line = "Turn: no Conversation selected"
        else:
            turn_summary = await asyncio.to_thread(
                self.repository.conversation_turn_summary,
                conversation.id,
            )
            schedules = await asyncio.to_thread(
                self.schedule_repository.list_for_conversation,
                conversation.id,
            )
            next_due = min(
                (
                    schedule.next_due_at
                    for schedule in schedules
                    if schedule.state.value == "active"
                    and schedule.next_due_at is not None
                ),
                default=None,
            )
            schedule_counts = {
                state: sum(schedule.state.value == state for schedule in schedules)
                for state in ("active", "paused", "blocked")
            }
            conversation_line = (
                f"Conversation: `{conversation.id[:8]}` · `{conversation.state.value}`"
            )
            provider_states = {
                "compact": "compacting-or-active",
                "external_active": "active outside codexD",
                "unknown_effect": "effect outcome unknown",
            }
            barrier_kind = conversation.provider_barrier_kind
            if barrier_kind is not None:
                provider_state = provider_states[barrier_kind]
            elif turn_summary["active"]:
                provider_state = "active"
            else:
                provider_state = "not observed (no local barrier)"
            provider_line = f"Provider: `{provider_state}`"
            schedule_line = (
                f"Schedule: active {schedule_counts['active']} · paused "
                f"{schedule_counts['paused']} · blocked {schedule_counts['blocked']} · "
                f"next {_discord_time(next_due)}"
            )
            turn_line = (
                f"Turn: queued {turn_summary['queued']} · active "
                f"{turn_summary['active']} · last completed "
                f"{_discord_time(turn_summary['last_completed_at'])}"
            )
        await interaction.followup.send(
            "\n".join(
                (
                    f"Service: `{service_state}`",
                    f"Discord: `{discord_state}`",
                    f"Codex auth: `{auth}`",
                    f"Project: **{discord.utils.escape_markdown(project.name)}** · "
                    f"`{routing}` · runtime `{runtime['state']}` generation "
                    f"`{runtime['generation']}`",
                    conversation_line,
                    provider_line,
                    schedule_line,
                    turn_line,
                    "Execution: **FULL ACCESS** / auto_review (fixed)",
                    f"Delivery: pending {counts['outbox_pending']} · retry "
                    f"{counts['outbox_retry']} · dead-letter "
                    f"{counts['outbox_dead_letter']}",
                    "Inbound recovery: scanning "
                    f"{inbound['scanning']} · retry {inbound['retry']} · "
                    f"blocked {inbound['blocked']}",
                )
            ),
            ephemeral=True,
        )

    async def _capabilities(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        manifest = self.capability_manifest
        core_available = [
            name for name, available in sorted(manifest.required.items()) if available
        ]
        core_unavailable = [
            name for name, available in sorted(manifest.required.items()) if not available
        ]
        optional_groups: dict[str, list[str]] = {}
        for name, value in sorted(manifest.optional.items()):
            optional_groups.setdefault(_capability_label(value), []).append(name)
        lines = [
            f"SDK `{manifest.sdk_version}` · runtime `{manifest.runtime_version}`",
            f"Compatibility: `{manifest.compatibility.matrix_tier}` · "
            f"`{manifest.compatibility.handshake}`",
            "**Core**",
            "  available: " + ", ".join(f"`{name}`" for name in core_available),
            *(
                [
                    "  unavailable: "
                    + ", ".join(f"`{name}`" for name in core_unavailable)
                ]
                if core_unavailable
                else []
            ),
            "**Optional**",
            *(
                [
                    f"  `{label}`: "
                    + ", ".join(f"`{name}`" for name in names)
                    for label, names in sorted(optional_groups.items())
                ]
                or ["  none"]
            ),
            "**Product-gated**",
            "  `review`, `plan_mode`, `agent_control`, `sdk_mention_input`, "
            "`account_mutation`",
            "**Discord ingress**",
            "  `bot_mention_input`: available",
            "  `conversation_thread_input`: available",
            "  `ordinary_file_materialization`: "
            + (
                "available"
                if manifest.optional.get("codexd.attachment_materialization") is True
                else "unavailable"
            ),
            "**codexD extension**",
            "  `schedule`: available",
            "**Excluded**",
            "  `workflow`, `/permissions`, raw app-server control",
        ]
        conversation = (
            await self.sessions.conversation_for_thread(interaction.channel_id)
            if interaction.channel_id is not None
            else None
        )
        if conversation is not None:
            catalog = await self.session_lifecycle.model_catalog(conversation.id)
            model = _effective_model(catalog.models, conversation.model_override)
            lines.extend(
                (
                    "**Selected model**",
                    f"  `{model.model}` · personality "
                    f"`{'supported' if model.supports_personality else 'unsupported'}`",
                    "  execution: `full_access` / `auto_review` (fixed)",
                )
            )
        await interaction.followup.send(
            "\n".join(lines),
            ephemeral=True,
        )

    async def _usage(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        payload = await asyncio.to_thread(
            self.repository.latest_event_payload, conversation.id, "usage.updated"
        )
        await interaction.followup.send(
            (
                format_usage(payload)
                if payload is not None
                else "Provider token usage has not been reported for this Conversation."
            ),
            ephemeral=True,
        )

    async def _diff(
        self,
        interaction: discord.Interaction[Any],
        turn: str | None = None,
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        record = (
            await asyncio.to_thread(
                self.repository.resolve_turn,
                conversation.id,
                turn,
            )
            if turn is not None
            else await asyncio.to_thread(
                self.repository.latest_turn_for_conversation,
                conversation.id,
            )
        )
        if record is None:
            await interaction.followup.send(
                "No Turn exists in this Conversation.",
                ephemeral=True,
            )
            return
        recorded_diff = await asyncio.to_thread(
            self.repository.turn_recorded_diff,
            record.id,
        )
        if recorded_diff is None:
            await interaction.followup.send(
                f"Turn `{record.id[:8]}` has no provider-recorded diff.",
                ephemeral=True,
            )
            return
        project = await asyncio.to_thread(
            self.repository.get_project,
            conversation.project_id,
        )
        safe_diff = redact_diff(recorded_diff, project_root=project.root_path)
        title = f"**Turn-recorded changes** · Turn `{record.id[:8]}`"
        inline = f"{title}\n```diff\n{safe_diff}\n```"
        if len(inline) <= 1900 and "```" not in safe_diff:
            await interaction.followup.send(
                inline,
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return
        await interaction.followup.send(
            title,
            file=discord.File(
                io.BytesIO(safe_diff.encode("utf-8")),
                filename=f"codexd-turn-{record.id[:8]}.diff",
                description="Codex provider-recorded changes for one Turn",
            ),
            ephemeral=True,
        )

    async def _diagnostics(
        self, interaction: discord.Interaction[Any]
    ) -> None:
        await self._defer_owner(interaction)
        runtime_leases_task = asyncio.create_task(
            asyncio.to_thread(self.repository.runtime_lease_diagnostics)
        )
        (
            counts,
            incidents,
            runtime,
            integrity,
            schema_version,
            foreign_keys,
        ) = await asyncio.gather(
            asyncio.to_thread(self.repository.health_counts),
            asyncio.to_thread(self.repository.unresolved_incidents, limit=5),
            self.runtimes.status(),
            asyncio.to_thread(self.repository.store.integrity_check),
            asyncio.to_thread(self.repository.store.validate_schema),
            asyncio.to_thread(self.repository.store.foreign_key_check),
        )
        runtime_leases = await runtime_leases_task
        lines = [
            f"Uptime: {_duration_text(utc_now_ms() - self._started_at)}",
            "Discord: `"
            + (
                "disconnected"
                if not self._gateway_ready
                else "degraded"
                if self._ready_preflight_degraded or self._command_sync_degraded
                else "ready"
            )
            + "` · "
            f"auth `{self._codex_auth_state}`",
            f"Database: schema `{schema_version}` · integrity `{integrity}` · "
            f"foreign-key violations `{len(foreign_keys)}`",
            f"Runtime: `{runtime['topology']}` · ready {runtime['ready']}/"
            f"{runtime['capacity_limit']} · starting {runtime['starting']} · waiting "
            f"{runtime['capacity_waiters']} · unhealthy {runtime['unhealthy']} · "
            f"idle TTL {runtime['idle_ttl_seconds']}s · SQLite isolated "
            f"{runtime['sqlite_isolated']}",
            f"Queued Turns: {counts['turns_queued']} · active: {counts['turns_active']}",
            f"Schedules active: {counts['schedules_active']} · blocked: "
            f"{counts['schedules_blocked']} · next "
            f"{_discord_time(counts['schedule_next_due_at'] or None)}",
            f"Attachments retained: {counts['attachments_total']} · cleanup due: "
            f"{counts['attachments_cleanup_due']}",
            f"Outbox pending: {counts['outbox_pending']} · retry: "
            f"{counts['outbox_retry']} · dead-letter: {counts['outbox_dead_letter']} · "
            f"lease losses: {counts['outbox_lease_losses']}",
            f"Provider barriers: {counts['provider_barriers']}",
            f"Capability manifest: `{self.capability_manifest.digest[:16]}`",
        ]
        lines.extend(
            "Runtime lease: "
            f"`{lease['scope_kind']}:{lease['scope_hash']}` generation "
            f"`{lease['generation']}` · `{lease['state']}` · SDK "
            f"`{lease['sdk_version'] or 'n/a'}` · runtime "
            f"`{lease['runtime_version'] or 'n/a'}`"
            for lease in runtime_leases
        )
        lines.extend(
            f"`{incident['severity']}` `{incident['code']}` x"
            f"{incident['occurrence_count']}: {incident['summary']}"
            for incident in incidents
        )
        if not incidents:
            lines.append("No unresolved incidents.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    async def _turn_cancel(
        self,
        interaction: discord.Interaction[Any],
        turn: str | None = None,
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        target = (
            await asyncio.to_thread(
                self.repository.resolve_turn,
                conversation.id,
                turn,
            )
            if turn is not None
            else await asyncio.to_thread(
                self.repository.active_turn_for_conversation,
                conversation.id,
            )
        )
        if target is None:
            await interaction.followup.send("There is no active Turn.", ephemeral=True)
            return
        cancelled = await self.turns.cancel(
            target.id,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Cancel requested for `{cancelled.id[:8]}` "
            f"(`{cancelled.state.value}`).",
            ephemeral=True,
        )

    async def _turn_list(
        self,
        interaction: discord.Interaction[Any],
        state: str | None = None,
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        turns = await asyncio.to_thread(
            self.repository.list_turns,
            conversation.id,
            limit=10,
            state=state,
        )
        text = "\n".join(
            f"`{turn.id[:8]}` · `{turn.state.value}` · `{turn.source_kind.value}` · "
            f"started {_discord_time(turn.started_at or turn.queued_at)} · "
            f"duration {_turn_duration(turn)} · "
            f"summary `{_inline_code(turn.input_summary)}` · "
            f"usage `{_inline_code(turn.usage_scope or 'pending')}`"
            for turn in turns
        ) or "No Turns."
        await interaction.followup.send(text, ephemeral=True)

    async def _turn_show(
        self, interaction: discord.Interaction[Any], turn: str
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        record = await asyncio.to_thread(
            self.repository.resolve_turn, conversation.id, turn
        )
        output, events = await asyncio.gather(
            asyncio.to_thread(self.repository.turn_output, record.id),
            asyncio.to_thread(self.repository.turn_event_summary, record.id),
        )
        provider_identity = (
            sha256_text(record.provider_turn_id)[:12]
            if record.provider_turn_id is not None
            else "not-accepted"
        )
        lines = [
            f"Turn `{record.id}` · **{record.state.value}**",
            f"Timeline: queued {_discord_time(record.queued_at)} · "
            f"started {_discord_time(record.started_at)} · "
            f"ended {_discord_time(record.ended_at)} · duration "
            f"{_turn_duration(record)}",
            f"Source: `{record.source_kind.value}` · provider hash: "
            f"`{provider_identity}`",
            f"Input summary: `{_inline_code(record.input_summary)}`",
            f"Runtime: generation `{record.runtime_generation or 'n/a'}` · "
            f"lease `{(record.runtime_lease_id or 'n/a')[:12]}`",
            f"Model: `{record.effective_model or 'default'}` · reasoning: "
            f"`{record.effective_reasoning_effort or 'default'}`",
            f"Reasoning summary: "
            f"`{record.effective_reasoning_summary or 'default'}` · personality "
            f"`{record.effective_personality or 'default'}` · tier "
            f"`{record.effective_service_tier or 'default'}`",
            f"Sandbox: **{record.effective_sandbox.value.upper()}** · "
            f"approval: `auto_review` · web search: "
            f"`{record.effective_web_search_mode}`",
            f"Terminal: `{record.terminal_code or record.error_code or 'n/a'}` · "
            f"interrupt "
            f"`{record.interrupt_origin.value if record.interrupt_origin else 'n/a'}` / "
            f"`{record.interrupt_reason or 'n/a'}`",
            f"Projection: tools {events['tool_events']} · file/diff results "
            f"{events['file_events']} · usage "
            f"`{record.usage_scope or _usage_observation(events['usage_observed'])}`",
        ]
        if record.input_message_id is not None:
            input_channel_id = (
                conversation.discord_parent_channel_id
                if record.input_message_id == str(conversation.discord_thread_id)
                else conversation.discord_thread_id
            )
            lines.append(
                "Original Discord input: "
                f"https://discord.com/channels/{conversation.discord_guild_id}/"
                f"{input_channel_id}/{record.input_message_id}"
            )
        elif record.schedule_fire_id is not None:
            lines.append(f"Schedule Fire: `{record.schedule_fire_id[:12]}`")
        if record.error_message_redacted:
            lines.append(
                f"Error: `{_inline_code(record.error_message_redacted[:300])}`"
            )
        incidents = cast(tuple[dict[str, str], ...], events["incidents"])
        if incidents:
            lines.append(
                "Incidents: "
                + ", ".join(
                    f"`{incident['severity']}/{incident['code']}` "
                    f"(`{incident['id'][:8]}`)"
                    for incident in incidents
                )
            )
        message_id = events["discord_message_id"]
        if isinstance(message_id, str):
            lines.append(
                "Latest delivered projection: "
                f"https://discord.com/channels/{conversation.discord_guild_id}/"
                f"{conversation.discord_thread_id}/{message_id}"
            )
        if output:
            lines.extend(("", "**Visible assistant transcript**", output[:900]))
        await interaction.followup.send(
            "\n".join(lines),
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    async def _open_modal(
        self,
        interaction: discord.Interaction[Any],
        prepare: Callable[[], Awaitable[discord.ui.Modal]],
    ) -> None:
        task = self._track_ingress()
        try:
            if not self._accepting_ingress:
                await self._respond_shutting_down(interaction)
                return
            preparation_budget = _modal_preparation_budget(interaction)
            try:
                if preparation_budget <= 0:
                    raise TimeoutError
                async with asyncio.timeout(preparation_budget):
                    modal = await prepare()
            except TimeoutError:
                message = (
                    "`interaction_timeout`: modal preparation took too long; "
                    "retry the command."
                )
                if interaction.response.is_done():
                    await interaction.followup.send(message, ephemeral=True)
                else:
                    await interaction.response.send_message(message, ephemeral=True)
                return
            if not self._accepting_ingress:
                await self._respond_shutting_down(interaction)
                return
            await interaction.response.send_modal(modal)
        finally:
            self._untrack_ingress(task)

    async def _steer(self, interaction: discord.Interaction[Any]) -> None:
        async def prepare() -> discord.ui.Modal:
            if not self._authorized_interaction(interaction):
                raise SecurityError("not authorized to steer this Conversation")
            conversation = await self._conversation(interaction)
            active = await asyncio.to_thread(
                self.repository.active_turn_for_conversation,
                conversation.id,
            )
            if active is None:
                raise ConflictError("there is no active Turn")
            if active.state.value == "starting":
                raise ConflictError("wait until the Turn is running before steering")
            if active.state.value != "running":
                raise ConflictError(f"the Turn is {active.state.value} and cannot be steered")
            modal_id = await self._create_modal_intent(
                interaction,
                kind="steer",
                conversation=conversation,
                turn_id=active.id,
            )
            return _SteerModal(custom_id=modal_id)

        await self._open_modal(interaction, prepare)

    async def _btw(
        self,
        interaction: discord.Interaction[Any],
        question: str | None = None,
    ) -> None:
        await self._side_query_command(
            interaction,
            command_name="btw",
            question=question,
        )

    async def _side(
        self,
        interaction: discord.Interaction[Any],
        question: str | None = None,
    ) -> None:
        await self._side_query_command(
            interaction,
            command_name="side",
            question=question,
        )

    async def _side_query_command(
        self,
        interaction: discord.Interaction[Any],
        *,
        command_name: str,
        question: str | None,
    ) -> None:
        if self.side_queries is None:
            raise ConflictError("Side Query is unavailable")
        if question is None:
            async def prepare() -> discord.ui.Modal:
                if not self._authorized_interaction(interaction):
                    raise SecurityError("Side Query is not authorized")
                conversation = await self._conversation(interaction)
                modal_id = await self._create_modal_intent(
                    interaction,
                    kind="side_query",
                    conversation=conversation,
                )
                return _SideQueryModal(custom_id=modal_id)

            await self._open_modal(interaction, prepare)
            return
        await self._run_intent_action(
            interaction,
            command_name=command_name,
            request={
                "question_hash": sha256_text(question),
                "question_size": len(question.encode()),
            },
            action=lambda staged: self._apply_side_query(staged, question),
        )

    async def _apply_side_query(
        self,
        interaction: discord.Interaction[Any],
        question: str,
    ) -> None:
        if self.side_queries is None:
            raise ConflictError("Side Query is unavailable")
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        await interaction.edit_original_response(
            content="BTW · asking Codex…",
            allowed_mentions=discord.AllowedMentions.none(),
        )
        answer = await self.side_queries.ask(
            interaction_id=str(interaction.id),
            conversation_id=conversation.id,
            requested_by_user_id=interaction.user.id,
            question=question,
        )
        footer = "\n\n-# Temporary side answer · main task unchanged"
        try:
            chunks = list(split_discord_text(answer, limit=1750))
        except ValueError:
            chunks = list(split_discord_code(answer, limit=1700))
        if not chunks:
            raise InvariantError("Side Query answer could not be rendered")
        if len(chunks) <= 6:
            await interaction.edit_original_response(
                content=chunks[0] + footer,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            for chunk in chunks[1:]:
                await interaction.followup.send(
                    chunk,
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                    suppress_embeds=True,
                )
            return
        file = discord.File(
            io.BytesIO(answer.encode("utf-8")),
            filename="btw-answer.md",
            description="Complete temporary Side Query answer",
        )
        await interaction.edit_original_response(
            content="The temporary answer is attached as Markdown." + footer,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await interaction.followup.send(
            "Complete temporary answer:",
            file=file,
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    async def _model_show(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        catalog = await self.session_lifecycle.model_catalog(conversation.id)
        selected = _effective_model(catalog.models, conversation.model_override)
        tiers = ", ".join(tier.id for tier in selected.service_tiers) or "none"
        await interaction.followup.send(
            "\n".join(
                (
                    f"Effective model: `{selected.model}` (`{selected.id}`)",
                    "Source: "
                    f"`{_model_source(conversation.model_override)}`",
                    f"Catalog: `{'complete' if catalog.complete else 'incomplete'}` · "
                    f"image input `{'image' in selected.input_modalities}`",
                    "Reasoning efforts: "
                    f"`{', '.join(selected.supported_reasoning_efforts) or 'provider default'}` "
                    "· "
                    f"default `{selected.default_reasoning_effort or 'provider default'}`",
                    f"Personality: "
                    f"`{'supported' if selected.supports_personality else 'unsupported'}`",
                    f"Service tiers: `{tiers}` · default "
                    f"`{selected.default_service_tier or 'provider default'}`",
                    f"Upgrade suggestion: `{'available' if selected.upgrade else 'none'}`",
                )
            ),
            ephemeral=True,
        )

    async def _model_list(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        catalog = await self.session_lifecycle.model_catalog(conversation.id)
        text = "\n".join(
            f"`{model.model}`{' **default**' if model.is_default else ''} · "
            f"input {', '.join(model.input_modalities)} · "
            f"reasoning {', '.join(model.supported_reasoning_efforts) or 'provider default'}"
            for model in catalog.models
        ) or "Codex returned an empty model catalog."
        suffix = "" if catalog.complete else "\n-# Catalog is incomplete."
        await interaction.followup.send(text + suffix, ephemeral=True)

    async def _model_set(
        self, interaction: discord.Interaction[Any], model: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        value = None if model == "default" else model
        await self.session_lifecycle.set_model(
            conversation.id,
            value,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(f"Model override: `{value or 'default'}`", ephemeral=True)

    async def _model_tier_show(
        self, interaction: discord.Interaction[Any]
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        catalog = await self.session_lifecycle.model_catalog(conversation.id)
        selected = next(
            (
                model
                for model in catalog.models
                if (
                    conversation.model_override in {model.id, model.model}
                    if conversation.model_override is not None
                    else model.is_default
                )
            ),
            None,
        )
        if selected is None:
            raise InvariantError("effective model is missing from the Codex catalog")
        tiers = "\n".join(
            f"`{tier.id}` · {tier.name}"
            f"{' · ' + tier.description if tier.description else ''}"
            for tier in selected.service_tiers
        ) or "This model exposes no service tiers."
        await interaction.followup.send(
            f"Model: `{selected.model}` · configured tier: "
            f"`{conversation.service_tier_override or 'default'}`\n{tiers}",
            ephemeral=True,
        )

    async def _model_tier_set(
        self, interaction: discord.Interaction[Any], tier: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        updated = await self.session_lifecycle.set_service_tier(
            conversation.id,
            tier,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Service tier: `{updated.service_tier_override}`.", ephemeral=True
        )

    async def _model_tier_default(
        self, interaction: discord.Interaction[Any]
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        await self.session_lifecycle.set_service_tier(
            conversation.id,
            None,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send("Service tier: `default`.", ephemeral=True)

    async def _reasoning_show(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        catalog = await self.session_lifecycle.model_catalog(conversation.id)
        selected = _effective_model(catalog.models, conversation.model_override)
        await interaction.followup.send(
            "\n".join(
                (
                    f"Model: `{selected.model}`",
                    "Supported efforts: "
                    f"`{', '.join(selected.supported_reasoning_efforts) or 'provider default'}`",
                    f"Model default: `{selected.default_reasoning_effort or 'provider default'}`",
                    f"Conversation override: "
                    f"`{conversation.reasoning_effort_override or 'default'}`",
                    f"Effective: "
                    f"`{_effective_reasoning(conversation, selected)}`",
                )
            ),
            ephemeral=True,
        )

    async def _reasoning_set(
        self, interaction: discord.Interaction[Any], effort: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        value = None if effort == "default" else effort
        await self.session_lifecycle.set_reasoning_effort(
            conversation.id,
            value,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Reasoning effort: `{value or 'default'}`", ephemeral=True
        )

    async def _reasoning_summary_show(
        self, interaction: discord.Interaction[Any]
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        await interaction.followup.send(
            "Reasoning summary: "
            f"`{conversation.reasoning_summary_override or 'default'}`",
            ephemeral=True,
        )

    async def _reasoning_summary_set(
        self, interaction: discord.Interaction[Any], summary: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        updated = await self.session_lifecycle.set_reasoning_summary(
            conversation.id,
            summary,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Reasoning summary: `{updated.reasoning_summary_override}`.",
            ephemeral=True,
        )

    async def _reasoning_summary_default(
        self, interaction: discord.Interaction[Any]
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        await self.session_lifecycle.set_reasoning_summary(
            conversation.id,
            None,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send("Reasoning summary: `default`.", ephemeral=True)

    async def _personality_show(
        self, interaction: discord.Interaction[Any]
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        catalog = await self.session_lifecycle.model_catalog(conversation.id)
        selected = _effective_model(catalog.models, conversation.model_override)
        await interaction.followup.send(
            f"Model `{selected.model}` personality support: "
            f"`{'available' if selected.supports_personality else 'unavailable'}` · "
            f"override `{conversation.personality_override or 'default'}`.",
            ephemeral=True,
        )

    async def _personality_set(
        self, interaction: discord.Interaction[Any], personality: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        value = None if personality == "default" else personality
        updated = await self.session_lifecycle.set_personality(
            conversation.id,
            value,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Personality: `{updated.personality_override or 'default'}`.",
            ephemeral=True,
        )

    async def _websearch_show(
        self, interaction: discord.Interaction[Any]
    ) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        await interaction.followup.send(
            f"Web search mode: `{conversation.web_search_mode}`", ephemeral=True
        )

    async def _websearch_set(
        self,
        interaction: discord.Interaction[Any],
        mode: str,
        confirm_live: bool = False,
    ) -> None:
        await self._defer_owner(interaction)
        if mode == "live" and not confirm_live:
            raise ConflictError("set confirm_live to true to enable live web search")
        conversation = await self._conversation(interaction)
        updated = await self.session_lifecycle.set_web_search(
            conversation.id,
            mode,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Web search mode: `{updated.web_search_mode}`.", ephemeral=True
        )

    async def _session_list(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        revisions = await self.session_lifecycle.list_revisions(conversation.id)
        text = "\n".join(
            f"`{revision.id[:8]}` · {revision.state} · "
            f"{discord.utils.escape_markdown(revision.name) + ' · ' if revision.name else ''}"
            f"provider hash `{sha256_text(revision.provider_thread_id)[:12]}` · "
            f"created {_discord_time(revision.created_at)} · "
            f"last active {_discord_time(revision.activated_at)} · "
            f"v{revision.provider_version}"
            for revision in revisions
        ) or "No Thread revisions."
        await interaction.followup.send(text, ephemeral=True)

    async def _session_status(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_authorized(interaction)
        conversation = await self._conversation(interaction)
        view = await self.session_lifecycle.status_view(conversation.id)
        await interaction.followup.send(
            embed=session_status_embed(
                view,
                disclose_provider_session_id=(
                    interaction.user.id == conversation.owner_user_id
                ),
            ),
            ephemeral=True,
        )

    async def _session_new(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        revision = await self.session_lifecycle.new(
            conversation.id,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Created and activated revision `{revision.id[:8]}`.",
            ephemeral=True,
        )

    async def _session_resume(
        self, interaction: discord.Interaction[Any], revision: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        active = await self.session_lifecycle.resume(
            conversation.id,
            revision,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Resumed revision `{active.id[:8]}`.", ephemeral=True
        )

    async def _session_fork(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        revision = await self.session_lifecycle.fork(
            conversation.id,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            f"Forked and activated revision `{revision.id[:8]}`.", ephemeral=True
        )

    async def _session_archive(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        await self.session_lifecycle.archive(
            conversation.id,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            "Archived the active Codex Thread. Resume a revision or create a new one.",
            ephemeral=True,
        )

    async def _session_rename(
        self, interaction: discord.Interaction[Any], name: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        revision = await self.session_lifecycle.rename(
            conversation.id,
            name,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            "Renamed revision "
            f"`{revision.id[:8]}` to "
            f"**{discord.utils.escape_markdown(revision.name or name)}**.",
            ephemeral=True,
        )

    async def _session_compact(
        self, interaction: discord.Interaction[Any], confirm: bool
    ) -> None:
        await self._defer_owner(interaction)
        if not confirm:
            raise ConflictError("set confirm to true to start compaction")
        conversation = await self._conversation(interaction)
        await self.session_lifecycle.compact(
            conversation.id,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            "Codex accepted the compaction request. New Turns wait until the "
            "provider Thread returns to idle.",
            ephemeral=True,
        )

    async def _session_clear(
        self, interaction: discord.Interaction[Any], confirm: bool
    ) -> None:
        await self._defer_owner(interaction)
        if not confirm:
            raise ConflictError("set confirm to true to detach the active revision")
        conversation = await self._conversation(interaction)
        await self.session_lifecycle.clear(
            conversation.id,
            interaction_id=str(interaction.id),
        )
        await interaction.followup.send(
            "Detached the active revision; the next message starts a new Codex Thread.",
            ephemeral=True,
        )

    async def _schedule_create(
        self,
        interaction: discord.Interaction[Any],
    ) -> None:
        async def prepare() -> discord.ui.Modal:
            await self._require_owner(interaction)
            conversation = await self._conversation(interaction)
            modal_id = await self._create_modal_intent(
                interaction,
                kind="schedule_create",
                conversation=conversation,
            )
            return _ScheduleModal(
                schedule=None,
                custom_id=modal_id,
                default_timezone=self.config.schedule.default_timezone,
                default_misfire_policy=self.config.schedule.default_misfire_policy,
            )

        await self._open_modal(interaction, prepare)

    async def _schedule_list(self, interaction: discord.Interaction[Any]) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        schedules = await asyncio.to_thread(
            self.schedule_repository.list_for_conversation, conversation.id
        )
        text = "\n".join(
            f"`{schedule.id[:8]}` **{_safe_embed_text(schedule.name)}** · "
            f"`{schedule.state.value}` · `{schedule.expression}` · "
            f"`{_timezone_with_offset(schedule.timezone, now_ms=schedule.next_due_at)}` · "
            f"next {_discord_time(schedule.next_due_at)} · "
            f"last {_discord_time(schedule.last_due_at)}"
            for schedule in schedules
        ) or "No schedules."
        await interaction.followup.send(text, ephemeral=True)

    async def _schedule_show(
        self, interaction: discord.Interaction[Any], schedule_id: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        schedule = await asyncio.to_thread(
            self.schedule_repository.resolve, conversation.id, schedule_id
        )
        fires = await asyncio.to_thread(
            self.schedule_repository.list_fires,
            schedule.id,
            limit=5,
        )
        fire_lines = "\n".join(
            f"`{fire.id[:8]}` · `{fire.trigger_kind}/{fire.state}` · "
            f"{_discord_time(fire.scheduled_for or fire.created_at)} · "
            f"Turn `{fire.turn_id[:8] if fire.turn_id else 'none'}`"
            + (f" · error `{fire.error_code}`" if fire.error_code else "")
            for fire in fires
        ) or "No Schedule Fires."
        await interaction.followup.send(
            "\n".join(
                (
                    f"Schedule `{schedule.id}` · **{_safe_embed_text(schedule.name)}** · "
                    f"{schedule.state.value} · v{schedule.version}",
                    f"Kind: `{schedule.kind.value}` · expression: "
                    f"`{schedule.expression}` · timezone: "
                    f"`{_timezone_with_offset(schedule.timezone, now_ms=schedule.next_due_at)}`",
                    f"Misfire: `{schedule.misfire_policy.value}` · next: "
                    f"{_discord_time(schedule.next_due_at)} · last: "
                    f"{_discord_time(schedule.last_due_at)}",
                    "Execution: **FULL ACCESS** / auto_review (fixed)",
                    f"Prompt: {discord.utils.escape_markdown(schedule.prompt_text or '[deleted]')}",
                    f"Recent fires:\n{fire_lines}",
                )
            ),
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    async def _schedule_pause(
        self, interaction: discord.Interaction[Any], schedule_id: str, version: int
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        target = await asyncio.to_thread(
            self.schedule_repository.resolve, conversation.id, schedule_id
        )
        schedule = await self.schedules.pause(
            target.id,
            expected_version=version,
            audit=_schedule_audit_context(interaction),
        )
        await interaction.followup.send(
            f"Schedule `{schedule.id[:8]}` paused (v{schedule.version}).", ephemeral=True
        )

    async def _schedule_update(
        self,
        interaction: discord.Interaction[Any],
        schedule_id: str,
    ) -> None:
        async def prepare() -> discord.ui.Modal:
            await self._require_owner(interaction)
            conversation = await self._conversation(interaction)
            target = await asyncio.to_thread(
                self.schedule_repository.resolve,
                conversation.id,
                schedule_id,
            )
            modal_id = await self._create_modal_intent(
                interaction,
                kind="schedule_update",
                conversation=conversation,
                schedule_id=target.id,
                expected_version=target.version,
            )
            return _ScheduleModal(
                schedule=target,
                custom_id=modal_id,
                default_timezone=self.config.schedule.default_timezone,
                default_misfire_policy=self.config.schedule.default_misfire_policy,
            )

        await self._open_modal(interaction, prepare)

    async def _create_modal_intent(
        self,
        interaction: discord.Interaction[Any],
        *,
        kind: str,
        conversation: ConversationRecord,
        turn_id: str | None = None,
        schedule_id: str | None = None,
        expected_version: int | None = None,
    ) -> str:
        if interaction.guild_id is None or interaction.channel_id is None:
            raise SecurityError("modal command has no Discord scope")
        nonce = secrets.token_urlsafe(9)
        expires_at = utc_now_ms() + 600_000
        modal = await asyncio.to_thread(
            self.repository.create_modal_intent,
            kind=kind,
            conversation_id=conversation.id,
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
            owner_user_id=interaction.user.id,
            nonce=nonce,
            expires_at=expires_at,
            turn_id=turn_id,
            schedule_id=schedule_id,
            expected_version=expected_version,
        )
        return self.signer.modal_id(
            intent_id=modal.id,
            kind=modal.kind,
            expires_at=modal.expires_at,
            nonce=nonce,
        )

    async def _schedule_resume(
        self, interaction: discord.Interaction[Any], schedule_id: str, version: int
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        target = await asyncio.to_thread(
            self.schedule_repository.resolve, conversation.id, schedule_id
        )
        schedule = await self.schedules.resume(
            target.id,
            expected_version=version,
            audit=_schedule_audit_context(interaction),
        )
        await interaction.followup.send(
            f"Schedule `{schedule.id[:8]}` resumed (v{schedule.version}).", ephemeral=True
        )

    async def _schedule_delete(
        self, interaction: discord.Interaction[Any], schedule_id: str, version: int
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        target = await asyncio.to_thread(
            self.schedule_repository.resolve, conversation.id, schedule_id
        )
        await self.schedules.delete(
            target.id,
            expected_version=version,
            audit=_schedule_audit_context(interaction),
        )
        await interaction.followup.send("Schedule deleted.", ephemeral=True)

    async def _schedule_run_now(
        self, interaction: discord.Interaction[Any], schedule_id: str
    ) -> None:
        await self._defer_owner(interaction)
        conversation = await self._conversation(interaction)
        target = await asyncio.to_thread(
            self.schedule_repository.resolve, conversation.id, schedule_id
        )
        turn_id = await self.schedules.run_now(
            target.id,
            interaction_id=str(interaction.id),
            audit=_schedule_audit_context(interaction),
        )
        await interaction.followup.send(
            f"Queued Turn `{turn_id[:8]}`." if turn_id else "Schedule target is blocked.",
            ephemeral=True,
        )

    async def _send_schedule_draft_preview(
        self,
        interaction: discord.Interaction[Any],
        *,
        draft: ScheduleDraftRecord,
        full_access: bool,
        nonce: str,
    ) -> None:
        payload = json.loads(draft.payload_json)
        occurrences = json.loads(draft.occurrences_json)
        if not isinstance(payload, dict) or not isinstance(occurrences, list):
            raise InvariantError("Schedule draft preview is invalid")
        view = discord.ui.View(timeout=600)
        view.add_item(
            discord.ui.Button(
                label="Confirm",
                style=discord.ButtonStyle.danger if full_access else discord.ButtonStyle.success,
                custom_id=self.signer.schedule_draft_id(
                    draft_id=draft.id,
                    action="confirm",
                    nonce=nonce,
                ),
            )
        )
        view.add_item(
            discord.ui.Button(
                label="Cancel",
                style=discord.ButtonStyle.secondary,
                custom_id=self.signer.schedule_draft_id(
                    draft_id=draft.id,
                    action="cancel",
                    nonce=nonce,
                ),
            )
        )
        preview = "\n".join(
            f"{index}. <t:{int(item['utc_ms']) // 1000}:F> "
            f"(`{_safe_embed_text(item['local_display'])}`)"
            for index, item in enumerate(occurrences, start=1)
            if isinstance(item, dict)
            and isinstance(item.get("utc_ms"), int)
            and isinstance(item.get("local_display"), str)
        )
        warning = (
            "⚠️ **FULL ACCESS:** this runs unattended with unrestricted project "
            "and system access."
            if full_access
            else "Review the schedule before confirming it."
        )
        embed = discord.Embed(
            title=f"Schedule · {draft.action}",
            description=warning,
            color=COLOR_FAILURE if full_access else COLOR_RUNNING,
        )
        embed.add_field(
            name="Name",
            value=_safe_embed_text(payload["name"])[:1024],
            inline=True,
        )
        embed.add_field(
            name="When",
            value=(
                f"`{_safe_embed_text(payload['expression'])}`\n"
                f"{_safe_embed_text(payload['timezone'])}"
            )[:1024],
            inline=True,
        )
        embed.add_field(
            name="Misfire",
            value=f"`{_safe_embed_text(payload['misfire_policy'])}`",
            inline=True,
        )
        embed.add_field(
            name="Prompt",
            value=_safe_embed_text(str(payload["prompt_text"])[:1000]),
            inline=False,
        )
        embed.add_field(
            name="Next occurrences",
            value=preview[:1024] or "No future occurrence resolved.",
            inline=False,
        )
        embed.set_footer(text="codexD · confirmation expires in 10 minutes")
        await interaction.followup.send(
            embed=embed,
            view=view,
            ephemeral=True,
        )

    async def _command_scope(
        self,
        interaction: discord.Interaction[Any],
    ) -> tuple[ProjectRecord, ConversationRecord | None, str]:
        if isinstance(interaction.channel, discord.Thread):
            conversation = await self._conversation(interaction)
            project = await asyncio.to_thread(
                self.repository.get_project,
                conversation.project_id,
            )
            return project, conversation, "conversation origin"
        if interaction.guild_id is None or interaction.channel_id is None:
            raise ConflictError("run this command in the configured guild")
        resolved = await self.sessions.resolve_project_for_channel(
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )
        return resolved.project, None, resolved.source

    async def _conversation(
        self, interaction: discord.Interaction[Any]
    ) -> ConversationRecord:
        if not isinstance(interaction.channel, discord.Thread):
            raise ConflictError("run this command in a codexD Conversation thread")
        conversation = await self.sessions.conversation_for_thread(interaction.channel.id)
        if conversation is None:
            raise ConflictError("this Discord thread is not a codexD Conversation")
        if (
            interaction.guild_id != conversation.discord_guild_id
            or interaction.channel.parent_id
            != conversation.discord_parent_channel_id
        ):
            raise SecurityError("Conversation Discord origin does not match")
        if conversation.state.value == "deleted":
            raise ConflictError("this codexD Conversation was deleted")
        return conversation

    async def _defer_authorized(self, interaction: discord.Interaction[Any]) -> None:
        if not self._authorized_interaction(interaction):
            if interaction.response.is_done():
                await interaction.followup.send("Not authorized.", ephemeral=True)
            else:
                await interaction.response.send_message("Not authorized.", ephemeral=True)
            if not isinstance(interaction, _DeferredInteraction):
                self._security_responses_sent.add(str(interaction.id))
            raise SecurityError("Discord interaction is not authorized")
        if (
            not isinstance(interaction, _DeferredInteraction)
            and not interaction.response.is_done()
        ):
            await interaction.response.defer(ephemeral=True)

    async def _defer_owner(self, interaction: discord.Interaction[Any]) -> None:
        await self._require_owner(interaction)
        if (
            not isinstance(interaction, _DeferredInteraction)
            and not interaction.response.is_done()
        ):
            await interaction.response.defer(ephemeral=True)

    async def _require_owner(self, interaction: discord.Interaction[Any]) -> None:
        if (
            not self._authorized_interaction(interaction)
            or interaction.user.id != self.config.discord.owner_user_id
        ):
            if interaction.response.is_done():
                await interaction.followup.send(
                    "Owner permission required.", ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "Owner permission required.", ephemeral=True
                )
            if not isinstance(interaction, _DeferredInteraction):
                self._security_responses_sent.add(str(interaction.id))
            raise SecurityError("Discord interaction requires owner")

    def _authorized_interaction(self, interaction: discord.Interaction[Any]) -> bool:
        return bool(
            interaction.guild_id is not None
            and self._authorized(interaction.user.id, interaction.guild_id)
        )

    def _authorized(self, user_id: int, guild_id: int) -> bool:
        return (
            guild_id == self.config.discord.guild_id
            and user_id in self.config.discord.allowed_user_ids
        )


class _ScheduleModal(discord.ui.Modal):
    def __init__(
        self,
        *,
        schedule: ScheduleRecord | None,
        custom_id: str,
        default_timezone: str,
        default_misfire_policy: str,
    ) -> None:
        super().__init__(
            title="Update codexD Schedule" if schedule else "Create codexD Schedule",
            timeout=600,
            custom_id=custom_id,
        )
        self._name = discord.ui.TextInput[_ScheduleModal](
            label="Name",
            custom_id="schedule_name",
            default=schedule.name if schedule else None,
            max_length=100,
        )
        self._when = discord.ui.TextInput[_ScheduleModal](
            label="When (ISO-8601 instant or 5-field cron)",
            custom_id="schedule_when",
            default=schedule.expression if schedule else None,
            max_length=100,
        )
        self._timezone = discord.ui.TextInput[_ScheduleModal](
            label="IANA timezone",
            custom_id="schedule_timezone",
            default=schedule.timezone if schedule else default_timezone,
            max_length=64,
        )
        self._misfire = discord.ui.TextInput[_ScheduleModal](
            label="Misfire policy (all, latest, or skip)",
            custom_id="schedule_misfire",
            default=(
                schedule.misfire_policy.value
                if schedule is not None
                else default_misfire_policy
            ),
            max_length=8,
        )
        self._prompt = discord.ui.TextInput[_ScheduleModal](
            label="Prompt",
            custom_id="schedule_prompt",
            style=discord.TextStyle.paragraph,
            default=schedule.prompt_text if schedule else None,
            max_length=4000,
        )
        for item in (
            self._name,
            self._when,
            self._timezone,
            self._misfire,
            self._prompt,
        ):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        return


class _SteerModal(discord.ui.Modal, title="Steer active Codex Turn"):
    instruction: discord.ui.TextInput[_SteerModal] = discord.ui.TextInput(
        label="Instruction",
        custom_id="steer_instruction",
        style=discord.TextStyle.paragraph,
        max_length=4000,
    )

    def __init__(self, *, custom_id: str) -> None:
        super().__init__(custom_id=custom_id)

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        return


class _SideQueryModal(discord.ui.Modal, title="Temporary Side Query"):
    question: discord.ui.TextInput[_SideQueryModal] = discord.ui.TextInput(
        label="Question",
        custom_id="side_query_question",
        style=discord.TextStyle.paragraph,
        max_length=4000,
    )

    def __init__(self, *, custom_id: str) -> None:
        super().__init__(custom_id=custom_id)

    async def on_submit(self, interaction: discord.Interaction[Any]) -> None:
        return


async def _respond_modal_error(
    interaction: discord.Interaction[Any],
    error: CodexDError | ValueError,
) -> None:
    message = _bounded_response(
        f"`{getattr(error, 'code', 'invalid_input')}`: {error}"
    )
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def _schedule_audit_context(
    interaction: discord.Interaction[Any],
) -> ScheduleAuditContext:
    return ScheduleAuditContext.discord_user(
        user_id=interaction.user.id,
        interaction_id=str(interaction.id),
    )


def _command_value(value: object) -> object:
    enum_value = getattr(value, "value", None)
    if enum_value is not None and isinstance(
        enum_value, (str, int, float, bool)
    ):
        return enum_value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _modal_values(data: object) -> dict[str, str]:
    if not isinstance(data, Mapping):
        raise ConflictError("modal submission payload is missing")
    rows = data.get("components")
    if not isinstance(rows, list):
        raise ConflictError("modal submission fields are missing")
    values: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        components = row.get("components")
        if not isinstance(components, list):
            continue
        for component in components:
            if not isinstance(component, Mapping):
                continue
            custom_id = component.get("custom_id")
            value = component.get("value")
            if isinstance(custom_id, str) and isinstance(value, str):
                values[custom_id] = value
    return values


def _bounded_response(value: str) -> str:
    if len(value) <= 1900:
        return value
    try:
        first = split_discord_text(value, limit=1899)[0]
    except ValueError:
        first = split_discord_code(value, limit=1899)[0]
    return first[:1899] + "…"


def _modal_preparation_budget(interaction: discord.Interaction[Any]) -> float:
    created_at = getattr(interaction, "created_at", None)
    elapsed = 0.0
    if isinstance(created_at, datetime):
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        elapsed = max(
            0.0,
            (datetime.now(UTC) - created_at.astimezone(UTC)).total_seconds(),
        )
    return max(
        0.0,
        _DISCORD_INTERACTION_ACK_DEADLINE_SECONDS
        - elapsed
        - _MODAL_RESPONSE_NETWORK_BUDGET_SECONDS,
    )


def _bounded_result(value: str) -> str:
    return value if len(value) <= 512 else value[:511] + "…"


def _inline_code(value: object) -> str:
    return str(value).replace("`", "'")


def _safe_embed_text(value: object) -> str:
    return discord.utils.escape_markdown(
        discord.utils.escape_mentions(str(value))
    )


def _redacted_project_root(value: object) -> str:
    return f"project-root#{sha256_text(str(value))[:12]}"


def _discord_time(value: object) -> str:
    if not isinstance(value, int):
        return "`n/a`"
    return f"<t:{value // 1000}:R>"


def _timezone_with_offset(timezone_name: str, *, now_ms: int | None = None) -> str:
    instant = datetime.fromtimestamp(
        (utc_now_ms() if now_ms is None else now_ms) / 1000,
        tz=UTC,
    ).astimezone(ZoneInfo(timezone_name))
    offset = instant.strftime("%z")
    return f"{timezone_name} (UTC{offset[:3]}:{offset[3:]})"


def _duration_text(milliseconds: int) -> str:
    seconds = max(0, milliseconds // 1000)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _turn_duration(turn: TurnRecord) -> str:
    if turn.started_at is None:
        return "queued"
    end = turn.ended_at if turn.ended_at is not None else utc_now_ms()
    return _duration_text(end - turn.started_at)


def _capability_label(value: object) -> str:
    if isinstance(value, EventCapability):
        return value.value
    return "available" if value is True else "unavailable"


def _usage_observation(observed: object) -> str:
    return "observed" if observed is True else "not reported"


def _model_source(override: str | None) -> str:
    return "conversation override" if override else "provider default"


def _effective_reasoning(
    conversation: ConversationRecord,
    model: ModelDescriptor,
) -> str:
    return (
        conversation.reasoning_effort_override
        or model.default_reasoning_effort
        or "provider default"
    )


def _effective_model(
    models: Sequence[ModelDescriptor],
    override: str | None,
) -> ModelDescriptor:
    if override is not None:
        selected = next(
            (model for model in models if override in {model.id, model.model}),
            None,
        )
    else:
        selected = next((model for model in models if model.is_default), None)
    if selected is None:
        raise InvariantError("effective model is missing from the Codex catalog")
    return selected


def _remove_bot_mention(message: discord.Message, user_id: int) -> str:
    if not any(user.id == user_id for user in message.mentions):
        return message.content.strip()
    match = re.search(rf"<@!?{user_id}>", message.content)
    if match is None:
        return message.content.strip()
    return (message.content[: match.start()] + message.content[match.end() :]).strip()


def _message_attachments(message: discord.Message) -> list[discord.Attachment]:
    # Filename and Content-Type are untrusted classification hints. The unified
    # ingestor downloads each item once and classifies its bounded content.
    return list(message.attachments)


def _attachment_manifest_hash(
    attachments: list[discord.Attachment],
) -> str:
    return sha256_text(
        canonical_json(
            [
                {
                    "id": str(item.id),
                    "filename": item.filename,
                    "size": item.size,
                    "content_type": item.content_type,
                }
                for item in attachments
            ]
        )
    )
