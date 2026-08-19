from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import io
import json
import logging
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, overload

import discord

from codexd.application.volatile_turns import VolatileTurnStore
from codexd.domain.ids import sha256_text, utc_now_ms
from codexd.errors import CodexDError, InvariantError, NotFoundError
from codexd.rendering.discord import (
    DISCORD_ATTACHMENT_LIMIT_BYTES,
    AttachmentKind,
    DiscordRenderPlanner,
    DurableRenderedAttachment,
    RenderedAttachment,
    split_discord_code,
    split_discord_text,
    suppress_visualization_markers,
)
from codexd.security.redaction import safe_thread_title_summary
from codexd.security.signing import ComponentSigner
from codexd.storage.records import OutboundImageInvocationRecord, OutboxRecord
from codexd.storage.repository import Repository
from codexd.transport.discord.presentation import (
    attachment_embed,
    notice_embed,
    progress_embed,
    schedule_draft_embed,
    table_copy_view,
    table_embed,
    table_source_embed,
    task_card_embed,
    terminal_footer,
)

logger = logging.getLogger(__name__)
_PROMPT_REACTIONS = {
    "waiting": "⏳",
    "completed": "✅",
    "failed": "❌",
}


@dataclass(frozen=True)
class DeliveryResult:
    discord_message_id: str | None = None
    task_card_view_id: str | None = None
    turn_progress_id: str | None = None
    initial_ingress_message_id: str | None = None
    schedule_draft_id: str | None = None


class DeliveryError(RuntimeError):
    def __init__(
        self,
        code: str,
        *,
        permanent: bool,
        retry_after: float | None = None,
        incident_code: str | None = None,
    ) -> None:
        self.code = code
        self.permanent = permanent
        self.retry_after = retry_after
        self.incident_code = incident_code
        super().__init__(code)


class OutboxTransport(Protocol):
    async def deliver(self, record: OutboxRecord) -> DeliveryResult: ...


class OutboxWorker:
    def __init__(
        self,
        *,
        repository: Repository,
        transport: OutboxTransport,
        worker_id: str,
        poll_seconds: float = 0.5,
        concurrency: int = 4,
        lease_ms: int = 30_000,
        lease_renew_seconds: float = 10.0,
        initial_ingress_ready: Callable[[str], Awaitable[None]] | None = None,
        acknowledged: Callable[[OutboxRecord], None] | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("outbox poll interval must be positive")
        if concurrency < 1 or concurrency > 32:
            raise ValueError("outbox concurrency must be between 1 and 32")
        if (
            lease_ms < 1
            or lease_renew_seconds <= 0
            or lease_renew_seconds * 1000 >= lease_ms
        ):
            raise ValueError("outbox lease renewal must occur before lease expiry")
        self._repository = repository
        self._transport = transport
        self._worker_id = worker_id
        self._poll_seconds = poll_seconds
        self._concurrency = concurrency
        self._lease_ms = lease_ms
        self._lease_renew_seconds = lease_renew_seconds
        self._initial_ingress_ready = initial_ingress_ready
        self._acknowledged = acknowledged
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="codexd-discord-outbox")

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def drain_once(self) -> bool:
        record = await asyncio.to_thread(
            self._repository.claim_outbox,
            worker_id=self._worker_id,
            lease_ms=self._lease_ms,
        )
        if record is None:
            return False
        lease_released = asyncio.Event()
        delivery = asyncio.create_task(
            self._deliver_claimed(record, lease_released),
            name=f"codexd-outbox-delivery-{record.id}",
        )
        renewal = asyncio.create_task(
            self._renew_lease(record, lease_released),
            name=f"codexd-outbox-lease-{record.id}",
        )
        try:
            done, _pending = await asyncio.wait(
                {delivery, renewal},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if delivery in done:
                return delivery.result()
            error = renewal.exception()
            if error is not None:
                raise error
            return await delivery
        finally:
            lease_released.set()
            if not delivery.done():
                delivery.cancel()
            await asyncio.gather(delivery, renewal, return_exceptions=True)

    async def _deliver_claimed(
        self,
        record: OutboxRecord,
        lease_released: asyncio.Event,
    ) -> bool:
        try:
            result = await self._transport.deliver(record)
        except asyncio.CancelledError:
            raise
        except DeliveryError as exc:
            delay = (
                exc.retry_after
                if exc.retry_after is not None
                else _retry_delay(record.attempts)
            )
            lease_released.set()
            if exc.permanent and record.operation == "create_thread":
                await asyncio.to_thread(
                    self._repository.fail_thread_creation_outbox,
                    record.id,
                    lease_owner=record.lease_owner,
                    lease_attempt=record.attempts,
                    error_code=exc.code,
                )
            elif exc.permanent:
                await asyncio.to_thread(
                    self._repository.fail_outbox_permanently,
                    record.id,
                    lease_owner=record.lease_owner,
                    lease_attempt=record.attempts,
                    error_code=exc.code,
                )
            else:
                await asyncio.to_thread(
                    self._repository.retry_outbox,
                    record.id,
                    lease_owner=record.lease_owner,
                    lease_attempt=record.attempts,
                    error_code=exc.code,
                    next_attempt_at=utc_now_ms() + int(delay * 1000),
                    permanent=exc.permanent,
                    incident_code=exc.incident_code,
                    incident_summary=(
                        "Discord reconciliation could not prove whether delivery "
                        "already completed"
                        if exc.incident_code is not None
                        else None
                    ),
                    incident_details=(
                        {
                            "outbox_id": record.id,
                            "destination_key": record.destination_key,
                            "error_code": exc.code,
                        }
                        if exc.incident_code is not None
                        else None
                    ),
                )
            return True

        except Exception as exc:
            logger.exception("Unexpected outbox delivery failure")
            await self._record_worker_incident(
                code="outbox_delivery_internal_error",
                summary="Discord outbox delivery failed unexpectedly",
                details={"outbox_id": record.id, "exception": type(exc).__name__},
            )
            lease_released.set()
            await asyncio.to_thread(
                self._repository.retry_outbox,
                record.id,
                lease_owner=record.lease_owner,
                lease_attempt=record.attempts,
                error_code="outbox_delivery_internal_error",
                next_attempt_at=utc_now_ms() + int(_retry_delay(record.attempts) * 1000),
                permanent=False,
            )
            return True
        lease_released.set()
        await asyncio.to_thread(
            self._repository.ack_outbox,
            record.id,
            lease_owner=record.lease_owner,
            lease_attempt=record.attempts,
            discord_message_id=result.discord_message_id,
            task_card_view_id=result.task_card_view_id,
            turn_progress_id=result.turn_progress_id,
            schedule_draft_id=result.schedule_draft_id,
        )
        if self._acknowledged is not None:
            self._acknowledged(record)
        if (
            result.initial_ingress_message_id is not None
            and self._initial_ingress_ready is not None
        ):
            try:
                await self._initial_ingress_ready(
                    result.initial_ingress_message_id
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await asyncio.to_thread(
                    self._repository.reject_ingress_message,
                    discord_message_id=result.initial_ingress_message_id,
                    error_code="initial_preflight_internal_error",
                )
                await self._record_worker_incident(
                    code="initial_ingress_callback_failed",
                    summary="Initial Conversation message preflight failed unexpectedly",
                    details={
                        "discord_message_id": result.initial_ingress_message_id,
                        "exception": type(exc).__name__,
                    },
                )
        return True

    async def _renew_lease(
        self,
        record: OutboxRecord,
        stop: asyncio.Event,
    ) -> None:
        while not stop.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self._lease_renew_seconds,
                )
            if stop.is_set():
                return
            renewed = await asyncio.to_thread(
                self._repository.renew_outbox_lease,
                record.id,
                lease_owner=record.lease_owner,
                lease_attempt=record.attempts,
                lease_ms=self._lease_ms,
            )
            if not renewed:
                if stop.is_set():
                    return
                await self._record_worker_incident(
                    code="outbox_delivery_lease_lost",
                    summary="Discord outbox delivery lease was lost",
                    details={"outbox_id": record.id},
                )
                raise OutboxLeaseLost(
                    f"outbox delivery lease was lost for {record.id}"
                )

    async def _run(self) -> None:
        async with asyncio.TaskGroup() as workers:
            for index in range(self._concurrency):
                workers.create_task(
                    self._worker_loop(),
                    name=f"codexd-discord-outbox-{index}",
                )

    async def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                processed = await self.drain_once()
            except asyncio.CancelledError:
                raise
            except OutboxLeaseLost:
                processed = False
            except Exception as exc:
                logger.exception("Outbox worker iteration failed")
                await self._record_worker_incident(
                    code="outbox_worker_internal_error",
                    summary="Discord outbox worker iteration failed",
                    details={"exception": type(exc).__name__},
                )
                processed = False
            if processed:
                continue
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_seconds)

    async def _record_worker_incident(
        self,
        *,
        code: str,
        summary: str,
        details: dict[str, str],
    ) -> None:
        try:
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="error",
                code=code,
                summary=summary,
                details=details,
            )
        except Exception:
            logger.exception("Failed to persist outbox worker incident")


class OutboxLeaseLost(RuntimeError):
    """The delivery worker no longer owns its durable outbox claim."""


class DiscordOutboxTransport:
    def __init__(
        self,
        *,
        client: discord.Client,
        repository: Repository,
        renderer: DiscordRenderPlanner,
        signer: ComponentSigner,
        volatile_turns: VolatileTurnStore | None = None,
    ) -> None:
        nonce_support = inspect.signature(discord.abc.Messageable.send).parameters.get(
            "nonce"
        )
        if nonce_support is None:
            raise InvariantError(
                "discord.py does not expose native message nonce idempotency"
            )
        self._client = client
        self._repository = repository
        self._renderer = renderer
        self._signer = signer
        self._volatile_turns = volatile_turns or VolatileTurnStore()

    def acknowledged(self, record: OutboxRecord) -> None:
        try:
            payload = json.loads(record.payload_json)
        except json.JSONDecodeError:
            return
        if isinstance(payload, dict) and payload.get("kind") == "turn_final":
            turn_id = payload.get("turn_id")
            if isinstance(turn_id, str):
                self._volatile_turns.discard(turn_id)

    async def deliver(self, record: OutboxRecord) -> DeliveryResult:
        try:
            payload = json.loads(record.payload_json)
        except json.JSONDecodeError as exc:
            raise DeliveryError("payload_invalid", permanent=True) from exc
        if not isinstance(payload, dict):
            raise DeliveryError("payload_invalid", permanent=True)
        try:
            if record.operation == "delete":
                return await self._deliver_turn_progress_delete(
                    record.id,
                )
            channel = await self._destination(record.destination_key)
            if payload.get("kind") == "prompt_reaction":
                return await self._deliver_prompt_reaction(channel, payload)
            if payload.get("kind") == "create_thread":
                return await self._deliver_create_thread(channel, payload)
            if payload.get("kind") == "thread_rename":
                if not isinstance(channel, discord.Thread):
                    raise DeliveryError(
                        "thread_rename_destination_invalid", permanent=True
                    )
                name = payload.get("name")
                if not isinstance(name, str) or not name:
                    raise DeliveryError("thread_rename_payload_invalid", permanent=True)
                await channel.edit(name=name, reason="codexD session rename")
                return DeliveryResult(None)
            if payload.get("kind") == "turn_final":
                return await self._deliver_final(
                    channel, payload, record.delivery_marker, record.state
                )
            if payload.get("kind") == "task_card":
                return await self._deliver_task_card(
                    channel,
                    payload,
                    record.operation,
                    record.delivery_marker,
                    record.state,
                )
            if payload.get("kind") == "schedule_draft_card":
                return await self._deliver_schedule_draft_card(
                    channel,
                    payload,
                    record.id,
                    record.operation,
                    record.delivery_marker,
                    record.state,
                )
            if payload.get("kind") == "turn_progress":
                return await self._deliver_turn_progress(
                    channel,
                    payload,
                    record.operation,
                    record.delivery_marker,
                    record.state,
                )
            if record.state != "pending":
                existing = await self._find_marker(channel, record.delivery_marker)
                if existing is not None:
                    return DeliveryResult(str(existing.id))
            raw_content_value = payload.get("content")
            if not isinstance(raw_content_value, str):
                raise DeliveryError("payload_invalid", permanent=True)
            raw_content = raw_content_value
            content = ""
            level = str(payload.get("level", "info"))
            raw_title = payload.get("title")
            title = str(raw_title) if raw_title is not None else None
            message = await channel.send(
                content,
                embed=notice_embed(
                    raw_content or "codexD update",
                    level=level,
                    title=title,
                ),
                allowed_mentions=discord.AllowedMentions.none(),
                nonce=_delivery_nonce(record.delivery_marker),
            )
            return DeliveryResult(str(message.id))
        except (KeyError, TypeError, ValueError) as exc:
            raise DeliveryError("payload_invalid", permanent=True) from exc
        except discord.Forbidden as exc:
            raise DeliveryError("discord_forbidden", permanent=True) from exc
        except discord.NotFound as exc:
            raise DeliveryError("discord_destination_not_found", permanent=True) from exc
        except discord.HTTPException as exc:
            permanent = 400 <= exc.status < 500 and exc.status != 429
            raise _discord_http_error(
                exc,
                code=f"discord_http_{exc.status}",
                permanent=permanent,
            ) from exc

    async def _deliver_turn_progress_delete(
        self,
        outbox_id: str,
    ) -> DeliveryResult:
        try:
            target = await asyncio.to_thread(
                self._repository.turn_progress_delete_target,
                outbox_id,
            )
        except (InvariantError, NotFoundError) as exc:
            raise DeliveryError(
                "turn_progress_delete_target_invalid",
                permanent=True,
            ) from exc
        message_id = target.discord_message_id
        if message_id is None:
            return DeliveryResult()
        try:
            parsed_message_id = int(message_id)
        except ValueError as exc:
            raise DeliveryError(
                "turn_progress_delete_message_id_invalid",
                permanent=True,
            ) from exc
        if parsed_message_id <= 0:
            raise DeliveryError(
                "turn_progress_delete_message_id_invalid",
                permanent=True,
            )
        channel = await self._destination(
            target.destination_key,
            missing_ok=True,
        )
        if channel is None:
            return DeliveryResult()
        try:
            message = await channel.fetch_message(parsed_message_id)
        except discord.NotFound:
            return DeliveryResult()
        bot_user = self._client.user
        if bot_user is None:
            raise DeliveryError(
                "discord_bot_identity_unavailable",
                permanent=False,
            )
        if message.author.id != bot_user.id:
            raise DeliveryError(
                "turn_progress_delete_author_mismatch",
                permanent=True,
            )
        try:
            await message.delete()
        except discord.NotFound:
            return DeliveryResult()
        return DeliveryResult()

    async def _deliver_create_thread(
        self,
        channel: discord.TextChannel | discord.Thread,
        payload: dict[str, object],
    ) -> DeliveryResult:
        if not isinstance(channel, discord.TextChannel):
            raise DeliveryError("thread_create_destination_invalid", permanent=True)
        starter_id = _snowflake(payload.get("starter_message_id"), "starter_message_id")
        expected_id = _snowflake(payload.get("expected_thread_id"), "expected_thread_id")
        if starter_id != expected_id:
            raise DeliveryError("thread_create_identity_invalid", permanent=True)
        owner_user_id = _snowflake(payload.get("owner_user_id"), "owner_user_id")
        legacy_name = payload.get("name")
        name_strategy = payload.get("name_strategy")
        name_suffix = payload.get("name_suffix")
        if not (
            isinstance(legacy_name, str)
            and legacy_name
            and len(legacy_name) <= 100
        ) and not (
            name_strategy == "starter_message"
            and isinstance(name_suffix, str)
            and 1 <= len(name_suffix) <= 12
            and name_suffix.isalnum()
        ):
            raise DeliveryError("thread_create_name_invalid", permanent=True)
        thread = await self._existing_thread(expected_id)
        if thread is None:
            message = await channel.fetch_message(starter_id)
            thread = message.thread
            if thread is None:
                name = legacy_name
                if name_strategy == "starter_message":
                    title = safe_thread_title_summary(
                        message.content,
                        has_image_attachment=any(
                            (attachment.content_type or "").startswith("image/")
                            for attachment in message.attachments
                        ),
                    )
                    name = f"{title} · {name_suffix}"
                assert isinstance(name, str)
                try:
                    thread = await message.create_thread(
                        name=name,
                        reason="codexD Conversation creation",
                    )
                except discord.HTTPException:
                    thread = await self._existing_thread(expected_id)
                    if thread is None:
                        raise
        if thread.id != expected_id:
            raise DeliveryError("thread_create_identity_mismatch", permanent=True)
        await asyncio.to_thread(
            self._repository.finalize_thread_creation,
            discord_message_id=str(starter_id),
            discord_thread_id=thread.id,
            owner_user_id=owner_user_id,
        )
        return DeliveryResult(
            discord_message_id=str(thread.id),
            initial_ingress_message_id=str(starter_id),
        )

    async def _deliver_prompt_reaction(
        self,
        channel: discord.TextChannel | discord.Thread,
        payload: dict[str, object],
    ) -> DeliveryResult:
        message_id = _snowflake(payload.get("message_id"), "message_id")
        state = payload.get("state")
        if not isinstance(state, str) or state not in _PROMPT_REACTIONS:
            raise DeliveryError("prompt_reaction_payload_invalid", permanent=True)
        bot_user = self._client.user
        if bot_user is None:
            raise DeliveryError("discord_bot_identity_unavailable", permanent=False)
        message = await channel.fetch_message(message_id)
        own_reactions = {
            str(reaction.emoji)
            for reaction in message.reactions
            if reaction.me
        }
        desired = _PROMPT_REACTIONS[state]
        for emoji in _PROMPT_REACTIONS.values():
            if emoji != desired and emoji in own_reactions:
                await message.remove_reaction(emoji, bot_user)
        if desired not in own_reactions:
            await message.add_reaction(desired)
        return DeliveryResult(str(message.id))

    async def _existing_thread(self, thread_id: int) -> discord.Thread | None:
        cached = self._client.get_channel(thread_id)
        if isinstance(cached, discord.Thread):
            return cached
        try:
            fetched = await self._client.fetch_channel(thread_id)
        except discord.NotFound:
            return None
        if isinstance(fetched, discord.Thread):
            return fetched
        raise DeliveryError("thread_create_identity_collision", permanent=True)

    @overload
    async def _destination(
        self,
        destination_key: str,
        *,
        missing_ok: Literal[False] = False,
    ) -> discord.TextChannel | discord.Thread: ...

    @overload
    async def _destination(
        self,
        destination_key: str,
        *,
        missing_ok: Literal[True],
    ) -> discord.TextChannel | discord.Thread | None: ...

    async def _destination(
        self,
        destination_key: str,
        *,
        missing_ok: bool = False,
    ) -> discord.TextChannel | discord.Thread | None:
        kind, separator, raw_id = destination_key.partition(":")
        if not separator or kind not in {"thread", "channel"}:
            raise DeliveryError("invalid_destination_key", permanent=True)
        try:
            channel_id = int(raw_id)
        except ValueError as exc:
            raise DeliveryError("invalid_destination_key", permanent=True) from exc
        channel = self._client.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self._client.fetch_channel(channel_id)
            except discord.HTTPException as exc:
                if missing_ok and exc.status == 404:
                    return None
                raise _discord_http_error(
                    exc,
                    code="discord_destination_lookup_failed",
                    permanent=exc.status in {403, 404},
                ) from exc
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            raise DeliveryError("invalid_destination_type", permanent=True)
        if isinstance(channel, discord.Thread) and channel.archived and not channel.locked:
            try:
                await channel.edit(archived=False)
            except discord.HTTPException as exc:
                if missing_ok and exc.status == 404:
                    return None
                raise
        return channel

    async def _deliver_final(
        self,
        channel: discord.TextChannel | discord.Thread,
        payload: dict[str, object],
        marker: str,
        record_state: str,
    ) -> DeliveryResult:
        turn_id = payload.get("turn_id")
        if not isinstance(turn_id, str):
            raise DeliveryError("turn_final_payload_invalid", permanent=True)
        volatile = self._volatile_turns.final(turn_id)
        visible_text = (
            volatile.visible_text
            if volatile is not None
            else _volatile_final_unavailable(
                str(payload.get("state", "interrupted")),
                was_volatile=payload.get("content_storage") == "volatile",
            )
        )
        failure_guidance = _attachment_failure_guidance(payload.get("terminal_code"))
        provider_guidance = _provider_failure_guidance(payload)
        recovery_guidance = _provider_thread_recovery_guidance(
            payload.get("terminal_code")
        )
        if failure_guidance is not None:
            visible_text += f"\n\n{failure_guidance}"
        if provider_guidance is not None:
            visible_text += f"\n\n{provider_guidance}"
        if recovery_guidance is not None:
            visible_text += f"\n\n{recovery_guidance}"
        raw_outbound_records = await asyncio.to_thread(
            self._repository.registered_outbound_images,
            turn_id,
        )
        outbound_records: Sequence[OutboundImageInvocationRecord] = (
            raw_outbound_records
            if isinstance(raw_outbound_records, (tuple, list))
            else ()
        )
        try:
            outbound_attachments = _registered_image_attachments(
                outbound_records,
                artifact_root=self._renderer.artifact_root,
            )
        except DeliveryError as exc:
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="error",
                code="outbound_image_artifact_unavailable",
                summary="A registered outbound image was unavailable at final delivery",
                turn_id=turn_id,
                details={"stable_code": exc.code},
            )
            outbound_attachments = ()
            visible_text += (
                "\n\n[The generated image could not be loaded for Discord delivery.]"
            )
        raw_dynamic_tools_enabled = payload.get("dynamic_tools_enabled")
        dynamic_tools_enabled = (
            raw_dynamic_tools_enabled
            if isinstance(raw_dynamic_tools_enabled, bool)
            else None
        )
        marker_result = suppress_visualization_markers(
            visible_text,
            has_registered_images=bool(outbound_attachments),
            has_registered_image_records=bool(outbound_records),
            dynamic_tools_enabled=dynamic_tools_enabled,
        )
        visible_text = marker_result.text
        incident_by_reason = {
            "legacy_session": (
                "visualization_legacy_session",
                "A legacy Codex session could not register an image for Discord",
            ),
            "publish_tool_not_used": (
                "visualization_publish_tool_not_used",
                "Codex emitted a visualization control without registering an image",
            ),
            "unknown": (
                "visualization_attachment_missing",
                "A visualization had no registered Discord image",
            ),
            "artifact_unavailable": (
                "visualization_artifact_unavailable",
                "A registered visualization artifact was unavailable for Discord",
            ),
        }
        marker_incident = (
            incident_by_reason.get(marker_result.missing_reason)
            if marker_result.missing_reason is not None
            else None
        )
        if marker_incident is not None:
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="warning",
                code=marker_incident[0],
                summary=marker_incident[1],
                turn_id=turn_id,
            )
        try:
            rendered = await self._renderer.render_markdown(visible_text)
        except (CodexDError, OSError, ValueError):
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="error",
                code="discord_render_fallback",
                summary="Discord in-memory rich rendering failed; bounded text was used",
                turn_id=turn_id,
                details={"stable_code": "volatile_render_failed"},
            )
            messages = list(_bounded_plain_text_fallback(visible_text))
            attachments: list[RenderedAttachment | DurableRenderedAttachment] = list(
                outbound_attachments
            )
        else:
            messages = list(rendered.messages)
            attachments = [*rendered.attachments, *outbound_attachments]
            for code in dict.fromkeys(rendered.incident_codes):
                await asyncio.to_thread(
                    self._repository.record_incident,
                    severity="warning",
                    code=code,
                    summary="Discord in-memory rendering used a bounded fallback",
                    turn_id=turn_id,
                )
        if not messages and not attachments:
            state = str(payload.get("state", "completed"))
            messages = [
                {
                    "completed": "Codex completed without a final response.",
                    "failed": "Codex failed before producing a final response.",
                    "cancelled": "Codex was cancelled before producing a final response.",
                    "interrupted": "Codex was interrupted before producing a final response.",
                }.get(state, f"Codex ended in state `{state}` without a final response.")
            ]
        reference = _final_message_reference(channel, payload)
        source_url = _final_source_url(channel, payload)
        if source_url is not None:
            prefix = f"-# Original request: <{source_url}>\n\n"
            if messages and len(prefix) + len(messages[0]) + 25 <= 2000:
                messages[0] = prefix + messages[0]
            else:
                messages.insert(0, prefix.rstrip())
        first: discord.Message | None = None
        for index, content in enumerate(messages):
            part_marker = marker if index == 0 else f"{marker}-{index}"
            if record_state != "pending":
                existing = await self._find_marker(channel, part_marker)
                if existing is not None:
                    first = first or existing
                    continue
            if index == 0 and reference is not None:
                message = await channel.send(
                    _bounded_message_content(content),
                    allowed_mentions=discord.AllowedMentions.none(),
                    suppress_embeds=True,
                    reference=reference,
                    mention_author=False,
                    nonce=_delivery_nonce(part_marker),
                )
            else:
                message = await channel.send(
                    _bounded_message_content(content),
                    allowed_mentions=discord.AllowedMentions.none(),
                    suppress_embeds=True,
                    nonce=_delivery_nonce(part_marker),
                )
            first = first or message
        table_groups, generic_attachments = _partition_table_attachments(attachments)
        for table_index, (source, images) in enumerate(table_groups):
            delivered = await self._deliver_table_attachments(
                channel,
                source=source,
                images=images,
                marker=f"{marker}-t{table_index}",
                record_state=record_state,
            )
            first = first or delivered

        parts = _attachment_parts(generic_attachments)
        for group_index, group in enumerate(_attachment_groups(parts)):
            part_marker = f"{marker}-a{group_index}"
            if record_state != "pending":
                existing = await self._find_marker(channel, part_marker)
                if existing is not None:
                    first = first or existing
                    continue
            try:
                message = await _send_files(
                    channel,
                    content="",
                    attachments=[item for _suffix, item in group],
                    embed=attachment_embed(
                        [item.filename for _suffix, item in group]
                    ),
                    nonce=_delivery_nonce(part_marker),
                )
            except discord.HTTPException as exc:
                if exc.status not in {400, 403, 413}:
                    raise
                fallback = await self._deliver_attachment_fallback(
                    channel,
                    group,
                    turn_id=turn_id,
                    marker=marker,
                    record_state=record_state,
                )
                first = first or fallback
            else:
                first = first or message

        footer_marker = f"{marker}-footer"
        if record_state != "pending":
            existing = await self._find_marker(channel, footer_marker)
            if existing is not None:
                first = first or existing
                return DeliveryResult(str(first.id) if first else None)
        footer = await channel.send(
            _bounded_message_content(terminal_footer(payload)),
            allowed_mentions=discord.AllowedMentions.none(),
            suppress_embeds=True,
            nonce=_delivery_nonce(footer_marker),
        )
        first = first or footer
        return DeliveryResult(str(first.id) if first else None)

    async def _deliver_table_attachments(
        self,
        channel: discord.TextChannel | discord.Thread,
        *,
        source: RenderedAttachment | DurableRenderedAttachment,
        images: list[RenderedAttachment | DurableRenderedAttachment],
        marker: str,
        record_state: str,
    ) -> discord.Message | None:
        first: discord.Message | None = None
        summary = _table_summary(source)
        source_delivered = False
        for index, image in enumerate(images):
            page_marker = f"{marker}-p{index}"
            if record_state != "pending":
                existing = await self._find_marker(channel, page_marker)
                if existing is not None:
                    first = first or existing
                    source_delivered = source_delivered or any(
                        attachment.filename == source.filename
                        for attachment in existing.attachments
                    )
                    continue
            include_source = (
                not source_delivered
                and _attachment_size(image) + _attachment_size(source)
                <= DISCORD_ATTACHMENT_LIMIT_BYTES
            )
            files = [image, source] if include_source else [image]
            try:
                delivered = await _send_files(
                    channel,
                    content="",
                    attachments=files,
                    embed=table_embed(
                        summary=summary,
                        image_filename=image.filename,
                        page_number=index + 1,
                        page_count=len(images),
                        source_attached=include_source,
                    ),
                    view=table_copy_view() if include_source else None,
                    nonce=_delivery_nonce(page_marker),
                )
            except discord.HTTPException as exc:
                if exc.status not in {400, 403, 413}:
                    raise
                return await self._deliver_table_source(
                    channel,
                    source=source,
                    marker=f"{marker}-source-fallback",
                    record_state=record_state,
                    reason="Image delivery failed; Markdown source preserved.",
                )
            first = first or delivered
            source_delivered = source_delivered or include_source

        if not source_delivered:
            source_message = await self._deliver_table_source(
                channel,
                source=source,
                marker=f"{marker}-source",
                record_state=record_state,
                reason=(
                    "The table exceeded image rendering limits."
                    if not images
                    else "Markdown source is attached separately."
                ),
            )
            first = first or source_message
        return first

    async def _deliver_table_source(
        self,
        channel: discord.TextChannel | discord.Thread,
        *,
        source: RenderedAttachment | DurableRenderedAttachment,
        marker: str,
        record_state: str,
        reason: str,
    ) -> discord.Message:
        if record_state != "pending":
            existing = await self._find_marker(channel, marker)
            if existing is not None:
                return existing
        return await _send_files(
            channel,
            content="",
            attachments=[source],
            embed=table_source_embed(
                summary=_table_summary(source),
                reason=reason,
            ),
            view=table_copy_view(),
            nonce=_delivery_nonce(marker),
        )

    async def _deliver_attachment_fallback(
        self,
        channel: discord.TextChannel | discord.Thread,
        group: list[
            tuple[str, RenderedAttachment | DurableRenderedAttachment]
        ],
        *,
        turn_id: str,
        marker: str,
        record_state: str,
    ) -> discord.Message | None:
        first: discord.Message | None = None
        for suffix, attachment in group:
            individual_marker = f"{marker}-ai{suffix}"
            if record_state != "pending":
                existing = await self._find_marker(channel, individual_marker)
                if existing is not None:
                    first = first or existing
                    continue
            if attachment.kind is AttachmentKind.IMAGE:
                if len(group) > 1:
                    try:
                        delivered = await _send_files(
                            channel,
                            content=_bounded_message_content(
                                f"Image attachment: `{attachment.filename[:120]}`"
                            ),
                            attachments=[attachment],
                            nonce=_delivery_nonce(individual_marker),
                        )
                    except discord.HTTPException as exc:
                        if exc.status not in {400, 403, 413}:
                            raise
                    else:
                        first = first or delivered
                        continue
                delivered = await self._deliver_image_failure_notice(
                    channel,
                    attachment,
                    turn_id=turn_id,
                    marker=f"{marker}-image-failed-{suffix}",
                    record_state=record_state,
                )
                first = first or delivered
                continue
            if len(group) == 1:
                delivered = await self._deliver_attachment_as_text(
                    channel,
                    attachment,
                    marker=f"{marker}-at{suffix}",
                    record_state=record_state,
                )
                first = first or delivered
                continue
            try:
                delivered = await _send_files(
                    channel,
                    content=_bounded_message_content(
                        f"Attachment fallback: `{attachment.filename[:120]}`"
                    ),
                    attachments=[attachment],
                    nonce=_delivery_nonce(individual_marker),
                )
            except discord.HTTPException as exc:
                if exc.status not in {400, 403, 413}:
                    raise
                delivered = await self._deliver_attachment_as_text(
                    channel,
                    attachment,
                    marker=f"{marker}-at{suffix}",
                    record_state=record_state,
                )
            first = first or delivered
        return first

    async def _deliver_image_failure_notice(
        self,
        channel: discord.TextChannel | discord.Thread,
        attachment: RenderedAttachment | DurableRenderedAttachment,
        *,
        turn_id: str,
        marker: str,
        record_state: str,
    ) -> discord.Message:
        if record_state != "pending":
            existing = await self._find_marker(channel, marker)
            if existing is not None:
                return existing
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="error",
            code="outbound_image_delivery_failed",
            summary="A registered outbound image could not be uploaded to Discord",
            turn_id=turn_id,
            details={
                "filename": attachment.filename[:128],
                "sha256": (
                    attachment.sha256
                    if isinstance(attachment, DurableRenderedAttachment)
                    else hashlib.sha256(attachment.content).hexdigest()
                ),
                "size_bytes": (
                    attachment.size_bytes
                    if isinstance(attachment, DurableRenderedAttachment)
                    else len(attachment.content)
                ),
            },
        )
        return await channel.send(
            _bounded_message_content(
                "The generated image could not be attached to Discord. "
                "Ask Codex to regenerate it or inspect diagnostics."
            ),
            embed=notice_embed(
                "The generated image was registered locally, but Discord rejected "
                "the attachment upload.",
                level="error",
                title="Image delivery failed",
            ),
            allowed_mentions=discord.AllowedMentions.none(),
            nonce=_delivery_nonce(marker),
        )

    async def _deliver_attachment_as_text(
        self,
        channel: discord.TextChannel | discord.Thread,
        attachment: RenderedAttachment | DurableRenderedAttachment,
        *,
        marker: str,
        record_state: str,
    ) -> discord.Message:
        content = _attachment_bytes(attachment)
        try:
            text = content.decode("utf-8-sig")
            encoding = "UTF-8 text"
        except UnicodeDecodeError:
            text = base64.b64encode(content).decode("ascii")
            encoding = "base64; decode to recover the original bytes"
        chunks = split_discord_code(text, limit=1650)
        first: discord.Message | None = None
        for index, chunk in enumerate(chunks):
            part_marker = f"{marker}-{index}"
            if record_state != "pending":
                existing = await self._find_marker(channel, part_marker)
                if existing is not None:
                    first = first or existing
                    continue
            header = (
                f"Attachment `{attachment.filename[:100]}` fallback "
                f"({index + 1}/{len(chunks)}, {encoding}):\n"
            )
            message = await channel.send(
                _bounded_message_content(header + chunk),
                allowed_mentions=discord.AllowedMentions.none(),
                suppress_embeds=True,
                nonce=_delivery_nonce(part_marker),
            )
            first = first or message
        if first is None:
            raise DeliveryError("attachment_fallback_empty", permanent=True)
        return first

    async def _deliver_task_card(
        self,
        channel: discord.TextChannel | discord.Thread,
        payload: dict[str, object],
        operation: str,
        marker: str,
        record_state: str,
    ) -> DeliveryResult:
        view_id = str(payload["view_id"])
        revision_value = payload["revision"]
        if isinstance(revision_value, bool) or not isinstance(
            revision_value, (int, str, bytes, bytearray)
        ):
            raise DeliveryError("payload_invalid", permanent=True)
        revision = int(revision_value)
        expanded = bool(payload.get("expanded"))
        action = "collapse" if expanded else "expand"
        content = ""
        embed = task_card_embed(payload, expanded=expanded)
        nonce = payload.get("nonce")
        if not isinstance(nonce, str) or not nonce:
            raise DeliveryError("task_card_nonce_missing", permanent=True)
        view = discord.ui.View(timeout=None)
        view.add_item(
            discord.ui.Button(
                label="收起" if expanded else "展开",
                style=discord.ButtonStyle.secondary,
                custom_id=self._signer.task_card_id(
                    view_id=view_id,
                    revision=revision,
                    action=action,
                    nonce=nonce,
                ),
            )
        )
        if operation == "edit":
            message_id = await asyncio.to_thread(
                self._repository.task_card_message, view_id
            )
            if message_id:
                try:
                    message = await channel.fetch_message(int(message_id))
                except discord.NotFound:
                    pass
                else:
                    await message.edit(
                        content=content,
                        embed=embed,
                        view=view,
                        allowed_mentions=discord.AllowedMentions.none(),
                        suppress=False,
                    )
                    return DeliveryResult(str(message.id), view_id)
        if record_state != "pending":
            existing = await self._find_marker(channel, marker)
            if existing is not None:
                return DeliveryResult(str(existing.id), view_id)
        message = await channel.send(
            content,
            embed=embed,
            view=view,
            allowed_mentions=discord.AllowedMentions.none(),
            nonce=_delivery_nonce(marker),
        )
        return DeliveryResult(str(message.id), view_id)

    async def _deliver_schedule_draft_card(
        self,
        channel: discord.TextChannel | discord.Thread,
        payload: dict[str, object],
        outbox_id: str,
        operation: str,
        marker: str,
        record_state: str,
    ) -> DeliveryResult:
        if not isinstance(channel, discord.Thread):
            raise DeliveryError(
                "schedule_draft_destination_invalid",
                permanent=True,
            )
        draft_id = payload.get("draft_id")
        state = payload.get("state")
        if (
            not isinstance(draft_id, str)
            or not draft_id
            or len(draft_id) > 64
            or state not in {"pending", "confirmed", "cancelled", "expired"}
        ):
            raise DeliveryError("schedule_draft_payload_invalid", permanent=True)
        effective_payload = dict(payload)
        view: discord.ui.View | None = None
        if state == "pending":
            nonce = payload.get("nonce")
            expires_at = payload.get("expires_at")
            if (
                not isinstance(nonce, str)
                or not nonce
                or isinstance(expires_at, bool)
                or not isinstance(expires_at, int)
            ):
                raise DeliveryError(
                    "schedule_draft_payload_invalid",
                    permanent=True,
                )
            if expires_at <= utc_now_ms():
                effective_payload["state"] = "expired"
            else:
                view = discord.ui.View(timeout=None)
                view.add_item(
                    discord.ui.Button(
                        label="Confirm",
                        style=discord.ButtonStyle.danger,
                        custom_id=self._signer.schedule_draft_id(
                            draft_id=draft_id,
                            action="confirm",
                            nonce=nonce,
                        ),
                    )
                )
                view.add_item(
                    discord.ui.Button(
                        label="Cancel",
                        style=discord.ButtonStyle.secondary,
                        custom_id=self._signer.schedule_draft_id(
                            draft_id=draft_id,
                            action="cancel",
                            nonce=nonce,
                        ),
                    )
                )

        if operation == "edit":
            message_id = await asyncio.to_thread(
                self._repository.schedule_draft_message,
                draft_id,
            )
            if message_id is None:
                raise DeliveryError(
                    "schedule_draft_message_unbound",
                    permanent=False,
                )
            try:
                message = await channel.fetch_message(int(message_id))
            except ValueError as exc:
                raise DeliveryError(
                    "schedule_draft_message_invalid",
                    permanent=True,
                ) from exc
            bot_user = self._client.user
            if bot_user is None or message.author.id != bot_user.id:
                raise DeliveryError(
                    "schedule_draft_message_author_mismatch",
                    permanent=True,
                )
            await message.edit(
                content="",
                embed=schedule_draft_embed(effective_payload),
                view=None,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return DeliveryResult(
                discord_message_id=str(message.id),
                schedule_draft_id=draft_id,
            )

        if record_state != "pending":
            existing = await self._find_marker(channel, marker)
            if existing is not None:
                await asyncio.to_thread(
                    self._repository.bind_schedule_draft_message,
                    draft_id=draft_id,
                    outbox_id=outbox_id,
                    discord_message_id=str(existing.id),
                )
                return DeliveryResult(
                    discord_message_id=str(existing.id),
                    schedule_draft_id=draft_id,
                )
        source_url = _final_source_url(channel, payload)
        visible_content = (
            f"-# Original request: <{source_url}>" if source_url is not None else ""
        )
        reference = _final_message_reference(channel, payload)
        send_options: dict[str, Any] = {
            "embed": schedule_draft_embed(effective_payload),
            "mention_author": False,
            "allowed_mentions": discord.AllowedMentions.none(),
        }
        if view is not None:
            send_options["view"] = view
        if reference is not None:
            send_options["reference"] = reference
        message = await channel.send(
            _bounded_message_content(visible_content),
            nonce=_delivery_nonce(marker),
            **send_options,
        )
        await asyncio.to_thread(
            self._repository.bind_schedule_draft_message,
            draft_id=draft_id,
            outbox_id=outbox_id,
            discord_message_id=str(message.id),
        )
        return DeliveryResult(
            discord_message_id=str(message.id),
            schedule_draft_id=draft_id,
        )

    async def _deliver_turn_progress(
        self,
        channel: discord.TextChannel | discord.Thread,
        payload: dict[str, object],
        operation: str,
        marker: str,
        record_state: str,
    ) -> DeliveryResult:
        turn_id = str(payload.get("turn_id", ""))
        content = payload.get("content")
        if not turn_id or not isinstance(content, str) or not content:
            raise DeliveryError("turn_progress_payload_invalid", permanent=True)
        plain_text = self._volatile_turns.preview(turn_id)
        embed = progress_embed(content)
        if operation == "edit":
            message_id = await asyncio.to_thread(
                self._repository.turn_progress_message,
                turn_id,
            )
            if message_id:
                try:
                    message = await channel.fetch_message(int(message_id))
                except discord.NotFound:
                    pass
                else:
                    visible_text = plain_text or _without_hidden_marker(
                        message.content if isinstance(message.content, str) else ""
                    )
                    await message.edit(
                        content=_bounded_message_content(visible_text),
                        embed=embed,
                        allowed_mentions=discord.AllowedMentions.none(),
                        suppress=False,
                    )
                    return DeliveryResult(
                        str(message.id),
                        turn_progress_id=turn_id,
                    )
        rendered = _bounded_message_content(plain_text)
        if record_state != "pending":
            existing = await self._find_marker(channel, marker)
            if existing is not None:
                return DeliveryResult(
                    str(existing.id),
                    turn_progress_id=turn_id,
                )
        message = await channel.send(
            rendered,
            embed=embed,
            allowed_mentions=discord.AllowedMentions.none(),
            nonce=_delivery_nonce(marker),
        )
        return DeliveryResult(
            str(message.id),
            turn_progress_id=turn_id,
        )

    async def _find_marker(
        self,
        channel: discord.TextChannel | discord.Thread,
        marker: str,
    ) -> discord.Message | None:
        bot_user = self._client.user
        if bot_user is None:
            raise DeliveryError(
                "discord_client_user_unavailable",
                permanent=False,
                incident_code="discord_reconciliation_uncertain",
            )
        count = 0
        try:
            async for message in channel.history(limit=500):
                count += 1
                if (
                    message.author.id == bot_user.id
                    and _message_has_delivery_marker(
                        message.content,
                        marker,
                        nonce=getattr(message, "nonce", None),
                    )
                ):
                    return message
        except discord.Forbidden as exc:
            raise DeliveryError(
                "discord_history_forbidden",
                permanent=False,
                incident_code="discord_reconciliation_uncertain",
            ) from exc
        except discord.NotFound as exc:
            raise DeliveryError(
                "discord_history_not_found",
                permanent=False,
                incident_code="discord_reconciliation_uncertain",
            ) from exc
        except discord.HTTPException as exc:
            error = _discord_http_error(
                exc,
                code=f"discord_history_{exc.status}",
                permanent=False,
            )
            error.incident_code = "discord_reconciliation_uncertain"
            raise error from exc
        if count >= 500:
            try:
                await asyncio.to_thread(
                    self._repository.record_incident,
                    severity="warning",
                    code="delivery_duplicate_possible",
                    summary=(
                        "Discord reconciliation history was exhausted; "
                        "at-least-once delivery may create a duplicate"
                    ),
                    details={
                        "destination_channel_id": str(channel.id),
                        "marker_hash": sha256_text(marker)[:16],
                    },
                )
            except Exception:
                logger.exception(
                    "Failed to persist possible duplicate-delivery incident"
                )
        return None


def _partition_table_attachments(
    attachments: list[RenderedAttachment | DurableRenderedAttachment],
) -> tuple[
    list[
        tuple[
            RenderedAttachment | DurableRenderedAttachment,
            list[RenderedAttachment | DurableRenderedAttachment],
        ]
    ],
    list[RenderedAttachment | DurableRenderedAttachment],
]:
    sources: dict[str, RenderedAttachment | DurableRenderedAttachment] = {}
    images: dict[str, list[RenderedAttachment | DurableRenderedAttachment]] = {}
    generic: list[RenderedAttachment | DurableRenderedAttachment] = []
    order: list[str] = []
    for attachment in attachments:
        group_id = attachment.group_id
        if attachment.kind is AttachmentKind.TABLE_SOURCE and group_id:
            if group_id not in sources:
                order.append(group_id)
                sources[group_id] = attachment
            else:
                generic.append(attachment)
        elif attachment.kind is AttachmentKind.TABLE_IMAGE and group_id:
            images.setdefault(group_id, []).append(attachment)
        else:
            generic.append(attachment)
    groups: list[
        tuple[
            RenderedAttachment | DurableRenderedAttachment,
            list[RenderedAttachment | DurableRenderedAttachment],
        ]
    ] = []
    for group_id in order:
        groups.append((sources[group_id], images.pop(group_id, [])))
    for unpaired in images.values():
        generic.extend(unpaired)
    return groups, generic


def _table_summary(source: RenderedAttachment | DurableRenderedAttachment) -> str:
    prefix = "Markdown source for "
    if source.description.startswith(prefix):
        return source.description.removeprefix(prefix)
    if source.description == "Markdown source for table rendering fallback":
        return "Table rendering fallback"
    return "Rendered table"


def _registered_image_attachments(
    records: Sequence[OutboundImageInvocationRecord],
    *,
    artifact_root: Path,
) -> tuple[DurableRenderedAttachment, ...]:
    root = artifact_root.resolve()
    attachments: list[DurableRenderedAttachment] = []
    used_names: set[str] = set()
    for record in records:
        if (
            not record.success
            or record.relative_path is None
            or record.normalized_sha256 is None
            or record.size_bytes is None
            or record.display_name is None
            or record.description is None
        ):
            raise DeliveryError("outbound_image_record_invalid", permanent=True)
        relative = Path(record.relative_path)
        path = (root / relative).resolve()
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or not path.is_relative_to(root)
            or path.is_symlink()
            or not path.is_file()
            or path.stat().st_size != record.size_bytes
            or _sha256_path(path) != record.normalized_sha256
        ):
            raise DeliveryError("outbound_image_changed", permanent=True)
        filename = _unique_attachment_name(record.display_name, used_names)
        used_names.add(filename.casefold())
        attachments.append(
            DurableRenderedAttachment(
                filename=filename,
                path=path,
                description=record.description,
                sha256=record.normalized_sha256,
                size_bytes=record.size_bytes,
                kind=AttachmentKind.IMAGE,
            )
        )
    return tuple(attachments)


def _unique_attachment_name(value: str, used: set[str]) -> str:
    if value.casefold() not in used:
        return value
    path = Path(value)
    for index in range(2, 1001):
        candidate = f"{path.stem[:110]}-{index}{path.suffix or '.png'}"
        if candidate.casefold() not in used:
            return candidate
    raise DeliveryError("outbound_image_name_conflict", permanent=True)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _attachment_parts(
    attachments: list[RenderedAttachment | DurableRenderedAttachment],
) -> Iterator[
    tuple[str, RenderedAttachment | DurableRenderedAttachment]
]:
    for attachment_index, attachment in enumerate(attachments):
        size_bytes = _attachment_size(attachment)
        if size_bytes <= DISCORD_ATTACHMENT_LIMIT_BYTES:
            yield str(attachment_index), attachment
            continue
        part_count = (
            size_bytes + DISCORD_ATTACHMENT_LIMIT_BYTES - 1
        ) // DISCORD_ATTACHMENT_LIMIT_BYTES
        if isinstance(attachment, RenderedAttachment):
            for part_index in range(part_count):
                start = part_index * DISCORD_ATTACHMENT_LIMIT_BYTES
                content = attachment.content[
                    start : start + DISCORD_ATTACHMENT_LIMIT_BYTES
                ]
                yield (
                    f"{attachment_index}p{part_index}",
                    RenderedAttachment(
                        filename=(
                            f"{attachment.filename[:80]}.part"
                            f"{part_index + 1:03d}-of-{part_count:03d}"
                        ),
                        content=content,
                        description=(
                            f"{attachment.description} "
                            f"(part {part_index + 1}/{part_count})"
                        ),
                        kind=attachment.kind,
                        group_id=attachment.group_id,
                    ),
                )
            continue
        with attachment.path.open("rb") as stream:
            for part_index in range(part_count):
                content = stream.read(DISCORD_ATTACHMENT_LIMIT_BYTES)
                if not content:
                    raise DeliveryError(
                        "render_plan_attachment_truncated",
                        permanent=True,
                    )
                yield (
                    f"{attachment_index}p{part_index}",
                    RenderedAttachment(
                        filename=(
                            f"{attachment.filename[:80]}.part"
                            f"{part_index + 1:03d}-of-{part_count:03d}"
                        ),
                        content=content,
                        description=(
                            f"{attachment.description} "
                            f"(part {part_index + 1}/{part_count})"
                        ),
                        kind=attachment.kind,
                        group_id=attachment.group_id,
                    ),
                )
            if stream.read(1):
                raise DeliveryError(
                    "render_plan_attachment_grew",
                    permanent=True,
                )


def _attachment_groups(
    parts: Iterator[
        tuple[str, RenderedAttachment | DurableRenderedAttachment]
    ],
) -> Iterator[
    list[tuple[str, RenderedAttachment | DurableRenderedAttachment]]
]:
    group: list[
        tuple[str, RenderedAttachment | DurableRenderedAttachment]
    ] = []
    group_bytes = 0
    for part in parts:
        size = (
            part[1].size_bytes
            if isinstance(part[1], DurableRenderedAttachment)
            else len(part[1].content)
        )
        if group and (
            len(group) == 10
            or group_bytes + size > DISCORD_ATTACHMENT_LIMIT_BYTES
        ):
            yield group
            group = []
            group_bytes = 0
        group.append(part)
        group_bytes += size
    if group:
        yield group


async def _send_files(
    channel: discord.TextChannel | discord.Thread,
    *,
    content: str,
    attachments: Sequence[
        RenderedAttachment | DurableRenderedAttachment
    ],
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
    nonce: int,
) -> discord.Message:
    files = [_discord_file(item) for item in attachments]
    try:
        if embed is not None and view is not None:
            return await channel.send(
                content,
                files=files,
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions.none(),
                nonce=nonce,
            )
        if embed is not None:
            return await channel.send(
                content,
                files=files,
                embed=embed,
                allowed_mentions=discord.AllowedMentions.none(),
                nonce=nonce,
            )
        if view is not None:
            return await channel.send(
                content,
                files=files,
                view=view,
                allowed_mentions=discord.AllowedMentions.none(),
                suppress_embeds=True,
                nonce=nonce,
            )
        return await channel.send(
            content,
            files=files,
            allowed_mentions=discord.AllowedMentions.none(),
            suppress_embeds=True,
            nonce=nonce,
        )
    finally:
        for file in files:
            file.close()


def _attachment_bytes(
    attachment: RenderedAttachment | DurableRenderedAttachment,
) -> bytes:
    if isinstance(attachment, DurableRenderedAttachment):
        return attachment.path.read_bytes()
    return attachment.content


def _attachment_size(
    attachment: RenderedAttachment | DurableRenderedAttachment,
) -> int:
    return (
        attachment.size_bytes
        if isinstance(attachment, DurableRenderedAttachment)
        else len(attachment.content)
    )


def _discord_file(
    attachment: RenderedAttachment | DurableRenderedAttachment,
) -> discord.File:
    source: io.BytesIO | str
    if isinstance(attachment, DurableRenderedAttachment):
        source = str(attachment.path)
    else:
        source = io.BytesIO(attachment.content)
    return discord.File(
        source,
        filename=attachment.filename,
        description=attachment.description[:1024],
    )


def _bounded_message_content(content: str) -> str:
    if len(content) > 2000:
        try:
            content = split_discord_text(content, limit=2000)[0]
        except ValueError:
            content = content[:2000]
    return content


def _delivery_nonce(marker: str) -> int:
    return int(sha256_text(f"discord-delivery-nonce:{marker}")[:16], 16)


def _hidden_delivery_marker(marker: str) -> str:
    digest = sha256_text(f"discord-delivery-marker:{marker}")[:24]
    encoded = "".join(chr(0xFE00 + int(character, 16)) for character in digest)
    return "\u200b" + encoded


def _message_has_delivery_marker(
    content: str,
    marker: str,
    *,
    nonce: object = None,
) -> bool:
    return str(nonce) == str(_delivery_nonce(marker)) or content.endswith(
        _hidden_delivery_marker(marker)
    ) or content.endswith(
        f"\n-# codexD:{marker}"
    )


def _without_hidden_marker(content: str) -> str:
    if len(content) < 25:
        return content
    marker_start = len(content) - 25
    if content[marker_start] != "\u200b":
        return content
    suffix = content[marker_start + 1 :]
    if len(suffix) == 24 and all("\ufe00" <= value <= "\ufe0f" for value in suffix):
        return content[:marker_start]
    return content


def _plain_text_fallback_chunks(source: str) -> tuple[str, ...]:
    text = discord.utils.escape_mentions(source)
    if not text.strip():
        return ()
    try:
        return split_discord_text(text, limit=1700)
    except ValueError:
        return split_discord_code(text, limit=1700)


def _attachment_failure_guidance(value: object) -> str | None:
    code = value if isinstance(value, str) else ""
    if code == "file_input_unsupported":
        return (
            "This runtime cannot safely expose the uploaded file to Codex. "
            "Workaround: place the file in the bound project workspace and "
            "reference its relative path in your prompt."
        )
    if code == "archive_unsupported":
        return "This archive format is unsupported. Upload a ZIP archive instead."
    archive_messages = {
        "archive_encrypted": "Encrypted ZIP archives are not supported.",
        "archive_entry_limit": "The ZIP contains too many entries.",
        "archive_uncompressed_size_limit": (
            "The ZIP exceeds the safe uncompressed-size limit."
        ),
        "archive_compression_ratio_limit": (
            "The ZIP exceeds the safe compression-ratio limit."
        ),
        "archive_path_unsafe": "The ZIP contains an unsafe or colliding path.",
        "archive_integrity_failed": "The ZIP failed integrity validation.",
        "attachment_materialization_failed": (
            "The attachment could not be prepared safely for Codex."
        ),
        "attachment_integrity_failed": (
            "The attachment changed or failed integrity validation before Codex started."
        ),
    }
    return archive_messages.get(code)


def _provider_failure_guidance(payload: Mapping[str, object]) -> str | None:
    code = payload.get("provider_error_code")
    if not isinstance(code, str) or not code.startswith("provider_"):
        return None
    messages = {
        "provider_stream_disconnected": "Provider stream disconnected before completion.",
        "provider_stream_connection_failed": "Provider stream connection failed.",
        "provider_retry_exhausted": "Codex exhausted its provider reconnect attempts.",
        "provider_connection_failed": "Provider connection failed.",
        "provider_overloaded": "The provider is overloaded; retry later.",
        "provider_context_window_exceeded": (
            "The session context limit was exceeded; run `/session compact` or `/session new`."
        ),
        "provider_session_budget_exceeded": "The provider session budget was exceeded.",
        "provider_usage_limit_exceeded": "The provider usage limit was exceeded.",
        "provider_unauthorized": "Provider authentication was rejected.",
        "provider_bad_request": "The provider rejected this request.",
        "provider_policy_blocked": "The provider policy blocked this request.",
    }
    lines = [messages.get(code, "The provider failed before completion.")]
    retry_count = payload.get("provider_retry_count")
    retry_limit = payload.get("provider_retry_limit")
    if isinstance(retry_count, int) and retry_count > 0:
        retry = f"{retry_count}/{retry_limit}" if isinstance(retry_limit, int) else str(retry_count)
        lines.append(f"Codex reconnect attempts: `{retry}`.")
    safe_message = payload.get("provider_safe_message")
    if isinstance(safe_message, str) and safe_message.strip():
        lines.append(safe_message[:300])
    if payload.get("provider_degraded") is True:
        lines.append(
            "This revision has repeated the same failure. Your history is preserved; "
            "run `/session new` instead of retrying this revision."
        )
    else:
        lines.append(
            "Your Thread history is preserved; this Turn was not replayed. "
            "Retry once, then use `/session new` if the same failure repeats."
        )
    lines.append(f"Code: `{code}`")
    return "\n".join(lines)


def _provider_thread_recovery_guidance(value: object) -> str | None:
    code = value if isinstance(value, str) else ""
    if code == "provider_thread_systemError":
        return (
            "The provider Thread remained unhealthy after a fresh runtime resumed it. "
            "This Turn was not sent or replayed; run `/session new`."
        )
    if code == "provider_thread_unknown":
        return (
            "Safety recovery required because the provider returned an unsupported "
            "Thread state. Run `/session new` or inspect `/session status`."
        )
    return None


def _bounded_plain_text_fallback(source: str) -> tuple[str, ...]:
    chunks = _plain_text_fallback_chunks(source)
    if not chunks:
        return ("Codex completed, but rich rendering was unavailable.",)
    prefix = "Rich rendering was unavailable; showing plain text.\n\n"
    if len(chunks) <= 4:
        return (prefix + chunks[0], *chunks[1:])
    suffix = "\n\n[Complete output could not be attached; fallback truncated.]"
    return (
        prefix + chunks[0],
        *chunks[1:3],
        chunks[3] + suffix,
    )


def _volatile_final_unavailable(state: str, *, was_volatile: bool) -> str:
    if was_volatile:
        return (
            "The final response is no longer available because conversation "
            "content is kept only in process memory and is not retained in SQLite."
        )
    return {
        "completed": "Codex completed without a final response.",
        "failed": "Codex failed before producing a final response.",
        "cancelled": "Codex was cancelled before producing a final response.",
        "interrupted": "Codex was interrupted before producing a final response.",
    }.get(state, f"Codex ended in state `{state}` without a final response.")


def _discord_http_error(
    exc: discord.HTTPException,
    *,
    code: str,
    permanent: bool,
) -> DeliveryError:
    retry_after = getattr(exc, "retry_after", None)
    return DeliveryError(
        code,
        permanent=permanent,
        retry_after=float(retry_after) if retry_after is not None else None,
    )


def _safe_card_value(value: object, limit: int) -> str:
    if value is None:
        return ""
    return discord.utils.escape_markdown(
        discord.utils.escape_mentions(str(value)[:limit])
    )


def _snowflake(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise DeliveryError(f"thread_create_{field}_invalid", permanent=True)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise DeliveryError(
            f"thread_create_{field}_invalid", permanent=True
        ) from exc
    if parsed <= 0:
        raise DeliveryError(f"thread_create_{field}_invalid", permanent=True)
    return parsed


def _final_message_reference(
    channel: discord.TextChannel | discord.Thread,
    payload: Mapping[str, object],
) -> discord.MessageReference | None:
    message_id = _optional_snowflake(payload.get("input_message_id"))
    input_channel_id = _optional_snowflake(payload.get("input_channel_id"))
    guild_id = _optional_snowflake(payload.get("discord_guild_id"))
    if (
        message_id is None
        or input_channel_id is None
        or input_channel_id != channel.id
    ):
        return None
    return discord.MessageReference(
        message_id=message_id,
        channel_id=input_channel_id,
        guild_id=guild_id,
        fail_if_not_exists=False,
    )


def _final_source_url(
    channel: discord.TextChannel | discord.Thread,
    payload: Mapping[str, object],
) -> str | None:
    message_id = _optional_snowflake(payload.get("input_message_id"))
    input_channel_id = _optional_snowflake(payload.get("input_channel_id"))
    guild_id = _optional_snowflake(payload.get("discord_guild_id"))
    if (
        message_id is None
        or input_channel_id is None
        or guild_id is None
        or input_channel_id == channel.id
    ):
        return None
    return (
        f"https://discord.com/channels/{guild_id}/{input_channel_id}/{message_id}"
    )


def _optional_snowflake(value: object) -> int | None:
    try:
        return _snowflake(value, "snowflake")
    except DeliveryError:
        return None


def _retry_delay(attempts: int) -> float:
    return min(300.0, float(2 ** min(max(attempts, 1), 8)))
