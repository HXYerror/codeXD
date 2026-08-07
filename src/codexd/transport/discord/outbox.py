from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol

import discord

from codexd.domain.ids import sha256_text, utc_now_ms
from codexd.errors import CodexDError, InvariantError, NotFoundError
from codexd.rendering.discord import (
    DISCORD_ATTACHMENT_LIMIT_BYTES,
    AttachmentKind,
    DiscordRenderPlanner,
    DurableDiscordRenderPlan,
    DurableRenderedAttachment,
    RenderedAttachment,
    split_discord_code,
    split_discord_text,
)
from codexd.security.signing import ComponentSigner
from codexd.storage.records import OutboxRecord
from codexd.storage.repository import Repository
from codexd.transport.discord.presentation import (
    attachment_embed,
    notice_embed,
    progress_embed,
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
        )
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
                raise RuntimeError(
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


class DiscordOutboxTransport:
    def __init__(
        self,
        *,
        client: discord.Client,
        repository: Repository,
        renderer: DiscordRenderPlanner,
        signer: ComponentSigner,
    ) -> None:
        self._client = client
        self._repository = repository
        self._renderer = renderer
        self._signer = signer

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
            content = _with_marker("", record.delivery_marker)
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
        channel = await self._destination(target.destination_key)
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
        name = payload.get("name")
        if not isinstance(name, str) or not name or len(name) > 100:
            raise DeliveryError("thread_create_name_invalid", permanent=True)
        thread = await self._existing_thread(expected_id)
        if thread is None:
            message = await channel.fetch_message(starter_id)
            thread = message.thread
            if thread is None:
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

    async def _destination(
        self, destination_key: str
    ) -> discord.TextChannel | discord.Thread:
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
                raise _discord_http_error(
                    exc,
                    code="discord_destination_lookup_failed",
                    permanent=exc.status in {403, 404},
                ) from exc
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            raise DeliveryError("invalid_destination_type", permanent=True)
        if isinstance(channel, discord.Thread) and channel.archived and not channel.locked:
            await channel.edit(archived=False)
        return channel

    async def _deliver_final(
        self,
        channel: discord.TextChannel | discord.Thread,
        payload: dict[str, object],
        marker: str,
        record_state: str,
    ) -> DeliveryResult:
        turn_id = payload.get("turn_id")
        plain_text = payload.get("plain_text")
        if not isinstance(turn_id, str) or not isinstance(plain_text, str):
            raise DeliveryError("turn_final_payload_invalid", permanent=True)
        source_sha256 = sha256_text(plain_text)
        stored = await asyncio.to_thread(self._repository.render_plan, turn_id)
        rendered: DurableDiscordRenderPlan
        if stored is None:
            try:
                generated = await self._renderer.create_durable_plan(
                    turn_id=turn_id,
                    source=plain_text,
                )
                plan_payload = generated.to_payload(self._renderer.artifact_root)
            except (CodexDError, OSError, ValueError):
                rendered = await self._render_fallback_plan(
                    turn_id=turn_id,
                    plain_text=plain_text,
                    stable_code="render_plan_creation_failed",
                )
            else:
                stored = await asyncio.to_thread(
                    self._repository.persist_render_plan,
                    turn_id=turn_id,
                    source_sha256=source_sha256,
                    plan=plan_payload,
                    retention_until=utc_now_ms()
                    + self._renderer.retention_days * 24 * 60 * 60 * 1000,
                    incident_codes=generated.incident_codes,
                )
        if stored is not None:
            if stored.source_sha256 != source_sha256:
                raise DeliveryError("turn_final_source_changed", permanent=True)
            try:
                rendered = self._renderer.load_durable_plan(stored.plan_json)
            except (CodexDError, OSError, ValueError):
                rendered = await self._render_fallback_plan(
                    turn_id=turn_id,
                    plain_text=plain_text,
                    stable_code="render_plan_invalid",
                )
        messages = list(rendered.messages)
        attachments = list(rendered.attachments)
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
                    _with_marker_strict(content, part_marker),
                    allowed_mentions=discord.AllowedMentions.none(),
                    suppress_embeds=True,
                    reference=reference,
                    mention_author=False,
                )
            else:
                message = await channel.send(
                    _with_marker_strict(content, part_marker),
                    allowed_mentions=discord.AllowedMentions.none(),
                    suppress_embeds=True,
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
                    content=_with_marker_strict(
                        "",
                        part_marker,
                    ),
                    attachments=[item for _suffix, item in group],
                    embed=attachment_embed(
                        [item.filename for _suffix, item in group]
                    ),
                )
            except discord.HTTPException as exc:
                if exc.status not in {400, 403, 413}:
                    raise
                fallback = await self._deliver_attachment_fallback(
                    channel,
                    group,
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
            _with_marker(terminal_footer(payload), footer_marker),
            allowed_mentions=discord.AllowedMentions.none(),
            suppress_embeds=True,
        )
        first = first or footer
        return DeliveryResult(str(first.id) if first else None)

    async def _render_fallback_plan(
        self,
        *,
        turn_id: str,
        plain_text: str,
        stable_code: str,
    ) -> DurableDiscordRenderPlan:
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="error",
            code="discord_render_fallback",
            summary="Discord rich rendering failed; bounded plain text was used",
            turn_id=turn_id,
            details={"stable_code": stable_code},
        )
        if len(_plain_text_fallback_chunks(plain_text)) > 4:
            try:
                return await self._renderer.create_plain_text_fallback_plan(
                    turn_id=turn_id,
                    source=plain_text,
                )
            except (CodexDError, OSError, ValueError):
                logger.exception(
                    "Could not persist complete rich-rendering fallback attachment"
                )
        return DurableDiscordRenderPlan(
            messages=_bounded_plain_text_fallback(plain_text),
            attachments=(),
        )

    async def _deliver_table_attachments(
        self,
        channel: discord.TextChannel | discord.Thread,
        *,
        source: DurableRenderedAttachment,
        images: list[DurableRenderedAttachment],
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
                and image.size_bytes + source.size_bytes
                <= DISCORD_ATTACHMENT_LIMIT_BYTES
            )
            files = [image, source] if include_source else [image]
            try:
                delivered = await _send_files(
                    channel,
                    content=_with_marker("", page_marker),
                    attachments=files,
                    embed=table_embed(
                        summary=summary,
                        image_filename=image.filename,
                        page_number=index + 1,
                        page_count=len(images),
                        source_attached=include_source,
                    ),
                    view=table_copy_view() if include_source else None,
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
        source: DurableRenderedAttachment,
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
            content=_with_marker("", marker),
            attachments=[source],
            embed=table_source_embed(
                summary=_table_summary(source),
                reason=reason,
            ),
            view=table_copy_view(),
        )

    async def _deliver_attachment_fallback(
        self,
        channel: discord.TextChannel | discord.Thread,
        group: list[
            tuple[str, RenderedAttachment | DurableRenderedAttachment]
        ],
        *,
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
                    content=_with_marker_strict(
                        f"Attachment fallback: `{attachment.filename[:120]}`",
                        individual_marker,
                    ),
                    attachments=[attachment],
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
                _with_marker_strict(header + chunk, part_marker),
                allowed_mentions=discord.AllowedMentions.none(),
                suppress_embeds=True,
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
        content = _with_marker("", marker)
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
        )
        return DeliveryResult(str(message.id), view_id)

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
        plain_text = payload.get("plain_text")
        if plain_text is not None and not isinstance(plain_text, str):
            raise DeliveryError("turn_progress_payload_invalid", permanent=True)
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
                    visible_text = (
                        plain_text
                        if isinstance(plain_text, str)
                        else _without_hidden_marker(
                            message.content
                            if isinstance(message.content, str)
                            else ""
                        )
                    )
                    await message.edit(
                        content=_with_marker(visible_text, marker),
                        embed=embed,
                        allowed_mentions=discord.AllowedMentions.none(),
                        suppress=False,
                    )
                    return DeliveryResult(
                        str(message.id),
                        turn_progress_id=turn_id,
                    )
        rendered = _with_marker(plain_text or "", marker)
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
                    and _message_has_delivery_marker(message.content, marker)
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
    attachments: list[DurableRenderedAttachment],
) -> tuple[
    list[tuple[DurableRenderedAttachment, list[DurableRenderedAttachment]]],
    list[DurableRenderedAttachment],
]:
    sources: dict[str, DurableRenderedAttachment] = {}
    images: dict[str, list[DurableRenderedAttachment]] = {}
    generic: list[DurableRenderedAttachment] = []
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
        tuple[DurableRenderedAttachment, list[DurableRenderedAttachment]]
    ] = []
    for group_id in order:
        groups.append((sources[group_id], images.pop(group_id, [])))
    for unpaired in images.values():
        generic.extend(unpaired)
    return groups, generic


def _table_summary(source: DurableRenderedAttachment) -> str:
    prefix = "Markdown source for "
    if source.description.startswith(prefix):
        return source.description.removeprefix(prefix)
    if source.description == "Markdown source for table rendering fallback":
        return "Table rendering fallback"
    return "Rendered table"


def _attachment_parts(
    attachments: list[DurableRenderedAttachment],
) -> Iterator[
    tuple[str, RenderedAttachment | DurableRenderedAttachment]
]:
    for attachment_index, attachment in enumerate(attachments):
        if attachment.size_bytes <= DISCORD_ATTACHMENT_LIMIT_BYTES:
            yield str(attachment_index), attachment
            continue
        part_count = (
            attachment.size_bytes + DISCORD_ATTACHMENT_LIMIT_BYTES - 1
        ) // DISCORD_ATTACHMENT_LIMIT_BYTES
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
            )
        if embed is not None:
            return await channel.send(
                content,
                files=files,
                embed=embed,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        if view is not None:
            return await channel.send(
                content,
                files=files,
                view=view,
                allowed_mentions=discord.AllowedMentions.none(),
                suppress_embeds=True,
            )
        return await channel.send(
            content,
            files=files,
            allowed_mentions=discord.AllowedMentions.none(),
            suppress_embeds=True,
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


def _with_marker(content: str, marker: str) -> str:
    marker_text = _hidden_delivery_marker(marker)
    maximum = 2000 - len(marker_text)
    if len(content) > maximum:
        try:
            content = split_discord_text(content, limit=maximum)[0]
        except ValueError:
            content = content[:maximum]
    return content + marker_text


def _with_marker_strict(content: str, marker: str) -> str:
    marker_text = _hidden_delivery_marker(marker)
    if len(content) + len(marker_text) > 2000:
        raise DeliveryError("render_plan_message_too_long", permanent=True)
    return content + marker_text


def _hidden_delivery_marker(marker: str) -> str:
    digest = sha256_text(f"discord-delivery-marker:{marker}")[:24]
    encoded = "".join(chr(0xFE00 + int(character, 16)) for character in digest)
    return "\u200b" + encoded


def _message_has_delivery_marker(content: str, marker: str) -> bool:
    return content.endswith(_hidden_delivery_marker(marker)) or content.endswith(
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
