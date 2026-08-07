from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from codexd.application.conversation_locks import ConversationLocks
from codexd.domain.ids import sha256_text, utc_now_ms
from codexd.domain.schedules import (
    CronExpression,
    MisfirePolicy,
    ScheduleAuditContext,
    ScheduleKind,
    ScheduleModalSubmission,
    ScheduleSpec,
    ScheduleState,
)
from codexd.errors import ConfigurationError, ConflictError, InvariantError, NotFoundError
from codexd.storage.records import ScheduleDraftRecord, ScheduleRecord
from codexd.storage.schedules import ScheduleRepository

WakeConversation = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class Occurrence:
    utc_ms: int
    local_display: str

    @property
    def key(self) -> str:
        return str(self.utc_ms)


class ScheduleCoordinator:
    _MAX_DUE_OCCURRENCES_PER_TICK = 100

    def __init__(
        self,
        *,
        repository: ScheduleRepository,
        wake_conversation: WakeConversation,
        poll_seconds: float = 1.0,
        conversation_locks: ConversationLocks | None = None,
        critical_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._repository = repository
        self._wake_conversation = wake_conversation
        self._poll_seconds = poll_seconds
        self._conversation_locks = conversation_locks or ConversationLocks()
        self._critical_failure = critical_failure or (lambda _exc: None)
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._restored = False

    def start(self) -> None:
        if not self._restored:
            raise InvariantError("Schedule coordinator must be restored before start")
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="codexd-schedule")

    async def restore(self) -> tuple[int, int]:
        if self._restored:
            return 0, 0
        blocked = await self.reconcile_startup()
        materialized = await self.tick(wake=False)
        self._restored = True
        return blocked, materialized

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def create(
        self,
        *,
        conversation_id: str,
        name: str,
        kind: str,
        expression: str,
        timezone: str,
        misfire_policy: str,
        prompt_text: str,
        owner_user_id: int,
        now_ms: int | None = None,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        spec = parse_schedule_spec(kind, expression, timezone, misfire_policy)
        now = utc_now_ms() if now_ms is None else now_ms
        next_due = next_occurrence(spec, now - 1)
        if next_due is None:
            raise ConfigurationError("schedule has no future occurrence")
        normalized_name = _schedule_name(name)
        prompt = _schedule_prompt(prompt_text)
        async with self._conversation_locks.hold(conversation_id):
            return await asyncio.to_thread(
                self._repository.create,
                conversation_id=conversation_id,
                name=normalized_name,
                kind=spec.kind,
                expression=spec.expression,
                timezone=spec.timezone,
                misfire_policy=spec.misfire_policy,
                prompt_text=prompt,
                next_due_at=next_due.utc_ms,
                created_by_user_id=owner_user_id,
                audit=audit,
            )

    async def create_draft(
        self,
        *,
        conversation_id: str,
        name: str,
        kind: str,
        expression: str,
        timezone: str,
        misfire_policy: str,
        prompt_text: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        component_nonce: str,
        now_ms: int | None = None,
        modal_submission: ScheduleModalSubmission | None = None,
    ) -> ScheduleDraftRecord:
        spec = parse_schedule_spec(kind, expression, timezone, misfire_policy)
        now = utc_now_ms() if now_ms is None else now_ms
        occurrences = preview_occurrences(spec, now - 1, limit=3)
        if not occurrences:
            raise ConfigurationError("schedule has no future occurrence")
        normalized_name = _schedule_name(name)
        prompt = _schedule_prompt(prompt_text)
        return await asyncio.to_thread(
            self._repository.create_draft,
            conversation_id=conversation_id,
            owner_user_id=owner_user_id,
            guild_id=guild_id,
            channel_id=channel_id,
            action="create",
            payload={
                "name": normalized_name,
                "kind": spec.kind.value,
                "expression": spec.expression,
                "timezone": spec.timezone,
                "misfire_policy": spec.misfire_policy.value,
                "prompt_text": prompt,
                "prompt_hash": sha256_text(prompt),
                "next_due_at": occurrences[0].utc_ms,
            },
            occurrences=tuple(
                {
                    "utc_ms": occurrence.utc_ms,
                    "local_display": occurrence.local_display,
                }
                for occurrence in occurrences
            ),
            component_nonce=component_nonce,
            expires_at=now + 10 * 60 * 1000,
            modal_submission=modal_submission,
        )

    async def update_draft(
        self,
        *,
        schedule_id: str,
        expected_version: int,
        name: str,
        kind: str,
        expression: str,
        timezone: str,
        misfire_policy: str,
        prompt_text: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        component_nonce: str,
        now_ms: int | None = None,
        modal_submission: ScheduleModalSubmission | None = None,
    ) -> ScheduleDraftRecord:
        schedule = await asyncio.to_thread(self._repository.get, schedule_id)
        spec = parse_schedule_spec(kind, expression, timezone, misfire_policy)
        now = utc_now_ms() if now_ms is None else now_ms
        occurrences = preview_occurrences(spec, now - 1, limit=3)
        if not occurrences:
            raise ConfigurationError("schedule has no future occurrence")
        prompt = _schedule_prompt(prompt_text)
        return await asyncio.to_thread(
            self._repository.create_draft,
            conversation_id=schedule.conversation_id,
            owner_user_id=owner_user_id,
            guild_id=guild_id,
            channel_id=channel_id,
            action="update",
            schedule_id=schedule.id,
            expected_version=expected_version,
            payload={
                "name": _schedule_name(name),
                "kind": spec.kind.value,
                "expression": spec.expression,
                "timezone": spec.timezone,
                "misfire_policy": spec.misfire_policy.value,
                "prompt_text": prompt,
                "prompt_hash": sha256_text(prompt),
                "next_due_at": occurrences[0].utc_ms,
            },
            occurrences=tuple(
                {
                    "utc_ms": occurrence.utc_ms,
                    "local_display": occurrence.local_display,
                }
                for occurrence in occurrences
            ),
            component_nonce=component_nonce,
            expires_at=now + 10 * 60 * 1000,
            modal_submission=modal_submission,
        )

    async def confirm_draft(
        self,
        *,
        draft_id: str,
        component_nonce: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        return await asyncio.to_thread(
            self._repository.confirm_draft,
            draft_id=draft_id,
            component_nonce=component_nonce,
            owner_user_id=owner_user_id,
            guild_id=guild_id,
            channel_id=channel_id,
            audit=audit,
        )

    async def cancel_draft(
        self,
        *,
        draft_id: str,
        component_nonce: str,
        owner_user_id: int,
        guild_id: int,
        channel_id: int,
        audit: ScheduleAuditContext | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._repository.cancel_draft,
            draft_id=draft_id,
            component_nonce=component_nonce,
            owner_user_id=owner_user_id,
            guild_id=guild_id,
            channel_id=channel_id,
            audit=audit,
        )

    async def run_now(
        self,
        schedule_id: str,
        *,
        interaction_id: str,
        audit: ScheduleAuditContext | None = None,
    ) -> str | None:
        schedule = await asyncio.to_thread(self._repository.get, schedule_id)
        now = utc_now_ms()
        async with self._conversation_locks.hold(schedule.conversation_id):
            result = await asyncio.to_thread(
                self._repository.materialize,
                schedule_id=schedule.id,
                occurrence_key=f"manual:{interaction_id}",
                trigger_kind="manual",
                scheduled_for=None,
                scheduled_local=datetime.fromtimestamp(now / 1000, UTC).isoformat(),
                next_due_at=schedule.next_due_at,
                expected_version=None,
                advance_schedule=False,
                audit=(
                    audit
                    or ScheduleAuditContext.system(
                        f"schedule:{schedule.id}:run_now:{interaction_id}"
                    )
                ),
            )
        if result.turn_id:
            await self._wake_conversation(result.conversation_id)
        return result.turn_id

    async def pause(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        schedule = await asyncio.to_thread(self._repository.get, schedule_id)
        async with self._conversation_locks.hold(schedule.conversation_id):
            return await asyncio.to_thread(
                self._repository.set_state,
                schedule_id,
                expected_version=expected_version,
                state=ScheduleState.PAUSED,
                next_due_at=None,
                audit=audit,
            )

    async def update(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        kind: str | None = None,
        expression: str | None = None,
        timezone: str | None = None,
        misfire_policy: str | None = None,
        prompt_text: str | None = None,
        now_ms: int | None = None,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        schedule = await asyncio.to_thread(self._repository.get, schedule_id)
        if schedule.state not in {
            ScheduleState.ACTIVE,
            ScheduleState.PAUSED,
            ScheduleState.BLOCKED,
        }:
            raise ConflictError(
                f"schedule cannot update from {schedule.state.value}"
            )
        spec = parse_schedule_spec(
            kind or schedule.kind.value,
            expression or schedule.expression,
            timezone or schedule.timezone,
            misfire_policy or schedule.misfire_policy.value,
        )
        now = utc_now_ms() if now_ms is None else now_ms
        occurrence = next_occurrence(spec, now - 1)
        if occurrence is None:
            raise ConfigurationError("schedule has no future occurrence")
        candidate_prompt = schedule.prompt_text if prompt_text is None else prompt_text
        if candidate_prompt is None:
            raise ConfigurationError("schedule prompt is unavailable")
        prompt = _schedule_prompt(candidate_prompt)
        async with self._conversation_locks.hold(schedule.conversation_id):
            return await asyncio.to_thread(
                self._repository.update,
                schedule_id,
                expected_version=expected_version,
                kind=spec.kind,
                expression=spec.expression,
                timezone=spec.timezone,
                misfire_policy=spec.misfire_policy,
                prompt_text=prompt,
                next_due_at=occurrence.utc_ms,
                audit=audit,
            )

    async def resume(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        now_ms: int | None = None,
        audit: ScheduleAuditContext | None = None,
    ) -> ScheduleRecord:
        schedule = await asyncio.to_thread(self._repository.get, schedule_id)
        spec = schedule_spec(schedule)
        now = utc_now_ms() if now_ms is None else now_ms
        occurrence = next_occurrence(spec, now - 1)
        if occurrence is None:
            raise ConfigurationError("schedule has no future occurrence")
        async with self._conversation_locks.hold(schedule.conversation_id):
            return await asyncio.to_thread(
                self._repository.set_state,
                schedule_id,
                expected_version=expected_version,
                state=ScheduleState.ACTIVE,
                next_due_at=occurrence.utc_ms,
                audit=audit,
            )

    async def delete(
        self,
        schedule_id: str,
        *,
        expected_version: int,
        audit: ScheduleAuditContext | None = None,
    ) -> None:
        schedule = await asyncio.to_thread(self._repository.get, schedule_id)
        async with self._conversation_locks.hold(schedule.conversation_id):
            await asyncio.to_thread(
                self._repository.delete,
                schedule_id,
                expected_version=expected_version,
                audit=audit,
            )

    async def tick(
        self,
        *,
        now_ms: int | None = None,
        wake: bool = True,
    ) -> int:
        now = utc_now_ms() if now_ms is None else now_ms
        due = await asyncio.to_thread(self._repository.due, now)
        materialized = 0
        for schedule in due:
            try:
                materialized += await self._process_due(schedule, now, wake=wake)
            except ConflictError:
                continue
            except (ConfigurationError, ZoneInfoNotFoundError) as exc:
                try:
                    await asyncio.to_thread(
                        self._repository.block,
                        schedule.id,
                        expected_version=schedule.version,
                        reason=f"{type(exc).__name__}: {exc}",
                        audit=ScheduleAuditContext.system(
                            f"schedule:{schedule.id}:block:{schedule.version}"
                        ),
                    )
                except (ConflictError, NotFoundError):
                    continue
        return materialized

    async def reconcile_startup(self) -> int:
        blocked = 0
        schedules = await asyncio.to_thread(self._repository.list_active)
        for schedule in schedules:
            try:
                _validate_active_schedule(schedule)
                await asyncio.to_thread(
                    self._repository.validate_active_target,
                    schedule.id,
                    expected_version=schedule.version,
                )
            except ConflictError:
                continue
            except (ConfigurationError, ZoneInfoNotFoundError) as exc:
                try:
                    await asyncio.to_thread(
                        self._repository.block,
                        schedule.id,
                        expected_version=schedule.version,
                        reason=f"{type(exc).__name__}: {exc}",
                        audit=ScheduleAuditContext.system(
                            f"schedule:{schedule.id}:block:{schedule.version}"
                        ),
                    )
                except (ConflictError, NotFoundError):
                    continue
                blocked += 1
        return blocked

    async def _process_due(
        self,
        schedule: ScheduleRecord,
        now_ms: int,
        *,
        wake: bool,
    ) -> int:
        spec = _validate_active_schedule(schedule)
        occurrences: list[Occurrence] = []
        current = schedule.next_due_at
        while (
            current is not None
            and current <= now_ms
            and len(occurrences) < self._MAX_DUE_OCCURRENCES_PER_TICK
        ):
            local = datetime.fromtimestamp(current / 1000, UTC).astimezone(
                ZoneInfo(schedule.timezone)
            )
            occurrences.append(Occurrence(current, local.isoformat()))
            following = next_occurrence(spec, current)
            current = following.utc_ms if following else None
        if not occurrences:
            return 0

        version = schedule.version
        batch_reaches_present = current is None or current > now_ms
        on_time_boundary = now_ms - max(
            int(self._poll_seconds * 2_000),
            5_000,
        )
        if schedule.misfire_policy is MisfirePolicy.SKIP:
            on_time = (
                occurrences[-1]
                if batch_reaches_present
                and occurrences[-1].utc_ms >= on_time_boundary
                else None
            )
            skipped = occurrences[:-1] if on_time is not None else occurrences
            for index, occurrence in enumerate(skipped):
                next_unprocessed = (
                    occurrences[index + 1].utc_ms
                    if index + 1 < len(occurrences)
                    else current
                )
                success = await asyncio.to_thread(
                    self._repository.record_skipped,
                    schedule_id=schedule.id,
                    occurrence_key=occurrence.key,
                    scheduled_for=occurrence.utc_ms,
                    scheduled_local=occurrence.local_display,
                    next_due_at=next_unprocessed,
                    expected_version=version,
                    audit=ScheduleAuditContext.system(
                        f"schedule:{schedule.id}:skip:{occurrence.key}"
                    ),
                )
                if not success:
                    break
                version += 1
            if on_time is None:
                return 0
            selected = [on_time]
        elif (
            schedule.misfire_policy is MisfirePolicy.LATEST
            and not batch_reaches_present
        ):
            for index, occurrence in enumerate(occurrences):
                next_unprocessed = (
                    occurrences[index + 1].utc_ms
                    if index + 1 < len(occurrences)
                    else current
                )
                success = await asyncio.to_thread(
                    self._repository.record_skipped,
                    schedule_id=schedule.id,
                    occurrence_key=occurrence.key,
                    scheduled_for=occurrence.utc_ms,
                    scheduled_local=occurrence.local_display,
                    next_due_at=next_unprocessed,
                    expected_version=version,
                    audit=ScheduleAuditContext.system(
                        f"schedule:{schedule.id}:skip:{occurrence.key}"
                    ),
                )
                if not success:
                    break
                version += 1
            return 0
        else:
            selected = (
                occurrences[-1:]
                if schedule.misfire_policy is MisfirePolicy.LATEST
                else occurrences
            )
        if (
            schedule.misfire_policy is MisfirePolicy.LATEST
            and batch_reaches_present
        ):
            for index, occurrence in enumerate(occurrences[:-1]):
                success = await asyncio.to_thread(
                    self._repository.record_skipped,
                    schedule_id=schedule.id,
                    occurrence_key=occurrence.key,
                    scheduled_for=occurrence.utc_ms,
                    scheduled_local=occurrence.local_display,
                    next_due_at=occurrences[index + 1].utc_ms,
                    expected_version=version,
                    audit=ScheduleAuditContext.system(
                        f"schedule:{schedule.id}:skip:{occurrence.key}"
                    ),
                )
                if not success:
                    return 0
                version += 1
        count = 0
        for index, occurrence in enumerate(selected):
            durable_next_due = current
            if (
                schedule.misfire_policy is MisfirePolicy.ALL
                and index + 1 < len(selected)
            ):
                durable_next_due = selected[index + 1].utc_ms
            async with self._conversation_locks.hold(schedule.conversation_id):
                result = await asyncio.to_thread(
                    self._repository.materialize,
                    schedule_id=schedule.id,
                    occurrence_key=occurrence.key,
                    trigger_kind=(
                        "timer"
                        if occurrence.utc_ms >= on_time_boundary
                        else "misfire"
                    ),
                    scheduled_for=occurrence.utc_ms,
                    scheduled_local=occurrence.local_display,
                    next_due_at=durable_next_due,
                    expected_version=version,
                    audit=ScheduleAuditContext.system(
                        f"schedule:{schedule.id}:fire:{occurrence.key}"
                    ),
                )
            version += 1
            if result.turn_id:
                count += 1
                if wake:
                    await self._wake_conversation(result.conversation_id)
        return count

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._critical_failure(exc)
                return
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_seconds)


def parse_schedule_spec(
    kind: str,
    expression: str,
    timezone: str,
    misfire_policy: str,
) -> ScheduleSpec:
    try:
        schedule_kind = ScheduleKind(kind)
        policy = MisfirePolicy(misfire_policy)
        ZoneInfo(timezone)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise ConfigurationError(f"invalid schedule configuration: {exc}") from exc
    expression = expression.strip()
    if schedule_kind is ScheduleKind.ONCE:
        instant = _parse_instant(expression, timezone=timezone)
        canonical = instant.isoformat().replace("+00:00", "Z")
    else:
        cron = CronExpression.parse(expression)
        canonical = cron.canonical
    return ScheduleSpec(schedule_kind, canonical, timezone, policy)


def schedule_spec(schedule: ScheduleRecord) -> ScheduleSpec:
    return ScheduleSpec(
        schedule.kind,
        schedule.expression,
        schedule.timezone,
        schedule.misfire_policy,
    )


def _validate_active_schedule(schedule: ScheduleRecord) -> ScheduleSpec:
    if _schedule_name(schedule.name) != schedule.name:
        raise ConfigurationError("persisted schedule name is not canonical")
    if schedule.prompt_text is None:
        raise ConfigurationError("active schedule prompt is unavailable")
    prompt = _schedule_prompt(schedule.prompt_text)
    if prompt != schedule.prompt_text:
        raise ConfigurationError("persisted schedule prompt is not canonical")
    if sha256_text(prompt) != schedule.prompt_hash:
        raise ConfigurationError("persisted schedule prompt hash does not match")
    if (
        schedule.next_due_at is None
        or isinstance(schedule.next_due_at, bool)
        or not isinstance(schedule.next_due_at, int)
        or schedule.next_due_at < 0
    ):
        raise ConfigurationError("active schedule has no valid next due cursor")
    return parse_schedule_spec(
        schedule.kind.value,
        schedule.expression,
        schedule.timezone,
        schedule.misfire_policy.value,
    )


def next_occurrence(spec: ScheduleSpec, after_ms: int) -> Occurrence | None:
    if spec.kind is ScheduleKind.ONCE:
        instant = _parse_instant(spec.expression)
        timestamp = int(instant.timestamp() * 1000)
        if timestamp <= after_ms:
            return None
        local = instant.astimezone(ZoneInfo(spec.timezone))
        return Occurrence(timestamp, local.isoformat())
    cron = CronExpression.parse(spec.expression)
    zone = ZoneInfo(spec.timezone)
    candidate = datetime.fromtimestamp(after_ms / 1000, UTC).replace(
        second=0, microsecond=0
    ) + timedelta(minutes=1)
    deadline = candidate + timedelta(days=366 * 5)
    while candidate <= deadline:
        local = candidate.astimezone(zone)
        if cron.matches(local):
            return Occurrence(int(candidate.timestamp() * 1000), local.isoformat())
        candidate += timedelta(minutes=1)
    return None


def preview_occurrences(
    spec: ScheduleSpec,
    after_ms: int,
    *,
    limit: int,
) -> tuple[Occurrence, ...]:
    if limit < 1 or limit > 10:
        raise ConfigurationError("occurrence preview limit must be between 1 and 10")
    occurrences: list[Occurrence] = []
    cursor = after_ms
    while len(occurrences) < limit:
        occurrence = next_occurrence(spec, cursor)
        if occurrence is None:
            break
        occurrences.append(occurrence)
        cursor = occurrence.utc_ms
    return tuple(occurrences)


def _parse_instant(value: str, *, timezone: str | None = None) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        instant = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ConfigurationError("once expression must be an ISO-8601 instant") from exc
    if instant.tzinfo is None or instant.utcoffset() is None:
        if timezone is None:
            raise ConfigurationError(
                "once expression must include a UTC offset or an IANA timezone"
            )
        zone = ZoneInfo(timezone)
        candidates: dict[int, datetime] = {}
        for fold in (0, 1):
            candidate = instant.replace(tzinfo=zone, fold=fold)
            utc_candidate = candidate.astimezone(UTC)
            round_trip = utc_candidate.astimezone(zone).replace(tzinfo=None)
            if round_trip == instant:
                candidates[int(utc_candidate.timestamp() * 1_000_000)] = utc_candidate
        if not candidates:
            raise ConfigurationError(
                "once expression is a nonexistent local time in its timezone"
            )
        if len(candidates) > 1:
            raise ConfigurationError(
                "once expression is ambiguous in its timezone; include a UTC offset"
            )
        return next(iter(candidates.values()))
    return instant.astimezone(UTC)


def _schedule_name(value: str) -> str:
    name = " ".join(value.split())
    if not name:
        raise ConfigurationError("schedule name may not be empty")
    if len(name) > 100:
        raise ConfigurationError("schedule name may not exceed 100 characters")
    return name


def _schedule_prompt(value: str) -> str:
    prompt = value.strip()
    if not prompt:
        raise ConfigurationError("schedule prompt may not be empty")
    if len(prompt.encode("utf-8")) > 16 * 1024:
        raise ConfigurationError("schedule prompt may not exceed 16 KiB")
    return prompt
