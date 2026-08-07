from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from codexd.errors import ConfigurationError


class ScheduleKind(StrEnum):
    ONCE = "once"
    CRON = "cron"


class ScheduleState(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    BLOCKED = "blocked"
    DELETED = "deleted"


class MisfirePolicy(StrEnum):
    SKIP = "skip"
    LATEST = "latest"
    ALL = "all"


class ScheduleFireState(StrEnum):
    DUE = "due"
    MATERIALIZED = "materialized"
    SKIPPED = "skipped"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class ScheduleAuditContext:
    actor_kind: str
    actor_id: str | None
    correlation_id: str

    @classmethod
    def discord_user(
        cls,
        *,
        user_id: int,
        interaction_id: str,
    ) -> ScheduleAuditContext:
        return cls(
            actor_kind="discord_user",
            actor_id=str(user_id),
            correlation_id=interaction_id,
        )

    @classmethod
    def system(cls, correlation_id: str) -> ScheduleAuditContext:
        return cls(
            actor_kind="system",
            actor_id=None,
            correlation_id=correlation_id,
        )


@dataclass(frozen=True)
class ScheduleModalSubmission:
    intent_id: str
    kind: str
    expires_at: int
    nonce: str
    interaction_id: str
    guild_id: int
    channel_id: int
    user_id: int


@dataclass(frozen=True)
class ScheduleSpec:
    kind: ScheduleKind
    expression: str
    timezone: str
    misfire_policy: MisfirePolicy


@dataclass(frozen=True)
class CronExpression:
    minute: frozenset[int]
    hour: frozenset[int]
    day: frozenset[int]
    month: frozenset[int]
    weekday: frozenset[int]
    day_wildcard: bool
    weekday_wildcard: bool
    canonical: str

    @classmethod
    def parse(cls, value: str) -> CronExpression:
        fields = value.split()
        if len(fields) != 5:
            raise ConfigurationError("cron expression must contain exactly five fields")
        minute, minute_canonical, _ = _parse_cron_field(fields[0], 0, 59)
        hour, hour_canonical, _ = _parse_cron_field(fields[1], 0, 23)
        day, day_canonical, day_wildcard = _parse_cron_field(fields[2], 1, 31)
        month, month_canonical, _ = _parse_cron_field(fields[3], 1, 12)
        weekday, weekday_canonical, weekday_wildcard = _parse_cron_field(
            fields[4], 0, 7, normalize_weekday=True
        )
        return cls(
            minute,
            hour,
            day,
            month,
            weekday,
            day_wildcard,
            weekday_wildcard,
            " ".join(
                (
                    minute_canonical,
                    hour_canonical,
                    day_canonical,
                    month_canonical,
                    weekday_canonical,
                )
            ),
        )

    def matches(self, value: datetime) -> bool:
        if value.minute not in self.minute or value.hour not in self.hour:
            return False
        if value.month not in self.month:
            return False
        day_match = value.day in self.day
        cron_weekday = (value.weekday() + 1) % 7
        weekday_match = cron_weekday in self.weekday
        if self.day_wildcard and self.weekday_wildcard:
            return True
        if self.day_wildcard:
            return weekday_match
        if self.weekday_wildcard:
            return day_match
        return day_match or weekday_match


def validate_persisted_schedule_spec(
    kind: ScheduleKind,
    expression: str,
    timezone: str,
) -> None:
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ConfigurationError(f"invalid schedule timezone: {timezone}") from exc
    if expression != expression.strip() or not expression:
        raise ConfigurationError("schedule expression must be non-empty and canonical")
    if kind is ScheduleKind.CRON:
        if CronExpression.parse(expression).canonical != expression:
            raise ConfigurationError("cron expression is not canonical")
        return
    normalized = expression[:-1] + "+00:00" if expression.endswith("Z") else expression
    try:
        instant = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ConfigurationError("once expression must be an ISO-8601 instant") from exc
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ConfigurationError("persisted once expression must contain a UTC offset")
    canonical = instant.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if canonical != expression:
        raise ConfigurationError("once expression is not canonical UTC")


def _parse_cron_field(
    value: str,
    minimum: int,
    maximum: int,
    *,
    normalize_weekday: bool = False,
) -> tuple[frozenset[int], str, bool]:
    wildcard = value == "*"
    selected: set[int] = set()
    try:
        for part in value.split(","):
            base, separator, step_raw = part.partition("/")
            step = int(step_raw) if separator else 1
            if step <= 0:
                raise ConfigurationError("cron step must be positive")
            if base == "*":
                start, end = minimum, maximum
            elif "-" in base:
                start_raw, end_raw = base.split("-", 1)
                start, end = int(start_raw), int(end_raw)
            else:
                start = end = int(base)
            if start < minimum or end > maximum or start > end:
                raise ConfigurationError(f"cron field {value!r} is out of range")
            selected.update(range(start, end + 1, step))
    except ValueError as exc:
        raise ConfigurationError(f"cron field {value!r} is invalid") from exc
    if normalize_weekday and 7 in selected:
        selected.remove(7)
        selected.add(0)
    return frozenset(selected), value, wildcard
