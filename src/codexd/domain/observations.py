from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from codexd.domain.ids import utc_now_ms

FIELDS = (
    "model",
    "model_provider",
    "reasoning_effort",
    "reasoning_summary",
    "personality",
    "service_tier",
)


@dataclass(frozen=True)
class ProviderSettings:
    thread_id: str
    generation: int
    values: dict[str, str | None]
    source: str
    observed_at: int
    sequence: int
    turn_id: str | None = None
    fresh: bool = True
    state: str = "unknown"


def scalar(value: Any) -> str | None:
    value = getattr(value, "value", value)
    if not isinstance(value, str) or not value or len(value) > 128:
        return None
    if not all(c.isalnum() or c in "-_.:/" for c in value) or "/" in value:
        return None
    return value


def settings_values(value: Any, *, notification: bool) -> dict[str, str | None]:
    names = {"reasoning_effort": "effort", "reasoning_summary": "summary"} if notification else {}
    return {name: scalar(getattr(value, names.get(name, name), None)) for name in FIELDS}


def unknown_settings(thread_id: str = "", generation: int = 0) -> ProviderSettings:
    return ProviderSettings(thread_id, generation, {}, "not observed", utc_now_ms(), 0, fresh=False)
