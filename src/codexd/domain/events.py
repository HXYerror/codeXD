from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from codexd.domain.ids import canonical_json, sha256_text, utc_now_ms


@dataclass(frozen=True)
class NormalizedEvent:
    kind: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    provider_event_id: str | None = None
    occurred_at: int = field(default_factory=utc_now_ms)
    raw_type: str | None = None
    raw_hash: str | None = None
    raw_size: int | None = None
    schema_version: int = 1

    @classmethod
    def unknown(
        cls,
        *,
        method: str,
        raw_payload: Mapping[str, Any],
        occurred_at: int | None = None,
    ) -> NormalizedEvent:
        serialized = canonical_json(raw_payload)
        return cls(
            kind="provider.unknown",
            payload={
                "method": method,
                "raw_hash": sha256_text(serialized),
                "raw_size": len(serialized),
            },
            occurred_at=occurred_at or utc_now_ms(),
            raw_type=method,
            raw_hash=sha256_text(serialized),
            raw_size=len(serialized.encode("utf-8")),
        )
