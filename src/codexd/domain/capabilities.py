from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from codexd.domain.ids import canonical_json
from codexd.errors import InvariantError


class EventCapability(StrEnum):
    SUPPORTED = "supported"
    SUPPORTED_NOT_OBSERVED = "supported_not_observed"
    UNSUPPORTED = "unsupported"


CapabilityValue = bool | EventCapability

REQUIRED_CAPABILITY_NAMES = frozenset(
    {
        "thread.start",
        "thread.resume",
        "thread.read",
        "turn.stream",
        "turn.interrupt",
        "turn.steer",
        "turn.image_input",
        "turn.model_override",
        "turn.reasoning_effort",
        "model.catalog",
        "event.turn_lifecycle",
        "thread.identity",
        "sandbox.configure",
        "approval.configure",
        "runtime.close",
    }
)


@dataclass(frozen=True)
class CompatibilityInfo:
    declared_range: str
    matrix_tier: str
    handshake: str


@dataclass(frozen=True)
class CapabilityManifest:
    adapter: str
    sdk_version: str
    runtime_version: str
    compatibility: CompatibilityInfo
    image_input_modes: tuple[str, ...]
    required: Mapping[str, bool]
    optional: Mapping[str, CapabilityValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "required", MappingProxyType(dict(self.required)))
        object.__setattr__(self, "optional", MappingProxyType(dict(self.optional)))

    def assert_required(self) -> None:
        missing = sorted(
            name
            for name in REQUIRED_CAPABILITY_NAMES
            if self.required.get(name) is not True
        )
        if missing:
            raise InvariantError(f"required SDK capabilities unavailable: {missing}")
        if not self.image_input_modes:
            raise InvariantError("required image input has no verified wire mode")

    def as_dict(self) -> dict[str, object]:
        return {
            "adapter": self.adapter,
            "sdk_version": self.sdk_version,
            "runtime_version": self.runtime_version,
            "compatibility": {
                "declared_range": self.compatibility.declared_range,
                "matrix_tier": self.compatibility.matrix_tier,
                "handshake": self.compatibility.handshake,
            },
            "image_input_modes": list(self.image_input_modes),
            "required": dict(self.required),
            "optional": {
                key: value.value if isinstance(value, EventCapability) else value
                for key, value in self.optional.items()
            },
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(canonical_json(self.as_dict()).encode("utf-8")).hexdigest()
