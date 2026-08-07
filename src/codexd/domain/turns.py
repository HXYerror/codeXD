from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from codexd.domain.ids import canonical_json, sha256_text
from codexd.errors import InvariantError


class TurnSource(StrEnum):
    DISCORD = "discord"
    SCHEDULE = "schedule"


class TurnState(StrEnum):
    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"

    @property
    def terminal(self) -> bool:
        return self in {
            TurnState.COMPLETED,
            TurnState.FAILED,
            TurnState.CANCELLED,
            TurnState.INTERRUPTED,
        }


class InterruptOrigin(StrEnum):
    USER = "user"
    SHUTDOWN = "shutdown"
    RUNTIME = "runtime"


@dataclass(frozen=True)
class TurnImage:
    attachment_id: str
    ordinal: int
    canonical_path: Path
    media_type: str
    source_sha256: str
    sha256: str
    size_bytes: int
    width: int
    height: int
    source_name_sanitized: str
    retention_until: int

    def snapshot(self) -> dict[str, Any]:
        return {
            "attachment_id": self.attachment_id,
            "ordinal": self.ordinal,
            "canonical_path": str(self.canonical_path),
            "media_type": self.media_type,
            "source_sha256": self.source_sha256,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "width": self.width,
            "height": self.height,
            "source_name_sanitized": self.source_name_sanitized,
            "retention_until": self.retention_until,
        }


@dataclass(frozen=True)
class TurnSkill:
    name: str
    canonical_path: Path
    content_hash: str

    def snapshot(self) -> dict[str, str]:
        return {
            "name": self.name,
            "canonical_path": str(self.canonical_path),
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True)
class TurnInput:
    text: str | None = None
    images: tuple[TurnImage, ...] = ()
    skill_inputs: tuple[TurnSkill, ...] = ()

    def __post_init__(self) -> None:
        normalized = self.text.strip() if self.text is not None else None
        object.__setattr__(self, "text", normalized or None)
        object.__setattr__(
            self, "images", tuple(sorted(self.images, key=lambda item: item.ordinal))
        )
        object.__setattr__(self, "skill_inputs", _dedupe_skills(self.skill_inputs))
        if self.text is None and not self.images:
            raise InvariantError("Turn input requires text or at least one image")
        image_ordinals = [image.ordinal for image in self.images]
        if len(image_ordinals) != len(set(image_ordinals)):
            raise InvariantError("Turn image ordinals must be unique")

    def snapshot(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "images": [image.snapshot() for image in self.images],
            "skill_inputs": [skill.snapshot() for skill in self.skill_inputs],
        }

    @property
    def input_hash(self) -> str:
        return sha256_text(canonical_json(self.snapshot()))


@dataclass(frozen=True)
class TurnIdentity:
    local_turn_id: str
    provider_turn_id: str | None
    runtime_generation: int


_TRANSITIONS: dict[TurnState, frozenset[TurnState]] = {
    TurnState.QUEUED: frozenset(
        {TurnState.STARTING, TurnState.CANCELLED, TurnState.INTERRUPTED}
    ),
    TurnState.STARTING: frozenset(
        {TurnState.RUNNING, TurnState.FAILED, TurnState.INTERRUPTED, TurnState.CANCELLING}
    ),
    TurnState.RUNNING: frozenset(
        {
            TurnState.RUNNING,
            TurnState.CANCELLING,
            TurnState.COMPLETED,
            TurnState.FAILED,
            TurnState.INTERRUPTED,
        }
    ),
    TurnState.CANCELLING: frozenset(
        {
            TurnState.CANCELLED,
            TurnState.COMPLETED,
            TurnState.FAILED,
            TurnState.INTERRUPTED,
        }
    ),
    TurnState.COMPLETED: frozenset(),
    TurnState.FAILED: frozenset(),
    TurnState.CANCELLED: frozenset(),
    TurnState.INTERRUPTED: frozenset(),
}


def assert_turn_transition(current: TurnState, target: TurnState) -> None:
    if target not in _TRANSITIONS[current]:
        raise InvariantError(f"invalid Turn transition: {current.value} -> {target.value}")


def _dedupe_skills(skills: tuple[TurnSkill, ...]) -> tuple[TurnSkill, ...]:
    seen: set[tuple[str, str]] = set()
    result: list[TurnSkill] = []
    for skill in skills:
        key = (skill.name, str(skill.canonical_path))
        if key not in seen:
            seen.add(key)
            result.append(skill)
    return tuple(result)
