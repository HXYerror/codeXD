from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from codexd.domain.ids import canonical_json, sha256_text
from codexd.errors import InvariantError

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DISCORD_MENTION = re.compile(
    r"<(?:@!?|@&|#)\d+>|@(?:everyone|here)(?![A-Za-z0-9_])",
    re.IGNORECASE,
)
_MAX_FILE_DISPLAY_NAME_CHARS = 128


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
class TurnFile:
    attachment_id: str
    ordinal: int
    canonical_path: Path
    display_name: str
    reported_media_type: str | None
    sha256: str
    size_bytes: int
    retention_until: int

    def __post_init__(self) -> None:
        if not self.attachment_id:
            raise InvariantError("Turn file attachment ID may not be empty")
        if isinstance(self.ordinal, bool) or self.ordinal < 0:
            raise InvariantError("Turn file ordinal must be a non-negative integer")
        if not self.canonical_path.is_absolute():
            raise InvariantError("Turn file path must be canonical and absolute")
        if not _safe_file_display_name(self.display_name):
            raise InvariantError("Turn file display name is unsafe")
        if self.reported_media_type is not None and (
            not self.reported_media_type
            or len(self.reported_media_type) > 255
            or any(_is_control(character) for character in self.reported_media_type)
        ):
            raise InvariantError("Turn file reported media type is invalid")
        if not _SHA256.fullmatch(self.sha256):
            raise InvariantError("Turn file SHA-256 is invalid")
        if isinstance(self.size_bytes, bool) or self.size_bytes <= 0:
            raise InvariantError("Turn file size must be positive")
        if isinstance(self.retention_until, bool) or self.retention_until <= 0:
            raise InvariantError("Turn file retention deadline must be positive")

    def snapshot(self) -> dict[str, Any]:
        return {
            "attachment_id": self.attachment_id,
            "ordinal": self.ordinal,
            "canonical_path": str(self.canonical_path),
            "display_name": self.display_name,
            "reported_media_type": self.reported_media_type,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
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
    files: tuple[TurnFile, ...] = ()

    def __post_init__(self) -> None:
        normalized = self.text.strip() if self.text is not None else None
        object.__setattr__(self, "text", normalized or None)
        object.__setattr__(
            self, "images", tuple(sorted(self.images, key=lambda item: item.ordinal))
        )
        object.__setattr__(
            self, "files", tuple(sorted(self.files, key=lambda item: item.ordinal))
        )
        object.__setattr__(self, "skill_inputs", _dedupe_skills(self.skill_inputs))
        if self.text is None and not self.images and not self.files:
            raise InvariantError("Turn input requires text or at least one attachment")
        attachment_ordinals = [image.ordinal for image in self.images]
        attachment_ordinals.extend(file.ordinal for file in self.files)
        if len(attachment_ordinals) != len(set(attachment_ordinals)):
            raise InvariantError("Turn attachment ordinals must be unique")

    def snapshot(self) -> dict[str, Any]:
        snapshot: dict[str, Any] = {
            "text": self.text,
            "images": [image.snapshot() for image in self.images],
            "files": [file.snapshot() for file in self.files],
            "skill_inputs": [skill.snapshot() for skill in self.skill_inputs],
        }
        return snapshot

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


def _safe_file_display_name(value: str) -> bool:
    return bool(
        value
        and len(value) <= _MAX_FILE_DISPLAY_NAME_CHARS
        and value not in {".", ".."}
        and not any(character in "/\\" or _is_control(character) for character in value)
        and _DISCORD_MENTION.search(value) is None
    )


def _is_control(value: str) -> bool:
    return unicodedata.category(value).startswith("C")
