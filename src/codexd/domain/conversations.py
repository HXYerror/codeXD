from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class ConversationState(StrEnum):
    UNINITIALIZED = "uninitialized"
    ACTIVE = "active"
    ARCHIVED = "archived"
    BLOCKED = "blocked"
    DELETED = "deleted"


class RevisionState(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    SUPERSEDED = "superseded"
    BLOCKED = "blocked"


class RuntimeLeaseState(StrEnum):
    STARTING = "starting"
    READY = "ready"
    UNHEALTHY = "unhealthy"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class SandboxProfile(StrEnum):
    FULL_ACCESS = "full_access"
    WORKSPACE_WRITE = "workspace_write"
    READ_ONLY = "read_only"


class ApprovalPolicy(StrEnum):
    AUTO_REVIEW = "auto_review"


class WebSearchMode(StrEnum):
    CACHED = "cached"
    INDEXED = "indexed"
    LIVE = "live"
    DISABLED = "disabled"
    PROVIDER_DEFAULT_UNCONTROLLED = "provider_default_uncontrolled"


class ThreadProviderState(StrEnum):
    IDLE = "idle"
    ACTIVE = "active"
    NOT_LOADED = "notLoaded"
    SYSTEM_ERROR = "systemError"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ThreadConfig:
    model: str | None
    personality: str | None
    sandbox: SandboxProfile
    approval_mode: ApprovalPolicy = ApprovalPolicy.AUTO_REVIEW
    service_tier: str | None = None
    web_search_mode: WebSearchMode = WebSearchMode.CACHED

    def as_dict(self) -> dict[str, str | None]:
        return {
            "model": self.model,
            "personality": self.personality,
            "sandbox": self.sandbox.value,
            "approval_mode": self.approval_mode.value,
            "service_tier": self.service_tier,
            "web_search_mode": self.web_search_mode.value,
        }


@dataclass(frozen=True)
class TurnConfig:
    cwd: Path
    sandbox: SandboxProfile
    approval_mode: ApprovalPolicy = ApprovalPolicy.AUTO_REVIEW
    model: str | None = None
    reasoning_effort: str | None = None
    personality: str | None = None
    service_tier: str | None = None
    reasoning_summary: str | None = None
    output_schema: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ThreadIdentity:
    thread_id: str
    requested_thread_id: str | None
    provider_session_id: str
    forked_from_thread_id: str | None
    parent_thread_id: str | None
    provider_version: str


@dataclass(frozen=True)
class ThreadSnapshot:
    identity: ThreadIdentity
    state: ThreadProviderState
    active_flags: tuple[str, ...] = ()
    error_message: str | None = None

