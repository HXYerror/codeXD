from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceTierDescriptor:
    id: str
    name: str
    description: str | None


@dataclass(frozen=True)
class ModelDescriptor:
    id: str
    model: str
    is_default: bool
    input_modalities: tuple[str, ...]
    supported_reasoning_efforts: tuple[str, ...]
    default_reasoning_effort: str | None
    supports_personality: bool
    service_tiers: tuple[ServiceTierDescriptor, ...]
    default_service_tier: str | None
    upgrade: dict[str, object] | None


@dataclass(frozen=True)
class ModelCatalogSnapshot:
    models: tuple[ModelDescriptor, ...]
    complete: bool
    next_cursor: str | None


@dataclass(frozen=True)
class AccountStatus:
    auth_required: bool
    account_type: str | None
    plan_type: str | None
    observed_at: int

