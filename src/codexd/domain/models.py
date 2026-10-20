from __future__ import annotations

from dataclasses import dataclass

from codexd.errors import InvariantError


@dataclass(frozen=True)
class ServiceTierDescriptor:
    id: str
    name: str
    description: str | None


@dataclass(frozen=True)
class ReasoningEffortDescriptor:
    value: str
    description: str


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
    display_name: str = ""
    description: str = ""
    reasoning_effort_options: tuple[ReasoningEffortDescriptor, ...] = ()


@dataclass(frozen=True)
class ModelCatalogSnapshot:
    models: tuple[ModelDescriptor, ...]
    complete: bool
    next_cursor: str | None


@dataclass(frozen=True)
class EffectiveModel:
    model: str | None
    source: str
    descriptor: ModelDescriptor | None = None

    def require_descriptor(self) -> ModelDescriptor:
        if self.descriptor is None:
            raise InvariantError("effective model capabilities are unknown; select a catalog model")
        return self.descriptor


def resolve_model(
    catalog: ModelCatalogSnapshot | None,
    override: str | None,
    project_default: str | None,
    configured: EffectiveModel,
) -> EffectiveModel:
    model, source = (
        (override, "conversation override")
        if override is not None
        else (project_default, "project default")
        if project_default is not None
        else (configured.model, configured.source)
    )
    matches = (
        tuple(
            item for item in catalog.models if model is not None and model in {item.id, item.model}
        )
        if catalog
        else ()
    )
    descriptor = matches[0] if len(matches) == 1 else None
    return EffectiveModel(descriptor.model if descriptor else model, source, descriptor)


@dataclass(frozen=True)
class AccountStatus:
    auth_required: bool
    account_type: str | None
    plan_type: str | None
    observed_at: int
