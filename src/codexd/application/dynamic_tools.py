from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import Mapping
from typing import Any, cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from openai_codex.models import JsonObject

from codexd.application.outbound_images import (
    PUBLISH_IMAGE_INPUT_SCHEMA,
    OutboundImageBroker,
)
from codexd.application.schedule_coordinator import (
    PreparedScheduleDraft,
    prepare_schedule_create_draft,
)
from codexd.domain.ids import canonical_json, sha256_text, utc_now_ms
from codexd.errors import ConfigurationError, ConflictError, SecurityError
from codexd.runtime.port import DynamicToolCall
from codexd.storage.records import DynamicToolInvocationRecord
from codexd.storage.schedules import ScheduleRepository

_MAX_ARGUMENT_BYTES = 20 * 1024

SCHEDULE_CREATE_INPUT_SCHEMA: JsonObject = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "name",
        "kind",
        "expression",
        "timezone",
        "misfire_policy",
        "prompt",
    ],
    "properties": {
        "name": {"type": "string", "minLength": 1, "maxLength": 100},
        "kind": {"type": "string", "enum": ["once", "cron"]},
        "expression": {"type": "string", "minLength": 1, "maxLength": 100},
        "timezone": {"type": "string", "minLength": 1, "maxLength": 64},
        "misfire_policy": {
            "type": "string",
            "enum": ["skip", "latest", "all"],
        },
        "prompt": {"type": "string", "minLength": 1},
    },
}

CODEXD_DYNAMIC_TOOLS: tuple[JsonObject, ...] = (
    {
        "type": "namespace",
        "name": "codexd",
        "description": (
            "Actions provided by codexD for the current Discord Conversation."
        ),
        "tools": [
            {
                "type": "function",
                "name": "schedule_create",
                "description": (
                    "Prepare a Schedule draft only when the user explicitly asks to "
                    "run work later or repeatedly. The Schedule is not active until "
                    "the configured Discord owner confirms the card. Never claim the "
                    "Schedule is active from this tool result alone."
                ),
                "inputSchema": SCHEDULE_CREATE_INPUT_SCHEMA,
            },
            {
                "type": "function",
                "name": "publish_image",
                "description": (
                    "Register a raster image generated for the user's current "
                    "request so codexD can attach it to the final Discord response. "
                    "Use only when the user asked to receive an image or visualization. "
                    "Inspect the generated image first, then pass that exact observed "
                    "path. Do not emit visualize control markup as a substitute. A "
                    "successful result means registered for durable delivery, not that "
                    "Discord upload has completed."
                ),
                "inputSchema": PUBLISH_IMAGE_INPUT_SCHEMA,
            },
        ],
    },
)

_SCHEDULE_CREATE_VALIDATOR = Draft202012Validator(SCHEDULE_CREATE_INPUT_SCHEMA)


class DynamicToolDispatcher:
    def __init__(
        self,
        *,
        schedules: ScheduleRepository,
        images: OutboundImageBroker | None = None,
        owner_user_id: int,
        guild_id: int,
    ) -> None:
        self._schedules = schedules
        self._images = images
        self._owner_user_id = owner_user_id
        self._guild_id = guild_id

    async def handle(self, call: DynamicToolCall) -> dict[str, object]:
        namespace = call.namespace or ""
        if namespace != "codexd":
            return _tool_response(
                False,
                {
                    "status": "error",
                    "code": "unsupported_tool",
                    "message": "This codexD tool is not available.",
                },
            )
        if call.tool == "publish_image":
            if self._images is None:
                return _tool_response(
                    False,
                    {
                        "status": "error",
                        "code": "tool_unavailable",
                        "message": "Image publication is unavailable.",
                    },
                )
            return await self._images.handle(call)
        if call.tool != "schedule_create":
            return _tool_response(
                False,
                {
                    "status": "error",
                    "code": "unsupported_tool",
                    "message": "This codexD tool is not available.",
                },
            )
        arguments_hash, arguments_error = _arguments_hash(call.arguments)
        try:
            preflight = await asyncio.to_thread(
                self._schedules.preflight_schedule_create_tool,
                local_turn_id=call.local_turn_id,
                runtime_generation=call.runtime_generation,
                provider_thread_id=call.provider_thread_id,
                provider_turn_id=call.provider_turn_id,
                provider_call_id=call.provider_call_id,
                namespace=namespace,
                tool_name=call.tool,
                arguments_hash=arguments_hash,
                configured_owner_user_id=self._owner_user_id,
                configured_guild_id=self._guild_id,
            )
            if preflight is not None:
                return _invocation_response(preflight)
            prepared: PreparedScheduleDraft | None = None
            validation_error = arguments_error
            if validation_error is None:
                prepared, validation_error = await asyncio.to_thread(
                    _prepare_schedule_arguments,
                    call.arguments,
                )
            invocation = await asyncio.to_thread(
                self._schedules.execute_schedule_create_tool,
                local_turn_id=call.local_turn_id,
                runtime_generation=call.runtime_generation,
                provider_thread_id=call.provider_thread_id,
                provider_turn_id=call.provider_turn_id,
                provider_call_id=call.provider_call_id,
                namespace=namespace,
                tool_name=call.tool,
                arguments_hash=arguments_hash,
                configured_owner_user_id=self._owner_user_id,
                configured_guild_id=self._guild_id,
                payload=prepared.payload if prepared is not None else None,
                occurrences=(prepared.occurrences if prepared is not None else ()),
                component_nonce=secrets.token_urlsafe(9),
                expires_at=(
                    prepared.expires_at
                    if prepared is not None
                    else utc_now_ms() + 10 * 60 * 1000
                ),
                validation_error=validation_error,
            )
        except ConflictError:
            return _tool_response(
                False,
                {
                    "status": "error",
                    "code": "call_identity_conflict",
                    "message": "This dynamic tool call identity was already used.",
                    "confirmation_required": False,
                },
            )
        except SecurityError:
            return _tool_response(
                False,
                {
                    "status": "error",
                    "code": "scope_mismatch",
                    "message": "The originating Discord Turn is unavailable.",
                    "confirmation_required": False,
                },
            )
        return _invocation_response(invocation)


def _arguments_hash(value: object) -> tuple[str, tuple[str, str] | None]:
    try:
        encoded = canonical_json(value)
    except (TypeError, ValueError):
        return sha256_text("invalid-json"), (
            "invalid_arguments",
            "Schedule arguments must be a JSON object.",
        )
    if len(encoded.encode("utf-8")) > _MAX_ARGUMENT_BYTES:
        return sha256_text(encoded), (
            "arguments_too_large",
            "Schedule arguments exceed the supported size.",
        )
    return sha256_text(encoded), None


def _prepare_schedule_arguments(
    value: object,
) -> tuple[PreparedScheduleDraft | None, tuple[str, str] | None]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        return None, (
            "invalid_arguments",
            "Schedule arguments must be a JSON object.",
        )
    arguments = cast(dict[str, Any], dict(value))
    errors = sorted(
        _SCHEDULE_CREATE_VALIDATOR.iter_errors(arguments),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        path = ".".join(str(part) for part in errors[0].absolute_path)
        location = f" at `{path}`" if path else ""
        return None, (
            "invalid_arguments",
            f"Schedule arguments do not match the required schema{location}.",
        )
    try:
        prepared = prepare_schedule_create_draft(
            name=str(arguments["name"]),
            kind=str(arguments["kind"]),
            expression=str(arguments["expression"]),
            timezone=str(arguments["timezone"]),
            misfire_policy=str(arguments["misfire_policy"]),
            prompt_text=str(arguments["prompt"]),
            now_ms=utc_now_ms(),
        )
    except ConfigurationError as exc:
        code, message = _schedule_validation_error(str(exc))
        return None, (code, message)
    return prepared, None


def _schedule_validation_error(message: str) -> tuple[str, str]:
    lowered = message.lower()
    if "timezone" in lowered:
        return "invalid_timezone", message[:512]
    if "cron" in lowered or "once expression" in lowered or "local time" in lowered:
        return "invalid_expression", message[:512]
    if "name" in lowered:
        return "invalid_name", message[:512]
    if "prompt" in lowered:
        return "invalid_prompt", message[:512]
    if "future occurrence" in lowered:
        return "no_future_occurrence", message[:512]
    return "invalid_schedule", message[:512]


def _invocation_response(
    invocation: DynamicToolInvocationRecord,
) -> dict[str, object]:
    try:
        result = json.loads(invocation.result_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("persisted dynamic tool result is invalid") from exc
    if not isinstance(result, dict):
        raise RuntimeError("persisted dynamic tool result is not an object")
    return _tool_response(invocation.success, result)


def _tool_response(
    success: bool,
    result: Mapping[str, object],
) -> dict[str, object]:
    return {
        "success": success,
        "contentItems": [
            {
                "type": "inputText",
                "text": canonical_json(dict(result)),
            }
        ],
    }
