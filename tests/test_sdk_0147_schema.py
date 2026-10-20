from __future__ import annotations

from pathlib import Path

import pytest
from openai_codex.generated.v2_all import (
    EnvironmentConnectionNotification,
    Model,
    ThreadEnvironmentConnectedServerNotification,
    TokenUsageBreakdown,
)
from openai_codex.models import Notification

from codexd.runtime.codex_sdk import _assert_notification_route, _normalize_notification
from codexd.runtime.errors import AdapterInvariantError
from codexd.storage.usage import validate_usage_payload


def test_0147_schema_extensions_are_explicit() -> None:
    assert "cache_write_input_tokens" in TokenUsageBreakdown.model_fields
    assert "model_specialty" in Model.model_fields
    assert "emitted_at_ms" in ThreadEnvironmentConnectedServerNotification.model_fields


@pytest.mark.parametrize(
    "method", ["thread/environment/connected", "thread/environment/disconnected"]
)
def test_environment_notification_is_route_checked_and_hash_only(method: str) -> None:
    notification = Notification(
        method,
        EnvironmentConnectionNotification(
            threadId="thread",
            environmentId="private-environment-path",
        ),
    )
    _assert_notification_route(
        notification, expected_thread_id="thread", expected_turn_id="turn", generation=1
    )
    event = _normalize_notification(notification, cwd=Path("/workspace"))
    assert event.kind == "provider.unknown"
    assert "private-environment-path" not in str(event.payload)
    assert set(event.payload) == {"method", "raw_hash", "raw_size"}
    with pytest.raises(AdapterInvariantError):
        _assert_notification_route(
            notification, expected_thread_id="other", expected_turn_id="turn", generation=1
        )


@pytest.mark.parametrize("cache_write", [None, 0, 10])
def test_cache_write_usage_preserves_snapshot(cache_write: int | None) -> None:
    breakdown = TokenUsageBreakdown(
        inputTokens=100,
        outputTokens=20,
        cachedInputTokens=5,
        reasoningOutputTokens=10,
        totalTokens=120,
        cacheWriteInputTokens=cache_write,
    ).model_dump(exclude_none=True)
    result = validate_usage_payload({"last": breakdown, "total": breakdown})
    assert result["last"] == breakdown


def test_dynamic_audio_output_remains_fail_closed() -> None:
    from codexd.runtime.app_server import _validated_tool_response

    with pytest.raises(ValueError, match="content type"):
        _validated_tool_response(
            {"success": True, "contentItems": [{"type": "inputAudio", "data": "private"}]}
        )
