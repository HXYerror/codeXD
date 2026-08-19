from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codexd.domain.ids import canonical_json, sha256_text
from codexd.security.redaction import redacted_summary

_TYPED_CODES = {
    "responseStreamDisconnected": "provider_stream_disconnected",
    "responseStreamConnectionFailed": "provider_stream_connection_failed",
    "responseTooManyFailedAttempts": "provider_retry_exhausted",
    "httpConnectionFailed": "provider_connection_failed",
    "serverOverloaded": "provider_overloaded",
    "contextWindowExceeded": "provider_context_window_exceeded",
    "sessionBudgetExceeded": "provider_session_budget_exceeded",
    "usageLimitExceeded": "provider_usage_limit_exceeded",
    "unauthorized": "provider_unauthorized",
    "badRequest": "provider_bad_request",
    "cyberPolicy": "provider_policy_blocked",
    "internalServerError": "provider_internal_error",
    "sandboxError": "provider_sandbox_error",
    "threadRollbackFailed": "provider_thread_rollback_failed",
    "activeTurnNotSteerable": "provider_active_turn_not_steerable",
    "other": "provider_failed",
}
_RETRY = re.compile(r"(?i)reconnecting\D+(\d+)\s*/\s*(\d+)")


@dataclass(frozen=True)
class ProviderFailure:
    code: str
    typed_code: str | None
    http_status: int | None
    retry_count: int
    retry_limit: int | None
    safe_message: str | None
    fingerprint: str
    unknown_typed: bool

    def payload(self) -> dict[str, object]:
        return {
            "failure_code": self.code,
            "typed_code": self.typed_code,
            "http_status": self.http_status,
            "retry_count": self.retry_count,
            "retry_limit": self.retry_limit,
            "safe_message": self.safe_message,
            "failure_fingerprint": self.fingerprint,
            "unknown_typed": self.unknown_typed,
        }


def classify_provider_failure(
    error: object | None,
    *,
    project_root: Path | None = None,
    retry_count: int = 0,
    retry_limit: int | None = None,
) -> ProviderFailure:
    raw_message = str(getattr(error, "message", "") or "")
    safe_message = (
        redacted_summary(
            raw_message,
            project_root=project_root,
            max_chars=300,
        )
        if raw_message.strip()
        else None
    )
    parsed_retry = _RETRY.search(raw_message)
    if parsed_retry is not None:
        retry_count = max(retry_count, int(parsed_retry.group(1)))
        retry_limit = max(retry_limit or 0, int(parsed_retry.group(2)))
    typed_code, http_status, unknown_typed = _typed_error(error)
    code = _TYPED_CODES.get(typed_code or "", "provider_failed")
    fingerprint_payload = {
        "code": code,
        "typed_code": typed_code,
        "http_status": http_status,
    }
    return ProviderFailure(
        code=code,
        typed_code=typed_code,
        http_status=http_status,
        retry_count=retry_count,
        retry_limit=retry_limit,
        safe_message=safe_message,
        fingerprint=sha256_text(canonical_json(fingerprint_payload)),
        unknown_typed=unknown_typed,
    )


def _typed_error(error: object | None) -> tuple[str | None, int | None, bool]:
    info = getattr(error, "codex_error_info", None)
    root = getattr(info, "root", None)
    if root is None:
        return None, None, False
    value = getattr(root, "value", None)
    if isinstance(value, str):
        return value, None, value not in _TYPED_CODES
    dumped = _model_dump(root)
    if len(dumped) != 1:
        return None, None, True
    typed_code, details = next(iter(dumped.items()))
    http_status = None
    if isinstance(details, dict):
        raw_status = details.get("httpStatusCode", details.get("http_status_code"))
        if isinstance(raw_status, int) and not isinstance(raw_status, bool):
            http_status = raw_status
    return str(typed_code), http_status, str(typed_code) not in _TYPED_CODES


def _model_dump(value: object) -> dict[str, Any]:
    dump = getattr(value, "model_dump", None)
    if not callable(dump):
        return {}
    result = dump(mode="json", by_alias=True, exclude_none=True)
    return result if isinstance(result, dict) else {}
