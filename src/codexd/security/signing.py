from __future__ import annotations

import base64
import hashlib
import hmac
from dataclasses import dataclass

from codexd.errors import SecurityError


@dataclass(frozen=True)
class TaskCardAction:
    view_id: str
    revision: int
    action: str
    nonce: str


@dataclass(frozen=True)
class ScheduleDraftAction:
    draft_id: str
    action: str
    nonce: str


@dataclass(frozen=True)
class ModalAction:
    intent_id: str
    kind: str
    expires_at: int
    nonce: str


class ComponentSigner:
    def __init__(self, key: bytes) -> None:
        if len(key) < 32:
            raise ValueError("component signing key must contain at least 32 bytes")
        self._key = key

    def task_card_id(
        self,
        *,
        view_id: str,
        revision: int,
        action: str,
        nonce: str,
    ) -> str:
        if action not in {"expand", "collapse"}:
            raise ValueError("invalid task-card action")
        if not nonce or ":" in nonce:
            raise ValueError("invalid task-card nonce")
        body = f"tc:v1:{view_id}:{revision}:{action}:{nonce}"
        signature = self._signature(body)
        return f"{body}:{signature}"

    def verify_task_card_id(self, value: str) -> TaskCardAction:
        try:
            (
                prefix,
                version,
                view_id,
                revision_raw,
                action,
                nonce,
                signature,
            ) = value.split(":")
        except ValueError as exc:
            raise SecurityError("invalid task-card component ID") from exc
        if prefix != "tc" or version != "v1" or action not in {"expand", "collapse"}:
            raise SecurityError("invalid task-card component ID")
        body = f"{prefix}:{version}:{view_id}:{revision_raw}:{action}:{nonce}"
        if not hmac.compare_digest(signature, self._signature(body)):
            raise SecurityError("invalid task-card component signature")
        try:
            revision = int(revision_raw)
        except ValueError as exc:
            raise SecurityError("invalid task-card component revision") from exc
        return TaskCardAction(view_id, revision, action, nonce)

    def schedule_draft_id(
        self,
        *,
        draft_id: str,
        action: str,
        nonce: str,
    ) -> str:
        if action not in {"confirm", "cancel"}:
            raise ValueError("invalid Schedule draft action")
        if not nonce or ":" in nonce:
            raise ValueError("invalid Schedule draft nonce")
        body = f"sd:v1:{draft_id}:{action}:{nonce}"
        return f"{body}:{self._signature(body)}"

    def verify_schedule_draft_id(self, value: str) -> ScheduleDraftAction:
        try:
            prefix, version, draft_id, action, nonce, signature = value.split(":")
        except ValueError as exc:
            raise SecurityError("invalid Schedule draft component ID") from exc
        if prefix != "sd" or version != "v1" or action not in {"confirm", "cancel"}:
            raise SecurityError("invalid Schedule draft component ID")
        body = f"{prefix}:{version}:{draft_id}:{action}:{nonce}"
        if not hmac.compare_digest(signature, self._signature(body)):
            raise SecurityError("invalid Schedule draft component signature")
        return ScheduleDraftAction(draft_id, action, nonce)

    def modal_id(
        self,
        *,
        intent_id: str,
        kind: str,
        expires_at: int,
        nonce: str,
    ) -> str:
        kind_code = {
            "schedule_create": "sc",
            "schedule_update": "su",
            "steer": "st",
            "side_query": "bt",
        }.get(kind)
        if kind_code is None:
            raise ValueError("invalid modal intent kind")
        if not nonce or ":" in nonce:
            raise ValueError("invalid modal intent nonce")
        expires = _base36(expires_at)
        body = f"mi:v1:{intent_id}:{kind_code}:{expires}:{nonce}"
        value = f"{body}:{self._signature(body)}"
        if len(value) > 100:
            raise ValueError("modal custom ID exceeds Discord's limit")
        return value

    def verify_modal_id(self, value: str) -> ModalAction:
        try:
            (
                prefix,
                version,
                intent_id,
                kind_code,
                expires_raw,
                nonce,
                signature,
            ) = value.split(":")
        except ValueError as exc:
            raise SecurityError("invalid modal custom ID") from exc
        kind = {
            "sc": "schedule_create",
            "su": "schedule_update",
            "st": "steer",
            "bt": "side_query",
        }.get(kind_code)
        if prefix != "mi" or version != "v1" or kind is None:
            raise SecurityError("invalid modal custom ID")
        body = (
            f"{prefix}:{version}:{intent_id}:{kind_code}:{expires_raw}:{nonce}"
        )
        if not hmac.compare_digest(signature, self._signature(body)):
            raise SecurityError("invalid modal custom ID signature")
        try:
            expires_at = int(expires_raw, 36)
        except ValueError as exc:
            raise SecurityError("invalid modal custom ID expiry") from exc
        return ModalAction(intent_id, kind, expires_at, nonce)

    def _signature(self, value: str) -> str:
        digest = hmac.new(self._key, value.encode(), hashlib.sha256).digest()[:9]
        return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _base36(value: int) -> str:
    if value < 0:
        raise ValueError("base36 value must be non-negative")
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value == 0:
        return "0"
    digits: list[str] = []
    while value:
        value, remainder = divmod(value, 36)
        digits.append(alphabet[remainder])
    return "".join(reversed(digits))
