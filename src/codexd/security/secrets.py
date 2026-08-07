from __future__ import annotations

import base64
import secrets

import keyring
from keyring.errors import KeyringError

from codexd.errors import SecurityError

_SERVICE = "codexD"
_DISCORD_TOKEN = "discord-bot-token"
_PROJECTION_KEY = "projection-hmac-key"
_COMPONENT_KEY = "component-signing-key"


class SecretStore:
    def discord_token(self) -> str | None:
        return self._get(_DISCORD_TOKEN)

    def set_discord_token(self, value: str) -> None:
        token = value.strip()
        if not token or any(character.isspace() for character in token):
            raise SecurityError("Discord token is empty or contains whitespace")
        self._set(_DISCORD_TOKEN, token)

    def clear_discord_token(self) -> None:
        self._delete(_DISCORD_TOKEN)

    def projection_key(self, *, allow_create: bool = True) -> bytes:
        return self._durable_key(_PROJECTION_KEY, allow_create=allow_create)

    def component_key(self, *, allow_create: bool = True) -> bytes:
        return self._durable_key(_COMPONENT_KEY, allow_create=allow_create)

    def _durable_key(self, name: str, *, allow_create: bool) -> bytes:
        encoded = self._get(name)
        if encoded:
            try:
                value = base64.urlsafe_b64decode(encoded.encode())
            except ValueError as exc:
                raise SecurityError(f"stored {name} is invalid") from exc
            if len(value) < 32:
                raise SecurityError(f"stored {name} is too short")
            return value
        if not allow_create:
            raise SecurityError(
                f"durable {name} is missing for an existing codexD database"
            )
        value = secrets.token_bytes(32)
        self._set(name, base64.urlsafe_b64encode(value).decode())
        return value

    @staticmethod
    def _get(name: str) -> str | None:
        try:
            return keyring.get_password(_SERVICE, name)
        except KeyringError as exc:
            raise SecurityError(f"OS secret store is unavailable: {exc}") from exc

    @staticmethod
    def _set(name: str, value: str) -> None:
        try:
            keyring.set_password(_SERVICE, name, value)
        except KeyringError as exc:
            raise SecurityError(f"OS secret store write failed: {exc}") from exc

    @staticmethod
    def _delete(name: str) -> None:
        try:
            keyring.delete_password(_SERVICE, name)
        except keyring.errors.PasswordDeleteError:
            return
        except KeyringError as exc:
            raise SecurityError(f"OS secret store delete failed: {exc}") from exc
