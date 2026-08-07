from __future__ import annotations


class CodexDError(Exception):
    """Base class for stable, user-facing codexD failures."""

    code = "codexd_error"


class ConfigurationError(CodexDError):
    code = "configuration_error"


class SecurityError(CodexDError):
    code = "security_error"


class StorageError(CodexDError):
    code = "storage_error"


class InvariantError(CodexDError):
    code = "invariant_error"


class ConflictError(CodexDError):
    code = "conflict"


class NotFoundError(CodexDError):
    code = "not_found"

