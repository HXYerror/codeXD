from __future__ import annotations

import math
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from codexd.errors import ConfigurationError, SecurityError
from codexd.paths import AppPaths, default_paths

_WEB_SEARCH_MODES = frozenset(
    {"cached", "indexed", "live", "disabled", "provider_default_uncontrolled"}
)
_MISFIRE_POLICIES = frozenset({"skip", "latest", "all"})
_TOP_LEVEL_KEYS = frozenset(
    {"discord", "runtime", "codex", "schedule", "security", "rendering", "retention"}
)


@dataclass(frozen=True)
class DiscordConfig:
    guild_id: int | None = None
    owner_user_id: int | None = None
    allowed_user_ids: frozenset[int] = field(default_factory=frozenset)
    command_scope: str = "guild"
    max_attachment_count: int = 10
    file_max_bytes: int = 25 * 1024 * 1024
    message_max_bytes: int = 50 * 1024 * 1024


@dataclass(frozen=True)
class RuntimeConfig:
    sdk_version_policy: str = "compatible_range"
    codex_bin: Path | None = None
    nonsecret_env_allowlist: tuple[str, ...] = ()
    topology: str = "project_scoped"
    shutdown_drain_seconds: int = 30


@dataclass(frozen=True)
class CodexSettings:
    web_search_mode: str = "cached"


@dataclass(frozen=True)
class ScheduleConfig:
    default_timezone: str = "UTC"
    default_misfire_policy: str = "latest"
    poll_seconds: float = 1.0


@dataclass(frozen=True)
class SecurityConfig:
    # Retained for config/API compatibility; project paths are unrestricted.
    allowed_roots: tuple[Path, ...] = ()
    default_sandbox_profile: str = "full_access"


@dataclass(frozen=True)
class RenderingConfig:
    stream_update_ms: int = 1000
    table_max_columns: int = 20
    table_max_rows_png: int = 200
    table_memory_mib: int = 128
    image_max_bytes: int = 25 * 1024 * 1024
    image_max_pixels: int = 40_000_000


@dataclass(frozen=True)
class RetentionConfig:
    events_days: int = 90
    input_attachments_days: int = 7
    render_attachments_days: int = 30
    logs_days: int = 14


@dataclass(frozen=True)
class AppConfig:
    paths: AppPaths
    discord: DiscordConfig = field(default_factory=DiscordConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    codex: CodexSettings = field(default_factory=CodexSettings)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    rendering: RenderingConfig = field(default_factory=RenderingConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)

    @property
    def daemon_ready_for_discord(self) -> bool:
        return (
            self.discord.guild_id is not None
            and self.discord.owner_user_id is not None
            and bool(self.discord.allowed_user_ids)
        )


def load_config(
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> AppConfig:
    env = os.environ if environment is None else environment
    paths = default_paths(env)
    config_path = path or Path(env.get("CODEXD_CONFIG", paths.data_dir / "config.toml"))
    raw: dict[str, Any] = {}
    if config_path.is_symlink():
        raise ConfigurationError(f"config path must not be a symlink: {config_path}")
    if config_path.exists():
        try:
            with config_path.open("rb") as stream:
                parsed = tomllib.load(stream)
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise ConfigurationError(f"cannot read config {config_path}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ConfigurationError("config root must be a TOML table")
        raw = parsed
    unknown = sorted(set(raw) - _TOP_LEVEL_KEYS)
    if unknown:
        raise ConfigurationError(f"unknown config sections: {unknown}")

    discord = _discord_config(_table(raw, "discord"), env)
    runtime = _runtime_config(_table(raw, "runtime"))
    codex = _codex_config(_table(raw, "codex"))
    schedule = _schedule_config(_table(raw, "schedule"))
    security = _security_config(_table(raw, "security"))
    rendering = _rendering_config(_table(raw, "rendering"))
    retention = _retention_config(_table(raw, "retention"))
    return AppConfig(
        paths=paths,
        discord=discord,
        runtime=runtime,
        codex=codex,
        schedule=schedule,
        security=security,
        rendering=rendering,
        retention=retention,
    )


def resolve_project_path(
    value: str,
    allowed_roots: tuple[Path, ...] = (),
    *,
    relative_to: Path | None = None,
) -> Path:
    if not value or "\x00" in value:
        raise SecurityError("project path is empty or contains NUL")
    # Retain the argument for configuration compatibility. Project binding runs
    # with full host access, so configured roots no longer restrict valid paths.
    del allowed_roots
    if os.name == "nt":
        windows_value = value.replace("/", "\\")
        if windows_value.startswith(("\\\\", "\\\\?\\", "\\\\.\\")):
            raise SecurityError("UNC and Windows device paths are not allowed")
    candidate = Path(value).expanduser()
    if os.name == "nt" and candidate.drive and not candidate.is_absolute():
        raise SecurityError("drive-relative project paths are not allowed")
    if not candidate.is_absolute():
        candidate = (relative_to or Path.home()) / candidate
    resolved = _resolve_existing_project_path(candidate)
    if not resolved.is_dir():
        raise SecurityError("project path must be a directory")
    access_mode = os.R_OK | (os.X_OK if os.name != "nt" else 0)
    if not os.access(resolved, access_mode):
        raise SecurityError("project path is not readable by the service user")
    return resolved


def _resolve_existing_project_path(candidate: Path) -> Path:
    try:
        return candidate.resolve(strict=True)
    except OSError as exc:
        raise SecurityError(f"project path cannot be resolved: {exc}") from exc


def _discord_config(raw: dict[str, Any], env: Mapping[str, str]) -> DiscordConfig:
    _reject_unknown(
        raw,
        {
            "guild_id",
            "owner_user_id",
            "allowed_user_ids",
            "command_scope",
            "max_attachment_count",
            "file_max_bytes",
            "message_max_bytes",
        },
        "discord",
    )
    guild_raw = env.get("CODEXD_DISCORD_GUILD_ID", raw.get("guild_id"))
    owner_raw = env.get(
        "CODEXD_DISCORD_OWNER_USER_ID",
        raw.get("owner_user_id"),
    )
    users_raw: object = raw.get("allowed_user_ids", [])
    if "CODEXD_ALLOWED_USER_IDS" in env:
        users_raw = [
            part.strip()
            for part in env["CODEXD_ALLOWED_USER_IDS"].split(",")
            if part.strip()
        ]
    guild_id = _snowflake(guild_raw, "discord.guild_id") if guild_raw is not None else None
    owner_user_id = (
        _snowflake(owner_raw, "discord.owner_user_id")
        if owner_raw is not None
        else None
    )
    if not isinstance(users_raw, list):
        raise ConfigurationError("discord.allowed_user_ids must be an array")
    users = frozenset(_snowflake(value, "discord.allowed_user_ids") for value in users_raw)
    if users and owner_user_id is None:
        raise ConfigurationError(
            "discord.owner_user_id is required when allowed_user_ids is configured"
        )
    if owner_user_id is not None and owner_user_id not in users:
        raise ConfigurationError(
            "discord.owner_user_id must also appear in discord.allowed_user_ids"
        )
    scope = _string(raw.get("command_scope", "guild"), "discord.command_scope")
    if scope != "guild":
        raise ConfigurationError("discord.command_scope must be 'guild'")
    max_attachment_count = _positive_int(
        raw.get("max_attachment_count", 10),
        "discord.max_attachment_count",
    )
    if max_attachment_count > 10:
        raise ConfigurationError("discord.max_attachment_count may not exceed 10")
    file_max_bytes = _positive_int(
        raw.get("file_max_bytes", 25 * 1024 * 1024),
        "discord.file_max_bytes",
    )
    message_max_bytes = _positive_int(
        raw.get("message_max_bytes", 50 * 1024 * 1024),
        "discord.message_max_bytes",
    )
    if file_max_bytes > message_max_bytes:
        raise ConfigurationError(
            "discord.file_max_bytes may not exceed discord.message_max_bytes"
        )
    return DiscordConfig(
        guild_id=guild_id,
        owner_user_id=owner_user_id,
        allowed_user_ids=users,
        command_scope=scope,
        max_attachment_count=max_attachment_count,
        file_max_bytes=file_max_bytes,
        message_max_bytes=message_max_bytes,
    )


def _runtime_config(raw: dict[str, Any]) -> RuntimeConfig:
    _reject_unknown(
        raw,
        {
            "sdk_version_policy",
            "codex_bin",
            "nonsecret_env_allowlist",
            "topology",
            "shutdown_drain_seconds",
        },
        "runtime",
    )
    allowlist = raw.get("nonsecret_env_allowlist", [])
    if not isinstance(allowlist, list) or not all(
        isinstance(item, str) for item in allowlist
    ):
        raise ConfigurationError("runtime.nonsecret_env_allowlist must be an array of strings")
    policy = _string(
        raw.get("sdk_version_policy", "compatible_range"),
        "runtime.sdk_version_policy",
    )
    if policy != "compatible_range":
        raise ConfigurationError("runtime.sdk_version_policy must be 'compatible_range'")
    topology = _string(raw.get("topology", "project_scoped"), "runtime.topology")
    if topology not in {"project_scoped", "shared"}:
        raise ConfigurationError("runtime.topology must be project_scoped or shared")
    codex_bin_raw = raw.get("codex_bin")
    codex_bin = (
        _executable_path(codex_bin_raw, "runtime.codex_bin")
        if codex_bin_raw is not None
        else None
    )
    return RuntimeConfig(
        sdk_version_policy=policy,
        codex_bin=codex_bin,
        nonsecret_env_allowlist=tuple(allowlist),
        topology=topology,
        shutdown_drain_seconds=_positive_int(
            raw.get("shutdown_drain_seconds", 30), "runtime.shutdown_drain_seconds"
        ),
    )


def _codex_config(raw: dict[str, Any]) -> CodexSettings:
    _reject_unknown(raw, {"web_search_mode"}, "codex")
    mode = _string(raw.get("web_search_mode", "cached"), "codex.web_search_mode")
    if mode not in _WEB_SEARCH_MODES:
        raise ConfigurationError(f"invalid codex.web_search_mode: {mode}")
    return CodexSettings(web_search_mode=mode)


def _schedule_config(raw: dict[str, Any]) -> ScheduleConfig:
    _reject_unknown(raw, {"default_timezone", "default_misfire_policy", "poll_seconds"}, "schedule")
    policy = _string(
        raw.get("default_misfire_policy", "latest"), "schedule.default_misfire_policy"
    )
    if policy not in _MISFIRE_POLICIES:
        raise ConfigurationError(f"invalid schedule.default_misfire_policy: {policy}")
    poll = raw.get("poll_seconds", 1.0)
    if not isinstance(poll, (int, float)) or isinstance(poll, bool) or poll <= 0:
        raise ConfigurationError("schedule.poll_seconds must be finite and positive")
    try:
        finite_poll = math.isfinite(poll)
    except OverflowError as exc:
        raise ConfigurationError(
            "schedule.poll_seconds must be finite and positive"
        ) from exc
    if not finite_poll:
        raise ConfigurationError("schedule.poll_seconds must be finite and positive")
    return ScheduleConfig(
        default_timezone=_string(raw.get("default_timezone", "UTC"), "schedule.default_timezone"),
        default_misfire_policy=policy,
        poll_seconds=float(poll),
    )


def _security_config(raw: dict[str, Any]) -> SecurityConfig:
    _reject_unknown(raw, {"allowed_roots", "default_sandbox_profile"}, "security")
    roots_raw = raw.get("allowed_roots", [])
    if not isinstance(roots_raw, list) or not all(isinstance(item, str) for item in roots_raw):
        raise ConfigurationError("security.allowed_roots must be an array of strings")
    profile = _string(
        raw.get("default_sandbox_profile", "full_access"),
        "security.default_sandbox_profile",
    )
    if profile != "full_access":
        raise ConfigurationError(
            "security.default_sandbox_profile is fixed to 'full_access'"
        )
    return SecurityConfig(default_sandbox_profile=profile)


def _rendering_config(raw: dict[str, Any]) -> RenderingConfig:
    names = {
        "stream_update_ms",
        "table_max_columns",
        "table_max_rows_png",
        "table_memory_mib",
        "image_max_bytes",
        "image_max_pixels",
    }
    _reject_unknown(raw, names, "rendering")
    return RenderingConfig(
        stream_update_ms=_positive_int(
            raw.get("stream_update_ms", 1000), "rendering.stream_update_ms"
        ),
        table_max_columns=_positive_int(
            raw.get("table_max_columns", 20), "rendering.table_max_columns"
        ),
        table_max_rows_png=_positive_int(
            raw.get("table_max_rows_png", 200), "rendering.table_max_rows_png"
        ),
        table_memory_mib=_positive_int(
            raw.get("table_memory_mib", 128), "rendering.table_memory_mib"
        ),
        image_max_bytes=_positive_int(
            raw.get("image_max_bytes", 25 * 1024 * 1024), "rendering.image_max_bytes"
        ),
        image_max_pixels=_positive_int(
            raw.get("image_max_pixels", 40_000_000), "rendering.image_max_pixels"
        ),
    )


def _retention_config(raw: dict[str, Any]) -> RetentionConfig:
    names = {"events_days", "input_attachments_days", "render_attachments_days", "logs_days"}
    _reject_unknown(raw, names, "retention")
    return RetentionConfig(
        events_days=_positive_int(raw.get("events_days", 90), "retention.events_days"),
        input_attachments_days=_positive_int(
            raw.get("input_attachments_days", 7), "retention.input_attachments_days"
        ),
        render_attachments_days=_positive_int(
            raw.get("render_attachments_days", 30), "retention.render_attachments_days"
        ),
        logs_days=_positive_int(raw.get("logs_days", 14), "retention.logs_days"),
    )


def _table(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw.get(name, {})
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be a TOML table")
    return value


def _reject_unknown(raw: dict[str, Any], allowed: set[str], section: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigurationError(f"unknown {section} config keys: {unknown}")


def _snowflake(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str, bytes, bytearray)):
        raise ConfigurationError(f"{name} must be a Discord snowflake")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be a Discord snowflake") from exc
    if result <= 0 or result >= 1 << 64:
        raise ConfigurationError(f"{name} must be a positive 64-bit snowflake")
    return result


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive integer")
    return value


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{name} must be a non-empty string")
    return value


def _executable_path(value: object, name: str) -> Path:
    text = _string(value, name)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        raise ConfigurationError(f"{name} must be an absolute path")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ConfigurationError(f"{name} cannot be resolved: {candidate}") from exc
    if not resolved.is_file():
        raise ConfigurationError(f"{name} must reference a regular file")
    if os.name != "nt" and not os.access(resolved, os.X_OK):
        raise ConfigurationError(f"{name} must be executable")
    return resolved
