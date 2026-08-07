from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from codexd.errors import SecurityError

_SAFE_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_SECRET_NAME = re.compile(
    r"(?:TOKEN|SECRET|PASSWORD|PASSWD|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY|CREDENTIAL)",
    re.IGNORECASE,
)
_BASE_ENV = frozenset(
    {
        "CODEX_HOME",
        "COMSPEC",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LOCALAPPDATA",
        "LOGNAME",
        "PATH",
        "PATHEXT",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USER",
        "USERPROFILE",
        "WINDIR",
    }
)
_PROXY_ENV = frozenset(
    {
        "ALL_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "NO_PROXY",
        "SSL_CERT_DIR",
        "SSL_CERT_FILE",
    }
)


@dataclass(frozen=True)
class BootstrapResult:
    discord_token: str | None
    child_environment: dict[str, str]
    environment_hash: str


def prepare_bootstrap(
    source: Mapping[str, str],
    *,
    extra_nonsecret_names: Sequence[str] = (),
    allow_proxy_environment: bool = True,
) -> BootstrapResult:
    names = set(_BASE_ENV)
    if allow_proxy_environment:
        names.update(_PROXY_ENV)

    for name in extra_nonsecret_names:
        if not _SAFE_NAME.fullmatch(name) or _SECRET_NAME.search(name):
            raise SecurityError(f"unsafe non-secret environment name: {name!r}")
        names.add(name)

    child: dict[str, str] = {}
    for name in sorted(names):
        value = source.get(name)
        if value is None:
            continue
        if name in _PROXY_ENV:
            _validate_proxy_or_certificate(name, value)
        if "\x00" in value:
            raise SecurityError(f"environment value for {name} contains NUL")
        child[name] = value

    token = source.get("CODEXD_DISCORD_TOKEN")
    digest = hashlib.sha256()
    for name, value in sorted(child.items()):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(value.encode())
        digest.update(b"\0")
    return BootstrapResult(
        discord_token=token,
        child_environment=child,
        environment_hash=digest.hexdigest(),
    )


def scrub_process_environment(
    prepared: BootstrapResult,
    *,
    environment: MutableMapping[str, str] | None = None,
) -> None:
    target = os.environ if environment is None else environment
    target.clear()
    target.update(prepared.child_environment)


def assert_environment_scrubbed(
    allowed: Mapping[str, str],
    *,
    environment: Mapping[str, str] | None = None,
) -> None:
    actual = os.environ if environment is None else environment
    unexpected = sorted(set(actual) - set(allowed))
    if unexpected:
        raise SecurityError(f"process environment was not scrubbed: {unexpected}")
    changed = sorted(
        name
        for name, value in actual.items()
        if name in allowed and allowed[name] != value
    )
    if changed:
        raise SecurityError(
            f"process environment values changed after bootstrap: {changed}"
        )
    if actual.get("CODEXD_DISCORD_TOKEN"):
        raise SecurityError("Discord token remained in process environment")


def load_service_environment(path: Path) -> dict[str, str]:
    try:
        if path.is_symlink() or not path.is_file():
            raise SecurityError("service environment must be a regular non-symlink file")
        stat = path.stat()
        if os.name != "nt" and (
            stat.st_uid != _current_uid() or stat.st_mode & 0o077
        ):
            raise SecurityError("service environment ownership or mode is unsafe")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except SecurityError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise SecurityError(f"cannot read protected service environment: {exc}") from exc
    if not isinstance(payload, dict):
        raise SecurityError("service environment payload must be an object")
    result: dict[str, str] = {}
    for name, value in payload.items():
        if (
            not isinstance(name, str)
            or not _SAFE_NAME.fullmatch(name)
            or _SECRET_NAME.search(name)
            or not isinstance(value, str)
            or "\x00" in value
        ):
            raise SecurityError("service environment contains an unsafe entry")
        result[name] = value
    return result


def _current_uid() -> int:
    return int(cast(Any, os).getuid())


def _validate_proxy_or_certificate(name: str, value: str) -> None:
    if name.endswith("_PROXY") and name != "NO_PROXY":
        parsed = urlsplit(value)
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise SecurityError(f"{name} may not contain credentials, query, or fragment")
    if name in {"SSL_CERT_DIR", "SSL_CERT_FILE"} and not value:
        raise SecurityError(f"{name} may not be empty")


def requires_windows_job_object() -> bool:
    return sys.platform == "win32"
