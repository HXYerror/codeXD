from __future__ import annotations

import json
import platform
import sys

from codexd.bootstrap import assert_environment_scrubbed
from codexd.config import AppConfig
from codexd.runtime.codex_sdk import _capability_manifest, _verify_public_contract
from codexd.security.secrets import SecretStore
from codexd.storage.sqlite import SQLiteStore


def run_doctor(config: AppConfig) -> int:
    checks: dict[str, dict[str, str]] = {}
    checks["python"] = {
        "state": "ok" if sys.version_info >= (3, 12) else "failed",
        "value": platform.python_version(),
    }
    try:
        assert_environment_scrubbed(dict(sys_environ()))
        checks["environment"] = {"state": "ok", "value": "scrubbed"}
    except Exception as exc:
        checks["environment"] = {"state": "failed", "value": str(exc)}
    try:
        if not config.paths.database.exists():
            checks["database"] = {
                "state": "missing",
                "value": "not initialized; the daemon will create it",
            }
        else:
            with SQLiteStore(config.paths.database) as store:
                version = store.validate_schema()
                integrity = store.integrity_check()
                foreign_keys = store.foreign_key_check()
            state = "ok" if integrity == "ok" and not foreign_keys else "failed"
            checks["database"] = {
                "state": state,
                "value": (
                    f"schema={version}, integrity={integrity}, "
                    f"fk={len(foreign_keys)}"
                ),
            }
    except Exception as exc:
        checks["database"] = {"state": "failed", "value": str(exc)}
    try:
        _verify_public_contract()
        manifest = _capability_manifest()
        manifest.assert_required()
        checks["sdk"] = {
            "state": "ok",
            "value": (
                f"{manifest.sdk_version}/{manifest.runtime_version}, "
                f"manifest={manifest.digest[:12]}"
            ),
        }
    except Exception as exc:
        checks["sdk"] = {"state": "failed", "value": str(exc)}
    try:
        token = SecretStore().discord_token()
        checks["discord_secret"] = {
            "state": "ok" if token else "missing",
            "value": "configured" if token else "not configured",
        }
    except Exception as exc:
        checks["discord_secret"] = {"state": "failed", "value": str(exc)}
    checks["discord_config"] = {
        "state": "ok" if config.daemon_ready_for_discord else "missing",
        "value": (
            "configured"
            if config.daemon_ready_for_discord
            else "guild/owner/allowed user missing"
        ),
    }
    print(json.dumps(checks, ensure_ascii=False, indent=2, sort_keys=True))
    return 1 if any(check["state"] == "failed" for check in checks.values()) else 0


def sys_environ() -> tuple[tuple[str, str], ...]:
    import os

    return tuple(os.environ.items())
