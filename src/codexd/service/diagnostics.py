from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import secrets
import tempfile
import zipfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from codexd import __version__
from codexd.config import AppConfig
from codexd.domain.ids import sha256_text, utc_now_ms
from codexd.errors import CodexDError, ConfigurationError
from codexd.observability.health import read_health
from codexd.runtime.codex_sdk import _capability_manifest
from codexd.security.redaction import redact_text, redact_value
from codexd.service.manager import service_logs, service_status
from codexd.storage.repository import Repository
from codexd.storage.sqlite import SQLiteStore


def export_diagnostics(
    config: AppConfig,
    *,
    output: Path | None = None,
    include_content: bool = False,
) -> Path:
    config.paths.diagnostics.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = (
        output.expanduser().resolve()
        if output is not None
        else config.paths.diagnostics
        / (
            f"codexd-diagnostics-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-"
            f"{secrets.token_hex(4)}.zip"
        )
    )
    if destination.exists():
        raise ConfigurationError(f"diagnostic bundle already exists: {destination}")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="bundle-",
        dir=config.paths.diagnostics,
    ) as temporary_name:
        root = Path(temporary_name)
        _write_json(
            root / "health.json",
            redact_value(read_health(config.paths.health) or {}),
        )
        _write_json(root / "versions.json", _versions())
        _write_json(
            root / "capabilities.json",
            redact_value(_capabilities()),
        )
        (root / "config.redacted.toml").write_text(
            _redacted_config(config),
            encoding="utf-8",
        )
        database_details = _database_details(
            config,
            root,
            include_content=include_content,
        )
        _write_json(
            root / "incidents.json",
            redact_value(database_details["incidents"]),
        )
        (root / "logs.tail.jsonl").write_text(
            "".join(
                redact_text(line)
                for line in service_logs(config, lines=500)
            ),
            encoding="utf-8",
        )
        _write_json(
            root / "service-status.txt",
            redact_value(asdict(service_status(config))),
        )
        manifest = {
            "schema_version": 1,
            "created_at": utc_now_ms(),
            "codexd_version": __version__,
            "include_content": include_content,
            "files": _file_manifest(root),
        }
        _write_json(root / "manifest.json", manifest)
        temporary_zip = destination.with_name(
            f".{destination.name}.{secrets.token_hex(6)}.tmp"
        )
        try:
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(temporary_zip, flags, 0o600)
            with os.fdopen(descriptor, "w+b") as output_file, zipfile.ZipFile(
                output_file,
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=6,
            ) as archive:
                for path in sorted(root.iterdir()):
                    archive.write(path, arcname=path.name)
            os.replace(temporary_zip, destination)
        finally:
            temporary_zip.unlink(missing_ok=True)
    if os.name != "nt":
        destination.chmod(0o600)
    _audit_export(config, destination, include_content=include_content)
    return destination


def _database_details(
    config: AppConfig,
    root: Path,
    *,
    include_content: bool,
) -> dict[str, Any]:
    if not config.paths.database.exists():
        (root / "database-schema.txt").write_text(
            "database missing\n",
            encoding="utf-8",
        )
        (root / "database-integrity.txt").write_text(
            "database missing\n",
            encoding="utf-8",
        )
        return {"incidents": []}
    with SQLiteStore(config.paths.database) as store:
        schema = "\n\n".join(
            str(row["sql"])
            for row in store.query_all(
                """
                SELECT sql FROM sqlite_master
                WHERE sql IS NOT NULL
                ORDER BY type, name
                """
            )
        )
        (root / "database-schema.txt").write_text(
            redact_text(schema),
            encoding="utf-8",
        )
        integrity = store.integrity_check()
        foreign_keys = store.foreign_key_check()
        (root / "database-integrity.txt").write_text(
            f"integrity={integrity}\nforeign_key_violations={len(foreign_keys)}\n",
            encoding="utf-8",
        )
        repository = Repository(store)
        incidents = list(repository.unresolved_incidents(limit=50))
        if include_content:
            content = {
                "messages": [
                    {
                        "turn_id": str(row["turn_id"]),
                        "plain_text": str(row["plain_text"]),
                    }
                    for row in store.query_all(
                        """
                        SELECT turn_id, plain_text
                        FROM message_projections
                        ORDER BY rowid DESC
                        LIMIT 100
                        """
                    )
                ],
                "events": [
                    {
                        "sequence": int(row["sequence"]),
                        "kind": str(row["kind"]),
                        "payload": json.loads(str(row["payload_json"])),
                    }
                    for row in store.query_all(
                        """
                        SELECT sequence, kind, payload_json
                        FROM events
                        ORDER BY sequence DESC
                        LIMIT 100
                        """
                    )
                ],
            }
            _write_json(
                root / "content.json",
                redact_value(content),
            )
    return {"incidents": incidents}


def _audit_export(
    config: AppConfig,
    destination: Path,
    *,
    include_content: bool,
) -> None:
    if not config.paths.database.exists():
        return
    with SQLiteStore(config.paths.database) as store:
        Repository(store).record_audit(
            actor_kind="local_cli",
            action="diagnostics_export",
            payload={
                "include_content": include_content,
                "destination_hash": sha256_text(str(destination)),
            },
        )


def _versions() -> dict[str, str]:
    versions = {"codexd": __version__}
    for package in ("openai-codex", "openai-codex-cli-bin", "discord.py"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "not-installed"
    return versions


def _capabilities() -> dict[str, object]:
    try:
        return _capability_manifest().as_dict()
    except (CodexDError, ImportError, RuntimeError) as exc:
        return {
            "state": "unavailable",
            "error": redact_text(f"{type(exc).__name__}: {exc}")[:512],
        }


def _redacted_config(config: AppConfig) -> str:
    return "\n".join(
        (
            "[discord]",
            f"guild_configured = {str(config.discord.guild_id is not None).lower()}",
            f"owner_configured = {str(config.discord.owner_user_id is not None).lower()}",
            f"allowed_user_count = {len(config.discord.allowed_user_ids)}",
            f"max_attachment_count = {config.discord.max_attachment_count}",
            f"file_max_bytes = {config.discord.file_max_bytes}",
            f"message_max_bytes = {config.discord.message_max_bytes}",
            "",
            "[runtime]",
            f'topology = "{config.runtime.topology}"',
            "",
            "[security]",
            f'new_conversation_profile = "{config.security.default_sandbox_profile}"',
            'project_path_scope = "unrestricted"',
            "",
            "[retention]",
            f"events_days = {config.retention.events_days}",
            f"input_attachments_days = {config.retention.input_attachments_days}",
            f"render_attachments_days = {config.retention.render_attachments_days}",
            f"logs_days = {config.retention.logs_days}",
            "",
        )
    )


def _file_manifest(root: Path) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for path in sorted(root.iterdir()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        result.append(
            {
                "name": path.name,
                "sha256": digest,
                "size_bytes": path.stat().st_size,
            }
        )
    return result


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
