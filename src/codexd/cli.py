from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import webbrowser
from collections.abc import Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from codexd import __version__
from codexd.bootstrap import (
    load_service_environment,
    prepare_bootstrap,
    scrub_process_environment,
)
from codexd.config import AppConfig, load_config
from codexd.errors import CodexDError, ConfigurationError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="codexd")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--config", type=Path, help="path to config.toml")
    parser.add_argument(
        "--service-environment",
        type=Path,
        help=argparse.SUPPRESS,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="validate configuration, storage, and SDK capabilities")
    subparsers.add_parser("daemon", help="run the Discord bridge")
    db = subparsers.add_parser("db", help="database operations")
    db_commands = db.add_subparsers(dest="db_command", required=True)
    db_commands.add_parser(
        "check", help="run SQLite integrity and migration checks"
    )
    backup = db_commands.add_parser("backup", help="checkpoint and back up SQLite")
    backup.add_argument("--output", type=Path, help="backup destination")
    compact = db_commands.add_parser(
        "compact",
        help="remove redundant event detail and physically shrink SQLite",
    )
    compact.add_argument("--yes", action="store_true", help="confirm offline compaction")
    compact.add_argument(
        "--no-backup",
        action="store_true",
        help="skip the default verified pre-compaction backup",
    )
    compact.add_argument("--backup-output", type=Path, help="backup destination")
    trim_codex_logs = db_commands.add_parser(
        "trim-codex-logs",
        help="remove verbose Codex feedback logs and shrink logs_2.sqlite",
    )
    trim_codex_logs.add_argument(
        "--yes",
        action="store_true",
        help="confirm offline Codex feedback-log compaction",
    )
    trim_codex_logs.add_argument(
        "--no-backup",
        action="store_true",
        help="skip the default verified pre-compaction backup",
    )
    trim_codex_logs.add_argument(
        "--backup-output",
        type=Path,
        help="backup destination",
    )
    auth = subparsers.add_parser("auth", help="manage local credentials")
    auth_services = auth.add_subparsers(dest="auth_service", required=True)
    discord = auth_services.add_parser("discord", help="manage the Discord bot token")
    discord_commands = discord.add_subparsers(dest="auth_action", required=True)
    discord_commands.add_parser("set", help="read and store a Discord token securely")
    discord_commands.add_parser("status", help="show whether a Discord token is configured")
    discord_commands.add_parser("clear", help="delete the stored Discord token")
    codex = auth_services.add_parser("codex", help="manage the native Codex account")
    codex_commands = codex.add_subparsers(dest="auth_action", required=True)
    codex_commands.add_parser("status", help="read the native Codex account state")
    codex_commands.add_parser("login-api-key", help="log in using a hidden API-key prompt")
    codex_commands.add_parser("login-chatgpt", help="log in using a browser")
    codex_commands.add_parser("login-device-code", help="log in using a device code")
    logout = codex_commands.add_parser("logout", help="log out of the native Codex account")
    logout.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    service = subparsers.add_parser("service", help="manage the user background service")
    service_commands = service.add_subparsers(dest="service_action", required=True)
    service_commands.add_parser("install", help="install and start the user service")
    service_commands.add_parser("start", help="start the installed user service")
    service_commands.add_parser("stop", help="stop the installed user service")
    service_commands.add_parser("restart", help="restart the installed user service")
    service_commands.add_parser("uninstall", help="stop and remove the user service")
    service_commands.add_parser("status", help="show service and heartbeat state")
    logs = service_commands.add_parser("logs", help="show recent structured service logs")
    logs.add_argument("--lines", type=int, default=100, help="number of lines (1-1000)")
    diagnostics = subparsers.add_parser("diagnostics", help="diagnostic bundle operations")
    diagnostics_commands = diagnostics.add_subparsers(
        dest="diagnostics_action",
        required=True,
    )
    export = diagnostics_commands.add_parser(
        "export",
        help="create a redacted diagnostic bundle",
    )
    export.add_argument("--output", type=Path, help="bundle output path")
    export.add_argument(
        "--include-content",
        action="store_true",
        help="write a marker confirming that content persistence is disabled",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    original_environment = dict(os.environ)
    try:
        if args.service_environment is not None:
            original_environment.update(
                load_service_environment(args.service_environment)
            )
        config = load_config(args.config, environment=original_environment)
        if args.config is None and original_environment.get("CODEXD_CONFIG"):
            args.config = Path(original_environment["CODEXD_CONFIG"])
        prepared = prepare_bootstrap(
            original_environment,
            extra_nonsecret_names=config.runtime.nonsecret_env_allowlist,
        )
        scrub_process_environment(prepared)
        config.paths.ensure()
        return _dispatch(
            args,
            config,
            prepared.discord_token,
            prepared.child_environment,
        )
    except CodexDError as exc:
        print(f"codexd: {exc.code}: {exc}", file=sys.stderr)
        return 2


def _dispatch(
    args: argparse.Namespace,
    config: AppConfig,
    discord_token: str | None,
    bootstrap_environment: dict[str, str],
) -> int:
    if args.command == "doctor":
        from codexd.service.doctor import run_doctor

        return run_doctor(
            config,
            expected_environment=bootstrap_environment,
            bootstrap_token_available=bool(discord_token),
        )
    if args.command == "db":
        from codexd.storage.sqlite import SQLiteStore

        if args.db_command == "trim-codex-logs":
            return _trim_codex_logs(config, args)
        if not config.paths.database.exists():
            raise ConfigurationError("database is not initialized")
        if args.db_command == "compact":
            return _compact_database(config, args)
        with SQLiteStore(config.paths.database) as store:
            store.validate_schema()
            if args.db_command == "backup":
                output = args.output or (
                    config.paths.backups
                    / f"codexd-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.sqlite3"
                )
                result_path = store.backup(output)
                print(result_path)
                return 0
            result = store.integrity_check()
            foreign_keys = store.foreign_key_check()
        print(result)
        return 0 if result == "ok" and not foreign_keys else 1
    if args.command == "daemon":
        from codexd.application.daemon import run_daemon

        return run_daemon(config, discord_token)
    if args.command == "auth":
        if args.auth_service == "discord":
            return _discord_auth(args.auth_action)
        return _codex_auth(
            config,
            args.auth_action,
            yes=bool(getattr(args, "yes", False)),
        )
    if args.command == "service":
        return _service(
            config,
            args.service_action,
            args.config,
            lines=int(getattr(args, "lines", 100)),
        )
    if args.command == "diagnostics":
        from codexd.service.diagnostics import export_diagnostics

        include_content = bool(args.include_content)
        if include_content:
            print(
                "WARNING: this bundle includes local message and event content.",
                file=sys.stderr,
            )
        print(
            export_diagnostics(
                config,
                output=args.output,
                include_content=include_content,
            )
        )
        return 0
    parser_error = f"unsupported command: {args.command}"
    raise AssertionError(parser_error)


def _compact_database(config: AppConfig, args: argparse.Namespace) -> int:
    if not bool(args.yes):
        raise ConfigurationError(
            "database compaction requires --yes and a stopped codexD service"
        )
    from codexd.service.locking import InstanceLock
    from codexd.storage.compaction import compact_database
    from codexd.storage.sqlite import SQLiteStore

    database = config.paths.database
    before_bytes = database.stat().st_size
    backup_path: Path | None = None
    with InstanceLock(config.paths.instance_lock), SQLiteStore(database) as store:
        print("codexd: checking database before compaction", file=sys.stderr, flush=True)
        store.migrate()
        store.validate_schema()
        if store.integrity_check() != "ok" or store.foreign_key_check():
            raise ConfigurationError("database checks failed before compaction")
        if not bool(args.no_backup):
            print("codexd: creating verified backup", file=sys.stderr, flush=True)
            backup_path = args.backup_output or (
                config.paths.backups
                / f"codexd-precompact-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.sqlite3"
            )
            backup_path = store.backup(backup_path)
        print("codexd: removing redundant durable detail", file=sys.stderr, flush=True)
        result = compact_database(
            store,
            progress=lambda stage: print(
                f"codexd: {stage}",
                file=sys.stderr,
                flush=True,
            ),
        )
        print("codexd: vacuuming database", file=sys.stderr, flush=True)
        store.vacuum()
        print("codexd: verifying compacted database", file=sys.stderr, flush=True)
        if store.integrity_check() != "ok" or store.foreign_key_check():
            raise ConfigurationError("database checks failed after compaction")
    after_bytes = database.stat().st_size
    print(
        json.dumps(
            {
                "database": str(database),
                "backup": str(backup_path) if backup_path is not None else None,
                "before_bytes": before_bytes,
                "after_bytes": after_bytes,
                "reclaimed_bytes": max(0, before_bytes - after_bytes),
                **result.as_dict(),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _trim_codex_logs(config: AppConfig, args: argparse.Namespace) -> int:
    if not bool(args.yes):
        raise ConfigurationError(
            "Codex feedback-log compaction requires --yes and all Codex apps stopped"
        )
    from codexd.service.locking import InstanceLock
    from codexd.storage.codex_feedback import (
        codex_feedback_log_path,
        compact_codex_feedback_logs,
    )

    path = codex_feedback_log_path(dict(os.environ), cwd=config.paths.data_dir)
    backup_path = None
    if not bool(args.no_backup):
        backup_path = args.backup_output or (
            config.paths.backups
            / f"codex-feedback-precompact-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.sqlite"
        )
    with InstanceLock(config.paths.instance_lock):
        print(
            "codexd: compacting Codex feedback log database",
            file=sys.stderr,
            flush=True,
        )
        result = compact_codex_feedback_logs(
            path,
            retention_days=config.retention.codex_logs_days,
            trace_hours=config.retention.codex_trace_hours,
            backup_path=backup_path,
        )
    print(
        json.dumps(
            {
                "backup": str(backup_path) if backup_path is not None else None,
                **result.as_dict(),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _discord_auth(action: str) -> int:
    from codexd.security.secrets import SecretStore

    store = SecretStore()
    if action == "set":
        token = getpass.getpass("Discord bot token: ")
        try:
            store.set_discord_token(token)
        finally:
            token = ""
        print("Discord token stored in the OS secret store.")
        return 0
    if action == "clear":
        store.clear_discord_token()
        print("Discord token removed.")
        return 0
    print("configured" if store.discord_token() else "not configured")
    return 0


def _codex_auth(config: AppConfig, action: str, *, yes: bool) -> int:
    from codexd.service.locking import InstanceLock

    if action == "status":
        projection = _running_daemon_auth_projection(config)
        if projection is not None:
            print(json.dumps(projection, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

    from openai_codex import Codex, CodexConfig

    config.paths.data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    client_config = CodexConfig(
        codex_bin=(
            str(config.runtime.codex_bin)
            if config.runtime.codex_bin is not None
            else None
        ),
        cwd=str(config.paths.data_dir),
        env=dict(os.environ),
        client_name="codexd",
        client_title="codexD",
        experimental_api=False,
    )
    with InstanceLock(config.paths.instance_lock), Codex(client_config) as codex:
        if action == "status":
            _print_account(codex.account(refresh_token=False))
            return 0
        if action == "login-api-key":
            api_key = getpass.getpass("OpenAI API key: ")
            try:
                if not api_key.strip():
                    raise ConfigurationError("API key may not be empty")
                codex.login_api_key(api_key.strip())
            finally:
                api_key = ""
            _print_account(codex.account(refresh_token=False))
            return 0
        if action == "login-chatgpt":
            browser_handle = codex.login_chatgpt()
            completed = False
            try:
                print(f"Open this URL to authenticate:\n{browser_handle.auth_url}")
                webbrowser.open(browser_handle.auth_url)
                browser_handle.wait()
                completed = True
            finally:
                if not completed:
                    with suppress(Exception):
                        browser_handle.cancel()
            _print_account(codex.account(refresh_token=False))
            return 0
        if action == "login-device-code":
            device_handle = codex.login_chatgpt_device_code()
            completed = False
            try:
                print(
                    f"Open {device_handle.verification_url} "
                    f"and enter code {device_handle.user_code}"
                )
                device_handle.wait()
                completed = True
            finally:
                if not completed:
                    with suppress(Exception):
                        device_handle.cancel()
            _print_account(codex.account(refresh_token=False))
            return 0
        if not yes:
            confirmation = input("Log out of Codex? [y/N] ").strip().lower()
            if confirmation not in {"y", "yes"}:
                print("Cancelled.")
                return 0
        codex.logout()
        print("Codex account logged out.")
        return 0


def _print_account(account: object) -> None:
    account_value = getattr(account, "account", None)
    root = getattr(account_value, "root", None)
    payload = {
        "requires_openai_auth": bool(
            getattr(account, "requires_openai_auth", True)
        ),
        "account_type": _safe_account_value(getattr(root, "type", None)),
        "plan_type": _safe_account_value(getattr(root, "plan_type", None)),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _safe_account_value(value: object) -> str | None:
    if value is None:
        return None
    raw = getattr(value, "value", value)
    if isinstance(raw, str):
        return raw[:128]
    return None


def _running_daemon_auth_projection(config: AppConfig) -> dict[str, object] | None:
    from codexd.observability.health import heartbeat_state, read_health
    from codexd.service.process import process_matches

    health = read_health(config.paths.health)
    if health is None:
        return None
    pid = health.get("pid")
    process_start_token = health.get("process_start_token")
    if (
        not isinstance(pid, int)
        or not isinstance(process_start_token, str)
        or not process_matches(pid, process_start_token)
    ):
        return None
    raw = health.get("codex_auth")
    state = "unknown"
    observed_at: int | None = None
    if isinstance(raw, dict):
        candidate = raw.get("state")
        if candidate in {"authenticated", "required", "unknown"}:
            state = str(candidate)
        timestamp = raw.get("observed_at")
        if isinstance(timestamp, int) and not isinstance(timestamp, bool):
            observed_at = timestamp
    return {
        "source": "daemon_projection",
        "state": state,
        "observed_at": observed_at,
        "heartbeat": heartbeat_state(config.paths.health),
    }


def _service(
    config: AppConfig,
    action: str,
    config_path: Path | None,
    *,
    lines: int,
) -> int:
    from dataclasses import asdict

    from codexd.security.secrets import SecretStore
    from codexd.service.locking import InstanceLock
    from codexd.service.manager import (
        install_service,
        restart_service,
        service_logs,
        service_status,
        start_service,
        stop_service,
        uninstall_service,
    )

    if action == "status":
        print(
            json.dumps(
                asdict(service_status(config)),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if action == "uninstall":
        uninstall_service(config)
        print("codexD user service removed.")
        return 0
    if action == "start":
        start_service(config)
        print("codexD user service started.")
        return 0
    if action == "stop":
        stop_service(config)
        print("codexD user service stopped.")
        return 0
    if action == "restart":
        restart_service(config)
        print("codexD user service restarted.")
        return 0
    if action == "logs":
        entries = service_logs(config, lines=lines)
        sys.stdout.write("".join(entries))
        return 0
    if not config.daemon_ready_for_discord:
        raise ConfigurationError(
            "Discord guild, owner, and allowed user configuration is required "
            "before install"
        )
    secrets = SecretStore()
    if not secrets.discord_token():
        raise ConfigurationError("Discord token is not configured")
    with InstanceLock(config.paths.data_dir / "durable-keys.lock"):
        allow_create = not config.paths.database.exists()
        secrets.projection_key(allow_create=allow_create)
        secrets.component_key(allow_create=allow_create)
    path = install_service(config, config_path)
    print(f"codexD user service installed at {path}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
