from __future__ import annotations

import json
import logging
import os
import plistlib
import sqlite3
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from codexd.bootstrap import prepare_bootstrap
from codexd.config import AppConfig
from codexd.errors import ConfigurationError, StorageError
from codexd.observability.health import heartbeat_state, read_health
from codexd.service.process import process_matches

_MAC_LABEL = "com.codexd.daemon"
_WINDOWS_TASK = "codexD"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServiceStatus:
    installed: bool
    heartbeat: str
    process: str
    service_manager: str
    database_lease: str
    boot_id: str | None


def install_service(config: AppConfig, config_path: Path | None) -> Path:
    _write_service_environment(config)
    if sys.platform == "darwin":
        return _install_macos(config, config_path)
    if os.name == "nt":
        return _install_windows(config, config_path)
    raise ConfigurationError("service installation is supported only on macOS and Windows")


def uninstall_service(config: AppConfig) -> None:
    if sys.platform == "darwin":
        path = _macos_plist_path()
        target = f"gui/{os.getuid()}/{_MAC_LABEL}"
        if _manager_state(["launchctl", "print", target]) == "loaded":
            stop_service(config)
            _run_manager_command(
                ["launchctl", "bootout", target],
                "launchctl uninstall",
            )
        path.unlink(missing_ok=True)
        return
    if os.name == "nt":
        query = subprocess.run(
            ["schtasks", "/Query", "/TN", _WINDOWS_TASK],
            check=False,
            capture_output=True,
            text=True,
        )
        if query.returncode == 0:
            stop_service(config)
            _run_manager_command(
                ["schtasks", "/Delete", "/TN", _WINDOWS_TASK, "/F"],
                "Task Scheduler uninstall",
            )
        return
    raise ConfigurationError("service installation is supported only on macOS and Windows")


def start_service(config: AppConfig | None = None) -> None:
    if config is not None and _service_process_state(config) is True:
        return
    previous_boot_id = _current_boot_id(config) if config is not None else None
    if sys.platform == "darwin":
        _run_manager_command(
            ["launchctl", "kickstart", f"gui/{os.getuid()}/{_MAC_LABEL}"],
            "launchctl kickstart",
        )
        if config is not None:
            _wait_for_fresh_heartbeat(
                config,
                previous_boot_id=previous_boot_id,
            )
        return
    if os.name == "nt":
        _run_manager_command(
            ["schtasks", "/Run", "/TN", _WINDOWS_TASK],
            "Task Scheduler start",
        )
        if config is not None:
            _wait_for_fresh_heartbeat(
                config,
                previous_boot_id=previous_boot_id,
            )
        return
    raise ConfigurationError("service control is supported only on macOS and Windows")


def stop_service(config: AppConfig | None = None) -> None:
    if sys.platform == "darwin":
        try:
            _run_manager_command(
                [
                    "launchctl",
                    "kill",
                    "SIGTERM",
                    f"gui/{os.getuid()}/{_MAC_LABEL}",
                ],
                "launchctl stop",
            )
        except ConfigurationError:
            if config is None or _service_process_state(config) is True:
                raise
            return
        if config is not None:
            _wait_for_process_exit(config)
        return
    if os.name == "nt":
        if config is not None:
            try:
                if _request_graceful_shutdown(config):
                    return
            except ConfigurationError:
                logger.warning(
                    "graceful Windows service shutdown timed out; "
                    "falling back to Task Scheduler termination"
                )
        try:
            _run_manager_command(
                ["schtasks", "/End", "/TN", _WINDOWS_TASK],
                "Task Scheduler stop",
            )
        except ConfigurationError:
            if config is None or _service_process_state(config) is True:
                raise
            return
        if config is not None:
            _wait_for_process_exit(config)
        return
    raise ConfigurationError("service control is supported only on macOS and Windows")


def restart_service(config: AppConfig | None = None) -> None:
    if sys.platform == "darwin":
        stop_service(config)
        start_service(config)
        return
    if os.name == "nt":
        stop_service(config)
        start_service(config)
        return
    raise ConfigurationError("service control is supported only on macOS and Windows")


def service_logs(config: AppConfig, *, lines: int = 100) -> tuple[str, ...]:
    if lines < 1 or lines > 1000:
        raise ConfigurationError("service log line count must be between 1 and 1000")
    try:
        with config.paths.log_file.open(encoding="utf-8", errors="replace") as stream:
            from collections import deque

            return tuple(deque(stream, maxlen=lines))
    except FileNotFoundError:
        return ()
    except OSError as exc:
        raise ConfigurationError(f"cannot read service log: {exc}") from exc


def service_status(config: AppConfig) -> ServiceStatus:
    health = read_health(config.paths.health)
    heartbeat = heartbeat_state(config.paths.health)
    process = "unknown"
    if health and isinstance(health.get("pid"), int) and isinstance(
        health.get("process_start_token"), str
    ):
        process = (
            "running"
            if process_matches(health["pid"], health["process_start_token"])
            else "not-running"
        )
    database_lease, lease_boot_id = _database_lease_state(config)
    health_boot_id = health.get("boot_id") if health else None
    boot_id = health_boot_id if isinstance(health_boot_id, str) else lease_boot_id
    if (
        isinstance(health_boot_id, str)
        and lease_boot_id is not None
        and health_boot_id != lease_boot_id
    ):
        database_lease = "boot-mismatch"
    if sys.platform == "darwin":
        installed = _macos_plist_path().exists()
        manager = _manager_state(
            ["launchctl", "print", f"gui/{os.getuid()}/{_MAC_LABEL}"]
        )
    elif os.name == "nt":
        query = subprocess.run(
            ["schtasks", "/Query", "/TN", _WINDOWS_TASK],
            check=False,
            capture_output=True,
            text=True,
        )
        installed = query.returncode == 0
        manager = "loaded" if installed else "not-loaded"
    else:
        installed = False
        manager = "unsupported"
    return ServiceStatus(
        installed,
        heartbeat,
        process,
        manager,
        database_lease,
        boot_id,
    )


def render_macos_plist(config: AppConfig, config_path: Path | None) -> bytes:
    arguments = _daemon_arguments(
        config_path,
        service_environment=config.paths.data_dir / "service" / "environment.json",
    )
    payload = {
        "Label": _MAC_LABEL,
        "ProgramArguments": arguments,
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": 10,
        "StandardOutPath": str(config.paths.log_file),
        "StandardErrorPath": str(config.paths.log_file),
        "WorkingDirectory": str(config.paths.data_dir),
    }
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)


def windows_task_command(
    config_path: Path | None,
    *,
    service_environment: Path | None = None,
) -> str:
    return subprocess.list2cmdline(
        _daemon_arguments(config_path, service_environment=service_environment)
    )


def render_windows_task(
    config_path: Path | None,
    *,
    user_id: str,
    service_environment: Path | None = None,
    working_directory: Path | None = None,
) -> bytes:
    namespace = "http://schemas.microsoft.com/windows/2004/02/mit/task"
    ET.register_namespace("", namespace)

    def element(parent: ET.Element, name: str, text: str | None = None) -> ET.Element:
        child = ET.SubElement(parent, f"{{{namespace}}}{name}")
        child.text = text
        return child

    root = ET.Element(f"{{{namespace}}}Task", {"version": "1.4"})
    registration = element(root, "RegistrationInfo")
    element(registration, "Description", "Durable single-user codexD Discord bridge")
    triggers = element(root, "Triggers")
    logon = element(triggers, "LogonTrigger")
    element(logon, "Enabled", "true")
    principals = element(root, "Principals")
    principal = element(principals, "Principal")
    principal.set("id", "Author")
    element(principal, "UserId", user_id)
    element(principal, "LogonType", "InteractiveToken")
    element(principal, "RunLevel", "LeastPrivilege")
    settings = element(root, "Settings")
    element(settings, "MultipleInstancesPolicy", "IgnoreNew")
    element(settings, "DisallowStartIfOnBatteries", "false")
    element(settings, "StopIfGoingOnBatteries", "false")
    element(settings, "StartWhenAvailable", "true")
    element(settings, "ExecutionTimeLimit", "PT0S")
    element(settings, "Enabled", "true")
    restart = element(settings, "RestartOnFailure")
    element(restart, "Interval", "PT1M")
    element(restart, "Count", "999")
    actions = element(root, "Actions")
    actions.set("Context", "Author")
    execute = element(actions, "Exec")
    arguments = _daemon_arguments(
        config_path,
        service_environment=service_environment,
    )
    element(execute, "Command", arguments[0])
    element(execute, "Arguments", subprocess.list2cmdline(arguments[1:]))
    if working_directory is not None:
        element(execute, "WorkingDirectory", str(working_directory.resolve()))
    return cast(bytes, ET.tostring(root, encoding="utf-16", xml_declaration=True))


def _install_macos(config: AppConfig, config_path: Path | None) -> Path:
    previous_boot_id = _current_boot_id(config)
    path = _macos_plist_path()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(render_macos_plist(config, config_path))
    temporary.chmod(0o600)
    validation = subprocess.run(
        ["plutil", "-lint", str(temporary)],
        check=False,
        capture_output=True,
        text=True,
    )
    if validation.returncode != 0:
        temporary.unlink(missing_ok=True)
        raise ConfigurationError(
            f"plutil validation failed: "
            f"{validation.stderr.strip() or validation.stdout.strip()}"
        )
    os.replace(temporary, path)
    domain = f"gui/{os.getuid()}"
    if _manager_state(["launchctl", "print", f"{domain}/{_MAC_LABEL}"]) == "loaded":
        subprocess.run(
            ["launchctl", "bootout", f"{domain}/{_MAC_LABEL}"],
            check=True,
            capture_output=True,
            text=True,
        )
    bootstrap = subprocess.run(
        ["launchctl", "bootstrap", domain, str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if bootstrap.returncode != 0:
        raise ConfigurationError(
            f"launchctl bootstrap failed: {bootstrap.stderr.strip() or bootstrap.returncode}"
        )
    subprocess.run(
        ["launchctl", "kickstart", "-k", f"{domain}/{_MAC_LABEL}"],
        check=True,
        capture_output=True,
        text=True,
    )
    if _manager_state(["launchctl", "print", f"{domain}/{_MAC_LABEL}"]) != "loaded":
        raise ConfigurationError("launchd did not load the installed codexD service")
    _wait_for_fresh_heartbeat(config, previous_boot_id=previous_boot_id)
    return path


def _install_windows(config: AppConfig, config_path: Path | None) -> Path:
    import getpass

    previous_boot_id = _current_boot_id(config)
    service_dir = config.paths.data_dir / "service"
    service_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    definition = service_dir / "codexd-task.xml"
    temporary = definition.with_suffix(".tmp")
    temporary.write_bytes(
        render_windows_task(
            config_path,
            user_id=getpass.getuser(),
            service_environment=service_dir / "environment.json",
            working_directory=config.paths.data_dir,
        )
    )
    os.replace(temporary, definition)
    result = subprocess.run(
        [
            "schtasks",
            "/Create",
            "/TN",
            _WINDOWS_TASK,
            "/XML",
            str(definition),
            "/F",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ConfigurationError(
            f"Task Scheduler install failed: {result.stderr.strip() or result.stdout.strip()}"
        )
    _validate_registered_windows_task(
        config_path,
        service_environment=service_dir / "environment.json",
        working_directory=config.paths.data_dir,
    )
    start_service()
    _wait_for_fresh_heartbeat(config, previous_boot_id=previous_boot_id)
    return definition


def _daemon_arguments(
    config_path: Path | None,
    *,
    service_environment: Path | None = None,
) -> list[str]:
    arguments = [sys.executable, "-m", "codexd.cli"]
    if config_path is not None:
        arguments.extend(["--config", str(config_path.resolve())])
    if service_environment is not None:
        arguments.extend(
            ["--service-environment", str(service_environment.resolve())]
        )
    arguments.append("daemon")
    return arguments


def _write_service_environment(config: AppConfig) -> Path:
    service_dir = config.paths.data_dir / "service"
    service_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = service_dir / "environment.json"
    temporary = service_dir / f".environment.{os.getpid()}.tmp"
    environment = prepare_bootstrap(
        os.environ,
        extra_nonsecret_names=config.runtime.nonsecret_env_allowlist,
    ).child_environment
    environment["CODEXD_DATA_DIR"] = str(config.paths.data_dir)
    environment["CODEXD_LOG_DIR"] = str(config.paths.log_dir)
    if config.discord.guild_id is not None:
        environment["CODEXD_DISCORD_GUILD_ID"] = str(config.discord.guild_id)
    if config.discord.owner_user_id is not None:
        environment["CODEXD_DISCORD_OWNER_USER_ID"] = str(
            config.discord.owner_user_id
        )
    if config.discord.allowed_user_ids:
        environment["CODEXD_ALLOWED_USER_IDS"] = ",".join(
            str(value) for value in sorted(config.discord.allowed_user_ids)
        )
    temporary.write_text(
        json.dumps(environment, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )
    if os.name != "nt":
        service_dir.chmod(0o700)
        temporary.chmod(0o600)
    os.replace(temporary, destination)
    if os.name != "nt":
        destination.chmod(0o600)
    return destination


def _macos_plist_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{_MAC_LABEL}.plist"


def _manager_state(command: list[str]) -> str:
    result = subprocess.run(
        command, check=False, capture_output=True, text=True
    )
    return "loaded" if result.returncode == 0 else "not-loaded"


def _validate_registered_windows_task(
    config_path: Path | None,
    *,
    service_environment: Path,
    working_directory: Path,
) -> None:
    result = subprocess.run(
        ["schtasks", "/Query", "/TN", _WINDOWS_TASK, "/XML"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ConfigurationError(
            "Task Scheduler validation query failed: "
            f"{result.stderr.strip() or result.stdout.strip() or result.returncode}"
        )
    xml_start = result.stdout.find("<")
    if xml_start < 0:
        raise ConfigurationError("Task Scheduler validation returned no XML")
    try:
        root = ET.fromstring(result.stdout[xml_start:])
    except ET.ParseError as exc:
        raise ConfigurationError(
            "Task Scheduler validation returned malformed XML"
        ) from exc
    namespace = {"task": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    expected_arguments = _daemon_arguments(
        config_path,
        service_environment=service_environment,
    )
    expected = {
        ".//task:Exec/task:Command": expected_arguments[0],
        ".//task:Exec/task:Arguments": subprocess.list2cmdline(
            expected_arguments[1:]
        ),
        ".//task:Exec/task:WorkingDirectory": str(working_directory.resolve()),
        ".//task:MultipleInstancesPolicy": "IgnoreNew",
        ".//task:ExecutionTimeLimit": "PT0S",
        ".//task:RunLevel": "LeastPrivilege",
        ".//task:LogonType": "InteractiveToken",
    }
    mismatches = [
        path
        for path, value in expected.items()
        if root.findtext(path, namespaces=namespace) != value
    ]
    if mismatches:
        raise ConfigurationError(
            "Task Scheduler registered task differs from the requested definition: "
            + ", ".join(mismatches)
        )


def _run_manager_command(command: list[str], label: str) -> None:
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ConfigurationError(
            f"{label} failed: "
            f"{result.stderr.strip() or result.stdout.strip() or result.returncode}"
        )


def _database_lease_state(config: AppConfig) -> tuple[str, str | None]:
    if not config.paths.database.exists():
        return "missing", None
    try:
        from codexd.domain.ids import utc_now_ms
        from codexd.storage.sqlite import SQLiteStore

        with SQLiteStore(config.paths.database) as store:
            row = store.query_one(
                "SELECT boot_id, heartbeat_at FROM daemon_leases "
                "WHERE lease_name = 'daemon'"
            )
        if row is None:
            return "missing", None
        age = utc_now_ms() - int(row["heartbeat_at"])
        return (
            "fresh" if age <= 20_000 else "degraded" if age <= 60_000 else "stale",
            str(row["boot_id"]),
        )
    except (OSError, sqlite3.Error, StorageError, TypeError, ValueError):
        return "unavailable", None


def _wait_for_fresh_heartbeat(
    config: AppConfig,
    *,
    timeout_seconds: float = 60.0,
    previous_boot_id: str | None = None,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        health = read_health(config.paths.health)
        boot_id = health.get("boot_id") if health else None
        if (
            health is not None
            and heartbeat_state(config.paths.health) == "fresh"
            and health.get("service") == "healthy"
            and health.get("discord") in {"ready", "degraded"}
            and isinstance(boot_id, str)
            and boot_id != previous_boot_id
            and isinstance(health.get("pid"), int)
            and isinstance(health.get("process_start_token"), str)
            and process_matches(
                health["pid"],
                health["process_start_token"],
            )
        ):
            lease_state, lease_boot_id = _database_lease_state(config)
            if lease_state == "fresh" and lease_boot_id == boot_id:
                return
        time.sleep(0.5)
    raise ConfigurationError("codexD service did not produce a fresh heartbeat")


def _current_boot_id(config: AppConfig | None) -> str | None:
    if config is None:
        return None
    health = read_health(config.paths.health)
    value = health.get("boot_id") if health else None
    return value if isinstance(value, str) else None


def _service_process_state(config: AppConfig) -> bool | None:
    health = read_health(config.paths.health)
    if health is None:
        return None
    pid = health.get("pid")
    token = health.get("process_start_token")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or not isinstance(token, str)
    ):
        return None
    return process_matches(pid, token)


def _wait_for_process_exit(
    config: AppConfig,
    *,
    timeout_seconds: float = 30.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        health = read_health(config.paths.health)
        if not health:
            return
        pid = health.get("pid")
        token = health.get("process_start_token")
        if (
            not isinstance(pid, int)
            or not isinstance(token, str)
            or not process_matches(pid, token)
        ):
            return
        time.sleep(0.25)
    raise ConfigurationError("codexD service did not stop within the shutdown deadline")


def _request_graceful_shutdown(config: AppConfig) -> bool:
    health = read_health(config.paths.health)
    if not health:
        return False
    boot_id = health.get("boot_id")
    pid = health.get("pid")
    token = health.get("process_start_token")
    if (
        not isinstance(boot_id, str)
        or not isinstance(pid, int)
        or not isinstance(token, str)
        or not process_matches(pid, token)
    ):
        return False
    request = config.paths.data_dir / "service" / "shutdown.request"
    request.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = request.with_name(f".shutdown.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps({"boot_id": boot_id}, sort_keys=True),
        encoding="utf-8",
    )
    if os.name != "nt":
        temporary.chmod(0o600)
    os.replace(temporary, request)
    _wait_for_process_exit(config)
    request.unlink(missing_ok=True)
    return True
