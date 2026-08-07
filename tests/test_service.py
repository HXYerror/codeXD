from __future__ import annotations

import asyncio
import plistlib
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import discord
import pytest

import codexd.application.daemon as daemon_module
import codexd.service.manager as service_manager
from codexd.application.daemon import (
    _consume_shutdown_request,
    _reset_discord_client,
    _retryable_discord_start_error,
    _start_discord_with_initial_retries,
)
from codexd.bootstrap import load_service_environment
from codexd.config import AppConfig, DiscordConfig, RuntimeConfig
from codexd.errors import ConfigurationError
from codexd.paths import AppPaths
from codexd.service.containment import _WindowsJobContainment
from codexd.service.locking import InstanceLock
from codexd.service.manager import (
    _validate_registered_windows_task,
    _wait_for_fresh_heartbeat,
    _write_service_environment,
    render_macos_plist,
    render_windows_task,
)
from codexd.service.process import current_process_identity, process_matches


@pytest.mark.asyncio
async def test_initial_discord_connection_retries_without_stopping_daemon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    attempts = 0

    async def login(_token: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise aiohttp.ClientConnectionError("offline")

    async def connect(*, reconnect: bool) -> None:
        assert reconnect is True
        stop.set()

    bot = SimpleNamespace(
        login=login,
        connect=connect,
        transport_initialized=False,
    )
    health = SimpleNamespace(observe_discord=Mock())
    reset = AsyncMock()
    monkeypatch.setattr(
        daemon_module,
        "_DISCORD_INITIAL_RETRY_DELAYS_SECONDS",
        (0.0,),
    )
    monkeypatch.setattr(daemon_module, "_reset_discord_client", reset)

    await _start_discord_with_initial_retries(
        bot=bot,
        token="test-token",
        stop=stop,
        health=health,
        logger=Mock(),
    )

    assert attempts == 2
    reset.assert_awaited_once_with(bot)
    health.observe_discord.assert_called_once_with("connecting")


@pytest.mark.asyncio
async def test_initial_discord_login_timeout_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    attempts = 0

    async def login(_token: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            await asyncio.Event().wait()

    async def connect(*, reconnect: bool) -> None:
        assert reconnect is True
        stop.set()

    bot = SimpleNamespace(
        login=login,
        connect=connect,
        transport_initialized=False,
    )
    reset = AsyncMock()
    monkeypatch.setattr(
        daemon_module,
        "_DISCORD_INITIAL_LOGIN_TIMEOUT_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        daemon_module,
        "_DISCORD_INITIAL_RETRY_DELAYS_SECONDS",
        (0.0,),
    )
    monkeypatch.setattr(daemon_module, "_reset_discord_client", reset)

    await _start_discord_with_initial_retries(
        bot=bot,
        token="test-token",
        stop=stop,
        health=SimpleNamespace(observe_discord=Mock()),
        logger=Mock(),
    )

    assert attempts == 2
    reset.assert_awaited_once_with(bot)


@pytest.mark.asyncio
async def test_initial_discord_connection_does_not_retry_fatal_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = SimpleNamespace(
        login=AsyncMock(side_effect=discord.LoginFailure("invalid token")),
        connect=AsyncMock(),
        transport_initialized=False,
    )
    reset = AsyncMock()
    monkeypatch.setattr(daemon_module, "_reset_discord_client", reset)

    with pytest.raises(discord.LoginFailure):
        await _start_discord_with_initial_retries(
            bot=bot,
            token="test-token",
            stop=asyncio.Event(),
            health=SimpleNamespace(observe_discord=Mock()),
            logger=Mock(),
        )

    reset.assert_not_awaited()


@pytest.mark.asyncio
async def test_initialized_discord_transport_is_not_blindly_rebuilt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = aiohttp.ClientConnectionError("offline")
    bot = SimpleNamespace(
        login=AsyncMock(side_effect=error),
        connect=AsyncMock(),
        transport_initialized=True,
    )
    reset = AsyncMock()
    monkeypatch.setattr(daemon_module, "_reset_discord_client", reset)

    with pytest.raises(aiohttp.ClientConnectionError):
        await _start_discord_with_initial_retries(
            bot=bot,
            token="test-token",
            stop=asyncio.Event(),
            health=SimpleNamespace(observe_discord=Mock()),
            logger=Mock(),
        )

    reset.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_discord_retry_backoff_stops_promptly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()

    async def reset(_bot: object) -> None:
        stop.set()

    bot = SimpleNamespace(
        login=AsyncMock(side_effect=aiohttp.ClientConnectionError("offline")),
        connect=AsyncMock(),
        transport_initialized=False,
    )
    monkeypatch.setattr(daemon_module, "_reset_discord_client", reset)

    await _start_discord_with_initial_retries(
        bot=bot,
        token="test-token",
        stop=stop,
        health=SimpleNamespace(observe_discord=Mock()),
        logger=Mock(),
    )

    bot.login.assert_awaited_once_with("test-token")
    bot.connect.assert_not_awaited()


@pytest.mark.parametrize(
    ("status", "expected"),
    [(400, False), (403, False), (429, True), (500, True), (503, True)],
)
def test_discord_http_retry_classification(status: int, expected: bool) -> None:
    response = SimpleNamespace(status=status, reason="test")
    error = discord.HTTPException(response, "test")

    assert _retryable_discord_start_error(error) is expected


@pytest.mark.asyncio
async def test_discord_retry_reset_rebuilds_closed_http_connector() -> None:
    intents = discord.Intents.none()
    bot = discord.Client(intents=intents)
    await bot._async_setup_hook()
    connector = aiohttp.TCPConnector()
    session = aiohttp.ClientSession(connector=connector)
    bot.http._HTTPClient__session = session
    bot.http.connector = connector

    await _reset_discord_client(bot)  # type: ignore[arg-type]

    assert session.closed
    assert bot.http.connector is discord.utils.MISSING
    assert bot.http._HTTPClient__session is discord.utils.MISSING


def test_macos_service_contains_no_secret_environment(tmp_path: Path) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    config_path = tmp_path / "config.toml"

    payload = plistlib.loads(render_macos_plist(config, config_path))

    assert payload["Label"] == "com.codexd.daemon"
    assert payload["KeepAlive"] == {"SuccessfulExit": False}
    assert payload["ThrottleInterval"] == 10
    assert "ProcessType" not in payload
    assert payload["ProgramArguments"] == [
        sys.executable,
        "-m",
        "codexd.cli",
        "--config",
        str(config_path),
        "--service-environment",
        str(tmp_path / "data" / "service" / "environment.json"),
        "daemon",
    ]
    assert "EnvironmentVariables" not in payload


def test_windows_task_restarts_without_execution_timeout(tmp_path: Path) -> None:
    payload = render_windows_task(
        tmp_path / "config.toml",
        user_id="test-user",
        service_environment=tmp_path / "data" / "service" / "environment.json",
        working_directory=tmp_path / "data",
    )
    root = ET.fromstring(payload)
    namespace = {"task": "http://schemas.microsoft.com/windows/2004/02/mit/task"}

    assert root.findtext(".//task:ExecutionTimeLimit", namespaces=namespace) == "PT0S"
    assert root.findtext(".//task:RestartOnFailure/task:Interval", namespaces=namespace) == "PT1M"
    assert root.findtext(".//task:LogonType", namespaces=namespace) == "InteractiveToken"
    assert root.find(".//task:Exec/task:Command", namespace) is not None
    assert root.findtext(
        ".//task:Exec/task:WorkingDirectory",
        namespaces=namespace,
    ) == str((tmp_path / "data").resolve())


def test_process_identity_detects_pid_reuse() -> None:
    identity = current_process_identity()

    assert process_matches(identity.pid, identity.start_token)
    assert not process_matches(identity.pid, "0.000000")


def test_instance_lock_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.lock"
    target.write_bytes(b"")
    linked = tmp_path / "linked.lock"
    linked.symlink_to(target)

    with pytest.raises(Exception, match="opened safely"):
        InstanceLock(linked).acquire()


@pytest.mark.parametrize(
    ("service", "discord"),
    [
        ("stopped", "ready"),
        ("healthy", "disconnected"),
        ("starting", "connecting"),
    ],
)
def test_service_readiness_rejects_nonready_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    service: str,
    discord: str,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    health = {
        "boot_id": "boot",
        "pid": 123,
        "process_start_token": "token",
        "service": service,
        "discord": discord,
    }
    times = iter((0.0, 0.0, 1.0))
    monkeypatch.setattr(service_manager, "read_health", lambda _path: health)
    monkeypatch.setattr(service_manager, "heartbeat_state", lambda _path: "fresh")
    monkeypatch.setattr(service_manager, "process_matches", lambda *_args: True)
    monkeypatch.setattr(
        service_manager,
        "_database_lease_state",
        lambda _config: ("fresh", "boot"),
    )
    monkeypatch.setattr(service_manager.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(service_manager.time, "sleep", lambda _seconds: None)

    with pytest.raises(ConfigurationError, match="fresh heartbeat"):
        _wait_for_fresh_heartbeat(config, timeout_seconds=0.5)


@pytest.mark.parametrize("discord", ["ready", "degraded"])
def test_service_readiness_accepts_ready_discord_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    discord: str,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    health = {
        "boot_id": "boot",
        "pid": 123,
        "process_start_token": "token",
        "service": "healthy",
        "discord": discord,
    }
    times = iter((0.0, 0.0))
    monkeypatch.setattr(service_manager, "read_health", lambda _path: health)
    monkeypatch.setattr(service_manager, "heartbeat_state", lambda _path: "fresh")
    monkeypatch.setattr(service_manager, "process_matches", lambda *_args: True)
    monkeypatch.setattr(
        service_manager,
        "_database_lease_state",
        lambda _config: ("fresh", "boot"),
    )
    monkeypatch.setattr(service_manager.time, "monotonic", lambda: next(times))

    _wait_for_fresh_heartbeat(config, timeout_seconds=0.5)


def test_service_environment_persists_only_protected_nonsecret_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("SAFE_FLAG", "enabled")
    monkeypatch.setenv("CODEXD_DISCORD_TOKEN", "must-not-be-persisted")
    config = AppConfig(
        paths=AppPaths(tmp_path / "data", tmp_path / "logs"),
        discord=DiscordConfig(
            guild_id=100,
            owner_user_id=400,
            allowed_user_ids=frozenset({400, 401}),
        ),
        runtime=RuntimeConfig(nonsecret_env_allowlist=("SAFE_FLAG",)),
    )

    path = _write_service_environment(config)
    loaded = load_service_environment(path)

    assert loaded["SAFE_FLAG"] == "enabled"
    assert loaded["CODEXD_DATA_DIR"] == str(config.paths.data_dir)
    assert loaded["CODEXD_LOG_DIR"] == str(config.paths.log_dir)
    assert loaded["CODEXD_DISCORD_OWNER_USER_ID"] == "400"
    assert loaded["CODEXD_ALLOWED_USER_IDS"] == "400,401"
    assert "CODEXD_DISCORD_TOKEN" not in loaded


def test_shutdown_request_is_boot_id_scoped_and_consumed(tmp_path: Path) -> None:
    request = tmp_path / "shutdown.request"
    request.write_text('{"boot_id":"old-boot"}', encoding="utf-8")

    assert not _consume_shutdown_request(request, "new-boot")
    assert not request.exists()

    request.write_text('{"boot_id":"new-boot"}', encoding="utf-8")
    assert _consume_shutdown_request(request, "new-boot")
    assert not request.exists()


def test_windows_job_handle_remains_open_until_process_exit() -> None:
    kernel32 = Mock()
    containment = _WindowsJobContainment(kernel32=kernel32, handle=123)

    containment.close()

    kernel32.CloseHandle.assert_not_called()
    assert containment.handle == 123


def test_registered_windows_task_is_validated(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config.toml"
    environment = tmp_path / "data" / "service" / "environment.json"
    working_directory = tmp_path / "data"
    payload = render_windows_task(
        config_path,
        user_id="test-user",
        service_environment=environment,
        working_directory=working_directory,
    ).decode("utf-16")
    query = Mock(returncode=0, stdout=payload, stderr="")
    run = Mock(return_value=query)
    monkeypatch.setattr(service_manager.subprocess, "run", run)

    _validate_registered_windows_task(
        config_path,
        service_environment=environment,
        working_directory=working_directory,
    )

    run.assert_called_once_with(
        ["schtasks", "/Query", "/TN", "codexD", "/XML"],
        check=False,
        capture_output=True,
        text=True,
    )


def test_macos_uninstall_stops_before_removing_definition(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    plist = tmp_path / "com.codexd.daemon.plist"
    plist.write_text("installed")
    order: list[str] = []
    monkeypatch.setattr(service_manager, "_macos_plist_path", lambda: plist)
    monkeypatch.setattr(service_manager, "_manager_state", lambda _command: "loaded")
    monkeypatch.setattr(
        service_manager,
        "stop_service",
        lambda _config: order.append("stopped"),
    )
    monkeypatch.setattr(
        service_manager,
        "_run_manager_command",
        lambda _command, _label: order.append("booted-out"),
    )

    service_manager.uninstall_service(config)

    assert order == ["stopped", "booted-out"]
    assert not plist.exists()


def test_macos_uninstall_preserves_definition_on_bootout_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    plist = tmp_path / "com.codexd.daemon.plist"
    plist.write_text("installed")
    monkeypatch.setattr(service_manager, "_macos_plist_path", lambda: plist)
    monkeypatch.setattr(service_manager, "_manager_state", lambda _command: "loaded")
    monkeypatch.setattr(service_manager, "stop_service", lambda _config: None)

    def fail(_command: object, _label: str) -> None:
        raise RuntimeError("bootout failed")

    monkeypatch.setattr(service_manager, "_run_manager_command", fail)

    with pytest.raises(RuntimeError, match="bootout failed"):
        service_manager.uninstall_service(config)

    assert plist.exists()


def test_windows_uninstall_stops_before_deleting_task(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    order: list[str] = []
    monkeypatch.setattr(service_manager, "sys", SimpleNamespace(platform="win32"))
    monkeypatch.setattr(service_manager, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(
        service_manager.subprocess,
        "run",
        Mock(return_value=Mock(returncode=0, stdout="", stderr="")),
    )
    monkeypatch.setattr(
        service_manager,
        "stop_service",
        lambda _config: order.append("stopped"),
    )
    monkeypatch.setattr(
        service_manager,
        "_run_manager_command",
        lambda _command, _label: order.append("deleted"),
    )

    service_manager.uninstall_service(config)

    assert order == ["stopped", "deleted"]


def test_service_start_is_idempotent_for_running_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    manager_command = Mock()
    monkeypatch.setattr(
        service_manager,
        "sys",
        SimpleNamespace(platform="darwin"),
    )
    monkeypatch.setattr(service_manager, "_run_manager_command", manager_command)

    monkeypatch.setattr(
        service_manager,
        "_service_process_state",
        lambda _config: True,
    )
    service_manager.start_service(config)

    manager_command.assert_not_called()


def test_service_stop_accepts_platform_not_running_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    monkeypatch.setattr(
        service_manager,
        "sys",
        SimpleNamespace(platform="darwin"),
    )
    monkeypatch.setattr(
        service_manager,
        "_service_process_state",
        lambda _config: None,
    )
    monkeypatch.setattr(
        service_manager,
        "_run_manager_command",
        Mock(side_effect=service_manager.ConfigurationError("not running")),
    )

    service_manager.stop_service(config)


def test_macos_uninstall_removes_already_stopped_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    plist = tmp_path / "com.codexd.daemon.plist"
    plist.write_text("installed")
    commands: list[str] = []
    monkeypatch.setattr(
        service_manager,
        "sys",
        SimpleNamespace(platform="darwin"),
    )
    monkeypatch.setattr(service_manager, "_macos_plist_path", lambda: plist)
    monkeypatch.setattr(service_manager, "_manager_state", lambda _command: "loaded")
    monkeypatch.setattr(
        service_manager,
        "_service_process_state",
        lambda _config: False,
    )

    def manager_command(_command: object, label: str) -> None:
        if label == "launchctl stop":
            raise service_manager.ConfigurationError("not running")
        commands.append(label)

    monkeypatch.setattr(
        service_manager,
        "_run_manager_command",
        manager_command,
    )

    service_manager.uninstall_service(config)

    assert commands == ["launchctl uninstall"]
    assert not plist.exists()


def test_windows_uninstall_removes_already_stopped_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))
    commands: list[str] = []
    monkeypatch.setattr(
        service_manager,
        "sys",
        SimpleNamespace(platform="win32"),
    )
    monkeypatch.setattr(service_manager, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(
        service_manager,
        "_service_process_state",
        lambda _config: False,
    )
    monkeypatch.setattr(
        service_manager.subprocess,
        "run",
        Mock(return_value=Mock(returncode=0, stdout="", stderr="")),
    )

    def manager_command(_command: object, label: str) -> None:
        if label == "Task Scheduler stop":
            raise service_manager.ConfigurationError("not running")
        commands.append(label)

    monkeypatch.setattr(
        service_manager,
        "_run_manager_command",
        manager_command,
    )

    service_manager.uninstall_service(config)

    assert commands == ["Task Scheduler uninstall"]
