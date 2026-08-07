from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from codexd.bootstrap import (
    assert_environment_scrubbed,
    prepare_bootstrap,
    scrub_process_environment,
)
from codexd.cli import (
    _codex_auth,
    _print_account,
    _running_daemon_auth_projection,
)
from codexd.config import AppConfig, load_config, resolve_project_path
from codexd.domain.ids import utc_now_ms
from codexd.errors import SecurityError
from codexd.observability.logging import JsonFormatter
from codexd.paths import AppPaths
from codexd.security.redaction import redact_diff, redact_text, redact_value
from codexd.security.secrets import SecretStore
from codexd.service.process import current_process_identity


def test_config_defaults_to_full_access_and_compatible_sdk(tmp_path: Path) -> None:
    allowed = tmp_path / "projects"
    allowed.mkdir()
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "\n".join(
            (
                "[discord]",
                "guild_id = 123",
                "owner_user_id = 456",
                "allowed_user_ids = [456]",
                "[security]",
                f'allowed_roots = ["{allowed}"]',
            )
        ),
        encoding="utf-8",
    )
    config = load_config(
        config_file,
        environment={
            "HOME": str(tmp_path),
            "CODEXD_DATA_DIR": str(tmp_path / "data"),
            "CODEXD_LOG_DIR": str(tmp_path / "logs"),
        },
    )

    assert config.daemon_ready_for_discord
    assert config.security.default_sandbox_profile == "full_access"
    assert config.runtime.sdk_version_policy == "compatible_range"
    assert config.runtime.codex_bin is None
    assert config.runtime.topology == "project_scoped"
    assert config.discord.owner_user_id == 456


def test_schedule_poll_rejects_unrepresentable_integer(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        f"[schedule]\npoll_seconds = {10**400}\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="finite and positive"):
        load_config(config_file, environment={"HOME": str(tmp_path)})


def test_runtime_accepts_an_explicit_codex_executable(tmp_path: Path) -> None:
    executable = tmp_path / "codex"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o700)
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        f'[runtime]\ncodex_bin = "{executable}"\n',
        encoding="utf-8",
    )

    config = load_config(config_file, environment={"HOME": str(tmp_path)})

    assert config.runtime.codex_bin == executable.resolve()


def test_config_rejects_nonfinite_poll_and_oversized_snowflake(
    tmp_path: Path,
) -> None:
    nonfinite = tmp_path / "nonfinite.toml"
    nonfinite.write_text("[schedule]\npoll_seconds = nan\n", encoding="utf-8")
    with pytest.raises(Exception, match="finite and positive"):
        load_config(nonfinite, environment={"HOME": str(tmp_path)})

    oversized = tmp_path / "oversized.toml"
    oversized.write_text(
        f"[discord]\nguild_id = {1 << 64}\n",
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="64-bit snowflake"):
        load_config(oversized, environment={"HOME": str(tmp_path)})


def test_runtime_rejects_a_non_executable_codex_file(tmp_path: Path) -> None:
    executable = tmp_path / "codex"
    executable.write_text("not executable", encoding="utf-8")
    executable.chmod(0o600)
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        f'[runtime]\ncodex_bin = "{executable}"\n',
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="must be executable"):
        load_config(config_file, environment={"HOME": str(tmp_path)})


def test_config_and_data_paths_reject_symlinks(tmp_path: Path) -> None:
    real_config = tmp_path / "real.toml"
    real_config.write_text("", encoding="utf-8")
    linked_config = tmp_path / "linked.toml"
    linked_config.symlink_to(real_config)
    with pytest.raises(Exception, match="must not be a symlink"):
        load_config(linked_config, environment={"HOME": str(tmp_path)})

    target = tmp_path / "target"
    target.mkdir()
    data_link = tmp_path / "data"
    data_link.symlink_to(target, target_is_directory=True)
    with pytest.raises(SecurityError, match="must not be a symlink"):
        AppPaths(data_link, tmp_path / "logs").ensure()


def test_discord_owner_must_be_explicit_and_allowed(tmp_path: Path) -> None:
    missing_owner = tmp_path / "missing-owner.toml"
    missing_owner.write_text(
        "[discord]\nguild_id = 123\nallowed_user_ids = [456]\n",
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="owner_user_id is required"):
        load_config(missing_owner, environment={"HOME": str(tmp_path)})

    invalid_owner = tmp_path / "invalid-owner.toml"
    invalid_owner.write_text(
        (
            "[discord]\nguild_id = 123\nowner_user_id = 789\n"
            "allowed_user_ids = [456]\n"
        ),
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="must also appear"):
        load_config(invalid_owner, environment={"HOME": str(tmp_path)})


def test_project_path_must_be_inside_an_allowed_root(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    inside = allowed / "repo"
    outside = tmp_path / "outside"
    inside.mkdir(parents=True)
    outside.mkdir()

    assert resolve_project_path(str(inside), (allowed.resolve(),)) == inside.resolve()
    with pytest.raises(SecurityError, match="outside"):
        resolve_project_path(str(outside), (allowed.resolve(),))


@pytest.mark.skipif(os.name == "nt", reason="POSIX protected-directory assertion")
def test_project_path_rejects_protected_system_directory() -> None:
    with pytest.raises(SecurityError, match="protected system"):
        resolve_project_path("/", (Path("/"),))


def test_bootstrap_removes_secrets_before_sdk_import() -> None:
    source = {
        "HOME": "/tmp/home",
        "PATH": "/usr/bin",
        "CODEXD_DISCORD_TOKEN": "discord-secret",
        "OPENAI_API_KEY": "provider-secret",
        "SAFE_FLAG": "on",
    }
    prepared = prepare_bootstrap(source, extra_nonsecret_names=("SAFE_FLAG",))
    process_environment = dict(source)

    scrub_process_environment(prepared, environment=process_environment)

    assert prepared.discord_token == "discord-secret"
    assert process_environment == {
        "HOME": "/tmp/home",
        "PATH": "/usr/bin",
        "SAFE_FLAG": "on",
    }
    assert_environment_scrubbed(
        prepared.child_environment, environment=process_environment
    )
    process_environment["SAFE_FLAG"] = "changed"
    with pytest.raises(SecurityError, match="values changed"):
        assert_environment_scrubbed(
            prepared.child_environment,
            environment=process_environment,
        )


def test_bootstrap_rejects_secret_named_allowlist_entries() -> None:
    with pytest.raises(SecurityError, match="unsafe"):
        prepare_bootstrap({}, extra_nonsecret_names=("MY_API_KEY",))


def test_missing_durable_key_fails_closed_for_existing_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored: dict[str, str] = {}
    monkeypatch.setattr(
        SecretStore,
        "_get",
        staticmethod(lambda name: stored.get(name)),
    )
    monkeypatch.setattr(
        SecretStore,
        "_set",
        staticmethod(lambda name, value: stored.__setitem__(name, value)),
    )
    secrets = SecretStore()

    with pytest.raises(SecurityError, match="existing codexD database"):
        secrets.component_key(allow_create=False)

    created = secrets.component_key()
    assert len(created) == 32
    assert secrets.component_key(allow_create=False) == created


def test_account_status_output_uses_strict_allowlist(capsys) -> None:
    account = SimpleNamespace(
        requires_openai_auth=False,
        account=SimpleNamespace(
            root=SimpleNamespace(
                type="chatgpt",
                plan_type="pro",
                email="private@example.com",
                access_token="secret-token",
            )
        ),
        model_dump=lambda **_kwargs: {"access_token": "secret-token"},
    )

    _print_account(account)

    output = capsys.readouterr().out
    assert "secret-token" not in output
    assert "private@example.com" not in output
    assert '"account_type": "chatgpt"' in output
    assert '"plan_type": "pro"' in output


def test_redaction_covers_headers_environment_flags_urls_and_mfa() -> None:
    value = "\n".join(
        (
            "Authorization: Basic dXNlcjpwYXNz",
            "OPENAI_API_KEY=provider-secret",
            "--password command-secret",
            "https://example.invalid/callback?access_token=url-secret&safe=yes",
            "https://alice:userinfo-secret@example.invalid/private",
            "mfa.abcdefghijklmnopqrstuvwxyz0123456789",
        )
    )

    redacted = redact_text(value)

    for secret in (
        "alice",
        "dXNlcjpwYXNz",
        "provider-secret",
        "command-secret",
        "url-secret",
        "userinfo-secret",
        "abcdefghijklmnopqrstuvwxyz0123456789",
    ):
        assert secret not in redacted
    assert redact_value({"OPENAI_API_KEY": "opaque-secret"}) == {
        "OPENAI_API_KEY": "<redacted>"
    }


def test_redaction_preserves_ordinary_basic_and_bearer_prose() -> None:
    prose = (
        "Use basic authentication, and note that the bearer, "
        "who represents the caller."
    )

    assert redact_text(prose) == prose
    assert (
        redact_text("Supports Basic ClassName behavior.")
        == "Supports Basic ClassName behavior."
    )
    assert redact_text("Run basic sha256sum now.") == "Run basic sha256sum now."
    assert redact_text("Basic dXNlcjpwYXNz") == "Basic <redacted>"
    assert redact_text("Basic YTph") == "Basic <redacted>"
    assert (
        redact_text("Bearer abcdefghijklmnopqrstuvwxyz")
        == "Bearer <redacted>"
    )
    assert (
        redact_text("Send bearer abcdefghijklmnopqrstuvwxyz")
        == "Send bearer <redacted>"
    )
    assert (
        redact_text("the bearer alphabeticonlytoken")
        == "the bearer <redacted>"
    )
    assert (
        redact_text("Send bearer eyJhbGciOiJIUzI1NiJ9.payload.signature")
        == "Send bearer <redacted>"
    )


def test_json_formatter_redacts_messages_extras_and_exceptions() -> None:
    try:
        raise RuntimeError("OPENAI_API_KEY=exception-secret")
    except RuntimeError:
        record = logging.LogRecord(
            name="codexd.test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="Authorization: Basic message-secret",
            args=(),
            exc_info=sys.exc_info(),
        )
    record.command = "--password extra-secret"
    record.api_key = "opaque-structured-secret"

    payload = JsonFormatter().format(record)

    assert "message-secret" not in payload
    assert "exception-secret" not in payload
    assert "extra-secret" not in payload
    assert "opaque-structured-secret" not in payload


def test_diff_redaction_makes_project_paths_relative_and_hides_outside_paths(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    diff = "\n".join(
        (
            f"--- {root / 'src' / 'inside.py'}",
            "+++ /etc/private.conf",
            r"rename from C:\Users\alice\secret.txt",
            "rename to src/public.txt",
        )
    )

    redacted = redact_diff(diff, project_root=root)

    assert str(root) not in redacted
    assert "/etc/private.conf" not in redacted
    assert r"C:\Users\alice" not in redacted
    assert "--- src/inside.py" in redacted
    assert "<outside-project>/private.conf" in redacted


def test_running_daemon_auth_status_uses_health_projection(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paths = AppPaths(tmp_path / "data", tmp_path / "logs")
    paths.ensure()
    identity = current_process_identity()
    paths.health.write_text(
        json.dumps(
            {
                "pid": identity.pid,
                "process_start_token": identity.start_token,
                "heartbeat_at": utc_now_ms(),
                "codex_auth": {
                    "state": "authenticated",
                    "observed_at": 123,
                    "email": "must-not-be-projected@example.com",
                },
            }
        ),
        encoding="utf-8",
    )

    projection = _running_daemon_auth_projection(AppConfig(paths=paths))

    assert projection == {
        "source": "daemon_projection",
        "state": "authenticated",
        "observed_at": 123,
        "heartbeat": "fresh",
    }
    assert _codex_auth(AppConfig(paths=paths), "status", yes=False) == 0
    output = json.loads(capsys.readouterr().out)
    assert output == projection


@pytest.mark.parametrize("action", ["login-chatgpt", "login-device-code"])
def test_interrupted_codex_login_cancels_handle(
    action: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = SimpleNamespace(
        auth_url="https://example.invalid/auth",
        verification_url="https://example.invalid/device",
        user_code="ABCD",
        wait=Mock(side_effect=KeyboardInterrupt),
        cancel=Mock(),
    )

    class FakeCodexConfig:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class FakeCodex:
        def __init__(self, _config: object) -> None:
            pass

        def __enter__(self) -> FakeCodex:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def login_chatgpt(self) -> object:
            return handle

        def login_chatgpt_device_code(self) -> object:
            return handle

    monkeypatch.setitem(
        sys.modules,
        "openai_codex",
        SimpleNamespace(Codex=FakeCodex, CodexConfig=FakeCodexConfig),
    )
    monkeypatch.setattr("codexd.cli.webbrowser.open", Mock())
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))

    with pytest.raises(KeyboardInterrupt):
        _codex_auth(config, action, yes=False)

    handle.cancel.assert_called_once_with()


def test_codex_login_cancels_handle_when_browser_open_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = SimpleNamespace(
        auth_url="https://example.invalid/auth",
        wait=Mock(),
        cancel=Mock(),
    )

    class FakeCodexConfig:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class FakeCodex:
        def __init__(self, _config: object) -> None:
            pass

        def __enter__(self) -> FakeCodex:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def login_chatgpt(self) -> object:
            return handle

    monkeypatch.setitem(
        sys.modules,
        "openai_codex",
        SimpleNamespace(Codex=FakeCodex, CodexConfig=FakeCodexConfig),
    )
    monkeypatch.setattr(
        "codexd.cli.webbrowser.open",
        Mock(side_effect=RuntimeError("browser failed")),
    )
    config = AppConfig(paths=AppPaths(tmp_path / "data", tmp_path / "logs"))

    with pytest.raises(RuntimeError, match="browser failed"):
        _codex_auth(config, "login-chatgpt", yes=False)

    handle.cancel.assert_called_once_with()
    handle.wait.assert_not_called()
