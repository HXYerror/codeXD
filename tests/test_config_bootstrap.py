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
from codexd.security.redaction import (
    redact_diff,
    redact_text,
    redact_value,
    safe_thread_title_summary,
)
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
    assert config.runtime.codex_log_filter.startswith("warn,")
    assert "codex_http_client::transport=error" in config.runtime.codex_log_filter
    assert config.runtime.max_active_runtimes == 10
    assert config.runtime.idle_ttl_seconds == 15 * 60
    assert config.retention.events_days == 14
    assert config.retention.logs_days == 7
    assert config.retention.tool_output_hours == 24
    assert config.retention.outbox_content_days == 7
    assert config.retention.codex_logs_days == 7
    assert config.retention.codex_trace_hours == 24
    assert config.retention.database_size_budget_mib == 512
    assert config.retention.runtime_sqlite_size_budget_mib == 1024
    assert config.discord.owner_user_id == 456
    assert config.discord.archive_max_entries == 256
    assert config.discord.archive_max_entry_bytes == 64 * 1024 * 1024
    assert config.discord.archive_max_total_bytes == 128 * 1024 * 1024
    assert config.discord.archive_max_compression_ratio == 100
    assert config.discord.archive_max_path_depth == 16
    assert config.discord.archive_max_path_chars == 240
    assert config.discord.archive_extract_timeout_seconds == 15
    assert config.discord.progress_update_ms == 5000
    assert config.discord.task_card_update_ms == 5000
    assert config.discord.channel_write_interval_ms == 250
    assert config.discord.global_write_interval_ms == 50


@pytest.mark.parametrize(
    "setting",
    (
        "progress_update_ms",
        "task_card_update_ms",
        "channel_write_interval_ms",
        "global_write_interval_ms",
    ),
)
def test_discord_egress_intervals_cannot_be_disabled(
    tmp_path: Path,
    setting: str,
) -> None:
    config_file = tmp_path / f"{setting}.toml"
    config_file.write_text(
        f"[discord]\n{setting} = 0\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="positive"):
        load_config(config_file, environment={"HOME": str(tmp_path)})


def test_archive_entry_limit_may_not_exceed_total_limit(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        (
            "[discord]\n"
            "archive_max_entry_bytes = 20\n"
            "archive_max_total_bytes = 10\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="may not exceed"):
        load_config(config_file, environment={"HOME": str(tmp_path)})


@pytest.mark.parametrize(
    ("setting", "value"),
    (
        ("archive_max_entries", 257),
        ("archive_max_entry_bytes", 64 * 1024 * 1024 + 1),
        ("archive_max_total_bytes", 128 * 1024 * 1024 + 1),
        ("archive_max_compression_ratio", 101),
        ("archive_max_path_depth", 17),
        ("archive_max_path_chars", 241),
        ("archive_extract_timeout_seconds", 16),
    ),
)
def test_archive_limits_have_hard_security_ceilings(
    tmp_path: Path,
    setting: str,
    value: int,
) -> None:
    config_file = tmp_path / f"{setting}.toml"
    config_file.write_text(
        f"[discord]\n{setting} = {value}\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="may not exceed"):
        load_config(config_file, environment={"HOME": str(tmp_path)})


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


def test_project_path_accepts_directories_outside_configured_roots(
    tmp_path: Path,
) -> None:
    allowed = tmp_path / "allowed"
    inside = allowed / "repo"
    outside = tmp_path / "outside"
    inside.mkdir(parents=True)
    outside.mkdir()

    assert resolve_project_path(str(inside), (allowed.resolve(),)) == inside.resolve()
    assert resolve_project_path(str(outside), (allowed.resolve(),)) == outside.resolve()
    assert resolve_project_path(str(outside)) == outside.resolve()


def test_relative_project_path_resolves_from_explicit_home(tmp_path: Path) -> None:
    project = tmp_path / "dev" / "repo"
    project.mkdir(parents=True)

    assert resolve_project_path("dev/repo", relative_to=tmp_path) == project.resolve()


def test_relative_project_path_may_resolve_outside_home(tmp_path: Path) -> None:
    home = tmp_path / "home"
    outside = tmp_path / "outside"
    home.mkdir()
    outside.mkdir()

    assert resolve_project_path("../outside", relative_to=home) == outside.resolve()


@pytest.mark.skipif(os.name == "nt", reason="POSIX root assertion")
def test_project_path_accepts_system_root() -> None:
    assert resolve_project_path("/") == Path("/")


def test_bootstrap_removes_secrets_before_sdk_import() -> None:
    source = {
        "HOME": "/tmp/home",
        "PATH": "/usr/bin",
        "CODEX_SQLITE_HOME": "/tmp/codex-state",
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
        "CODEX_SQLITE_HOME": "/tmp/codex-state",
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


def test_thread_title_summary_removes_discord_mentions_and_controls() -> None:
    summary = safe_thread_title_summary(
        "修复\x00\n <@123> <@!234> <@&345> <#456> @everyone @HERE 登录"
    )

    assert summary == "修复 登录"
    assert all(
        not (ord(character) < 32 or 127 <= ord(character) <= 159)
        for character in summary
    )


def test_thread_title_summary_removes_unsafe_unicode_formats() -> None:
    assert safe_thread_title_summary("\u200b\u200c\u200d\ufeff") == "新任务"
    assert (
        safe_thread_title_summary("\u202efix\u2069 \u2066login\u2069")
        == "fix login"
    )


def test_thread_title_summary_strips_default_ignorables_before_matching(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "private-project"
    raw_root = str(project_root)
    midpoint = len(raw_root) // 2
    value = (
        "https://example.invalid/?access_to\u034fken=url-secret "
        "Authori\u3164zation: Bearer auth-secret "
        f"{raw_root[:midpoint]}\u115f{raw_root[midpoint:]}"
    )

    redacted = redact_text(value, project_root=project_root)

    assert "url-secret" not in redacted
    assert "auth-secret" not in redacted
    assert raw_root not in redacted
    assert "<project>" in redacted


def test_thread_title_summary_default_ignorables_do_not_create_or_pad_title() -> None:
    invisible = "\u034f\u115f\u1160\u3164\uffa0\u2800\u2066\ufe0f\U000e0100"

    assert safe_thread_title_summary(invisible, has_image_attachment=True) == "新任务"
    assert safe_thread_title_summary(invisible * 12 + "x" * 72) == "x" * 72


@pytest.mark.parametrize(
    "value",
    (
        "repair the 👩‍💻 workflow",
        "repair the 👩🏽‍💻 workflow",
        "keep ☀️‍☁️ visible",
        "keep 🐈‍⬛ visible",
        "keep ↔️ and 〰️ visible",
        "press 1️⃣ now",
    ),
)
def test_thread_title_summary_preserves_structural_emoji_sequences(
    value: str,
) -> None:

    assert safe_thread_title_summary(value) == value


def test_thread_title_summary_requires_immediate_emoji_base_after_zwj() -> None:
    summary = safe_thread_title_summary("☀\u200d\ufe0f☁")

    assert summary == "☀☁"
    assert "\u200d" not in summary
    assert safe_thread_title_summary("A\ufe0f") == "A"
    assert safe_thread_title_summary("1\ufe0f") == "1"
    assert safe_thread_title_summary("1\u200d2") == "12"


def test_thread_title_summary_removes_dangling_zwj_after_truncation() -> None:
    summary = safe_thread_title_summary("x" * 67 + "👩‍💻 tail")

    assert summary == "x" * 67 + "👩..."
    assert "\u200d" not in summary


@pytest.mark.parametrize(
    ("value", "forbidden"),
    (
        ("OPENAI_API_KEY=provider-secret fix auth", "provider-secret"),
        (
            "Authorization: Bearer abcdefghijklmnopqrstuvwxyz fix auth",
            "abcdefghijklmnopqrstuvwxyz",
        ),
        ("inspect /Users/alice/private/file.py", "/Users/alice"),
    ),
)
def test_thread_title_summary_redacts_sensitive_values(
    value: str,
    forbidden: str,
) -> None:
    summary = safe_thread_title_summary(value)

    assert forbidden not in summary
    assert 1 <= len(summary) <= 72


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("请轮换sk-aaaaaaaaaaaaaaaaaaaa密钥", "请轮换<redacted>密钥"),
        (
            "请用Bearer abcdefghijklmnopqrstuvwxyz",
            "请用Bearer <redacted>",
        ),
        (
            "请用Authorization: Token authorization-secret",
            "请用Authorization: <redacted> <redacted>",
        ),
        ("请用Basic dXNlcjpwYXNz登录", "请用Basic <redacted>登录"),
        (
            "检查aaaaaaaaaaaaaaaaaaaa.bbbbbb.cccccccccccccccccccc密钥",
            "检查<redacted>密钥",
        ),
        (
            "检查mfa.abcdefghijklmnopqrstuvwxyz密钥",
            "检查<redacted>密钥",
        ),
        (
            "请用Set-Cookie: theme=private-value",
            "请用Set-Cookie: <redacted>",
        ),
        ("检查/Users/alice/private", "检查<home>/private"),
        ("检查/home/alice/private", "检查<home>/private"),
        (r"检查C:\Users\alice\private", r"检查<home>\private"),
        ("提醒@everyone修复", "提醒 修复"),
        ("提醒@here修复", "提醒 修复"),
    ),
)
def test_thread_title_summary_uses_ascii_security_boundaries(
    value: str,
    expected: str,
) -> None:
    assert safe_thread_title_summary(value) == expected


def test_ascii_security_boundaries_preserve_identifiers_and_urls() -> None:
    values = (
        "notbearer abcdefghijklmnopqrstuvwxyz",
        "databaseBasic dXNlcjpwYXNz",
        "read https://example.invalid/Users/alice/guide",
    )

    assert tuple(safe_thread_title_summary(value) for value in values) == values


def test_thread_title_summary_redacts_full_project_root(tmp_path: Path) -> None:
    project_root = tmp_path / "private-project"

    summary = safe_thread_title_summary(
        f"inspect {project_root}/src/main.py",
        project_root=project_root,
    )

    assert str(project_root) not in summary
    assert "<project>/src/main.py" in summary


def test_thread_title_summary_redacts_control_interleaved_project_root(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "private-project"
    raw_root = str(project_root)
    midpoint = len(raw_root) // 2
    interleaved_root = f"{raw_root[:midpoint]}\x00{raw_root[midpoint:]}"

    summary = safe_thread_title_summary(
        f"inspect {interleaved_root}/src/main.py",
        project_root=project_root,
    )

    assert raw_root not in summary
    assert "<project>/src/main.py" in summary


def test_thread_title_summary_redacts_control_interleaved_secret_name() -> None:
    secret = "control-interleaved-secret"

    summary = safe_thread_title_summary(
        f"OPENAI_API_KE\u200bY={secret} fix authentication"
    )

    assert secret not in summary
    assert "<redacted>" in summary


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("OPENAI_API_\nKEY=line-secret", "OPENAI_API_KEY=<redacted>"),
        ("OPENAI_API_\rKEY=cr-secret", "OPENAI_API_KEY=<redacted>"),
        ("OPENAI_API_\x85KEY=c1-secret", "OPENAI_API_KEY=<redacted>"),
        ("OPENAI_API_KEY+\t=tab-secret", "OPENAI_API_KEY+=<redacted>"),
        (
            "OPENAI_API_KEY1\ufe0f=selector-secret",
            "OPENAI_API_KEY1=<redacted>",
        ),
        (
            "xoxb-12345\u200d67890-" + "a" * 24,
            "<redacted>",
        ),
        (
            "xoxb-12345\ufe0f67890-" + "a" * 24,
            "<redacted>",
        ),
    ),
)
def test_thread_title_summary_uses_aggressive_security_detection(
    value: str,
    expected: str,
) -> None:
    assert safe_thread_title_summary(value) == expected


def test_thread_title_summary_preserves_prose_line_boundaries() -> None:
    assert safe_thread_title_summary("fix login\nthen add tests") == (
        "fix login then add tests"
    )


def test_thread_title_summary_detects_ignorable_split_paths(tmp_path: Path) -> None:
    project_root = tmp_path / "project12"
    raw_root = str(project_root)
    obfuscated_root = raw_root.replace("12", "1\u200d2")

    assert safe_thread_title_summary(
        f"inspect {obfuscated_root}/src/main.py",
        project_root=project_root,
    ) == "inspect <project>/src/main.py"
    assert (
        safe_thread_title_summary("inspect /Us\ufe0fers/alice/private")
        == "inspect <home>/private"
    )


@pytest.mark.parametrize(
    "invisible",
    (
        "\u034f",
        "\u115f",
        "\u1160",
        "\u2800",
        "\u3164",
        "\uffa0",
        "\U000e007f",
    ),
)
def test_thread_title_summary_redacts_default_ignorable_interleaved_secret_name(
    invisible: str,
) -> None:
    secret = "default-ignorable-secret"

    summary = safe_thread_title_summary(
        f"OPENAI_API_KE{invisible}Y={secret} fix authentication"
    )

    assert secret not in summary
    assert "<redacted>" in summary


@pytest.mark.parametrize(
    "credential",
    (
        "ghp_" + "a" * 36,
        "github_pat_" + "a" * 22 + "_" + "b" * 59,
        "xoxb-" + "1234567890-" + "a" * 24,
        "xapp-" + "1-" + "a" * 24,
        "xoxe-" + "1-" + "a" * 24,
        "xoxe.xoxp-" + "1-" + "a" * 24,
        "AKIA" + "A" * 16,
        "ASIA" + "A" * 16,
        "AIza" + "a" * 35,
        "sk_live_" + "a" * 24,
        "rk_live_" + "a" * 24,
    ),
)
def test_thread_title_summary_redacts_recognizable_credentials(
    credential: str,
) -> None:
    summary = safe_thread_title_summary(f"rotate {credential} now")

    assert credential not in summary
    assert "<redacted>" in summary


def test_thread_title_summary_preserves_short_github_token_shape() -> None:
    short_shape = "ghp_" + "a" * 35

    assert safe_thread_title_summary(f"review {short_shape}") == f"review {short_shape}"


def test_thread_title_summary_redacts_credential_completed_by_truncation() -> None:
    credential = "ghp_" + "a" * 36
    value = "x" * 28 + " " + credential + "_tail"

    assert safe_thread_title_summary(value) == "x" * 28 + " <redacted>..."


@pytest.mark.parametrize(
    ("value", "secret"),
    (
        ("Cookie: session_id=cookie-header-secret; theme=dark", "cookie-header-secret"),
        (
            "Set-Cookie: session_id=set-cookie-secret; HttpOnly",
            "set-cookie-secret",
        ),
        ("session_id=session-assignment-secret", "session-assignment-secret"),
        ("cookie=cookie-assignment-secret", "cookie-assignment-secret"),
        ("PHPSESSID=php-session-secret", "php-session-secret"),
        ("connect.sid=express-session-secret", "express-session-secret"),
    ),
)
def test_thread_title_summary_redacts_cookie_and_session_values(
    value: str,
    secret: str,
) -> None:
    summary = safe_thread_title_summary(value)

    assert secret not in summary
    assert "<redacted>" in summary


@pytest.mark.parametrize(
    "operator",
    (":=", "+=", "-=", "*=", "/=", "%=", "?=", ".="),
)
def test_thread_title_summary_redacts_compound_secret_assignments(
    operator: str,
) -> None:
    secret = "compound-assignment-secret"

    summary = safe_thread_title_summary(f"OPENAI_API_KEY{operator}{secret}")

    assert secret not in summary
    assert f"OPENAI_API_KEY{operator}<redacted>" == summary


@pytest.mark.parametrize(
    "value",
    (
        "修复登录后刷新白屏",
        "Fix the blank page after login",
        "修复登录 👩‍💻🚀",
    ),
)
def test_thread_title_summary_preserves_readable_unicode(value: str) -> None:
    assert safe_thread_title_summary(value) == value


def test_thread_title_summary_is_bounded_in_unicode_characters() -> None:
    summary = safe_thread_title_summary("界🙂" * 100)

    assert len(summary) == 72
    assert summary.endswith("...")
    assert summary.encode("utf-8")


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("x" * 60 + "@everyonexxx tail", "x" * 60 + " ..."),
        ("x" * 64 + "@herexxx tail", "x" * 64 + " ..."),
    ),
)
def test_thread_title_summary_resanitizes_mentions_created_by_truncation(
    value: str,
    expected: str,
) -> None:
    summary = safe_thread_title_summary(value)

    assert summary == expected
    assert "@everyone" not in summary.casefold()
    assert "@here" not in summary.casefold()


@pytest.mark.parametrize(
    ("value", "has_image_attachment", "expected"),
    (
        ("", False, "新任务"),
        (" \n\t", True, "图片任务"),
        ("<@123> @everyone \x00", True, "新任务"),
    ),
)
def test_thread_title_summary_uses_deterministic_fallbacks(
    value: str,
    has_image_attachment: bool,
    expected: str,
) -> None:
    assert (
        safe_thread_title_summary(
            value,
            has_image_attachment=has_image_attachment,
        )
        == expected
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
