from __future__ import annotations

import re
import shlex
from pathlib import Path, PureWindowsPath
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_SECRET_NAME = re.compile(
    r"(?:token|secret|password|passwd|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|credential|authorization|signature)",
    re.IGNORECASE,
)
_AUTH_HEADER = re.compile(
    r"(?i)\b(?P<header>authorization|proxy-authorization)\s*:\s*"
    r"(?P<scheme>[A-Za-z][A-Za-z0-9_-]*)\s+(?P<value>\S+)"
)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(?P<prefix>[\"']?"
    r"(?=[A-Za-z0-9_.-]*(?:token|secret|password|passwd|api[_-]?key|"
    r"access[_-]?key|private[_-]?key|credential|authorization|signature))"
    r"[A-Za-z_][A-Za-z0-9_.-]*[\"']?\s*[:=]\s*)"
    r"(?P<value>\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;&]+)"
)
_SECRET_FLAG = re.compile(
    r"(?i)(?P<prefix>--(?:password|passwd|token|secret|api[_-]?key|"
    r"authorization|credential)(?:=|\s+))"
    r"(?P<value>\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;&]+)"
)
_TOKEN_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{16,}\b"),
    re.compile(
        r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{6,}"
        r"\.[A-Za-z0-9_-]{20,}\b"
    ),
    re.compile(r"\bmfa\.[A-Za-z0-9_-]{20,}\b"),
)
_URL = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_HOME_PATHS = (
    re.compile(r"(?<![\w.-])/Users/[^/\s]+"),
    re.compile(r"(?<![\w.-])/home/[^/\s]+"),
    re.compile(r"(?i)\b[A-Z]:\\Users\\[^\\\s]+"),
)
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_DIFF_PATH_HEADER = re.compile(
    r"^(?P<prefix>--- |\+\+\+ |rename from |rename to )"
    r"(?P<path>[^\t]+)(?P<suffix>\t.*)?$"
)


def redact_text(value: str, *, project_root: Path | None = None) -> str:
    redacted = _URL.sub(_redact_url, value)
    if project_root is not None:
        variants = {str(project_root), project_root.as_posix()}
        for variant in sorted(variants, key=len, reverse=True):
            if variant:
                redacted = redacted.replace(variant, "<project>")
    redacted = _AUTH_HEADER.sub(
        lambda match: (
            f"{match.group('header')}: {match.group('scheme')} <redacted>"
        ),
        redacted,
    )
    redacted = _SECRET_FLAG.sub(_redact_named_value, redacted)
    redacted = _SECRET_ASSIGNMENT.sub(_redact_named_value, redacted)
    for pattern in _TOKEN_PATTERNS:
        redacted = pattern.sub("<redacted>", redacted)
    for pattern in _HOME_PATHS:
        redacted = pattern.sub("<home>", redacted)
    return _CONTROL_CHARACTERS.sub("", redacted)


def redacted_summary(
    value: str,
    *,
    project_root: Path | None = None,
    max_chars: int = 180,
) -> str:
    if max_chars < 4:
        raise ValueError("max_chars must be at least 4")
    summary = " ".join(redact_text(value, project_root=project_root).split())
    if not summary:
        return "[non-text input]"
    if len(summary) <= max_chars:
        return summary
    return f"{summary[: max_chars - 3].rstrip()}..."


def redact_value(value: Any, *, project_root: Path | None = None) -> Any:
    if isinstance(value, str):
        return redact_text(value, project_root=project_root)
    if isinstance(value, list):
        return [redact_value(item, project_root=project_root) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_value(item, project_root=project_root) for item in value)
    if isinstance(value, dict):
        return {
            key: (
                "<redacted>"
                if isinstance(key, str) and _SECRET_NAME.search(key)
                else redact_value(item, project_root=project_root)
            )
            for key, item in value.items()
        }
    return value


def redact_diff(value: str, *, project_root: Path) -> str:
    root_variants = {
        str(project_root).rstrip("/\\"),
        project_root.as_posix().rstrip("/"),
    }
    redacted = value
    for root in sorted(root_variants, key=len, reverse=True):
        if not root:
            continue
        redacted = redacted.replace(f"{root}/", "")
        redacted = redacted.replace(f"{root}\\", "")
        redacted = redacted.replace(root, "<project>")
    lines = [_redact_diff_header(line) for line in redacted.splitlines(keepends=True)]
    return redact_text("".join(lines))


def _redact_named_value(match: re.Match[str]) -> str:
    value = match.group("value")
    replacement = "<redacted>"
    if len(value) >= 2 and value[0] in {"\"", "'"} and value[-1] == value[0]:
        replacement = f"{value[0]}<redacted>{value[0]}"
    return f"{match.group('prefix')}{replacement}"


def _redact_url(match: re.Match[str]) -> str:
    raw = match.group(0)
    suffix = ""
    while raw and raw[-1] in ".,);]":
        suffix = raw[-1] + suffix
        raw = raw[:-1]
    try:
        parsed = urlsplit(raw)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        port = parsed.port
    except ValueError:
        return "<redacted-url>" + suffix
    changed = parsed.username is not None or parsed.password is not None
    if changed:
        hostname = parsed.hostname
        if hostname is None:
            return "<redacted-url>" + suffix
        safe_host = f"[{hostname}]" if ":" in hostname else hostname
        safe_netloc = f"{safe_host}:{port}" if port is not None else safe_host
        parsed = parsed._replace(netloc=safe_netloc)
    safe_query: list[tuple[str, str]] = []
    for name, value in query:
        if _SECRET_NAME.search(name) or name.casefold() in {
            "auth",
            "code",
            "key",
            "sig",
        }:
            safe_query.append((name, "<redacted>"))
            changed = True
        else:
            safe_query.append((name, value))
    if not changed:
        return raw + suffix
    return (
        urlunsplit(
            parsed._replace(
                query=urlencode(safe_query) if query else parsed.query
            )
        )
        + suffix
    )


def _redact_diff_header(line: str) -> str:
    ending = "\n" if line.endswith("\n") else ""
    body = line[:-1] if ending else line
    if body.startswith("diff --git "):
        try:
            paths = shlex.split(body[len("diff --git ") :])
        except ValueError:
            paths = []
        if len(paths) == 2:
            safe = " ".join(shlex.quote(_safe_diff_path(path)) for path in paths)
            return f"diff --git {safe}{ending}"
    match = _DIFF_PATH_HEADER.match(body)
    if match is None:
        return line
    path = _safe_diff_path(match.group("path"))
    return (
        f"{match.group('prefix')}{path}{match.group('suffix') or ''}{ending}"
    )


def _safe_diff_path(value: str) -> str:
    if value == "/dev/null":
        return value
    path = value.strip("\"'")
    if path.startswith(("/", "\\\\")) or PureWindowsPath(path).is_absolute():
        name = path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
        return f"<outside-project>/{name or 'path'}"
    return value
