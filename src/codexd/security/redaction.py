from __future__ import annotations

import base64
import binascii
import re
import shlex
import unicodedata
from pathlib import Path, PureWindowsPath
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_SECRET_NAME = re.compile(
    r"(?:token|secret|password|passwd|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|credential|authorization|signature)",
    re.IGNORECASE,
)
_ASSIGNMENT_OPERATOR = r"(?::=|\+=|-=|\*=|/=|%=|\?=|\.=|=|:)"
_AUTH_HEADER = re.compile(
    r"(?i)\b(?P<header>authorization|proxy-authorization)\s*:\s*"
    r"(?P<scheme>[A-Za-z][A-Za-z0-9_-]*)\s+(?P<value>\S+)"
)
_COOKIE_HEADER = re.compile(
    r"(?im)\b(?P<header>set-cookie|cookie)\s*:\s*(?P<value>[^\r\n]*)"
)
_STANDALONE_BEARER = re.compile(
    r"\b(?P<scheme>(?i:bearer))\s+"
    r"(?P<value>[A-Za-z0-9._~+/=-]+)"
)
_STANDALONE_BASIC = re.compile(
    r"\b(?P<scheme>(?i:basic))\s+"
    r"(?P<value>[A-Za-z0-9+/]{4,}={0,2})(?![A-Za-z0-9+/=])"
)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(?P<prefix>[\"']?"
    r"(?=[A-Za-z0-9_.-]*(?:token|secret|password|passwd|api[_-]?key|"
    r"access[_-]?key|private[_-]?key|credential|authorization|signature))"
    rf"[A-Za-z_][A-Za-z0-9_.-]*[\"']?\s*{_ASSIGNMENT_OPERATOR}\s*)"
    r"(?P<value>\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;&]+)"
)
_SECRET_FLAG = re.compile(
    r"(?i)(?P<prefix>--(?:password|passwd|token|secret|api[_-]?key|"
    r"authorization|credential)(?:=|\s+))"
    r"(?P<value>\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;&]+)"
)
_SESSION_COOKIE_ASSIGNMENT = re.compile(
    r"(?i)(?P<prefix>(?<![A-Za-z0-9_.-])[\"']?"
    r"(?:PHPSESSID|connect\.sid|session(?:[._-]?(?:id|key|token|cookie))?|"
    r"(?:auth[._-]?)?cookies?(?:[._-]?(?:id|key|token|value|session))?)"
    rf"[\"']?\s*{_ASSIGNMENT_OPERATOR}\s*)"
    r"(?P<value>\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\s,;&]+)"
)
_TOKEN_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{16,}\b"),
    re.compile(
        r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{6,}"
        r"\.[A-Za-z0-9_-]{20,}\b"
    ),
    re.compile(r"\bmfa\.[A-Za-z0-9_-]{20,}\b"),
    re.compile(
        r"(?<![A-Za-z0-9_])ghp_[A-Za-z0-9]{36,}(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])github_pat_[A-Za-z0-9]{22}_"
        r"[A-Za-z0-9]{59,}(?![A-Za-z0-9_])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9-])(?:xox[baprs]|xapp|xoxe(?:\.xox[bp])?)-"
        r"[A-Za-z0-9-]{10,}[A-Za-z0-9](?![A-Za-z0-9-])"
    ),
    re.compile(r"(?<![A-Za-z0-9])(?:AKIA|ASIA)[A-Z0-9]{16,}(?![A-Za-z0-9])"),
    re.compile(
        r"(?<![A-Za-z0-9_-])AIza[A-Za-z0-9_-]{35,}(?![A-Za-z0-9_-])"
    ),
    re.compile(
        r"(?<![A-Za-z0-9_])(?:sk|rk)_(?:live|test)_"
        r"[A-Za-z0-9]{16,}(?![A-Za-z0-9_])"
    ),
)
_URL = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_HOME_PATHS = (
    re.compile(r"(?<![\w.-])/Users/[^/\s]+"),
    re.compile(r"(?<![\w.-])/home/[^/\s]+"),
    re.compile(r"(?i)\b[A-Z]:\\Users\\[^\\\s]+"),
)
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
_DEFAULT_IGNORABLE_RANGES = (
    (0x034F, 0x034F),
    (0x115F, 0x1160),
    (0x17B4, 0x17B5),
    (0x180B, 0x180F),
    (0x2065, 0x2065),
    (0x2800, 0x2800),
    (0x3164, 0x3164),
    (0xFE00, 0xFE0F),
    (0xFFA0, 0xFFA0),
    (0xFFF0, 0xFFF8),
    (0xE0000, 0xE0FFF),
)
_DISCORD_ENTITY_MENTION = re.compile(r"<(?:@!?\d+|@&\d+|#\d+)>")
_DISCORD_BROADCAST_MENTION = re.compile(r"@(?:everyone|here)\b", re.IGNORECASE)
_NON_TEXT_SUMMARY = "[non-text input]"
_THREAD_TITLE_SUMMARY_MAX_CHARS = 72
_DIFF_PATH_HEADER = re.compile(
    r"^(?P<prefix>--- |\+\+\+ |rename from |rename to )"
    r"(?P<path>[^\t]+)(?P<suffix>\t.*)?$"
)


def redact_text(value: str, *, project_root: Path | None = None) -> str:
    redacted = _strip_unsafe_unicode_controls(value)
    redacted = _URL.sub(_redact_url, redacted)
    if project_root is not None:
        variants = {
            _strip_unsafe_unicode_controls(str(project_root)),
            _strip_unsafe_unicode_controls(project_root.as_posix()),
        }
        for variant in sorted(variants, key=len, reverse=True):
            if variant:
                redacted = redacted.replace(variant, "<project>")
    redacted = _AUTH_HEADER.sub(
        lambda match: (
            f"{match.group('header')}: {match.group('scheme')} <redacted>"
        ),
        redacted,
    )
    redacted = _COOKIE_HEADER.sub(
        lambda match: f"{match.group('header')}: <redacted>",
        redacted,
    )
    redacted = _STANDALONE_BEARER.sub(
        lambda match: f"{match.group('scheme')} <redacted>",
        redacted,
    )
    redacted = _STANDALONE_BASIC.sub(
        _redact_basic_auth,
        redacted,
    )
    redacted = _SECRET_FLAG.sub(_redact_named_value, redacted)
    redacted = _SECRET_ASSIGNMENT.sub(_redact_named_value, redacted)
    redacted = _SESSION_COOKIE_ASSIGNMENT.sub(_redact_named_value, redacted)
    for pattern in _TOKEN_PATTERNS:
        redacted = pattern.sub("<redacted>", redacted)
    for pattern in _HOME_PATHS:
        redacted = pattern.sub("<home>", redacted)
    return redacted


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
        return _NON_TEXT_SUMMARY
    return _truncate_summary(summary, max_chars)


def safe_thread_title_summary(
    value: str,
    *,
    project_root: Path | None = None,
    has_image_attachment: bool = False,
) -> str:
    if not value.strip():
        return "图片任务" if has_image_attachment else "新任务"
    mention_stripped = _strip_discord_mentions(value)
    redacted = redact_text(mention_stripped, project_root=project_root)
    safe_summary = " ".join(_strip_discord_mentions(redacted).split())
    if not _has_visible_character(safe_summary):
        return "新任务"
    truncated = _truncate_summary(safe_summary, _THREAD_TITLE_SUMMARY_MAX_CHARS)
    redacted_again = redact_text(truncated, project_root=project_root)
    bounded_again = _truncate_summary(
        " ".join(redacted_again.split()),
        _THREAD_TITLE_SUMMARY_MAX_CHARS,
    )
    final_summary = " ".join(
        _strip_discord_mentions(
            _strip_unsafe_unicode_controls(bounded_again)
        ).split()
    )
    return final_summary if _has_visible_character(final_summary) else "新任务"


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
                if isinstance(key, str)
                and _SECRET_NAME.search(_strip_unsafe_unicode_controls(key))
                else redact_value(item, project_root=project_root)
            )
            for key, item in value.items()
        }
    return value


def redact_diff(value: str, *, project_root: Path) -> str:
    root_variants = {
        _strip_unsafe_unicode_controls(str(project_root).rstrip("/\\")),
        _strip_unsafe_unicode_controls(project_root.as_posix().rstrip("/")),
    }
    redacted = _strip_unsafe_unicode_controls(value)
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


def _strip_discord_mentions(value: str) -> str:
    without_entities = _DISCORD_ENTITY_MENTION.sub(" ", value)
    return _DISCORD_BROADCAST_MENTION.sub(" ", without_entities)


def _truncate_summary(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return f"{value[: max_chars - 3].rstrip()}..."


def _strip_unsafe_unicode_controls(value: str) -> str:
    candidates = "".join(
        character
        for character in _CONTROL_CHARACTERS.sub("", value)
        if character == "\u200d"
        or _variation_selector(character)
        or not _default_ignorable(character)
    )
    return "".join(
        character
        for index, character in enumerate(candidates)
        if (
            not _variation_selector(character)
            or (index > 0 and _emoji_base(candidates[index - 1]))
        )
        and (
            character != "\u200d" or _valid_emoji_joiner(candidates, index)
        )
    )


def _valid_emoji_joiner(value: str, index: int) -> bool:
    left = index - 1
    while left >= 0 and (
        _variation_selector(value[left]) or _emoji_modifier(value[left])
    ):
        left -= 1
    return (
        left >= 0
        and _emoji_base(value[left])
        and index + 1 < len(value)
        and _emoji_base(value[index + 1])
    )


def _emoji_base(value: str) -> bool:
    return unicodedata.category(value) == "So" or value in "#*0123456789"


def _default_ignorable(value: str) -> bool:
    codepoint = ord(value)
    return unicodedata.category(value) == "Cf" or any(
        start <= codepoint <= end for start, end in _DEFAULT_IGNORABLE_RANGES
    )


def _variation_selector(value: str) -> bool:
    return "\ufe00" <= value <= "\ufe0f" or "\U000e0100" <= value <= "\U000e01ef"


def _emoji_modifier(value: str) -> bool:
    return "\U0001f3fb" <= value <= "\U0001f3ff"


def _has_visible_character(value: str) -> bool:
    return any(
        unicodedata.category(character)[0] in {"L", "N", "P", "S"}
        for character in value
    )


def _redact_basic_auth(match: re.Match[str]) -> str:
    scheme = match.group("scheme")
    value = match.group("value")
    if not _is_basic_credential(value):
        return match.group(0)
    return f"{scheme} <redacted>"


def _is_basic_credential(value: str) -> bool:
    if re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", value) is None:
        return False
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            validate=True,
        )
    except (binascii.Error, ValueError):
        return False
    return b":" in decoded


def _redact_url(match: re.Match[str]) -> str:
    raw = match.group(0)
    suffix = ""
    while raw and raw[-1] in ".,);]":
        suffix = raw[-1] + suffix
        raw = raw[:-1]
    try:
        parsed = urlsplit(raw)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        fragment = parse_qsl(parsed.fragment, keep_blank_values=True)
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
    safe_fragment: list[tuple[str, str]] = []
    for name, value in fragment:
        if _SECRET_NAME.search(name) or name.casefold() in {
            "auth",
            "code",
            "key",
            "sig",
        }:
            safe_fragment.append((name, "<redacted>"))
            changed = True
        else:
            safe_fragment.append((name, value))
    if not changed:
        return raw + suffix
    return (
        urlunsplit(
            parsed._replace(
                query=urlencode(safe_query) if query else parsed.query,
                fragment=urlencode(safe_fragment) if fragment else parsed.fragment,
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
