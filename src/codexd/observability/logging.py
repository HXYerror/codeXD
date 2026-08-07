from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from codexd.security.redaction import redact_text, redact_value


class JsonFormatter(logging.Formatter):
    _STANDARD = frozenset(
        {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
            "taskName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_text(record.getMessage())[:4096],
        }
        for name, value in record.__dict__.items():
            if name not in self._STANDARD and _safe_value(value):
                payload[name] = redact_value({name: value})[name]
        if record.exc_info:
            payload["exception"] = redact_text(
                self.formatException(record.exc_info)
            )[:8192]
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def configure_logging(path: Path, *, level: int = logging.INFO) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    if os.name != "nt":
        path.chmod(0o600)


def _safe_value(value: object) -> bool:
    if value is None or isinstance(value, (bool, int, float)):
        return True
    if isinstance(value, str):
        return len(value) <= 512
    return False
