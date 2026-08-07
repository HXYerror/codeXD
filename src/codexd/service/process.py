from __future__ import annotations

import os
from dataclasses import dataclass

import psutil


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    start_token: str


def current_process_identity() -> ProcessIdentity:
    process = psutil.Process(os.getpid())
    return ProcessIdentity(process.pid, _token(process.create_time()))


def process_matches(pid: int, start_token: str) -> bool:
    try:
        process = psutil.Process(pid)
        return process.is_running() and _token(process.create_time()) == start_token
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
        return False


def _token(create_time: float) -> str:
    return f"{create_time:.6f}"

