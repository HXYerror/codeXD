from __future__ import annotations

import importlib
import os
import stat
from pathlib import Path
from typing import Any, BinaryIO

from codexd.errors import ConflictError, SecurityError


class InstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._file: BinaryIO | None = None

    def __enter__(self) -> InstanceLock:
        self.acquire()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.release()

    def acquire(self) -> None:
        if self._file is not None:
            return
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_APPEND | os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise SecurityError("instance lock path could not be opened safely") from exc
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            raise SecurityError("instance lock path is not a regular file")
        file = os.fdopen(descriptor, "a+b")
        try:
            _lock(file)
        except OSError as exc:
            file.close()
            raise ConflictError("another codexD process holds the instance lock") from exc
        self._file = file

    def release(self) -> None:
        if self._file is None:
            return
        try:
            _unlock(self._file)
        finally:
            self._file.close()
            self._file = None


if os.name == "nt":
    msvcrt: Any = importlib.import_module("msvcrt")

    def _lock(file: BinaryIO) -> None:
        file.seek(0, os.SEEK_END)
        if file.tell() == 0:
            file.write(b"\0")
            file.flush()
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock(file: BinaryIO) -> None:
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)

else:
    fcntl: Any = importlib.import_module("fcntl")

    def _lock(file: BinaryIO) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(file: BinaryIO) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)
