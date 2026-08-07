from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO

from codexd.errors import ConflictError


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
        file = self.path.open("a+b")
        if os.name != "nt":
            self.path.chmod(0o600)
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
    import msvcrt

    def _lock(file: BinaryIO) -> None:
        file.seek(0, os.SEEK_END)
        if file.tell() == 0:
            file.write(b"\0")
            file.flush()
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]

    def _unlock(file: BinaryIO) -> None:
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]

else:
    import fcntl

    def _lock(file: BinaryIO) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(file: BinaryIO) -> None:
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)
