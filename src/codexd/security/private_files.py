from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Protocol, cast


class _WindowsBackend(Protocol):
    def available(self) -> bool: ...

    def ensure_private_directory(self, path: Path) -> None: ...

    def secure_private_file(self, path: Path) -> None: ...

    def validate_private_directory(self, path: Path) -> None: ...

    def validate_private_file(self, path: Path) -> None: ...

    def validate_directory_no_reparse(self, path: Path) -> None: ...

    def validate_file_no_reparse(self, path: Path) -> None: ...

    def open_file_no_reparse(
        self,
        path: Path,
        *,
        require_private: bool,
        deny_write_delete: bool,
    ) -> int: ...

    def validate_private_file_descriptor(self, descriptor: int) -> None: ...


class PrivateFileSecurityUnavailable(OSError):
    """The platform cannot enforce codexD's owner-only file contract."""


def private_file_security_supported() -> bool:
    """Return whether the owner-only contract has a verified implementation."""

    if _platform_name() != "nt":
        return True
    # A monkeypatched platform facade must not attempt to load WinDLL on a
    # non-Windows host. Native Windows uses the stdlib ctypes backend.
    if os.name != "nt":
        return False
    try:
        return _windows_backend().available()
    except (AttributeError, OSError, TypeError, ValueError):
        return False


def legacy_image_ingestion_supported() -> bool:
    """Return whether the pre-existing bounded Windows image path is available.

    This is deliberately not an owner-only storage capability.  It exists only
    so Discord raster images keep their legacy Windows behavior; opaque file
    storage must continue to use :func:`private_file_security_supported`.
    """

    return _platform_name() == "nt"


def ensure_private_directory(path: Path) -> None:
    _require_supported()
    if _platform_name() == "nt":
        _windows_backend().ensure_private_directory(path)
        return
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise OSError("private storage path is not a regular directory")
    path.chmod(0o700)
    validate_private_directory_metadata(path.lstat())


def secure_private_file(path: Path) -> None:
    _require_supported()
    if _platform_name() == "nt":
        _windows_backend().secure_private_file(path)
        return
    path.chmod(0o600)
    validate_private_file_metadata(path.lstat())


def validate_private_directory(path: Path) -> None:
    _require_supported()
    if _platform_name() == "nt":
        _windows_backend().validate_private_directory(path)
        return
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise OSError("private storage path is not a regular directory")
    validate_private_directory_metadata(metadata)


def validate_private_file(path: Path) -> None:
    _require_supported()
    if _platform_name() == "nt":
        _windows_backend().validate_private_file(path)
        return
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise OSError("private storage path is not a regular file")
    validate_private_file_metadata(metadata)


def validate_directory_no_reparse(path: Path) -> None:
    if _platform_name() == "nt":
        _require_supported()
        _windows_backend().validate_directory_no_reparse(path)
        return
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise OSError("storage path is not a regular directory")


def validate_file_no_reparse(path: Path) -> None:
    if _platform_name() == "nt":
        _require_supported()
        _windows_backend().validate_file_no_reparse(path)
        return
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise OSError("storage path is not a regular file")


def open_file_no_reparse(
    path: Path,
    *,
    require_private: bool,
    deny_write_delete: bool,
) -> int:
    if _platform_name() == "nt":
        _require_supported()
        return _windows_backend().open_file_no_reparse(
            path,
            require_private=require_private,
            deny_write_delete=deny_write_delete,
        )
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0))
    try:
        if require_private:
            validate_private_file_descriptor(descriptor)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def validate_private_file_descriptor(descriptor: int) -> None:
    _require_supported()
    if _platform_name() == "nt":
        _windows_backend().validate_private_file_descriptor(descriptor)
        return
    validate_private_file_metadata(os.fstat(descriptor))


def validate_private_directory_metadata(metadata: os.stat_result) -> None:
    _require_supported()
    if _platform_name() == "nt":
        raise PrivateFileSecurityUnavailable(
            "Windows private directory validation requires a path handle"
        )
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise OSError("private directory ownership or mode is unsafe")


def validate_private_file_metadata(metadata: os.stat_result) -> None:
    _require_supported()
    if _platform_name() == "nt":
        raise PrivateFileSecurityUnavailable(
            "Windows private file validation requires a path handle"
        )
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise OSError("private file ownership or mode is unsafe")


def _require_supported() -> None:
    if not private_file_security_supported():
        raise PrivateFileSecurityUnavailable(
            "owner-only private file security is unavailable on this platform"
        )


def _platform_name() -> str:
    return os.name


def _windows_backend() -> _WindowsBackend:
    from codexd.security import windows_private_files

    return cast(_WindowsBackend, windows_private_files)
