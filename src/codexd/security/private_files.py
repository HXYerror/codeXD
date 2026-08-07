from __future__ import annotations

import os
import stat
from pathlib import Path


class PrivateFileSecurityUnavailable(OSError):
    """The platform cannot enforce codexD's owner-only file contract."""


def private_file_security_supported() -> bool:
    """Return whether the owner-only contract has a verified implementation."""

    # Windows needs an owner-only DACL plus no-reparse handle semantics. Until
    # both are implemented and verified together, accepting attachment bytes
    # would advertise a security property that codexD cannot provide.
    return _platform_name() != "nt"


def legacy_image_ingestion_supported() -> bool:
    """Return whether the pre-existing bounded Windows image path is available.

    This is deliberately not an owner-only storage capability.  It exists only
    so Discord raster images keep their legacy Windows behavior; opaque file
    storage must continue to use :func:`private_file_security_supported`.
    """

    return _platform_name() == "nt"


def ensure_private_directory(path: Path) -> None:
    _require_supported()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise OSError("private storage path is not a regular directory")
    path.chmod(0o700)
    validate_private_directory_metadata(path.lstat())


def secure_private_file(path: Path) -> None:
    _require_supported()
    path.chmod(0o600)
    validate_private_file_metadata(path.lstat())


def validate_private_directory_metadata(metadata: os.stat_result) -> None:
    _require_supported()
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise OSError("private directory ownership or mode is unsafe")


def validate_private_file_metadata(metadata: os.stat_result) -> None:
    _require_supported()
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise OSError("private file ownership or mode is unsafe")


def _require_supported() -> None:
    if not private_file_security_supported():
        raise PrivateFileSecurityUnavailable(
            "owner-only private file security is unavailable on this platform"
        )


def _platform_name() -> str:
    return os.name
