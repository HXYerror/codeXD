from __future__ import annotations

import os
import stat
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from codexd.errors import SecurityError
from codexd.security import private_files


@dataclass(frozen=True)
class AppPaths:
    data_dir: Path
    log_dir: Path

    @property
    def database(self) -> Path:
        return self.data_dir / "codexd.sqlite3"

    @property
    def attachments(self) -> Path:
        return self.data_dir / "attachments"

    @property
    def diagnostics(self) -> Path:
        return self.data_dir / "diagnostics"

    @property
    def backups(self) -> Path:
        return self.data_dir / "backups"

    @property
    def health(self) -> Path:
        return self.data_dir / "health.json"

    @property
    def instance_lock(self) -> Path:
        return self.data_dir / "instance.lock"

    @property
    def log_file(self) -> Path:
        return self.log_dir / "codexd.jsonl"

    def ensure(self) -> None:
        for path in (
            self.data_dir,
            self.log_dir,
            self.attachments,
            self.diagnostics,
            self.backups,
        ):
            if path.is_symlink():
                raise SecurityError(f"codexD data path must not be a symlink: {path}")
            try:
                if os.name == "nt":
                    private_files.ensure_private_directory(path)
                else:
                    path.mkdir(mode=0o700, parents=True, exist_ok=True)
            except OSError as exc:
                raise SecurityError("codexD data path cannot be secured") from exc
            if not stat.S_ISDIR(path.lstat().st_mode):
                raise SecurityError(f"codexD data path is not a directory: {path}")
            if os.name != "nt":
                path.chmod(0o700)


def default_paths(environment: Mapping[str, str] | None = None) -> AppPaths:
    env = os.environ if environment is None else environment
    home = Path(env.get("HOME") or env.get("USERPROFILE") or Path.home()).expanduser()

    data_override = env.get("CODEXD_DATA_DIR")
    log_override = env.get("CODEXD_LOG_DIR")
    if data_override:
        data_dir = Path(data_override).expanduser()
    elif sys.platform == "darwin":
        data_dir = home / "Library" / "Application Support" / "codexD"
    elif os.name == "nt":
        data_dir = Path(env.get("LOCALAPPDATA", str(home / "AppData" / "Local"))) / "codexD"
    else:
        data_dir = Path(env.get("XDG_DATA_HOME", str(home / ".local" / "share"))) / "codexD"

    if log_override:
        log_dir = Path(log_override).expanduser()
    elif sys.platform == "darwin":
        log_dir = home / "Library" / "Logs" / "codexD"
    elif os.name == "nt":
        log_dir = data_dir / "logs"
    else:
        log_dir = Path(env.get("XDG_STATE_HOME", str(home / ".local" / "state"))) / "codexD"

    return AppPaths(data_dir=data_dir.resolve(), log_dir=log_dir.resolve())
