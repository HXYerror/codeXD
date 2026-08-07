from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pickle
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps, UnidentifiedImageError

from codexd.domain.content_blocks import TableBlock
from codexd.rendering.tables import RenderedTable, TableLimits, render_table

_IS_WINDOWS = os.name == "nt"
_RESOURCE: Any = None
if not _IS_WINDOWS:
    import resource as _resource_module

    _RESOURCE = _resource_module

_WINDOWS_JOB_HANDLES: list[int] = []


@dataclass(frozen=True)
class NormalizedImage:
    output_path: Path
    media_type: str
    source_sha256: str
    normalized_sha256: str
    size_bytes: int
    width: int
    height: int


class MediaWorkerError(RuntimeError):
    pass


class MediaWorker:
    def __init__(
        self,
        *,
        timeout_seconds: float = 5.0,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("media worker timeout must be positive")
        self.timeout_seconds = timeout_seconds
        self._environment = _subprocess_environment(environment or {})
        source_root = Path(__file__).resolve().parents[2]
        self._command = (
            sys.executable,
            "-I",
            "-c",
            (
                "import json, os, sys;"
                "allowed=set(json.loads(sys.argv[2]));"
                "current=dict(os.environ);"
                "os.environ.clear();"
                "os.environ.update({key: value for key, value in "
                "current.items() if key in allowed});"
                "sys.path.insert(0, sys.argv[1]);"
                "from codexd.rendering.media_worker import _subprocess_main;"
                "raise SystemExit(_subprocess_main())"
            ),
            str(source_root),
            json.dumps(sorted(self._environment)),
        )

    async def render_table(self, table: TableBlock, limits: TableLimits) -> RenderedTable:
        result = await self._run(("table", table, limits))
        if not isinstance(result, RenderedTable):
            raise MediaWorkerError("media worker returned an invalid table result")
        return result

    async def normalize_image(
        self,
        *,
        source: Path,
        output: Path,
        max_bytes: int,
        max_pixels: int,
    ) -> NormalizedImage:
        result = await self._run(
            ("image", str(source), str(output), max_bytes, max_pixels)
        )
        if not isinstance(result, NormalizedImage):
            raise MediaWorkerError("media worker returned an invalid image result")
        return result

    async def _run(self, request: tuple[Any, ...]) -> object:
        process = await asyncio.create_subprocess_exec(
            *self._command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=self._environment,
            close_fds=True,
        )
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(
                    pickle.dumps(request, protocol=pickle.HIGHEST_PROTOCOL),
                ),
                timeout=self.timeout_seconds,
            )
        except asyncio.CancelledError:
            await _terminate_process(process)
            raise
        except TimeoutError as exc:
            await _terminate_process(process)
            raise MediaWorkerError("media worker timed out") from exc
        if process.returncode != 0:
            raise MediaWorkerError(
                f"media worker exited without a result (exit code {process.returncode})"
            )
        try:
            response = pickle.loads(output)
        except (EOFError, pickle.UnpicklingError, ValueError, TypeError) as exc:
            raise MediaWorkerError("media worker returned an invalid response") from exc
        if (
            not isinstance(response, tuple)
            or len(response) != 2
            or response[0] not in {"ok", "error"}
        ):
            raise MediaWorkerError("media worker returned an invalid response")
        status, payload = response
        if status != "ok":
            raise MediaWorkerError(str(payload))
        return payload


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=2)
    except TimeoutError:
        process.kill()
        await asyncio.wait_for(process.wait(), timeout=2)


def _subprocess_main() -> int:
    try:
        request = pickle.loads(sys.stdin.buffer.read())
        if not isinstance(request, tuple) or not request:
            raise ValueError("invalid media worker request")
        response = ("ok", _execute_request(request))
    except BaseException as exc:
        response = ("error", f"{type(exc).__name__}: {str(exc)[:512]}")
    sys.stdout.buffer.write(pickle.dumps(response, protocol=pickle.HIGHEST_PROTOCOL))
    sys.stdout.buffer.flush()
    return 0


def _execute_request(request: tuple[Any, ...]) -> object:
    if request[0] == "table":
        if (
            len(request) != 3
            or not isinstance(request[1], TableBlock)
            or not isinstance(request[2], TableLimits)
        ):
            raise ValueError("invalid table render request")
        limits = request[2]
        _apply_limits(memory_mib=limits.memory_mib, cpu_seconds=10)
        return render_table(request[1], limits)
    if request[0] == "image":
        if len(request) != 5:
            raise ValueError("invalid image normalization request")
        _apply_limits(memory_mib=256, cpu_seconds=10)
        return _normalize_image(
            Path(request[1]),
            Path(request[2]),
            max_bytes=int(request[3]),
            max_pixels=int(request[4]),
        )
    if request[0] == "environment":
        return dict(os.environ)
    raise ValueError("unknown media worker request")


def _subprocess_environment(environment: Mapping[str, str]) -> dict[str, str]:
    safe: dict[str, str] = {}
    for key, value in environment.items():
        if not key or "=" in key or "\x00" in key or "\x00" in value:
            raise ValueError("media worker environment contains an invalid entry")
        safe[key] = value
    if os.name == "nt":
        for key in ("SYSTEMROOT", "WINDIR"):
            if key in os.environ and key not in safe:
                safe[key] = os.environ[key]
    return safe


def _apply_limits(*, memory_mib: int, cpu_seconds: int) -> None:
    if memory_mib < 1 or cpu_seconds < 1:
        raise ValueError("media worker resource limits must be positive")
    if _IS_WINDOWS:
        _apply_windows_job_limits(
            memory_bytes=memory_mib * 1024 * 1024,
            cpu_seconds=cpu_seconds,
        )
        return
    if _RESOURCE is None:
        raise RuntimeError("media worker resource containment is unavailable")
    memory_bytes = memory_mib * 1024 * 1024
    for name in ("RLIMIT_AS", "RLIMIT_DATA"):
        limit = getattr(_RESOURCE, name, None)
        if limit is None:
            continue
        try:
            _RESOURCE.setrlimit(limit, (memory_bytes, memory_bytes))
        except (OSError, ValueError):
            continue
    _RESOURCE.setrlimit(_RESOURCE.RLIMIT_CPU, (cpu_seconds, cpu_seconds + 1))


def _apply_windows_job_limits(*, memory_bytes: int, cpu_seconds: int) -> None:
    import ctypes
    from ctypes import wintypes

    ctypes_api: Any = ctypes

    class IOCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimitInformation),
            ("IoInfo", IOCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes_api.WinDLL("kernel32", use_last_error=True)
    create_job = kernel32.CreateJobObjectW
    create_job.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
    create_job.restype = wintypes.HANDLE
    set_information = kernel32.SetInformationJobObject
    set_information.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    set_information.restype = wintypes.BOOL
    assign_process = kernel32.AssignProcessToJobObject
    assign_process.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
    assign_process.restype = wintypes.BOOL
    current_process = kernel32.GetCurrentProcess
    current_process.restype = wintypes.HANDLE
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)

    job = create_job(None, None)
    if not job:
        raise OSError(ctypes_api.get_last_error(), "CreateJobObjectW failed")
    limits = ExtendedLimitInformation()
    limits.BasicLimitInformation.PerProcessUserTimeLimit = cpu_seconds * 10_000_000
    limits.BasicLimitInformation.LimitFlags = 0x2 | 0x100 | 0x2000
    limits.ProcessMemoryLimit = memory_bytes
    if not set_information(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
        error = ctypes_api.get_last_error()
        close_handle(job)
        raise OSError(error, "SetInformationJobObject failed")
    if not assign_process(job, current_process()):
        error = ctypes_api.get_last_error()
        close_handle(job)
        raise OSError(error, "AssignProcessToJobObject failed")
    _WINDOWS_JOB_HANDLES.append(int(job))


def _normalize_image(
    source: Path,
    output: Path,
    *,
    max_bytes: int,
    max_pixels: int,
) -> NormalizedImage:
    if source.is_symlink() or not source.is_file():
        raise ValueError("image source must be a regular non-symlink file")
    source_size = source.stat().st_size
    if source_size <= 0 or source_size > max_bytes:
        raise ValueError("image source exceeds byte limit")
    source_hash = _sha256(source)
    Image.MAX_IMAGE_PIXELS = max(1, max_pixels // 2)
    try:
        with Image.open(source) as image:
            if image.width * image.height > max_pixels:
                raise ValueError("image exceeds pixel limit")
            if getattr(image, "is_animated", False):
                image.seek(0)
            image.load()
            normalized = ImageOps.exif_transpose(image)
            target_mode = (
                "RGBA"
                if "A" in normalized.getbands() or "transparency" in normalized.info
                else "RGB"
            )
            if normalized.mode != target_mode:
                normalized = normalized.convert(target_mode)
            output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with Image.new(normalized.mode, normalized.size) as clean:
                clean.paste(normalized)
                clean.save(output, format="PNG", optimize=True)
    except (UnidentifiedImageError, Image.DecompressionBombError) as exc:
        raise ValueError("image decode failed") from exc
    size = output.stat().st_size
    if size > max_bytes:
        output.unlink(missing_ok=True)
        raise ValueError("normalized image exceeds byte limit")
    if os.name != "nt":
        output.chmod(0o600)
    with Image.open(output) as result:
        width, height = result.size
        if result.getexif() or result.info:
            output.unlink(missing_ok=True)
            raise ValueError("normalized image retained metadata")
    return NormalizedImage(
        output_path=output,
        media_type="image/png",
        source_sha256=source_hash,
        normalized_sha256=_sha256(output),
        size_bytes=size,
        width=width,
        height=height,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
