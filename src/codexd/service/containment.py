from __future__ import annotations

import ctypes
import os
from dataclasses import dataclass
from typing import Any

from codexd.errors import SecurityError

_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000


class ProcessContainment:
    def __enter__(self) -> ProcessContainment:
        return self

    def __exit__(
        self,
        _exc_type: object,
        _exc: object,
        _traceback: object,
    ) -> None:
        self.close()

    def close(self) -> None:
        return None


@dataclass
class _WindowsJobContainment(ProcessContainment):
    kernel32: Any
    handle: int

    def close(self) -> None:
        # Closing the last KILL_ON_JOB_CLOSE handle while this process is still
        # running would terminate the daemon itself. The kernel closes this
        # process-owned handle during process teardown and then reaps descendants.
        return None


def create_process_containment() -> ProcessContainment:
    if os.name != "nt":
        return ProcessContainment()

    class BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", ctypes.c_uint32),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", ctypes.c_uint32),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", ctypes.c_uint32),
            ("SchedulingClass", ctypes.c_uint32),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]

    class ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimitInformation),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL(
        "kernel32",
        use_last_error=True,
    )
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    kernel32.CreateJobObjectW.restype = ctypes.c_void_p
    kernel32.SetInformationJobObject.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    ]
    kernel32.SetInformationJobObject.restype = ctypes.c_int
    kernel32.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    kernel32.AssignProcessToJobObject.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    handle_value = kernel32.CreateJobObjectW(None, None)
    if not handle_value:
        raise SecurityError(
            "cannot create Windows containment Job Object: "
            f"{ctypes.get_last_error()}"
        )
    handle = int(handle_value)
    limits = ExtendedLimitInformation()
    limits.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    configured = kernel32.SetInformationJobObject(
        ctypes.c_void_p(handle),
        _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    )
    if not configured:
        error = ctypes.get_last_error()
        kernel32.CloseHandle(ctypes.c_void_p(handle))
        raise SecurityError(f"cannot configure Windows Job Object: {error}")
    assigned = kernel32.AssignProcessToJobObject(
        ctypes.c_void_p(handle),
        kernel32.GetCurrentProcess(),
    )
    if not assigned:
        error = ctypes.get_last_error()
        kernel32.CloseHandle(ctypes.c_void_p(handle))
        raise SecurityError(
            f"cannot assign codexD to Windows Job Object: {error}"
        )
    return _WindowsJobContainment(kernel32=kernel32, handle=handle)
