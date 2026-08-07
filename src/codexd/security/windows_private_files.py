"""Native Windows owner-only storage and no-reparse file handles.

This module is imported lazily by :mod:`codexd.security.private_files`.  It uses
only Windows APIs from the standard library so an existing codexD virtual
environment does not need an extra package before ordinary attachments become
available after an upgrade.
"""

from __future__ import annotations

import ctypes
import importlib
import os
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_TOKEN_QUERY = 0x0008
_TOKEN_USER = 1
_ERROR_INSUFFICIENT_BUFFER = 122

_GENERIC_READ = 0x80000000
_READ_CONTROL = 0x00020000
_WRITE_DAC = 0x00040000
_WRITE_OWNER = 0x00080000
_FILE_READ_ATTRIBUTES = 0x0080
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_FILE_SHARE_DELETE = 0x00000004
_OPEN_EXISTING = 3
_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_ALL_ACCESS = 0x001F01FF

_OWNER_SECURITY_INFORMATION = 0x00000001
_DACL_SECURITY_INFORMATION = 0x00000004
_PROTECTED_DACL_SECURITY_INFORMATION = 0x80000000
_SE_FILE_OBJECT = 1
_SE_DACL_PROTECTED = 0x1000

_ACL_REVISION = 2
_ACL_SIZE_INFORMATION = 2
_ACCESS_ALLOWED_ACE_TYPE = 0
_OBJECT_INHERIT_ACE = 0x01
_CONTAINER_INHERIT_ACE = 0x02
_INHERIT_ONLY_ACE = 0x08
_INHERITED_ACE = 0x10
_INHERITANCE_FLAGS = (
    _OBJECT_INHERIT_ACE
    | _CONTAINER_INHERIT_ACE
    | _INHERIT_ONLY_ACE
    | _INHERITED_ACE
)


class _ACL(ctypes.Structure):
    _fields_ = [
        ("AclRevision", wintypes.BYTE),
        ("Sbz1", wintypes.BYTE),
        ("AclSize", wintypes.WORD),
        ("AceCount", wintypes.WORD),
        ("Sbz2", wintypes.WORD),
    ]


class _ACEHeader(ctypes.Structure):
    _fields_ = [
        ("AceType", wintypes.BYTE),
        ("AceFlags", wintypes.BYTE),
        ("AceSize", wintypes.WORD),
    ]


class _AccessAllowedACE(ctypes.Structure):
    _fields_ = [
        ("Header", _ACEHeader),
        ("Mask", wintypes.DWORD),
        ("SidStart", wintypes.DWORD),
    ]


class _ACLSizeInformation(ctypes.Structure):
    _fields_ = [
        ("AceCount", wintypes.DWORD),
        ("AclBytesInUse", wintypes.DWORD),
        ("AclBytesFree", wintypes.DWORD),
    ]


class _SIDAndAttributes(ctypes.Structure):
    _fields_ = [
        ("Sid", ctypes.c_void_p),
        ("Attributes", wintypes.DWORD),
    ]


class _TokenUser(ctypes.Structure):
    _fields_ = [("User", _SIDAndAttributes)]


class _ByHandleFileInformation(ctypes.Structure):
    _fields_ = [
        ("FileAttributes", wintypes.DWORD),
        ("CreationTime", wintypes.FILETIME),
        ("LastAccessTime", wintypes.FILETIME),
        ("LastWriteTime", wintypes.FILETIME),
        ("VolumeSerialNumber", wintypes.DWORD),
        ("FileSizeHigh", wintypes.DWORD),
        ("FileSizeLow", wintypes.DWORD),
        ("NumberOfLinks", wintypes.DWORD),
        ("FileIndexHigh", wintypes.DWORD),
        ("FileIndexLow", wintypes.DWORD),
    ]


@dataclass(frozen=True)
class _API:
    kernel32: Any
    advapi32: Any


@dataclass(frozen=True)
class _SIDBuffer:
    storage: Any
    pointer: int


@dataclass(frozen=True)
class _ACE:
    kind: int
    flags: int
    mask: int
    sid: str


@dataclass(frozen=True)
class _SecuritySnapshot:
    owner_sid: str
    dacl_protected: bool
    aces: tuple[_ACE, ...]


_CACHED_API: _API | None = None


def available() -> bool:
    """Return whether the required native APIs and current-user SID are usable."""

    if os.name != "nt":
        return False
    try:
        _current_user_sid(_api())
    except (AttributeError, OSError, TypeError, ValueError):
        return False
    return True


def ensure_private_directory(path: Path) -> None:
    absolute = _absolute_local_path(path)
    current = Path(absolute.anchor)
    _validate_directory_handle_path(current)
    for part in absolute.parts[1:]:
        current /= part
        created = False
        try:
            current.mkdir()
            created = True
        except FileExistsError:
            pass
        _validate_directory_handle_path(current)
        if created:
            _secure_path(current, directory=True)
    _secure_path(absolute, directory=True)


def secure_private_file(path: Path) -> None:
    absolute = _absolute_local_path(path)
    _validate_ancestor_directories(absolute.parent)
    _secure_path(absolute, directory=False)


def validate_private_directory(path: Path) -> None:
    absolute = _absolute_local_path(path)
    _validate_ancestor_directories(absolute.parent)
    handle = _open_path(
        absolute,
        access=_READ_CONTROL | _FILE_READ_ATTRIBUTES,
        share=_FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        directory=True,
    )
    try:
        _validate_owner_only(handle, directory=True)
    finally:
        _close_handle(handle)


def validate_private_file(path: Path) -> None:
    absolute = _absolute_local_path(path)
    _validate_ancestor_directories(absolute.parent)
    handle = _open_path(
        absolute,
        access=_READ_CONTROL | _FILE_READ_ATTRIBUTES,
        share=_FILE_SHARE_READ,
        directory=False,
    )
    try:
        _validate_owner_only(handle, directory=False)
    finally:
        _close_handle(handle)


def validate_directory_no_reparse(path: Path) -> None:
    _validate_directory_handle_path(_absolute_local_path(path))


def validate_file_no_reparse(path: Path) -> None:
    absolute = _absolute_local_path(path)
    _validate_ancestor_directories(absolute.parent)
    handle = _open_path(
        absolute,
        access=_FILE_READ_ATTRIBUTES,
        share=_FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        directory=False,
    )
    _close_handle(handle)


def open_file_no_reparse(
    path: Path,
    *,
    require_private: bool,
    deny_write_delete: bool,
) -> int:
    """Open a regular file without following reparse points and return a CRT fd.

    With ``deny_write_delete`` the retained handle shares only reads.  Windows
    therefore enforces the provider-lifetime lease: other handles cannot open
    the artifact for writes or deletion until codexD closes the returned fd.
    """

    absolute = _absolute_local_path(path)
    _validate_ancestor_directories(absolute.parent)
    access = _GENERIC_READ | _FILE_READ_ATTRIBUTES
    if require_private:
        access |= _READ_CONTROL
    share = _FILE_SHARE_READ
    if not deny_write_delete:
        share |= _FILE_SHARE_WRITE | _FILE_SHARE_DELETE
    handle = _open_path(
        absolute,
        access=access,
        share=share,
        directory=False,
    )
    try:
        if require_private:
            _validate_owner_only(handle, directory=False)
        msvcrt: Any = importlib.import_module("msvcrt")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        descriptor = int(msvcrt.open_osfhandle(handle, flags))
    except BaseException:
        _close_handle(handle)
        raise
    return descriptor


def validate_private_file_descriptor(descriptor: int) -> None:
    msvcrt: Any = importlib.import_module("msvcrt")
    handle = int(msvcrt.get_osfhandle(descriptor))
    _validate_handle_kind(handle, directory=False)
    _validate_owner_only(handle, directory=False)


def _api() -> _API:
    global _CACHED_API
    if _CACHED_API is not None:
        return _CACHED_API
    if os.name != "nt":
        raise OSError("Windows private-file APIs are unavailable")

    ctypes_api: Any = ctypes
    kernel32 = ctypes_api.WinDLL("kernel32", use_last_error=True)
    advapi32 = ctypes_api.WinDLL("advapi32", use_last_error=True)

    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = (ctypes.c_void_p,)
    kernel32.LocalFree.restype = ctypes.c_void_p
    kernel32.CreateFileW.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.GetFileInformationByHandle.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    )
    kernel32.GetFileInformationByHandle.restype = wintypes.BOOL

    advapi32.OpenProcessToken.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    )
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    advapi32.GetTokenInformation.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    )
    advapi32.GetTokenInformation.restype = wintypes.BOOL
    advapi32.GetLengthSid.argtypes = (ctypes.c_void_p,)
    advapi32.GetLengthSid.restype = wintypes.DWORD
    advapi32.IsValidSid.argtypes = (ctypes.c_void_p,)
    advapi32.IsValidSid.restype = wintypes.BOOL
    advapi32.ConvertSidToStringSidW.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    )
    advapi32.ConvertSidToStringSidW.restype = wintypes.BOOL
    advapi32.InitializeAcl.argtypes = (
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    advapi32.InitializeAcl.restype = wintypes.BOOL
    advapi32.AddAccessAllowedAceEx.argtypes = (
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
    )
    advapi32.AddAccessAllowedAceEx.restype = wintypes.BOOL
    advapi32.SetSecurityInfo.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    )
    advapi32.SetSecurityInfo.restype = wintypes.DWORD
    advapi32.GetSecurityInfo.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_void_p),
    )
    advapi32.GetSecurityInfo.restype = wintypes.DWORD
    advapi32.GetSecurityDescriptorControl.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(wintypes.WORD),
        ctypes.POINTER(wintypes.DWORD),
    )
    advapi32.GetSecurityDescriptorControl.restype = wintypes.BOOL
    advapi32.GetAclInformation.argtypes = (
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_int,
    )
    advapi32.GetAclInformation.restype = wintypes.BOOL
    advapi32.GetAce.argtypes = (
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.c_void_p),
    )
    advapi32.GetAce.restype = wintypes.BOOL

    _CACHED_API = _API(kernel32=kernel32, advapi32=advapi32)
    return _CACHED_API


def _absolute_local_path(path: Path) -> Path:
    absolute = path.absolute()
    value = str(absolute)
    if not absolute.is_absolute() or not absolute.anchor:
        raise OSError("private storage path is not absolute")
    if value.startswith("\\\\") or absolute.drive.startswith("\\\\"):
        raise OSError("UNC private storage is not supported")
    return absolute


def _extended_path(path: Path) -> str:
    value = str(path)
    if value.startswith("\\\\?\\"):
        return value
    return f"\\\\?\\{value}"


def _validate_ancestor_directories(path: Path) -> None:
    absolute = _absolute_local_path(path)
    current = Path(absolute.anchor)
    _validate_directory_handle_path(current)
    for part in absolute.parts[1:]:
        current /= part
        _validate_directory_handle_path(current)


def _validate_directory_handle_path(path: Path) -> None:
    handle = _open_path(
        path,
        access=_FILE_READ_ATTRIBUTES,
        share=_FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        directory=True,
    )
    _close_handle(handle)


def _secure_path(path: Path, *, directory: bool) -> None:
    share = _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE
    if not directory:
        share = _FILE_SHARE_READ
    handle = _open_path(
        path,
        access=_READ_CONTROL | _WRITE_DAC | _WRITE_OWNER | _FILE_READ_ATTRIBUTES,
        share=share,
        directory=directory,
    )
    try:
        api = _api()
        _apply_owner_only_security(api, handle, directory=directory)
        _validate_owner_only(handle, directory=directory)
    finally:
        _close_handle(handle)


def _open_path(
    path: Path,
    *,
    access: int,
    share: int,
    directory: bool,
) -> int:
    api = _api()
    flags = _FILE_FLAG_OPEN_REPARSE_POINT
    if directory:
        flags |= _FILE_FLAG_BACKUP_SEMANTICS
    handle_value = api.kernel32.CreateFileW(
        _extended_path(path),
        access,
        share,
        None,
        _OPEN_EXISTING,
        flags,
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle_value in {None, invalid}:
        raise OSError(_last_error(), "CreateFileW failed")
    handle = int(handle_value)
    try:
        _validate_handle_kind(handle, directory=directory)
    except BaseException:
        _close_handle(handle)
        raise
    return handle


def _validate_handle_kind(handle: int, *, directory: bool) -> None:
    information = _ByHandleFileInformation()
    if not _api().kernel32.GetFileInformationByHandle(
        wintypes.HANDLE(handle),
        ctypes.byref(information),
    ):
        raise OSError(_last_error(), "GetFileInformationByHandle failed")
    attributes = int(information.FileAttributes)
    if attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
        raise OSError("private storage path contains a reparse point")
    is_directory = bool(attributes & _FILE_ATTRIBUTE_DIRECTORY)
    if is_directory != directory:
        kind = "directory" if directory else "regular file"
        raise OSError(f"private storage path is not a {kind}")


def _current_user_sid(api: _API) -> _SIDBuffer:
    token = wintypes.HANDLE()
    if not api.advapi32.OpenProcessToken(
        api.kernel32.GetCurrentProcess(),
        _TOKEN_QUERY,
        ctypes.byref(token),
    ):
        raise OSError(_last_error(), "OpenProcessToken failed")
    try:
        required = wintypes.DWORD()
        api.advapi32.GetTokenInformation(
            token,
            _TOKEN_USER,
            None,
            0,
            ctypes.byref(required),
        )
        if _last_error() != _ERROR_INSUFFICIENT_BUFFER or not required.value:
            raise OSError(_last_error(), "GetTokenInformation size failed")
        storage = ctypes.create_string_buffer(required.value)
        if not api.advapi32.GetTokenInformation(
            token,
            _TOKEN_USER,
            storage,
            required.value,
            ctypes.byref(required),
        ):
            raise OSError(_last_error(), "GetTokenInformation failed")
        token_user = ctypes.cast(storage, ctypes.POINTER(_TokenUser)).contents
        if not token_user.User.Sid:
            raise OSError("current process token has no user SID")
        return _SIDBuffer(storage=storage, pointer=int(token_user.User.Sid))
    finally:
        api.kernel32.CloseHandle(token)


def _apply_owner_only_security(api: _API, handle: int, *, directory: bool) -> None:
    current = _current_user_sid(api)
    sid_length = int(api.advapi32.GetLengthSid(ctypes.c_void_p(current.pointer)))
    if sid_length <= 0:
        raise OSError(_last_error(), "GetLengthSid failed")
    acl_size = (
        ctypes.sizeof(_ACL)
        + ctypes.sizeof(_AccessAllowedACE)
        - ctypes.sizeof(wintypes.DWORD)
        + sid_length
    )
    storage = ctypes.create_string_buffer(acl_size)
    acl = ctypes.cast(storage, ctypes.c_void_p)
    if not api.advapi32.InitializeAcl(acl, acl_size, _ACL_REVISION):
        raise OSError(_last_error(), "InitializeAcl failed")
    flags = _OBJECT_INHERIT_ACE | _CONTAINER_INHERIT_ACE if directory else 0
    if not api.advapi32.AddAccessAllowedAceEx(
        acl,
        _ACL_REVISION,
        flags,
        _FILE_ALL_ACCESS,
        ctypes.c_void_p(current.pointer),
    ):
        raise OSError(_last_error(), "AddAccessAllowedAceEx failed")
    result = int(
        api.advapi32.SetSecurityInfo(
            wintypes.HANDLE(handle),
            _SE_FILE_OBJECT,
            _OWNER_SECURITY_INFORMATION
            | _DACL_SECURITY_INFORMATION
            | _PROTECTED_DACL_SECURITY_INFORMATION,
            ctypes.c_void_p(current.pointer),
            None,
            acl,
            None,
        )
    )
    if result:
        raise OSError(result, "SetSecurityInfo failed")


def _validate_owner_only(handle: int, *, directory: bool) -> None:
    api = _api()
    current = _current_user_sid(api)
    current_sid = _sid_to_string(api, current.pointer)
    snapshot = _security_snapshot(api, handle)
    expected_flags = _OBJECT_INHERIT_ACE | _CONTAINER_INHERIT_ACE if directory else 0
    if snapshot.owner_sid != current_sid:
        raise OSError("private storage owner is not the service user")
    if not snapshot.dacl_protected:
        raise OSError("private storage DACL inherits permissions")
    if len(snapshot.aces) != 1:
        raise OSError("private storage DACL grants additional principals")
    ace = snapshot.aces[0]
    if (
        ace.kind != _ACCESS_ALLOWED_ACE_TYPE
        or ace.sid != current_sid
        or ace.mask != _FILE_ALL_ACCESS
        or ace.flags & _INHERITANCE_FLAGS != expected_flags
    ):
        raise OSError("private storage DACL is not owner-only")


def _security_snapshot(api: _API, handle: int) -> _SecuritySnapshot:
    owner = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    result = int(
        api.advapi32.GetSecurityInfo(
            wintypes.HANDLE(handle),
            _SE_FILE_OBJECT,
            _OWNER_SECURITY_INFORMATION | _DACL_SECURITY_INFORMATION,
            ctypes.byref(owner),
            None,
            ctypes.byref(dacl),
            None,
            ctypes.byref(descriptor),
        )
    )
    if result:
        raise OSError(result, "GetSecurityInfo failed")
    try:
        if not owner.value or not dacl.value:
            raise OSError("private storage security descriptor is incomplete")
        control = wintypes.WORD()
        revision = wintypes.DWORD()
        if not api.advapi32.GetSecurityDescriptorControl(
            descriptor,
            ctypes.byref(control),
            ctypes.byref(revision),
        ):
            raise OSError(
                _last_error(),
                "GetSecurityDescriptorControl failed",
            )
        size = _ACLSizeInformation()
        if not api.advapi32.GetAclInformation(
            dacl,
            ctypes.byref(size),
            ctypes.sizeof(size),
            _ACL_SIZE_INFORMATION,
        ):
            raise OSError(_last_error(), "GetAclInformation failed")
        aces: list[_ACE] = []
        for index in range(int(size.AceCount)):
            pointer = ctypes.c_void_p()
            if not api.advapi32.GetAce(dacl, index, ctypes.byref(pointer)):
                raise OSError(_last_error(), "GetAce failed")
            value = int(pointer.value or 0)
            if not value:
                raise OSError("private storage DACL contains an invalid ACE")
            header = ctypes.cast(pointer, ctypes.POINTER(_ACEHeader)).contents
            if int(header.AceType) != _ACCESS_ALLOWED_ACE_TYPE:
                aces.append(
                    _ACE(
                        kind=int(header.AceType),
                        flags=int(header.AceFlags),
                        mask=0,
                        sid="",
                    )
                )
                continue
            if int(header.AceSize) < _AccessAllowedACE.SidStart.offset + 8:
                raise OSError("private storage DACL contains a truncated ACE")
            ace = ctypes.cast(pointer, ctypes.POINTER(_AccessAllowedACE)).contents
            sid_pointer = value + _AccessAllowedACE.SidStart.offset
            if not api.advapi32.IsValidSid(ctypes.c_void_p(sid_pointer)):
                raise OSError("private storage DACL contains an invalid SID")
            aces.append(
                _ACE(
                    kind=int(ace.Header.AceType),
                    flags=int(ace.Header.AceFlags),
                    mask=int(ace.Mask),
                    sid=_sid_to_string(api, sid_pointer),
                )
            )
        return _SecuritySnapshot(
            owner_sid=_sid_to_string(api, int(owner.value)),
            dacl_protected=bool(int(control.value) & _SE_DACL_PROTECTED),
            aces=tuple(aces),
        )
    finally:
        if descriptor.value:
            api.kernel32.LocalFree(descriptor)


def _sid_to_string(api: _API, pointer: int) -> str:
    value = ctypes.c_void_p()
    if not api.advapi32.ConvertSidToStringSidW(
        ctypes.c_void_p(pointer),
        ctypes.byref(value),
    ):
        raise OSError(_last_error(), "ConvertSidToStringSidW failed")
    try:
        if not value.value:
            raise OSError("SID conversion returned no value")
        return ctypes.wstring_at(value.value)
    finally:
        if value.value:
            api.kernel32.LocalFree(value)


def _close_handle(handle: int) -> None:
    if not _api().kernel32.CloseHandle(wintypes.HANDLE(handle)):
        raise OSError(_last_error(), "CloseHandle failed")


def _last_error() -> int:
    ctypes_api: Any = ctypes
    return int(ctypes_api.get_last_error())
