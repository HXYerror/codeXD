from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
import tempfile
import unicodedata
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from openai_codex.models import JsonObject

from codexd.domain.ids import canonical_json, sha256_text, utc_now_ms
from codexd.errors import ConflictError, SecurityError
from codexd.rendering.media_worker import MediaWorker, MediaWorkerError
from codexd.runtime.port import DynamicToolCall
from codexd.security import private_files
from codexd.storage.outbound_images import OutboundImageRepository
from codexd.storage.records import (
    OutboundImageInvocationRecord,
    OutboundImageScope,
)

PUBLISH_IMAGE_INPUT_SCHEMA: JsonObject = {
    "type": "object",
    "additionalProperties": False,
    "required": ["source_path", "display_name", "description"],
    "properties": {
        "source_path": {"type": "string", "minLength": 1},
        "display_name": {"type": "string", "minLength": 1, "maxLength": 128},
        "description": {"type": "string", "minLength": 1, "maxLength": 1024},
    },
}

_VALIDATOR = Draft202012Validator(PUBLISH_IMAGE_INPUT_SCHEMA)
_MAX_ARGUMENT_BYTES = 24 * 1024
_SOURCE_CLOCK_SKEW_MS = 5_000
_MENTION = re.compile(r"@(?:everyone|here)|<@", re.IGNORECASE)


@dataclass(frozen=True)
class _PublishArguments:
    source_path: str
    display_name: str
    description: str


class _PublishValidationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class OutboundImageBroker:
    def __init__(
        self,
        *,
        repository: OutboundImageRepository,
        media_worker: MediaWorker,
        artifact_root: Path,
        configured_guild_id: int,
        configured_owner_user_id: int,
        allowed_user_ids: frozenset[int],
        max_bytes: int,
        max_pixels: int,
        retention_days: int,
    ) -> None:
        if max_bytes < 1 or max_pixels < 1 or retention_days < 1:
            raise ValueError("outbound image limits must be positive")
        self._repository = repository
        self._media_worker = media_worker
        self._artifact_root = artifact_root.resolve()
        self._configured_guild_id = configured_guild_id
        self._configured_owner_user_id = configured_owner_user_id
        self._allowed_user_ids = allowed_user_ids
        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._retention_days = retention_days

    async def handle(self, call: DynamicToolCall) -> dict[str, object]:
        arguments_hash, hash_error = _arguments_hash(call.arguments)
        try:
            existing, scope = await asyncio.to_thread(
                self._repository.preflight,
                local_turn_id=call.local_turn_id,
                runtime_generation=call.runtime_generation,
                provider_thread_id=call.provider_thread_id,
                provider_turn_id=call.provider_turn_id,
                provider_call_id=call.provider_call_id,
                arguments_hash=arguments_hash,
                configured_guild_id=self._configured_guild_id,
                configured_owner_user_id=self._configured_owner_user_id,
                allowed_user_ids=self._allowed_user_ids,
            )
            if existing is not None:
                return _record_response(existing)
            assert scope is not None
            if hash_error is not None:
                return await self._complete_failure(
                    call,
                    arguments_hash=arguments_hash,
                    error=hash_error,
                )
            try:
                arguments = _parse_arguments(call.arguments)
                source = await asyncio.to_thread(
                    _validate_source,
                    arguments.source_path,
                    observed_paths=call.observed_image_paths,
                    scope=scope,
                    max_bytes=self._max_bytes,
                )
                record = await self._stage_and_register(
                    call,
                    arguments_hash=arguments_hash,
                    arguments=arguments,
                    source=source,
                )
            except _PublishValidationError as exc:
                return await self._complete_failure(
                    call,
                    arguments_hash=arguments_hash,
                    error=(exc.code, str(exc)),
                )
            except OSError:
                return await self._complete_failure(
                    call,
                    arguments_hash=arguments_hash,
                    error=(
                        "artifact_staging_failed",
                        "The image could not be staged in private storage.",
                    ),
                )
        except ConflictError:
            return _tool_response(
                False,
                {
                    "status": "error",
                    "code": "call_identity_conflict",
                    "message": "This dynamic tool call identity was already used.",
                },
            )
        except SecurityError:
            return _tool_response(
                False,
                {
                    "status": "error",
                    "code": "scope_mismatch",
                    "message": "The originating Turn is unavailable.",
                },
            )
        return _record_response(record)

    async def _stage_and_register(
        self,
        call: DynamicToolCall,
        *,
        arguments_hash: str,
        arguments: _PublishArguments,
        source: Path,
    ) -> OutboundImageInvocationRecord:
        artifact_token = str(uuid.uuid4())
        turn_root = self._artifact_root / call.local_turn_id
        staging_root = turn_root / ".outbound-staging"
        final_root = turn_root / "outbound"
        await asyncio.to_thread(
            _ensure_private_roots,
            self._artifact_root.parent,
            self._artifact_root,
            turn_root,
            staging_root,
            final_root,
        )
        source_snapshot = staging_root / f"{artifact_token}.source"
        normalized_staging = staging_root / f"{artifact_token}.png"
        final_path = final_root / f"{artifact_token}.png"
        registered = False
        try:
            await asyncio.to_thread(
                _snapshot_source,
                source,
                source_snapshot,
                max_bytes=self._max_bytes,
            )
            try:
                normalized = await self._media_worker.normalize_image(
                    source=source_snapshot,
                    output=normalized_staging,
                    max_bytes=self._max_bytes,
                    max_pixels=self._max_pixels,
                )
            except MediaWorkerError as exc:
                raise _PublishValidationError(
                    "image_decode_failed",
                    "The selected file is not a safe supported raster image.",
                ) from exc
            await asyncio.to_thread(
                _commit_normalized,
                normalized_staging,
                final_path,
                expected_sha256=normalized.normalized_sha256,
                expected_size=normalized.size_bytes,
            )
            relative_path = final_path.relative_to(self._artifact_root).as_posix()
            record = await asyncio.to_thread(
                self._repository.complete,
                local_turn_id=call.local_turn_id,
                runtime_generation=call.runtime_generation,
                provider_thread_id=call.provider_thread_id,
                provider_turn_id=call.provider_turn_id,
                provider_call_id=call.provider_call_id,
                arguments_hash=arguments_hash,
                configured_guild_id=self._configured_guild_id,
                configured_owner_user_id=self._configured_owner_user_id,
                allowed_user_ids=self._allowed_user_ids,
                relative_path=relative_path,
                source_sha256=normalized.source_sha256,
                normalized_sha256=normalized.normalized_sha256,
                size_bytes=normalized.size_bytes,
                width=normalized.width,
                height=normalized.height,
                display_name=arguments.display_name,
                description=arguments.description,
                retention_until=(
                    utc_now_ms()
                    + self._retention_days * 24 * 60 * 60 * 1000
                ),
            )
            if not record.success or record.relative_path != relative_path:
                final_path.unlink(missing_ok=True)
            else:
                registered = True
            return record
        finally:
            source_snapshot.unlink(missing_ok=True)
            normalized_staging.unlink(missing_ok=True)
            if not registered:
                final_path.unlink(missing_ok=True)

    async def _complete_failure(
        self,
        call: DynamicToolCall,
        *,
        arguments_hash: str,
        error: tuple[str, str],
    ) -> dict[str, object]:
        record = await asyncio.to_thread(
            self._repository.complete,
            local_turn_id=call.local_turn_id,
            runtime_generation=call.runtime_generation,
            provider_thread_id=call.provider_thread_id,
            provider_turn_id=call.provider_turn_id,
            provider_call_id=call.provider_call_id,
            arguments_hash=arguments_hash,
            configured_guild_id=self._configured_guild_id,
            configured_owner_user_id=self._configured_owner_user_id,
            allowed_user_ids=self._allowed_user_ids,
            validation_error=error,
        )
        return _record_response(record)


def _arguments_hash(value: object) -> tuple[str, tuple[str, str] | None]:
    try:
        encoded = canonical_json(value)
    except (TypeError, ValueError):
        return sha256_text("invalid-json"), (
            "invalid_arguments",
            "Image publication arguments must be a JSON object.",
        )
    if len(encoded.encode("utf-8")) > _MAX_ARGUMENT_BYTES:
        return sha256_text(encoded), (
            "arguments_too_large",
            "Image publication arguments exceed the supported size.",
        )
    return sha256_text(encoded), None


def _parse_arguments(value: object) -> _PublishArguments:
    if not isinstance(value, dict):
        raise _PublishValidationError(
            "invalid_arguments",
            "Image publication arguments must be a JSON object.",
        )
    errors = sorted(
        _VALIDATOR.iter_errors(value),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        raise _PublishValidationError(
            "invalid_arguments",
            "Image publication arguments do not match the required schema.",
        )
    raw = cast(dict[str, Any], value)
    return _PublishArguments(
        source_path=str(raw["source_path"]),
        display_name=_safe_display_name(str(raw["display_name"])),
        description=_safe_description(str(raw["description"])),
    )


def _safe_display_name(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value).strip()
    if (
        not normalized
        or "/" in normalized
        or "\\" in normalized
        or _MENTION.search(normalized)
        or any(unicodedata.category(char).startswith("C") for char in normalized)
    ):
        raise _PublishValidationError(
            "invalid_display_name",
            "The image display name is unsafe.",
        )
    stem = Path(normalized).stem
    stem = re.sub(r"[^\w.-]+", "-", stem, flags=re.UNICODE).strip(" .-")
    if not stem:
        stem = "image"
    return f"{stem[:120]}.png"


def _safe_description(value: str) -> str:
    normalized = " ".join(unicodedata.normalize("NFC", value).split())
    normalized = normalized.replace("@everyone", "@​everyone")
    normalized = normalized.replace("@here", "@​here").replace("<@", "<@​")
    if not normalized:
        raise _PublishValidationError(
            "invalid_description",
            "The image description may not be empty.",
        )
    return normalized[:1024]


def _validate_source(
    raw_path: str,
    *,
    observed_paths: tuple[str, ...],
    scope: OutboundImageScope,
    max_bytes: int,
) -> Path:
    if not raw_path or "\x00" in raw_path:
        raise _PublishValidationError("invalid_source_path", "The image path is invalid.")
    candidate = Path(raw_path)
    if not candidate.is_absolute() or ".." in candidate.parts:
        raise _PublishValidationError(
            "invalid_source_path",
            "The image path must be an observed absolute path.",
        )
    absolute = Path(os.path.abspath(candidate))
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise _PublishValidationError(
            "source_not_found",
            "The observed image is no longer available.",
        ) from exc
    if os.path.normcase(str(absolute)) != os.path.normcase(str(resolved)):
        raise _PublishValidationError(
            "source_link_forbidden",
            "Linked or redirected image paths cannot be published.",
        )
    observed = {
        os.path.normcase(os.path.abspath(path))
        for path in observed_paths
        if path and "\x00" not in path
    }
    if os.path.normcase(str(resolved)) not in observed:
        raise _PublishValidationError(
            "source_not_observed",
            "The image was not inspected in the current Turn.",
        )
    temp_roots = tuple(
        dict.fromkeys(
            root.resolve()
            for root in (Path(tempfile.gettempdir()), Path("/tmp"))
            if root.exists() and root.is_dir()
        )
    )
    boundary = next(
        (
            root
            for root in (scope.project_root.resolve(), *temp_roots)
            if resolved == root or resolved.is_relative_to(root)
        ),
        None,
    )
    if boundary is None:
        raise _PublishValidationError(
            "source_outside_output_boundary",
            "The image is outside the current Turn output boundary.",
        )
    try:
        private_files.validate_directory_no_reparse(boundary)
        current = boundary
        for part in resolved.relative_to(boundary).parts[:-1]:
            current /= part
            private_files.validate_directory_no_reparse(current)
        private_files.validate_file_no_reparse(resolved)
        metadata = resolved.lstat()
    except OSError as exc:
        raise _PublishValidationError(
            "source_integrity_failed",
            "The image source path is unsafe.",
        ) from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size <= 0
        or metadata.st_nlink != 1
    ):
        raise _PublishValidationError(
            "source_not_regular",
            "The image source must be a regular file.",
        )
    if metadata.st_size > max_bytes:
        raise _PublishValidationError(
            "image_size_limit",
            "The image exceeds the publication byte limit.",
        )
    if os.name != "nt" and metadata.st_uid != os.getuid():
        raise _PublishValidationError(
            "source_owner_mismatch",
            "The image is owned by another local user.",
        )
    modified_at = int(metadata.st_mtime * 1000)
    if (
        modified_at < scope.turn_started_at - _SOURCE_CLOCK_SKEW_MS
        or modified_at > utc_now_ms() + _SOURCE_CLOCK_SKEW_MS
    ):
        raise _PublishValidationError(
            "source_not_created_for_turn",
            "The image timestamp does not belong to the current Turn.",
        )
    return resolved


def _ensure_private_roots(*paths: Path) -> None:
    for path in paths:
        private_files.ensure_private_directory(path)


def _snapshot_source(source: Path, target: Path, *, max_bytes: int) -> None:
    descriptor = private_files.open_file_no_reparse(
        source,
        require_private=False,
        deny_write_delete=True,
    )
    target_descriptor = -1
    try:
        before = os.fstat(descriptor)
        target_descriptor = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        copied = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            copied += len(chunk)
            if copied > max_bytes:
                raise _PublishValidationError(
                    "image_size_limit",
                    "The image exceeds the publication byte limit.",
                )
            view = memoryview(chunk)
            while view:
                written = os.write(target_descriptor, view)
                if written <= 0:
                    raise OSError("private image snapshot write made no progress")
                view = view[written:]
        os.fsync(target_descriptor)
        after = os.fstat(descriptor)
        named = source.lstat()
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        named_identity = (
            named.st_dev,
            named.st_ino,
            named.st_size,
            named.st_mtime_ns,
        )
        if copied != before.st_size or identity_before != identity_after or (
            identity_after != named_identity
        ):
            raise _PublishValidationError(
                "source_changed",
                "The image changed while it was being staged.",
            )
    except OSError as exc:
        raise _PublishValidationError(
            "source_integrity_failed",
            "The image could not be staged safely.",
        ) from exc
    finally:
        if target_descriptor >= 0:
            os.close(target_descriptor)
        os.close(descriptor)
    private_files.secure_private_file(target)


def _commit_normalized(
    staging: Path,
    target: Path,
    *,
    expected_sha256: str,
    expected_size: int,
) -> None:
    if target.exists():
        raise _PublishValidationError(
            "artifact_identity_conflict",
            "The outbound image identity already exists.",
        )
    metadata = staging.lstat()
    if (
        staging.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size != expected_size
        or _sha256_file(staging) != expected_sha256
    ):
        raise _PublishValidationError(
            "normalized_image_changed",
            "The normalized image changed before registration.",
        )
    os.replace(staging, target)
    private_files.secure_private_file(target)
    private_files.validate_private_file(target)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _record_response(record: OutboundImageInvocationRecord) -> dict[str, object]:
    try:
        result = json.loads(record.result_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError("persisted image publication result is invalid") from exc
    if not isinstance(result, dict):
        raise RuntimeError("persisted image publication result is not an object")
    return _tool_response(record.success, result)


def _tool_response(success: bool, result: dict[str, object]) -> dict[str, object]:
    return {
        "success": success,
        "contentItems": [
            {"type": "inputText", "text": canonical_json(result)}
        ],
    }
