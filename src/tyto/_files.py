from __future__ import annotations

import os
import uuid
import errno
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import grpc

from ._errors import (
    CapabilityRejectedError,
    FilesystemLimitError,
    InvalidRequestError,
    SandboxDeletedError,
    SandboxFailedError,
)
from ._grpc_errors import map_rpc_error
from ._transport import Deadline
from ._proto.tyto.runtime.v1 import guest_pb2
from ._types import Status

if TYPE_CHECKING:
    from ._sandbox import Sandbox

_guest_pb2: Any = guest_pb2
_T = TypeVar("_T")

TRANSFER_CHUNK_BYTES = 64 * 1024


class FileKind(Enum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    OTHER = "other"


@dataclass(frozen=True)
class FileInfo:
    path: str
    name: str
    kind: FileKind
    size: int
    mode: int
    modified_at: datetime


class SandboxFiles:
    """Dedicated sandbox filesystem RPC surface.

    ``read`` buffers subject to the client's memory cap. ``upload`` and
    ``download`` stream in 64 KiB chunks without a total transfer cap.
    """

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def read(self, path: str) -> bytes:
        path = _validate_remote_path(path)

        def call() -> bytes:
            sandbox = self._sandbox
            request = _guest_pb2.ReadFileRequest(sandbox_id=sandbox.id, path=path)
            stream = self._stub().ReadFile(
                request,
                timeout=Deadline.start(sandbox._client._timeout).remaining(),
                metadata=self._metadata(),
            )
            data = bytearray()
            try:
                for response in stream:
                    chunk = bytes(getattr(response, "data", b""))
                    if len(data) + len(chunk) > sandbox._client._filesystem_read_limit:
                        cancel = getattr(stream, "cancel", None)
                        if callable(cancel):
                            cancel()
                        raise FilesystemLimitError(
                            "filesystem read exceeded client memory limit",
                            sandbox_id=sandbox.id,
                            operation_id=sandbox.operation_id,
                        )
                    data.extend(chunk)
            except FilesystemLimitError:
                raise
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return bytes(data)

        return self._with_capability_refresh(call)

    def write(self, path: str, data: bytes | str) -> None:
        path = _validate_remote_path(path)
        payload = _normalize_write_data(data)

        def requests() -> Any:
            yield _guest_pb2.WriteFileRequest(
                start=_guest_pb2.WriteFileStart(sandbox_id=self._sandbox.id, path=path)
            )
            if not payload:
                return
            for offset in range(0, len(payload), TRANSFER_CHUNK_BYTES):
                yield _guest_pb2.WriteFileRequest(
                    chunk=_guest_pb2.WriteFileChunk(data=payload[offset : offset + TRANSFER_CHUNK_BYTES])
                )

        self._write_stream(requests)

    def upload(self, local_path: str | os.PathLike[str], remote_path: str) -> None:
        remote_path = _validate_remote_path(remote_path)
        source = Path(local_path)

        def requests() -> Any:
            yield _guest_pb2.WriteFileRequest(
                start=_guest_pb2.WriteFileStart(sandbox_id=self._sandbox.id, path=remote_path)
            )
            with source.open("rb") as file:
                while True:
                    chunk = file.read(TRANSFER_CHUNK_BYTES)
                    if not chunk:
                        break
                    yield _guest_pb2.WriteFileRequest(chunk=_guest_pb2.WriteFileChunk(data=chunk))

        self._write_stream(requests)

    def download(self, remote_path: str, local_path: str | os.PathLike[str]) -> None:
        remote_path = _validate_remote_path(remote_path)
        destination = Path(local_path)
        parent = destination.parent if str(destination.parent) else Path(".")
        temp = parent / f".{destination.name}.bonya-download-{uuid.uuid4().hex}.tmp"
        replaced = False
        try:
            with temp.open("xb") as file:
                self._download_to_file(remote_path, file)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp, destination)
            replaced = True
            _fsync_parent(parent)
        finally:
            if not replaced:
                try:
                    temp.unlink()
                except FileNotFoundError:
                    pass

    def list(self, path: str) -> list[FileInfo]:
        path = _validate_remote_path(path)

        def call() -> list[FileInfo]:
            sandbox = self._sandbox
            request = _guest_pb2.ListDirectoryRequest(sandbox_id=sandbox.id, path=path)
            stream = self._stub().ListDirectory(
                request,
                timeout=Deadline.start(sandbox._client._timeout).remaining(),
                metadata=self._metadata(),
            )
            files: list[FileInfo] = []
            try:
                for response in stream:
                    file = getattr(response, "file", None)
                    if file is None:
                        raise InvalidRequestError("ListDirectory response is missing file metadata")
                    files.append(_file_info_from_proto(file))
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return sorted(files, key=lambda item: item.name)

        return self._with_capability_refresh(call)

    def stat(self, path: str) -> FileInfo:
        path = _validate_remote_path(path)

        def call() -> FileInfo:
            sandbox = self._sandbox
            request = _guest_pb2.StatFileRequest(sandbox_id=sandbox.id, path=path)
            try:
                response = self._stub().StatFile(
                    request,
                    timeout=Deadline.start(sandbox._client._timeout).remaining(),
                    metadata=self._metadata(),
                )
            except BaseException as exc:
                raise self._map_error(exc) from exc
            file = getattr(response, "file", None)
            if file is None:
                raise InvalidRequestError("StatFile response is missing file metadata")
            return _file_info_from_proto(file)

        return self._with_capability_refresh(call)

    def mkdir(self, path: str) -> None:
        path = _validate_remote_path(path)
        self._unary_mutation(lambda: _guest_pb2.MakeDirectoryRequest(sandbox_id=self._sandbox.id, path=path), "MakeDirectory")

    def remove(self, path: str, recursive: bool = False) -> None:
        path = _validate_remote_path(path)
        if not isinstance(recursive, bool):
            raise InvalidRequestError("recursive must be a boolean")
        self._unary_mutation(
            lambda: _guest_pb2.RemoveFileRequest(sandbox_id=self._sandbox.id, path=path, recursive=recursive),
            "RemoveFile",
        )

    def move(self, source: str, destination: str) -> None:
        source = _validate_remote_path(source)
        destination = _validate_remote_path(destination)
        self._unary_mutation(
            lambda: _guest_pb2.MoveFileRequest(
                sandbox_id=self._sandbox.id,
                source_path=source,
                destination_path=destination,
            ),
            "MoveFile",
        )

    def _write_stream(self, request_factory: Callable[[], Any]) -> None:
        def call() -> None:
            try:
                self._stub().WriteFile(
                    request_factory(),
                    timeout=Deadline.start(self._sandbox._client._timeout).remaining(),
                    metadata=self._metadata(),
                )
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return None

        self._with_capability_refresh(call)

    def _download_to_file(self, remote_path: str, file: Any) -> None:
        def call() -> None:
            sandbox = self._sandbox
            request = _guest_pb2.ReadFileRequest(sandbox_id=sandbox.id, path=remote_path)
            stream = self._stub().ReadFile(
                request,
                timeout=Deadline.start(sandbox._client._timeout).remaining(),
                metadata=self._metadata(),
            )
            try:
                for response in stream:
                    file.write(bytes(getattr(response, "data", b"")))
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return None

        self._with_capability_refresh(call)

    def _unary_mutation(self, request_factory: Callable[[], object], method_name: str) -> None:
        def call() -> None:
            try:
                method = getattr(self._stub(), method_name)
                method(
                    request_factory(),
                    timeout=Deadline.start(self._sandbox._client._timeout).remaining(),
                    metadata=self._metadata(),
                )
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return None

        self._with_capability_refresh(call)

    def _with_capability_refresh(self, call: Callable[[], _T]) -> _T:
        self._ensure_files_allowed()
        try:
            return call()
        except CapabilityRejectedError:
            self._sandbox._refresh_capability_once()
            return call()

    def _ensure_files_allowed(self) -> None:
        sandbox = self._sandbox
        if sandbox._deleted or sandbox.last_observed_status is Status.DELETED:
            raise SandboxDeletedError("sandbox has been deleted", sandbox_id=sandbox.id, operation_id=sandbox.operation_id)
        if sandbox.last_observed_status is Status.FAILED:
            message = sandbox._failure_message or sandbox._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=sandbox.id, operation_id=sandbox.operation_id)

    def _stub(self) -> Any:
        return self._sandbox._client._exec_stub(self._sandbox._exec_endpoint)

    def _metadata(self) -> tuple[tuple[str, str], tuple[str, str]]:
        return (
            ("bonya-sandbox-id", self._sandbox.id),
            ("bonya-exec-capability", self._sandbox._capability),
        )

    def _map_error(self, error: BaseException) -> BaseException:
        mapped = map_rpc_error(
            error,
            secrets=self._sandbox._client._secrets(self._sandbox._capability),
            sandbox_id=self._sandbox.id,
            operation_id=self._sandbox.operation_id,
            filesystem_rpc=True,
        )
        if isinstance(mapped, SandboxDeletedError):
            self._sandbox._deleted = True
            self._sandbox.last_observed_status = Status.DELETED
        return mapped


def _validate_remote_path(path: str) -> str:
    if not isinstance(path, str) or not path or "\0" in path:
        raise InvalidRequestError("path must be a non-empty string without NUL")
    return path


def _normalize_write_data(data: bytes | str) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, bytes):
        return data
    raise InvalidRequestError("data must be bytes or str")


def _file_info_from_proto(file: Any) -> FileInfo:
    return FileInfo(
        path=getattr(file, "path", ""),
        name=getattr(file, "name", ""),
        kind=_file_kind_from_proto(int(getattr(file, "kind", 0))),
        size=int(getattr(file, "size", 0)),
        mode=int(getattr(file, "mode", 0)),
        modified_at=_datetime_from_unix_nanos(int(getattr(file, "modified_at_unix_nanos", 0))),
    )


def _file_kind_from_proto(kind: int) -> FileKind:
    mapping = {
        _guest_pb2.FILE_KIND_FILE: FileKind.FILE,
        _guest_pb2.FILE_KIND_DIRECTORY: FileKind.DIRECTORY,
        _guest_pb2.FILE_KIND_SYMLINK: FileKind.SYMLINK,
        _guest_pb2.FILE_KIND_OTHER: FileKind.OTHER,
    }
    return mapping.get(kind, FileKind.OTHER)


def _datetime_from_unix_nanos(nanos: int) -> datetime:
    seconds, remainder = divmod(nanos, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc) + timedelta(microseconds=remainder // 1000)


def _fsync_parent(parent: Path) -> None:
    try:
        fd = os.open(parent, os.O_RDONLY)
    except OSError as exc:
        if _is_unsupported_directory_fsync_error(exc):
            return
        raise
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if not _is_unsupported_directory_fsync_error(exc):
                raise
    finally:
        os.close(fd)


def _is_unsupported_directory_fsync_error(error: OSError) -> bool:
    unsupported = {errno.EINVAL}
    for name in ("ENOTSUP", "EOPNOTSUPP"):
        value = getattr(errno, name, None)
        if value is not None:
            unsupported.add(value)
    return error.errno in unsupported
