from __future__ import annotations

import os
import errno
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from ._errors import InvalidRequestError
from ._proto.tyto.runtime.v1 import guest_pb2

_guest_pb2: Any = guest_pb2

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
