from __future__ import annotations

import os
import queue
import time
from pathlib import Path
from typing import Any

import grpc
import pytest

from tyto import (
    Tyto,
    CapabilityRejectedError,
    CrossFilesystemMoveError,
    FileInfo,
    FileKind,
    FilesystemError,
    FilesystemLimitError,
    RemoteFileExistsError,
    RemoteFileNotFoundError,
    Sandbox,
    Status,
)
from tyto._proto.tyto.runtime.v1 import guest_pb2, tapi_pb2
from tyto._files import TRANSFER_CHUNK_BYTES

from test_contract import FakeTapi, FakeTransport, RpcFailure, make_client


class IterableFailure:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.cancel_called = False

    def __iter__(self) -> "IterableFailure":
        return self

    def __next__(self) -> Any:
        raise self.error

    def cancel(self) -> None:
        self.cancel_called = True


class FakeFilesystemGuest:
    def __init__(self) -> None:
        self.read_chunks: list[bytes] = [b"bin\x00", b"\xff"]
        self.list_files: list[guest_pb2.FileInfo] = []
        self.stat_file = guest_pb2.FileInfo(
            path="/tmp/blob",
            name="blob",
            kind=guest_pb2.FILE_KIND_FILE,
            size=5,
            mode=0o100640,
            modified_at_unix_nanos=1_700_000_000_123_456_789,
        )
        self.read_errors: queue.Queue[BaseException] = queue.Queue()
        self.write_errors: queue.Queue[BaseException] = queue.Queue()
        self.list_errors: queue.Queue[BaseException] = queue.Queue()
        self.stat_errors: queue.Queue[BaseException] = queue.Queue()
        self.mkdir_errors: queue.Queue[BaseException] = queue.Queue()
        self.remove_errors: queue.Queue[BaseException] = queue.Queue()
        self.move_errors: queue.Queue[BaseException] = queue.Queue()
        self.read_requests: list[guest_pb2.ReadFileRequest] = []
        self.write_requests: list[list[guest_pb2.WriteFileRequest]] = []
        self.list_requests: list[guest_pb2.ListDirectoryRequest] = []
        self.stat_requests: list[guest_pb2.StatFileRequest] = []
        self.mkdir_requests: list[guest_pb2.MakeDirectoryRequest] = []
        self.remove_requests: list[guest_pb2.RemoveFileRequest] = []
        self.move_requests: list[guest_pb2.MoveFileRequest] = []
        self.metadata: list[object] = []

    def ReadFile(self, request, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.read_requests.append(request)
        self.metadata.append(metadata)
        if not self.read_errors.empty():
            return IterableFailure(self.read_errors.get())
        return iter([guest_pb2.ReadFileResponse(data=chunk) for chunk in self.read_chunks])

    def WriteFile(self, requests, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.metadata.append(metadata)
        if not self.write_errors.empty():
            raise self.write_errors.get()
        frames = list(requests)
        self.write_requests.append(frames)
        total = sum(len(frame.chunk.data) for frame in frames if frame.WhichOneof("frame") == "chunk")
        return guest_pb2.WriteFileResponse(bytes_written=total)

    def ListDirectory(self, request, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.list_requests.append(request)
        self.metadata.append(metadata)
        if not self.list_errors.empty():
            return IterableFailure(self.list_errors.get())
        return iter([guest_pb2.ListDirectoryResponse(file=file) for file in self.list_files])

    def StatFile(self, request, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.stat_requests.append(request)
        self.metadata.append(metadata)
        if not self.stat_errors.empty():
            raise self.stat_errors.get()
        return guest_pb2.StatFileResponse(file=self.stat_file)

    def MakeDirectory(self, request, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.mkdir_requests.append(request)
        self.metadata.append(metadata)
        if not self.mkdir_errors.empty():
            raise self.mkdir_errors.get()
        return guest_pb2.MakeDirectoryResponse()

    def RemoveFile(self, request, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.remove_requests.append(request)
        self.metadata.append(metadata)
        if not self.remove_errors.empty():
            raise self.remove_errors.get()
        return guest_pb2.RemoveFileResponse()

    def MoveFile(self, request, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.move_requests.append(request)
        self.metadata.append(metadata)
        if not self.move_errors.empty():
            raise self.move_errors.get()
        return guest_pb2.MoveFileResponse()


def make_files_client(monkeypatch: pytest.MonkeyPatch, guest: FakeFilesystemGuest, *, read_limit: int = 64 * 1024 * 1024) -> tuple[Tyto, FakeTransport]:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = guest  # type: ignore[assignment]
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    client = Tyto(
        endpoint="https://api.example.test/",
        timeout=2,
        max_retries=2,
        filesystem_read_limit=read_limit,
        _channel_factory=transport.channel_factory,
        _tapi_stub_factory=transport.tapi_stub,
        _guest_stub_factory=transport.guest_stub,
    )
    return client, transport


def test_files_public_surface_binary_empty_utf8_and_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    assert sandbox.files.read("/tmp/blob") == b"bin\x00\xff"
    sandbox.files.write("/tmp/empty", b"")
    sandbox.files.write("/tmp/text", "snowman: \u2603")
    info = sandbox.files.stat("/tmp/blob")

    assert guest.read_requests[0].path == "/tmp/blob"
    assert [frame.WhichOneof("frame") for frame in guest.write_requests[0]] == ["start"]
    assert guest.write_requests[1][1].chunk.data == "snowman: \u2603".encode()
    assert isinstance(info, FileInfo)
    assert info.kind is FileKind.FILE
    assert info.modified_at.tzinfo is not None
    assert info.modified_at.timestamp() == pytest.approx(1_700_000_000.123456, abs=0.000001)


def test_upload_uses_64k_chunks_and_download_replaces_atomically(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    guest = FakeFilesystemGuest()
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    source = tmp_path / "source.bin"
    source.write_bytes(b"a" * TRANSFER_CHUNK_BYTES + b"b" * 3)

    sandbox.files.upload(source, "/tmp/source.bin")

    chunks = [frame.chunk.data for frame in guest.write_requests[0] if frame.WhichOneof("frame") == "chunk"]
    assert [len(chunk) for chunk in chunks] == [TRANSFER_CHUNK_BYTES, 3]

    destination = tmp_path / "dest.bin"
    destination.write_bytes(b"old")
    guest.read_chunks = [b"new", b"\x00data"]
    sandbox.files.download("/tmp/source.bin", destination)

    assert destination.read_bytes() == b"new\x00data"
    assert list(tmp_path.glob(".dest.bin.bonya-download-*.tmp")) == []


def test_download_pre_replace_error_cleans_temp_and_preserves_destination(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    guest = FakeFilesystemGuest()
    guest.read_errors.put(RpcFailure(grpc.StatusCode.NOT_FOUND, "open file failed: missing"))
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    destination = tmp_path / "dest.bin"
    destination.write_bytes(b"old")

    with pytest.raises(RemoteFileNotFoundError):
        sandbox.files.download("/tmp/missing", destination)

    assert destination.read_bytes() == b"old"
    assert list(tmp_path.glob(".dest.bin.bonya-download-*.tmp")) == []


def test_download_tolerates_unsupported_parent_fsync(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    guest = FakeFilesystemGuest()
    guest.read_chunks = [b"new"]
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    destination = tmp_path / "dest.bin"
    destination.write_bytes(b"old")
    real_open = os.open
    real_fsync = os.fsync
    real_close = os.close
    parent_fd = 987654

    def fake_open(path: object, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        if Path(path) == tmp_path:
            return parent_fd
        return real_open(path, flags, mode, dir_fd=dir_fd)

    def fake_fsync(fd: int) -> None:
        if fd == parent_fd:
            raise OSError(22, "invalid argument")
        real_fsync(fd)

    def fake_close(fd: int) -> None:
        if fd != parent_fd:
            real_close(fd)

    monkeypatch.setattr(os, "open", fake_open)
    monkeypatch.setattr(os, "fsync", fake_fsync)
    monkeypatch.setattr(os, "close", fake_close)

    sandbox.files.download("/tmp/source.bin", destination)

    assert destination.read_bytes() == b"new"
    assert list(tmp_path.glob(".dest.bin.bonya-download-*.tmp")) == []


@pytest.mark.parametrize("failing_call", ["open", "fsync"])
def test_download_parent_fsync_real_failure_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failing_call: str
) -> None:
    guest = FakeFilesystemGuest()
    guest.read_chunks = [b"new"]
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    destination = tmp_path / "dest.bin"
    destination.write_bytes(b"old")
    original_open = os.open
    original_fsync = os.fsync
    original_close = os.close
    parent_fd = 987654

    if failing_call == "open":
        def fake_open(path: object, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
            if Path(path) == tmp_path:
                raise PermissionError(13, "permission denied")
            return original_open(path, flags, mode, dir_fd=dir_fd)

        monkeypatch.setattr(os, "open", fake_open)
    else:
        def fake_open(path: object, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
            if Path(path) == tmp_path:
                return parent_fd
            return original_open(path, flags, mode, dir_fd=dir_fd)

        def fake_fsync(fd: int) -> None:
            if fd == parent_fd:
                raise OSError(5, "io error")
            original_fsync(fd)

        def fake_close(fd: int) -> None:
            if fd != parent_fd:
                original_close(fd)

        monkeypatch.setattr(os, "open", fake_open)
        monkeypatch.setattr(os, "fsync", fake_fsync)
        monkeypatch.setattr(os, "close", fake_close)

    with pytest.raises(OSError):
        sandbox.files.download("/tmp/source.bin", destination)

    assert destination.read_bytes() == b"new"
    assert list(tmp_path.glob(".dest.bin.bonya-download-*.tmp")) == []


def test_read_cap_cancels_before_unbounded_growth(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    guest.read_chunks = [b"1234", b"5"]
    client, _ = make_files_client(monkeypatch, guest, read_limit=4)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(FilesystemLimitError):
        sandbox.files.read("/tmp/blob")


def test_list_returns_complete_sorted_immediate_children(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    guest.list_files = [
        guest_pb2.FileInfo(path="/tmp/b", name="b", kind=guest_pb2.FILE_KIND_DIRECTORY),
        guest_pb2.FileInfo(path="/tmp/a", name="a", kind=guest_pb2.FILE_KIND_SYMLINK),
        guest_pb2.FileInfo(path="/tmp/c", name="c", kind=guest_pb2.FILE_KIND_OTHER),
    ]
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    files = sandbox.files.list("/tmp")

    assert [file.name for file in files] == ["a", "b", "c"]
    assert [file.kind for file in files] == [FileKind.SYMLINK, FileKind.DIRECTORY, FileKind.OTHER]


@pytest.mark.parametrize(
    ("error", "error_cls", "method"),
    [
        (RpcFailure(grpc.StatusCode.NOT_FOUND, "missing"), RemoteFileNotFoundError, "stat"),
        (RpcFailure(grpc.StatusCode.ALREADY_EXISTS, "destination already exists"), RemoteFileExistsError, "move"),
        (RpcFailure(grpc.StatusCode.FAILED_PRECONDITION, "cross_filesystem_move"), CrossFilesystemMoveError, "move"),
        (RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED, "roxy filesystem frame limit exceeded"), FilesystemLimitError, "read"),
        (RpcFailure(grpc.StatusCode.INTERNAL, "disk failed"), FilesystemError, "mkdir"),
    ],
)
def test_filesystem_error_mapping(monkeypatch: pytest.MonkeyPatch, error: BaseException, error_cls: type[Exception], method: str) -> None:
    guest = FakeFilesystemGuest()
    getattr(guest, method + "_errors", guest.move_errors).put(error)
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(error_cls):
        if method == "stat":
            sandbox.files.stat("/tmp/missing")
        elif method == "move":
            sandbox.files.move("/tmp/a", "/tmp/b")
        elif method == "read":
            sandbox.files.read("/tmp/a")
        else:
            sandbox.files.mkdir("/tmp/a")


def test_filesystem_capability_rejection_refreshes_unexpired_token_once(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    guest.stat_errors.put(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "filesystem capability rejected"))
    client, transport = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    info = sandbox.files.stat("/tmp/blob")

    assert info.name == "blob"
    assert len(transport.tapi.get_requests) == 1
    assert sandbox._capability == "fresh-get-cap"
    assert len(guest.stat_requests) == 2


def test_filesystem_sandbox_binding_rejection_refreshes_unexpired_token_once(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    guest.stat_errors.put(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "filesystem capability sandbox binding rejected"))
    client, transport = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    info = sandbox.files.stat("/tmp/blob")

    assert info.name == "blob"
    assert len(transport.tapi.get_requests) == 1
    assert sandbox._capability == "fresh-get-cap"
    assert len(guest.stat_requests) == 2


def test_remote_file_permission_denied_does_not_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    guest.stat_errors.put(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "stat file failed: permission denied"))
    client, transport = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(FilesystemError):
        sandbox.files.stat("/root/secret")

    assert len(transport.tapi.get_requests) == 0
    assert len(guest.stat_requests) == 1


def test_filesystem_capability_rejection_retries_only_once(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    guest.stat_errors.put(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "filesystem capability rejected"))
    guest.stat_errors.put(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "filesystem capability rejected"))
    client, transport = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(CapabilityRejectedError):
        sandbox.files.stat("/tmp/blob")

    assert len(transport.tapi.get_requests) == 1
    assert len(guest.stat_requests) == 2


def test_mutating_filesystem_unavailable_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    guest.move_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "uncertain outcome"))
    client, transport = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(FilesystemError):
        sandbox.files.move("/tmp/a", "/tmp/b")

    assert len(guest.move_requests) == 1
    assert len(transport.tapi.get_requests) == 0


def test_filesystem_namespace_operations_serialize(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeFilesystemGuest()
    client, _ = make_files_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    sandbox.files.mkdir("/tmp/a")
    sandbox.files.remove("/tmp/a", recursive=True)
    sandbox.files.move("/tmp/a", "/tmp/b")

    assert guest.mkdir_requests[0].path == "/tmp/a"
    assert guest.remove_requests[0].recursive is True
    assert guest.move_requests[0].source_path == "/tmp/a"
    assert guest.move_requests[0].destination_path == "/tmp/b"


def test_exec_surface_still_refreshes_only_expired_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    from test_contract import FakeGuest, make_test_capability

    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    unexpired = make_test_capability(exp=int(time.time()) + 3600)
    transport.tapi.Create = lambda request, timeout=None: tapi_pb2.TApiServiceCreateResponse(  # type: ignore[method-assign]
        operation_id="op-1",
        sandbox_id="sbx-1",
        exec_capability_jws=unexpired,
        exec_endpoint="https://exec.example.test/edge",
        resolved_template_id="ubuntu-24.04",
        resolved_template_version="dev",
    )
    transport.guest.fail = True
    transport.guest.failure = RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "exec capability rejected")
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(CapabilityRejectedError):
        sandbox.exec(["printf", "x"])

    assert len(transport.tapi.get_requests) == 0
    assert transport.guest.calls == 1
