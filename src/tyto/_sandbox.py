from __future__ import annotations

import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import base64
import binascii
import json
import secrets
import time
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeVar

import grpc

from ._errors import (
    AuthenticationError,
    CapabilityRejectedError,
    ExecFailedError,
    FilesystemLimitError,
    InvalidRequestError,
    SandboxDeletedError,
    SandboxFailedError,
    SandboxSuspendedError,
)
from ._files import (
    TRANSFER_CHUNK_BYTES,
    FileInfo,
    FileKind,
    _file_info_from_proto,
    _fsync_parent,
    _normalize_write_data,
    _validate_remote_path,
)
from ._grpc_errors import is_retryable_transport_error, map_rpc_error
from ._transport import Deadline, sleep_with_deadline
from ._types import Exit, Status, Stderr, Stdout, Wait
from ._proto.tyto.runtime.v1 import guest_pb2, tapi_pb2
from ._session import ExecSession
from ._previews import (
    _MAX_PREVIEW_NAME_BYTES,
    _MAX_PREVIEW_PORT,
    _MIN_PREVIEW_PORT,
    _TOKEN_QUERY_PARAM,
    _AUTH_TO_PROTO,
    _preview_from_info,
    Preview,
    PreviewAuth,
)
from ._sessions import (
    SessionInfo,
    SessionList,
    SessionStream,
    _session_info_from_proto,
    _validate_session_command,
    _validate_session_cwd,
    _validate_session_dimension,
    _validate_session_env,
    _validate_session_name,
)

if TYPE_CHECKING:
    from ._client import Bonya


Command = str | Sequence[str]
_tapi_pb2: Any = tapi_pb2
_guest_pb2: Any = guest_pb2
_T = TypeVar("_T")


@dataclass(frozen=True)
class DeleteResult:
    sandbox_id: str
    already_deleted: bool


@dataclass(frozen=True)
class ResumeResult:
    sandbox_id: str
    lifecycle_operation_id: str
    already_running: bool


class Snapshot:
    def __init__(self, *, client: Bonya, snapshot_id: str, source_sandbox_id: str) -> None:
        self._client = client
        self.id = snapshot_id
        self.source_sandbox_id = source_sandbox_id
        self._deleted = False

    def __repr__(self) -> str:
        return f"Snapshot(id={self.id!r}, source_sandbox_id={self.source_sandbox_id!r})"

    def delete(self) -> None:
        if self._deleted:
            return None
        request = _tapi_pb2.TApiDeleteSnapshotRequest(
            api_key=self._client._api_key,
            source_sandbox_id=self.source_sandbox_id,
            snapshot_id=self.id,
        )
        deadline = Deadline.start(self._client._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                self._client._tapi_stub().DeleteSnapshot(request, timeout=deadline.remaining())
                self._deleted = True
                return None
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._client._max_retries:
                    raise map_rpc_error(
                        exc,
                        secrets=self._client._secrets(self.id),
                        sandbox_id=self.source_sandbox_id,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)


@dataclass(frozen=True)
class ExecResult:
    stdout_bytes: bytes
    stderr_bytes: bytes
    exit_code: int
    signaled: bool = False
    signal: int = 0
    sandbox_id: str | None = None

    @property
    def stdout(self) -> str:
        return self.stdout_bytes.decode("utf-8", errors="replace")

    @property
    def stderr(self) -> str:
        return self.stderr_bytes.decode("utf-8", errors="replace")

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.signaled

    def check(self) -> "ExecResult":
        if not self.ok:
            raise ExecFailedError(f"command failed with exit code {self.exit_code}", result=self)
        return self

    def __str__(self) -> str:
        return self.stdout

    def __repr__(self) -> str:
        stdout = _bounded(self.stdout)
        stderr = _bounded(self.stderr)
        return (
            "ExecResult("
            f"exit_code={self.exit_code}, signaled={self.signaled}, signal={self.signal}, "
            f"stdout={stdout!r}, stderr={stderr!r})"
        )


class Sandbox:
    def __init__(
        self,
        *,
        client: Bonya,
        sandbox_id: str,
        operation_id: str,
        template: str,
        version: str,
        status: Status,
        exec_endpoint: str,
        capability: str,
        failure_code: str | None = None,
        failure_message: str | None = None,
        name: str = "",
    ) -> None:
        self._client = client
        self.id = sandbox_id
        self.operation_id = operation_id
        self.template = template
        self.version = version
        self.last_observed_status = status
        # The display name. The service generates one when create() is not
        # given a name. Names are not unique; every operation is keyed by id.
        self.name = name
        self._exec_endpoint = exec_endpoint
        self._capability = capability
        self._failure_code = failure_code
        self._failure_message = failure_message
        self._deleted = False

    def __repr__(self) -> str:
        return (
            "Sandbox("
            f"id={self.id!r}, last_observed_status={self.last_observed_status.value!r}, "
            f"template={self.template!r}, version={self.version!r})"
        )

    def __enter__(self) -> "Sandbox":
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> Literal[False]:
        try:
            self.delete()
        except BaseException as cleanup_error:
            if exc is not None:
                exc.__context__ = cleanup_error
                return False
            raise
        return False

    def exec(
        self,
        command: Command,
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        tty: bool = False,
        cols: int | None = None,
        rows: int | None = None,
        timeout: float | None = None,
        check: bool = False,
        input: str | bytes | None = None,
    ) -> ExecResult:
        """Run a command and buffer stdout, stderr, and exit status.

        ``env`` overlays string environment variables for the process, and
        ``cwd`` sets its working directory. In TTY mode stdout and stderr share
        the terminal and are returned in stdout; stderr remains empty. ``input``
        may provide UTF-8 string data or raw bytes for non-TTY stdin; when set,
        stdin is half-closed before output is collected.
        """
        stdin = _normalize_exec_input(input, tty=tty)
        result = self._exec_buffered(
            command,
            env=env,
            cwd=cwd,
            tty=tty,
            cols=cols,
            rows=rows,
            timeout=timeout,
            input=stdin,
        )
        return result.check() if check else result

    def exec_stream(
        self,
        command: Command,
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        tty: bool = False,
        cols: int | None = None,
        rows: int | None = None,
        timeout: float | None = None,
    ) -> "ExecSession":
        """Start a streaming Exec session.

        ``env`` overlays string environment variables for the process, and
        ``cwd`` sets its working directory. In TTY mode stdout and stderr share
        the terminal and are emitted only as ``Stdout`` events; ``Stderr``
        events remain empty.
        """
        self._ensure_exec_allowed()
        tty_config = _validate_exec_tty_options(tty=tty, cols=cols, rows=rows)
        return _RefreshableExecSession(
            sandbox=self,
            command=_normalize_command(command),
            env=_normalize_env(env),
            cwd=_normalize_cwd(cwd),
            tty=tty_config.tty,
            cols=tty_config.cols,
            rows=tty_config.rows,
            timeout=timeout if timeout is not None else self._client._timeout,
        )

    def delete(self) -> DeleteResult:
        """Delete this sandbox. Idempotent: calling it again on the same
        handle is local and returns already_deleted=True without another RPC.

        The RPC itself is client.delete_sandbox(); this adds the local
        already-deleted short-circuit and updates the handle's own status,
        which only make sense with a handle to check and update.
        """
        if self._deleted:
            return DeleteResult(sandbox_id=self.id, already_deleted=True)
        result = self._client.delete_sandbox(self.id)
        self._deleted = True
        self.last_observed_status = Status.DELETED
        return result

    def snapshot(self, *, idempotency_key: str | None = None) -> Snapshot:
        if self._deleted or self.last_observed_status is Status.DELETED:
            raise SandboxDeletedError("sandbox has been deleted", sandbox_id=self.id, operation_id=self.operation_id)
        if self.last_observed_status is Status.FAILED:
            message = self._failure_message or self._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=self.id, operation_id=self.operation_id)
        if self.last_observed_status is Status.SUSPENDED:
            raise SandboxSuspendedError("sandbox is suspended", sandbox_id=self.id, operation_id=self.operation_id)
        key = idempotency_key or secrets.token_urlsafe(32)
        request = _tapi_pb2.TApiCreateSnapshotRequest(
            api_key=self._client._api_key,
            sandbox_id=self.id,
            idempotency_key=key,
        )
        deadline = Deadline.start(self._client._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                stub: Any = self._client._tapi_stub()
                response = stub.CreateSnapshot(request, timeout=deadline.remaining())
                snapshot_id = getattr(response, "snapshot_id", "")
                source_sandbox_id = getattr(response, "source_sandbox_id", "")
                if not snapshot_id or not source_sandbox_id:
                    raise InvalidRequestError(
                        "CreateSnapshot response is missing snapshot identity",
                        sandbox_id=self.id,
                        operation_id=self.operation_id,
                        idempotency_key=key,
                    )
                if source_sandbox_id != self.id:
                    raise InvalidRequestError(
                        "CreateSnapshot response is missing source identity",
                        sandbox_id=self.id,
                        operation_id=self.operation_id,
                        idempotency_key=key,
                    )
                return Snapshot(client=self._client, snapshot_id=snapshot_id, source_sandbox_id=source_sandbox_id)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._client._max_retries:
                    raise map_rpc_error(
                        exc,
                        secrets=self._client._secrets(key),
                        sandbox_id=self.id,
                        operation_id=self.operation_id,
                        idempotency_key=key,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def resume(self, *, idempotency_key: str | None = None) -> ResumeResult:
        """Explicitly resume a suspended sandbox before running work.

        The RPC itself is client._resume_sandbox(); this additionally
        copies the refreshed capability and exec endpoint onto the handle,
        which only makes sense with a handle to update, and checks for a
        locally known failed status before making a request the server
        would refuse anyway.
        """
        if self.last_observed_status is Status.FAILED:
            message = self._failure_message or self._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=self.id, operation_id=self.operation_id)
        result, response = self._client._resume_sandbox(self.id, idempotency_key=idempotency_key)
        capability = getattr(response, "exec_capability_jws", "")
        endpoint = getattr(response, "exec_endpoint", "")
        if capability:
            self._capability = capability
        if endpoint:
            self._exec_endpoint = endpoint
        self.last_observed_status = Status.RUNNING
        return result

    # Managed console sessions (Sprint 14): persistent, guest-owned command
    # sessions that outlive the client connection. Capability refresh follows
    # S14.7's contract: an UNAUTHENTICATED rejection (an expired token)
    # transparently calls reissue_capability() and retries exactly once, at
    # admission time only, never mid-stream. PERMISSION_DENIED never triggers
    # a refresh.

    def create_session(
        self,
        name: str,
        command: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        cols: int = 0,
        rows: int = 0,
        replace: bool = False,
    ) -> SessionInfo:
        """Create a named TTY session. Create over an existing record raises
        ``SessionExistsError``; ``replace=True`` replaces a terminal record only
        -- a running or attached session must be killed first."""
        name = _validate_session_name(name)
        argv = _validate_session_command(command)
        normalized_env = _validate_session_env(env)
        normalized_cwd = _validate_session_cwd(cwd)
        cols = _validate_session_dimension("cols", cols)
        rows = _validate_session_dimension("rows", rows)
        if not isinstance(replace, bool):
            raise InvalidRequestError("replace must be a boolean")

        def call() -> SessionInfo:
            request = _guest_pb2.CreateSessionRequest(
                name=name,
                command=argv,
                env=normalized_env,
                working_dir=normalized_cwd,
                cols=cols,
                rows=rows,
                replace=replace,
            )
            try:
                response = self._session_stub().CreateSession(
                    request, timeout=self._session_timeout(), metadata=self._session_metadata()
                )
            except BaseException as exc:
                raise self._map_session_error(exc) from exc
            return _session_info_from_proto(response.session)

        return self._with_session_capability_refresh(call)

    def list_sessions(self) -> SessionList:
        """List sessions. Works on a suspended sandbox without waking it
        (D13/F1): the result's ``sandbox_suspended`` is True when served
        from the suspend-time snapshot rather than the live guest."""

        def call() -> SessionList:
            request = _guest_pb2.ListSessionsRequest()
            try:
                response = self._session_stub().ListSessions(
                    request, timeout=self._session_timeout(), metadata=self._session_metadata()
                )
            except BaseException as exc:
                raise self._map_session_error(exc) from exc
            return SessionList(
                sessions=tuple(_session_info_from_proto(info) for info in response.sessions),
                sandbox_suspended=bool(response.sandbox_suspended),
            )

        return self._with_session_capability_refresh(call)

    def kill_session(self, name: str, *, signal: str = "TERM", grace_ms: int = 5000) -> SessionInfo:
        """Signal (default TERM), then SIGKILL after grace_ms if still alive."""
        name = _validate_session_name(name)
        if not isinstance(signal, str) or not signal:
            raise InvalidRequestError("signal must be a non-empty string")
        if isinstance(grace_ms, bool) or not isinstance(grace_ms, int) or grace_ms < 0:
            raise InvalidRequestError("grace_ms must be a non-negative integer")

        def call() -> SessionInfo:
            request = _guest_pb2.KillSessionRequest(name=name, signal=signal, grace_ms=grace_ms)
            try:
                response = self._session_stub().KillSession(
                    request, timeout=self._session_timeout(), metadata=self._session_metadata()
                )
            except BaseException as exc:
                raise self._map_session_error(exc) from exc
            return _session_info_from_proto(response.session)

        return self._with_session_capability_refresh(call)

    def attach_session(
        self, name: str, *, cols: int = 0, rows: int = 0, max_replay_bytes: int = 0
    ) -> SessionStream:
        """Attach to a session by name, replaying bounded output produced
        while detached. A second attach preempts an existing one -- the
        loser's stream ends with a TAKEOVER ``SessionEnded`` event."""
        name = _validate_session_name(name)
        cols = _validate_session_dimension("cols", cols)
        rows = _validate_session_dimension("rows", rows)
        if isinstance(max_replay_bytes, bool) or not isinstance(max_replay_bytes, int) or max_replay_bytes < 0:
            raise InvalidRequestError("max_replay_bytes must be a non-negative integer")
        self._ensure_sessions_allowed()

        def open_stream() -> SessionStream:
            return SessionStream(
                sandbox_id=self.id,
                name=name,
                cols=cols,
                rows=rows,
                max_replay_bytes=max_replay_bytes,
                stub=self._client._exec_stub(self._exec_endpoint),
                capability=self._capability,
                timeout=self._client._timeout,
                secrets=self._client._secrets(self._capability),
            )

        try:
            return open_stream()
        except AuthenticationError:
            self.reissue_capability()
            return open_stream()

    def _with_session_capability_refresh(self, call: Callable[[], _T]) -> _T:
        self._ensure_sessions_allowed()
        try:
            return call()
        except AuthenticationError:
            self.reissue_capability()
            return call()

    def _ensure_sessions_allowed(self) -> None:
        if self._deleted or self.last_observed_status is Status.DELETED:
            raise SandboxDeletedError("sandbox has been deleted", sandbox_id=self.id, operation_id=self.operation_id)
        if self.last_observed_status is Status.FAILED:
            message = self._failure_message or self._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=self.id, operation_id=self.operation_id)

    def _session_stub(self) -> Any:
        return self._client._exec_stub(self._exec_endpoint)

    def _session_timeout(self) -> float:
        return Deadline.start(self._client._timeout).remaining()

    def _session_metadata(self) -> tuple[tuple[str, str], tuple[str, str]]:
        return (
            ("bonya-sandbox-id", self.id),
            ("bonya-exec-capability", self._capability),
        )

    def _map_session_error(self, error: BaseException) -> BaseException:
        return map_rpc_error(
            error,
            secrets=self._client._secrets(self._capability),
            sandbox_id=self.id,
            operation_id=self.operation_id,
            session_rpc=True,
        )

    # Preview URLs: TApi calls authenticated with the API key, not
    # data-plane calls, so the capability-refresh wrapper that guards exec,
    # files, and sessions does not apply here -- there is no capability in
    # play on the request.

    def create_preview(
        self,
        port: int,
        *,
        auth: PreviewAuth = PreviewAuth.TOKEN,
        name: str | None = None,
        idempotency_key: str | None = None,
    ) -> Preview:
        """Publish a preview URL for a guest port.

        On success the sandbox's stored capability is replaced with the one
        returned. The preview scope is newer than the capability a sandbox was
        created with, and a token lacking it is refused by the preview ingress
        with a permission error that is deliberately not a refresh signal, so
        create hands back a usable token rather than leaving the caller to
        discover the gap on their first request.
        """
        if not isinstance(port, int) or isinstance(port, bool):
            raise InvalidRequestError("port must be an integer", sandbox_id=self.id)
        if port < _MIN_PREVIEW_PORT or port > _MAX_PREVIEW_PORT:
            raise InvalidRequestError(
                f"port must be between {_MIN_PREVIEW_PORT} and {_MAX_PREVIEW_PORT}",
                sandbox_id=self.id,
            )
        if not isinstance(auth, PreviewAuth):
            raise InvalidRequestError("auth must be a PreviewAuth", sandbox_id=self.id)
        display_name = name or ""
        if len(display_name.encode("utf-8")) > _MAX_PREVIEW_NAME_BYTES:
            raise InvalidRequestError(
                f"name exceeds {_MAX_PREVIEW_NAME_BYTES} bytes",
                sandbox_id=self.id,
            )
        key = idempotency_key or str(uuid.uuid4())
        if not key:
            raise InvalidRequestError("idempotency key must be non-empty", sandbox_id=self.id)

        request = _tapi_pb2.TApiCreatePreviewRequest(
            api_key=self._client._api_key,
            sandbox_id=self.id,
            port=port,
            auth_mode=_AUTH_TO_PROTO[auth],
            name=display_name,
            idempotency_key=key,
        )
        deadline = Deadline.start(self._client._timeout)
        try:
            stub: Any = self._client._tapi_stub()
            response = stub.CreatePreview(request, timeout=deadline.remaining())
        except Exception as error:  # noqa: BLE001 - re-raised as a typed error
            raise map_rpc_error(
                error,
                secrets=self._client._secrets(self._capability),
                sandbox_id=self.id,
            ) from error

        capability = getattr(response, "capability_jws", "")
        if capability:
            self._capability = capability
        if not response.preview.record.preview_id:
            raise InvalidRequestError(
                "CreatePreview response is missing the preview identity",
                sandbox_id=self.id,
                idempotency_key=key,
            )
        return _preview_from_info(response.preview)

    def list_previews(self) -> list[Preview]:
        """Every published preview for this sandbox."""
        request = _tapi_pb2.TApiListPreviewsRequest(
            api_key=self._client._api_key,
            sandbox_id=self.id,
        )
        deadline = Deadline.start(self._client._timeout)
        try:
            stub: Any = self._client._tapi_stub()
            response = stub.ListPreviews(request, timeout=deadline.remaining())
        except Exception as error:  # noqa: BLE001 - re-raised as a typed error
            raise map_rpc_error(
                error,
                secrets=self._client._secrets(self._capability),
                sandbox_id=self.id,
            ) from error
        return [_preview_from_info(info) for info in response.previews]

    def delete_preview(self, preview_id: str) -> None:
        """Revoke a preview URL."""
        if not preview_id:
            raise InvalidRequestError("preview id is required", sandbox_id=self.id)
        request = _tapi_pb2.TApiDeletePreviewRequest(
            api_key=self._client._api_key,
            sandbox_id=self.id,
            preview_id=preview_id,
        )
        deadline = Deadline.start(self._client._timeout)
        try:
            stub: Any = self._client._tapi_stub()
            stub.DeletePreview(request, timeout=deadline.remaining())
        except Exception as error:  # noqa: BLE001 - re-raised as a typed error
            raise map_rpc_error(
                error,
                secrets=self._client._secrets(self._capability),
                sandbox_id=self.id,
            ) from error

    def preview_browser_url(self, preview: Preview) -> str:
        """A one-time URL that logs a browser into a token-mode preview.

        The gateway validates the token, trades it for a host-scoped HttpOnly
        cookie, and redirects to the same URL without it -- so no page is ever
        rendered at an address containing the credential. Open it once; the
        cookie carries the session from there.

        This is never a content URL, and it must not be shared: anyone who
        receives it holds the sandbox's data-plane capability until it expires.

        Raises on a public preview, which has no token to exchange and whose
        plain ``url`` already works.
        """
        if preview.auth is PreviewAuth.PUBLIC:
            raise InvalidRequestError(
                "a public preview needs no token; use preview.url",
                sandbox_id=self.id,
            )
        capability = self._capability
        if not capability:
            raise InvalidRequestError(
                "no capability is available for this sandbox",
                sandbox_id=self.id,
            )
        separator = "&" if "?" in preview.url else "?"
        return f"{preview.url}{separator}{_TOKEN_QUERY_PARAM}={capability}"

    def read_file(self, path: str) -> bytes:
        """Buffer an entire remote file and return its bytes.

        Raises FilesystemLimitError before exceeding the client's filesystem
        read limit.
        """
        path = _validate_remote_path(path)

        def call() -> bytes:
            request = _guest_pb2.ReadFileRequest(sandbox_id=self.id, path=path)
            stream = self._file_stub().ReadFile(
                request,
                timeout=Deadline.start(self._client._timeout).remaining(),
                metadata=self._file_metadata(),
            )
            data = bytearray()
            try:
                for response in stream:
                    chunk = bytes(getattr(response, "data", b""))
                    if len(data) + len(chunk) > self._client._filesystem_read_limit:
                        cancel = getattr(stream, "cancel", None)
                        if callable(cancel):
                            cancel()
                        raise FilesystemLimitError(
                            "filesystem read exceeded client memory limit",
                            sandbox_id=self.id,
                            operation_id=self.operation_id,
                        )
                    data.extend(chunk)
            except FilesystemLimitError:
                raise
            except BaseException as exc:
                raise self._map_file_error(exc) from exc
            return bytes(data)

        return self._with_file_capability_refresh(call)

    def write_file(self, path: str, data: bytes | str) -> None:
        """Write data to a remote path, streamed in 64 KiB chunks through a
        guest-side temporary file and published atomically."""
        path = _validate_remote_path(path)
        payload = _normalize_write_data(data)

        def requests() -> Any:
            yield _guest_pb2.WriteFileRequest(start=_guest_pb2.WriteFileStart(sandbox_id=self.id, path=path))
            if not payload:
                return
            for offset in range(0, len(payload), TRANSFER_CHUNK_BYTES):
                yield _guest_pb2.WriteFileRequest(
                    chunk=_guest_pb2.WriteFileChunk(data=payload[offset : offset + TRANSFER_CHUNK_BYTES])
                )

        self._write_file_stream(requests)

    def upload_file(self, local_path: str | os.PathLike[str], remote_path: str) -> None:
        """Stream a local file to the remote path in 64 KiB chunks."""
        remote_path = _validate_remote_path(remote_path)
        source = Path(local_path)

        def requests() -> Any:
            yield _guest_pb2.WriteFileRequest(
                start=_guest_pb2.WriteFileStart(sandbox_id=self.id, path=remote_path)
            )
            with source.open("rb") as file:
                while True:
                    chunk = file.read(TRANSFER_CHUNK_BYTES)
                    if not chunk:
                        break
                    yield _guest_pb2.WriteFileRequest(chunk=_guest_pb2.WriteFileChunk(data=chunk))

        self._write_file_stream(requests)

    def download_file(self, remote_path: str, local_path: str | os.PathLike[str]) -> None:
        """Stream a remote file into a hidden temporary file in the
        destination directory, fsync it, and atomically replace the
        destination."""
        remote_path = _validate_remote_path(remote_path)
        destination = Path(local_path)
        parent = destination.parent if str(destination.parent) else Path(".")
        temp = parent / f".{destination.name}.bonya-download-{uuid.uuid4().hex}.tmp"
        replaced = False
        try:
            with temp.open("xb") as file:
                self._download_file_to(remote_path, file)
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

    def list_files(self, path: str) -> list[FileInfo]:
        """Return immediate children of a remote directory, sorted by name."""
        path = _validate_remote_path(path)

        def call() -> list[FileInfo]:
            request = _guest_pb2.ListDirectoryRequest(sandbox_id=self.id, path=path)
            stream = self._file_stub().ListDirectory(
                request,
                timeout=Deadline.start(self._client._timeout).remaining(),
                metadata=self._file_metadata(),
            )
            files: list[FileInfo] = []
            try:
                for response in stream:
                    file = getattr(response, "file", None)
                    if file is None:
                        raise InvalidRequestError("ListDirectory response is missing file metadata")
                    files.append(_file_info_from_proto(file))
            except BaseException as exc:
                raise self._map_file_error(exc) from exc
            return sorted(files, key=lambda item: item.name)

        return self._with_file_capability_refresh(call)

    def stat_file(self, path: str) -> FileInfo:
        """Return lstat-style metadata for a remote path."""
        path = _validate_remote_path(path)

        def call() -> FileInfo:
            request = _guest_pb2.StatFileRequest(sandbox_id=self.id, path=path)
            try:
                response = self._file_stub().StatFile(
                    request,
                    timeout=Deadline.start(self._client._timeout).remaining(),
                    metadata=self._file_metadata(),
                )
            except BaseException as exc:
                raise self._map_file_error(exc) from exc
            file = getattr(response, "file", None)
            if file is None:
                raise InvalidRequestError("StatFile response is missing file metadata")
            return _file_info_from_proto(file)

        return self._with_file_capability_refresh(call)

    def mkdir_file(self, path: str) -> None:
        """Create a remote directory."""
        path = _validate_remote_path(path)
        self._unary_file_mutation(lambda: _guest_pb2.MakeDirectoryRequest(sandbox_id=self.id, path=path), "MakeDirectory")

    def remove_file(self, path: str, recursive: bool = False) -> None:
        """Remove a remote path, recursively if recursive is True."""
        path = _validate_remote_path(path)
        if not isinstance(recursive, bool):
            raise InvalidRequestError("recursive must be a boolean")
        self._unary_file_mutation(
            lambda: _guest_pb2.RemoveFileRequest(sandbox_id=self.id, path=path, recursive=recursive),
            "RemoveFile",
        )

    def move_file(self, source: str, destination: str) -> None:
        """Move a remote file or directory. Same-filesystem, atomic, and
        no-overwrite."""
        source = _validate_remote_path(source)
        destination = _validate_remote_path(destination)
        self._unary_file_mutation(
            lambda: _guest_pb2.MoveFileRequest(sandbox_id=self.id, source_path=source, destination_path=destination),
            "MoveFile",
        )

    def _write_file_stream(self, request_factory: Callable[[], Any]) -> None:
        def call() -> None:
            try:
                self._file_stub().WriteFile(
                    request_factory(),
                    timeout=Deadline.start(self._client._timeout).remaining(),
                    metadata=self._file_metadata(),
                )
            except BaseException as exc:
                raise self._map_file_error(exc) from exc
            return None

        self._with_file_capability_refresh(call)

    def _download_file_to(self, remote_path: str, file: Any) -> None:
        def call() -> None:
            request = _guest_pb2.ReadFileRequest(sandbox_id=self.id, path=remote_path)
            stream = self._file_stub().ReadFile(
                request,
                timeout=Deadline.start(self._client._timeout).remaining(),
                metadata=self._file_metadata(),
            )
            try:
                for response in stream:
                    file.write(bytes(getattr(response, "data", b"")))
            except BaseException as exc:
                raise self._map_file_error(exc) from exc
            return None

        self._with_file_capability_refresh(call)

    def _unary_file_mutation(self, request_factory: Callable[[], object], method_name: str) -> None:
        def call() -> None:
            try:
                method = getattr(self._file_stub(), method_name)
                method(
                    request_factory(),
                    timeout=Deadline.start(self._client._timeout).remaining(),
                    metadata=self._file_metadata(),
                )
            except BaseException as exc:
                raise self._map_file_error(exc) from exc
            return None

        self._with_file_capability_refresh(call)

    def _with_file_capability_refresh(self, call: Callable[[], _T]) -> _T:
        self._ensure_files_allowed()
        try:
            return call()
        except CapabilityRejectedError:
            self._refresh_capability_once()
            return call()

    def _ensure_files_allowed(self) -> None:
        if self._deleted or self.last_observed_status is Status.DELETED:
            raise SandboxDeletedError("sandbox has been deleted", sandbox_id=self.id, operation_id=self.operation_id)
        if self.last_observed_status is Status.FAILED:
            message = self._failure_message or self._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=self.id, operation_id=self.operation_id)

    def _file_stub(self) -> Any:
        return self._client._exec_stub(self._exec_endpoint)

    def _file_metadata(self) -> tuple[tuple[str, str], tuple[str, str]]:
        return (
            ("bonya-sandbox-id", self.id),
            ("bonya-exec-capability", self._capability),
        )

    def _map_file_error(self, error: BaseException) -> BaseException:
        mapped = map_rpc_error(
            error,
            secrets=self._client._secrets(self._capability),
            sandbox_id=self.id,
            operation_id=self.operation_id,
            filesystem_rpc=True,
        )
        if isinstance(mapped, SandboxDeletedError):
            self._deleted = True
            self.last_observed_status = Status.DELETED
        return mapped

    def _exec_buffered(
        self,
        command: Command,
        *,
        env: Mapping[str, str] | None,
        cwd: str | None,
        tty: bool,
        cols: int | None,
        rows: int | None,
        timeout: float | None,
        input: bytes | None,
    ) -> ExecResult:
        with self.exec_stream(command, env=env, cwd=cwd, tty=tty, cols=cols, rows=rows, timeout=timeout) as session:
            if input is not None:
                session.write(input)
                session.close_stdin()
            stdout = bytearray()
            stderr = bytearray()
            terminal: Exit | None = None
            try:
                for event in session:
                    if isinstance(event, Stdout):
                        stdout.extend(event.data)
                    elif isinstance(event, Stderr):
                        stderr.extend(event.data)
                    elif isinstance(event, Exit):
                        terminal = event
            except BaseException:
                session.cancel()
                raise
            if terminal is None:
                raise InvalidRequestError("Exec stream ended without an exit event", sandbox_id=self.id)
            return ExecResult(
                stdout_bytes=bytes(stdout),
                stderr_bytes=bytes(stderr),
                exit_code=terminal.exit_code,
                signaled=terminal.signaled,
                signal=terminal.signal,
                sandbox_id=self.id,
            )

    def _ensure_exec_allowed(self) -> None:
        if self._deleted or self.last_observed_status is Status.DELETED:
            raise SandboxDeletedError("sandbox has been deleted", sandbox_id=self.id, operation_id=self.operation_id)
        if self.last_observed_status is Status.FAILED:
            message = self._failure_message or self._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=self.id, operation_id=self.operation_id)

    def _refresh_capability_once(self) -> None:
        refreshed = self._client.get_sandbox(self.id)
        if refreshed.last_observed_status is Status.FAILED:
            message = refreshed._failure_message or refreshed._failure_code or "sandbox failed"
            self.last_observed_status = Status.FAILED
            self._failure_code = refreshed._failure_code
            self._failure_message = refreshed._failure_message
            raise SandboxFailedError(message, sandbox_id=self.id, operation_id=self.operation_id)
        self.operation_id = refreshed.operation_id
        self.template = refreshed.template
        self.version = refreshed.version
        self.last_observed_status = refreshed.last_observed_status
        self._exec_endpoint = refreshed._exec_endpoint
        self._capability = refreshed._capability
        self._failure_code = None
        self._failure_message = None

    def _refresh_exec_capability_once(self) -> None:
        self._refresh_capability_once()

    def reissue_capability(self) -> None:
        """Mint a fresh data-plane capability via TApi's ReissueCapability
        and use it for subsequent calls on this Sandbox.

        ``sessions`` calls this transparently on an ``UNAUTHENTICATED``
        (expired-token) rejection, at most once per call, before any stream
        effect (Sprint 14.7's contract). Call it directly only if you manage
        tokens yourself.
        """
        request = _tapi_pb2.TApiReissueCapabilityRequest(api_key=self._client._api_key, sandbox_id=self.id)
        deadline = Deadline.start(self._client._timeout)
        try:
            response = self._client._tapi_stub().ReissueCapability(request, timeout=deadline.remaining())
        except BaseException as exc:
            raise map_rpc_error(
                exc,
                secrets=self._client._secrets(self._capability),
                sandbox_id=self.id,
                operation_id=self.operation_id,
            ) from exc
        capability = getattr(response, "capability_jws", "")
        if not capability:
            raise InvalidRequestError(
                "ReissueCapability response is missing capability_jws",
                sandbox_id=self.id,
                operation_id=self.operation_id,
            )
        self._capability = capability

    def _observe_exec_error(self, error: BaseException) -> BaseException:
        mapped = map_rpc_error(
            error,
            secrets=self._client._secrets(self._capability),
            sandbox_id=self.id,
            operation_id=self.operation_id,
            exec_rpc=True,
        )
        if isinstance(mapped, SandboxDeletedError):
            self._deleted = True
            self.last_observed_status = Status.DELETED
        elif isinstance(mapped, SandboxSuspendedError):
            self.last_observed_status = Status.SUSPENDED
        elif isinstance(mapped, InvalidRequestError):
            pass
        elif isinstance(error, grpc.RpcError) and error.code() == grpc.StatusCode.FAILED_PRECONDITION:
            self.last_observed_status = Status.FAILED
        return mapped


class _RefreshableExecSession(ExecSession):
    def __init__(
        self,
        *,
        sandbox: Sandbox,
        command: list[str],
        env: dict[str, str],
        cwd: str,
        tty: bool,
        cols: int,
        rows: int,
        timeout: float,
    ) -> None:
        self._sandbox = sandbox
        self._command = command
        self._env = env
        self._cwd = cwd
        self._tty = tty
        self._cols = cols
        self._rows = rows
        self._timeout = timeout
        self._refreshed = False
        self._responses_started = False
        self._pending_inputs: list[tuple[str, bytes | int, int]] = []
        self._session = self._new_session()
        self._reader = self._session._reader

    def __enter__(self) -> "_RefreshableExecSession":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def __iter__(self) -> "_RefreshableExecSession":
        return self

    def __next__(self) -> Exit | Stderr | Stdout:
        try:
            event = next(self._session)
        except CapabilityRejectedError:
            if self._refreshed or not _capability_is_expired(self._sandbox._capability):
                raise
            self._refreshed = True
            self._session.close()
            self._sandbox._refresh_exec_capability_once()
            self._session = self._new_session()
            self._reader = self._session._reader
            self._replay_pending_inputs()
            event = next(self._session)
            self._responses_started = True
            return event
        self._responses_started = True
        return event

    def write(self, data: bytes) -> None:
        self._session.write(data)
        if not self._responses_started and not self._refreshed:
            self._pending_inputs.append(("write", bytes(data), 0))

    def close_stdin(self) -> None:
        self._session.close_stdin()
        if not self._responses_started and not self._refreshed:
            self._pending_inputs.append(("close_stdin", b"", 0))

    def resize(self, *, cols: int, rows: int) -> None:
        self._session.resize(cols=cols, rows=rows)
        if not self._responses_started and not self._refreshed:
            self._pending_inputs.append(("resize", cols, rows))

    def cancel(self) -> None:
        self._session.cancel()

    def close(self) -> None:
        self._session.close()

    def _new_session(self) -> ExecSession:
        sandbox = self._sandbox
        return ExecSession(
            sandbox_id=sandbox.id,
            operation_id=sandbox.operation_id,
            command=self._command,
            env=self._env,
            cwd=self._cwd,
            tty=self._tty,
            cols=self._cols,
            rows=self._rows,
            stub=sandbox._client._exec_stub(sandbox._exec_endpoint),
            capability=sandbox._capability,
            timeout=self._timeout,
            secrets=sandbox._client._secrets(sandbox._capability),
            on_error=sandbox._observe_exec_error,
        )

    def _replay_pending_inputs(self) -> None:
        for kind, first, second in self._pending_inputs:
            if kind == "write":
                self._session.write(first if isinstance(first, bytes) else bytes(first))
            elif kind == "close_stdin":
                self._session.close_stdin()
            elif kind == "resize":
                self._session.resize(cols=int(first), rows=second)


def _normalize_command(command: Command) -> list[str]:
    if isinstance(command, str):
        if not command:
            raise InvalidRequestError("command must not be empty")
        return ["/bin/sh", "-c", command]
    argv = list(command)
    if not argv or any(not isinstance(arg, str) or arg == "" for arg in argv):
        raise InvalidRequestError("command must be a non-empty string sequence")
    return argv


def _normalize_exec_input(input: object, *, tty: bool) -> bytes | None:
    if input is None:
        return None
    if tty:
        raise InvalidRequestError("input requires tty=False")
    if isinstance(input, str):
        return input.encode("utf-8")
    if isinstance(input, bytes):
        return input
    raise InvalidRequestError("input must be str, bytes, or None")


def _normalize_env(env: Mapping[str, str] | None) -> dict[str, str]:
    if env is None:
        return {}
    if not isinstance(env, Mapping):
        raise InvalidRequestError("env must be a mapping of string keys to string values")
    normalized: dict[str, str] = {}
    for key, value in env.items():
        if not isinstance(key, str) or key == "" or "=" in key or "\0" in key:
            raise InvalidRequestError("env keys must be non-empty strings without '=' or NUL")
        if not isinstance(value, str) or "\0" in value:
            raise InvalidRequestError("env values must be strings without NUL")
        normalized[key] = value
    return normalized


def _normalize_cwd(cwd: str | None) -> str:
    if cwd is None:
        return ""
    if not isinstance(cwd, str) or cwd == "" or "\0" in cwd:
        raise InvalidRequestError("cwd must be a non-empty string without NUL")
    return cwd


@dataclass(frozen=True)
class _ExecTtyOptions:
    tty: bool
    cols: int
    rows: int


def _validate_exec_tty_options(*, tty: bool, cols: int | None, rows: int | None) -> _ExecTtyOptions:
    if not isinstance(tty, bool):
        raise InvalidRequestError("tty must be a boolean")
    if not tty:
        if cols is not None or rows is not None:
            raise InvalidRequestError("tty dimensions require tty=True")
        return _ExecTtyOptions(tty=False, cols=0, rows=0)
    if cols is None and rows is None:
        return _ExecTtyOptions(tty=True, cols=0, rows=0)
    return _ExecTtyOptions(
        tty=True,
        cols=_validate_tty_dimension("cols", cols),
        rows=_validate_tty_dimension("rows", rows),
    )


def _validate_tty_dimension(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidRequestError(f"{name} must be a positive integer <= 512")
    if not 1 <= value <= 512:
        raise InvalidRequestError(f"{name} must be a positive integer <= 512")
    return value


def _capability_is_expired(capability: str) -> bool:
    parts = capability.split(".")
    if len(parts) != 3:
        return False
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        exp = claims.get("exp")
    except (binascii.Error, ValueError, TypeError, json.JSONDecodeError):
        return False
    return isinstance(exp, (int, float)) and exp <= time.time()


def _bounded(value: str, limit: int = 160) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "...[truncated]"
