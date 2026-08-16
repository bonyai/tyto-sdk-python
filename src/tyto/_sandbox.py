from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import base64
import binascii
import json
import secrets
import time
from typing import TYPE_CHECKING, Any, Literal

import grpc

from ._errors import (
    CapabilityRejectedError,
    ExecFailedError,
    InvalidRequestError,
    SandboxDeletedError,
    SandboxFailedError,
    SandboxSuspendedError,
)
from ._grpc_errors import is_retryable_transport_error, map_rpc_error
from ._transport import Deadline, sleep_with_deadline
from ._types import Exit, Status, Stderr, Stdout, Wait
from ._proto.tyto.runtime.v1 import guest_pb2, tapi_pb2
from ._session import ExecSession
from ._files import SandboxFiles
from ._previews import SandboxPreviews
from ._sessions import SandboxSessions

if TYPE_CHECKING:
    from ._client import Bonya


Command = str | Sequence[str]
_tapi_pb2: Any = tapi_pb2


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
        self.files = SandboxFiles(self)
        self.sessions = SandboxSessions(self)
        self.previews = SandboxPreviews(self)

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

        The RPC itself is client.sandboxes.delete(); this adds the local
        already-deleted short-circuit and updates the handle's own status,
        which only make sense with a handle to check and update.
        """
        if self._deleted:
            return DeleteResult(sandbox_id=self.id, already_deleted=True)
        result = self._client.sandboxes.delete(self.id)
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

        The RPC itself is client.sandboxes._resume(); this additionally
        copies the refreshed capability and exec endpoint onto the handle,
        which only makes sense with a handle to update, and checks for a
        locally known failed status before making a request the server
        would refuse anyway.
        """
        if self.last_observed_status is Status.FAILED:
            message = self._failure_message or self._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=self.id, operation_id=self.operation_id)
        result, response = self._client.sandboxes._resume(self.id, idempotency_key=idempotency_key)
        capability = getattr(response, "exec_capability_jws", "")
        endpoint = getattr(response, "exec_endpoint", "")
        if capability:
            self._capability = capability
        if endpoint:
            self._exec_endpoint = endpoint
        self.last_observed_status = Status.RUNNING
        return result

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
        refreshed = self._client.sandboxes.get(self.id)
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
