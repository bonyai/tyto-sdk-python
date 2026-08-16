from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar, cast

from ._errors import (
    AuthenticationError,
    InvalidRequestError,
    SandboxDeletedError,
    SandboxFailedError,
    TimeoutError,
)
from ._grpc_errors import map_rpc_error
from ._session import _validate_resize_dimension
from ._transport import Deadline
from ._types import Exit, Status, Stdout
from ._proto.tyto.runtime.v1 import guest_pb2

if TYPE_CHECKING:
    from ._sandbox import Sandbox

_guest_pb2: Any = guest_pb2
_T = TypeVar("_T")

_SESSION_NAME_PATTERN_FIRST = "abcdefghijklmnopqrstuvwxyz"
_SESSION_NAME_PATTERN_REST = _SESSION_NAME_PATTERN_FIRST + "0123456789-"
_MAX_SESSION_NAME_LENGTH = 32

_END_REQUESTS = object()


class SessionStatus(str, Enum):
    UNSPECIFIED = "unspecified"
    STARTING = "starting"
    IDLE = "idle"
    ATTACHED = "attached"
    EXITED = "exited"
    KILLED = "killed"
    FAILED = "failed"


class SessionEndedReason(str, Enum):
    UNSPECIFIED = "unspecified"
    DETACHED = "detached"
    TAKEOVER = "takeover"


@dataclass(frozen=True)
class SessionInfo:
    name: str
    command: tuple[str, ...]
    working_dir: str
    status: SessionStatus
    attached: bool
    started_at: datetime
    last_activity_at: datetime
    ended_at: datetime | None
    exit: Exit | None


@dataclass(frozen=True)
class SessionEnded:
    reason: SessionEndedReason


@dataclass(frozen=True)
class SessionOutputDropped:
    dropped_bytes: int


SessionEvent = Stdout | Exit | SessionEnded | SessionOutputDropped


@dataclass(frozen=True)
class SessionList(Sequence[SessionInfo]):
    sessions: tuple[SessionInfo, ...]
    sandbox_suspended: bool

    def __len__(self) -> int:
        return len(self.sessions)

    def __getitem__(self, index: Any) -> Any:
        return self.sessions[index]


class SessionStream:
    """A live attach to a managed session (Sprint 14).

    Mirrors ``ExecSession``'s streaming mechanics (background reader thread,
    bounded request queue, context manager, iterator) but proxies
    ``AttachSession`` instead of ``Exec``: the constructor blocks for the
    first (``accepted``) frame so ``info``/``replayed_bytes``/
    ``history_dropped`` are available immediately, before any iteration.
    """

    def __init__(
        self,
        *,
        sandbox_id: str,
        name: str,
        cols: int,
        rows: int,
        max_replay_bytes: int,
        stub: Any,
        capability: str,
        timeout: float,
        secrets: list[str],
    ) -> None:
        self._sandbox_id = sandbox_id
        self.name = name
        self._capability = capability
        self._secrets = secrets
        self._deadline = Deadline.start(timeout)
        self._cleanup_timeout = min(5.0, max(0.5, timeout))
        self._request_queue_size = 16
        self._request_cv = threading.Condition()
        self._requests: list[object] = []
        self._lock = threading.Lock()
        self._closed = False
        self._request_ended = False
        self._responses: queue.Queue[SessionEvent | BaseException | object] = queue.Queue(maxsize=16)
        start = _guest_pb2.AttachStart(name=name, cols=cols, rows=rows, max_replay_bytes=max_replay_bytes)
        metadata = (
            ("bonya-sandbox-id", sandbox_id),
            ("bonya-exec-capability", capability),
        )
        self._requests.append(_guest_pb2.AttachSessionRequest(start=start))
        self._stream = stub.AttachSession(
            self._request_iter(),
            timeout=self._deadline.remaining() + self._cleanup_timeout + 1.0,
            metadata=metadata,
        )
        try:
            first = next(self._stream)
        except BaseException as exc:
            self._cancel_rpc()
            raise map_rpc_error(exc, secrets=secrets, sandbox_id=sandbox_id, session_rpc=True) from exc
        if first.WhichOneof("frame") != "accepted":
            self._cancel_rpc()
            raise InvalidRequestError(
                "AttachSession response did not begin with an accepted frame", sandbox_id=sandbox_id
            )
        accepted = first.accepted
        self.info = _session_info_from_proto(accepted.session)
        self.replayed_bytes = int(accepted.replayed_bytes)
        self.history_dropped = bool(accepted.history_dropped)
        self.cols = int(accepted.cols)
        self.rows = int(accepted.rows)
        self._reader = threading.Thread(
            target=self._read_responses, name=f"bonya-session-{sandbox_id}-{name}", daemon=True
        )
        self._reader_started = False

    def __enter__(self) -> "SessionStream":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def __iter__(self) -> Iterator[SessionEvent]:
        return self

    def __next__(self) -> SessionEvent:
        self._ensure_reader()
        try:
            item = self._responses.get(timeout=self._deadline.remaining())
        except queue.Empty as exc:
            self.close()
            raise TimeoutError("session attach timed out", sandbox_id=self._sandbox_id) from exc
        except TimeoutError:
            self.close()
            raise
        if item is _END_REQUESTS:
            raise StopIteration
        if isinstance(item, BaseException):
            raise item
        return cast(SessionEvent, item)

    def _read_responses(self) -> None:
        try:
            for response in self._stream:
                event = self._response_event(response)
                if not self._put_response(event):
                    return
                if isinstance(event, (Exit, SessionEnded)):
                    self._put_response(_END_REQUESTS)
                    self._mark_terminal()
                    return
            self._put_response(_END_REQUESTS)
        except BaseException as exc:
            if self._closed:
                self._put_response(_END_REQUESTS)
                return
            mapped = map_rpc_error(exc, secrets=self._secrets, sandbox_id=self._sandbox_id, session_rpc=True)
            self._put_response(mapped)

    def _response_event(self, response: Any) -> SessionEvent:
        frame = response.WhichOneof("frame")
        if frame == "output":
            return Stdout(bytes(response.output.data))
        if frame == "exit":
            return Exit(
                exit_code=int(response.exit.exit_code),
                signaled=bool(response.exit.signaled),
                signal=int(response.exit.signal),
            )
        if frame == "ended":
            return SessionEnded(_session_ended_reason_from_proto(int(response.ended.reason)))
        if frame == "output_dropped":
            return SessionOutputDropped(int(response.output_dropped.dropped_bytes))
        raise InvalidRequestError("AttachSession response contained no frame", sandbox_id=self._sandbox_id)

    def write(self, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise InvalidRequestError("write() requires bytes")
        with self._lock:
            if self._closed:
                raise InvalidRequestError("session is closed", sandbox_id=self._sandbox_id)
        self._put(_guest_pb2.AttachSessionRequest(stdin=_guest_pb2.StdinData(data=bytes(data))))

    def resize(self, *, cols: int, rows: int) -> None:
        cols = _validate_resize_dimension("cols", cols)
        rows = _validate_resize_dimension("rows", rows)
        with self._lock:
            if self._closed:
                raise InvalidRequestError("session is closed", sandbox_id=self._sandbox_id)
        self._put(_guest_pb2.AttachSessionRequest(resize=_guest_pb2.ExecResize(cols=cols, rows=rows)))

    def detach(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if not self._request_ended:
            self._put_cleanup(_guest_pb2.AttachSessionRequest(detach=_guest_pb2.AttachDetach()))
            self._put_cleanup(_END_REQUESTS)
        else:
            self._cancel_rpc()
        self._wait_for_cleanup()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
        self.detach()

    def _mark_terminal(self) -> None:
        with self._lock:
            self._closed = True
        self._put_cleanup(_END_REQUESTS)

    def _request_iter(self) -> Iterator[Any]:
        try:
            while True:
                item = self._take_request()
                if item is _END_REQUESTS:
                    return
                yield item
        finally:
            with self._lock:
                self._request_ended = True

    def _put(self, item: object) -> None:
        stop_at = self._deadline.expires_at
        with self._request_cv:
            while len(self._requests) >= self._request_queue_size:
                remaining = stop_at - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("session request queue did not drain before deadline", sandbox_id=self._sandbox_id)
                self._request_cv.wait(timeout=remaining)
            self._requests.append(item)
            self._request_cv.notify_all()

    def _put_cleanup(self, item: object) -> None:
        stop_at = time.monotonic() + self._cleanup_timeout
        with self._request_cv:
            while len(self._requests) >= self._request_queue_size:
                remaining = stop_at - time.monotonic()
                if remaining <= 0:
                    return
                self._request_cv.wait(timeout=remaining)
            self._requests.append(item)
            self._request_cv.notify_all()

    def _put_response(self, item: SessionEvent | BaseException | object) -> bool:
        while True:
            with self._lock:
                if self._closed:
                    return False
            try:
                self._responses.put(item, timeout=0.05)
                return True
            except queue.Full:
                continue

    def _take_request(self) -> object:
        with self._request_cv:
            while not self._requests:
                self._request_cv.wait()
            item = self._requests.pop(0)
            self._request_cv.notify_all()
            return item

    def _wait_for_cleanup(self) -> None:
        if not self._reader_started:
            self._cancel_rpc()
            return
        self._reader.join(timeout=self._cleanup_timeout)
        if self._reader.is_alive():
            self._cancel_rpc()
            self._reader.join(timeout=0.1)

    def _cancel_rpc(self) -> None:
        cancel = getattr(self._stream, "cancel", None)
        if callable(cancel):
            cancel()

    def _ensure_reader(self) -> None:
        with self._lock:
            if self._reader_started:
                return
            self._reader_started = True
            self._reader.start()


class SandboxSessions:
    """Managed console session RPC surface (Sprint 14): persistent,
    guest-owned command sessions that outlive the client connection.

    Capability refresh follows S14.7's contract: an ``UNAUTHENTICATED``
    rejection (an expired token) transparently calls ``ReissueCapability``
    and retries exactly once, at admission time only, never mid-stream.
    ``PERMISSION_DENIED`` never triggers a refresh.
    """

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def create(
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
                response = self._stub().CreateSession(request, timeout=self._timeout(), metadata=self._metadata())
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return _session_info_from_proto(response.session)

        return self._with_capability_refresh(call)

    def list(self) -> SessionList:
        """List sessions. Works on a suspended sandbox without waking it
        (D13/F1): the result's ``sandbox_suspended`` is True when served
        from the suspend-time snapshot rather than the live guest."""

        def call() -> SessionList:
            request = _guest_pb2.ListSessionsRequest()
            try:
                response = self._stub().ListSessions(request, timeout=self._timeout(), metadata=self._metadata())
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return SessionList(
                sessions=tuple(_session_info_from_proto(info) for info in response.sessions),
                sandbox_suspended=bool(response.sandbox_suspended),
            )

        return self._with_capability_refresh(call)

    def kill(self, name: str, *, signal: str = "TERM", grace_ms: int = 5000) -> SessionInfo:
        """Signal (default TERM), then SIGKILL after grace_ms if still alive."""
        name = _validate_session_name(name)
        if not isinstance(signal, str) or not signal:
            raise InvalidRequestError("signal must be a non-empty string")
        if isinstance(grace_ms, bool) or not isinstance(grace_ms, int) or grace_ms < 0:
            raise InvalidRequestError("grace_ms must be a non-negative integer")

        def call() -> SessionInfo:
            request = _guest_pb2.KillSessionRequest(name=name, signal=signal, grace_ms=grace_ms)
            try:
                response = self._stub().KillSession(request, timeout=self._timeout(), metadata=self._metadata())
            except BaseException as exc:
                raise self._map_error(exc) from exc
            return _session_info_from_proto(response.session)

        return self._with_capability_refresh(call)

    def attach(self, name: str, *, cols: int = 0, rows: int = 0, max_replay_bytes: int = 0) -> SessionStream:
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
            sandbox = self._sandbox
            return SessionStream(
                sandbox_id=sandbox.id,
                name=name,
                cols=cols,
                rows=rows,
                max_replay_bytes=max_replay_bytes,
                stub=sandbox._client._exec_stub(sandbox._exec_endpoint),
                capability=sandbox._capability,
                timeout=sandbox._client._timeout,
                secrets=sandbox._client._secrets(sandbox._capability),
            )

        try:
            return open_stream()
        except AuthenticationError:
            self._sandbox.reissue_capability()
            return open_stream()

    def _with_capability_refresh(self, call: Callable[[], _T]) -> _T:
        self._ensure_sessions_allowed()
        try:
            return call()
        except AuthenticationError:
            self._sandbox.reissue_capability()
            return call()

    def _ensure_sessions_allowed(self) -> None:
        sandbox = self._sandbox
        if sandbox._deleted or sandbox.last_observed_status is Status.DELETED:
            raise SandboxDeletedError("sandbox has been deleted", sandbox_id=sandbox.id, operation_id=sandbox.operation_id)
        if sandbox.last_observed_status is Status.FAILED:
            message = sandbox._failure_message or sandbox._failure_code or "sandbox failed"
            raise SandboxFailedError(message, sandbox_id=sandbox.id, operation_id=sandbox.operation_id)

    def _stub(self) -> Any:
        return self._sandbox._client._exec_stub(self._sandbox._exec_endpoint)

    def _timeout(self) -> float:
        return Deadline.start(self._sandbox._client._timeout).remaining()

    def _metadata(self) -> tuple[tuple[str, str], tuple[str, str]]:
        return (
            ("bonya-sandbox-id", self._sandbox.id),
            ("bonya-exec-capability", self._sandbox._capability),
        )

    def _map_error(self, error: BaseException) -> BaseException:
        return map_rpc_error(
            error,
            secrets=self._sandbox._client._secrets(self._sandbox._capability),
            sandbox_id=self._sandbox.id,
            operation_id=self._sandbox.operation_id,
            session_rpc=True,
        )


def _validate_session_name(name: object) -> str:
    if not isinstance(name, str) or not name:
        raise InvalidRequestError("session name must be a non-empty string")
    if len(name) > _MAX_SESSION_NAME_LENGTH:
        raise InvalidRequestError(f"session name must be at most {_MAX_SESSION_NAME_LENGTH} characters")
    if name[0] not in _SESSION_NAME_PATTERN_FIRST or any(c not in _SESSION_NAME_PATTERN_REST for c in name[1:]):
        raise InvalidRequestError("session name must match ^[a-z][a-z0-9-]{0,31}$")
    return name


def _validate_session_command(command: object) -> list[str]:
    if isinstance(command, (str, bytes)) or not isinstance(command, Sequence):
        raise InvalidRequestError("command must be a non-empty sequence of strings")
    argv = list(command)
    if not argv or any(not isinstance(arg, str) or arg == "" for arg in argv):
        raise InvalidRequestError("command must be a non-empty sequence of non-empty strings")
    return argv


def _validate_session_env(env: Mapping[str, str] | None) -> dict[str, str]:
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


def _validate_session_cwd(cwd: str | None) -> str:
    if cwd is None:
        return ""
    if not isinstance(cwd, str) or cwd == "" or "\0" in cwd:
        raise InvalidRequestError("cwd must be a non-empty string without NUL")
    return cwd


def _validate_session_dimension(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidRequestError(f"{name} must be a non-negative integer <= 512")
    if not 0 <= value <= 512:
        raise InvalidRequestError(f"{name} must be a non-negative integer <= 512")
    return value


def _session_info_from_proto(info: Any) -> SessionInfo:
    ended_at_nanos = int(info.ended_at_unix_nanos)
    exit_proto = info.exit if info.HasField("exit") else None
    return SessionInfo(
        name=info.name,
        command=tuple(info.command),
        working_dir=info.working_dir,
        status=_session_status_from_proto(int(info.status)),
        attached=bool(info.attached),
        started_at=_datetime_from_unix_nanos(int(info.started_at_unix_nanos)),
        last_activity_at=_datetime_from_unix_nanos(int(info.last_activity_unix_nanos)),
        ended_at=_datetime_from_unix_nanos(ended_at_nanos) if ended_at_nanos else None,
        exit=(
            Exit(exit_code=int(exit_proto.exit_code), signaled=bool(exit_proto.signaled), signal=int(exit_proto.signal))
            if exit_proto is not None
            else None
        ),
    )


def _session_status_from_proto(value: int) -> SessionStatus:
    # SESSION_STATUS_UNSPECIFIED (0) is a value real, compatible servers can
    # send deliberately: Roxy falls back to it for a suspend-time snapshot
    # whose status string it doesn't recognize, and proto3's zero default
    # means an unset live status also decodes to 0. Any other unrecognized
    # value (e.g. a newer server) degrades the same way rather than raising,
    # so an older SDK stays forward compatible with new status values.
    mapping = {
        _guest_pb2.SessionStatus.SESSION_STATUS_STARTING: SessionStatus.STARTING,
        _guest_pb2.SessionStatus.SESSION_STATUS_IDLE: SessionStatus.IDLE,
        _guest_pb2.SessionStatus.SESSION_STATUS_ATTACHED: SessionStatus.ATTACHED,
        _guest_pb2.SessionStatus.SESSION_STATUS_EXITED: SessionStatus.EXITED,
        _guest_pb2.SessionStatus.SESSION_STATUS_KILLED: SessionStatus.KILLED,
        _guest_pb2.SessionStatus.SESSION_STATUS_FAILED: SessionStatus.FAILED,
    }
    return mapping.get(value, SessionStatus.UNSPECIFIED)


def _session_ended_reason_from_proto(value: int) -> SessionEndedReason:
    mapping = {
        _guest_pb2.AttachEnded.REASON_UNSPECIFIED: SessionEndedReason.UNSPECIFIED,
        _guest_pb2.AttachEnded.REASON_DETACHED: SessionEndedReason.DETACHED,
        _guest_pb2.AttachEnded.REASON_TAKEOVER: SessionEndedReason.TAKEOVER,
    }
    return mapping.get(value, SessionEndedReason.UNSPECIFIED)


def _datetime_from_unix_nanos(nanos: int) -> datetime:
    seconds, remainder = divmod(nanos, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc) + timedelta(microseconds=remainder // 1000)
