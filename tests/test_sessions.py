from __future__ import annotations

import queue
from collections.abc import Iterator
from typing import Any

import grpc
import pytest

from tyto import (
    AuthenticationError,
    Tyto,
    CapabilityRejectedError,
    InvalidRequestError,
    SessionEnded,
    SessionEndedReason,
    SessionExists,
    SessionInfo,
    SessionNotFoundError,
    SessionOutputDropped,
    SessionStatus,
    SessionStream,
    Stdout,
)
from tyto._types import Exit
from tyto._proto.tyto.runtime.v1 import guest_pb2

from test_contract import FakeTapi, FakeTransport, RpcFailure


def _accepted_response(
    *, name: str = "server", replayed_bytes: int = 0, history_dropped: bool = False, cols: int = 80, rows: int = 24
) -> Any:
    return guest_pb2.AttachSessionResponse(
        accepted=guest_pb2.AttachAccepted(
            session=guest_pb2.SessionInfo(
                name=name,
                command=["bash"],
                status=guest_pb2.SESSION_STATUS_ATTACHED,
                attached=True,
                started_at_unix_nanos=1_700_000_000_000_000_000,
            ),
            replayed_bytes=replayed_bytes,
            history_dropped=history_dropped,
            cols=cols,
            rows=rows,
        )
    )


class FakeSessionStream:
    def __init__(self, requests: Iterator[Any], responses: list[Any]) -> None:
        self.requests = requests
        self.responses = responses
        self.index = 0
        self.seen: list[Any] = []
        self.cancel_called = False

    def __iter__(self) -> "FakeSessionStream":
        return self

    def __next__(self) -> Any:
        if self.index >= len(self.responses):
            raise StopIteration
        response = self.responses[self.index]
        self.index += 1
        return response

    def cancel(self) -> None:
        self.cancel_called = True

    def collect_requests(self, limit: int = 10) -> list[str]:
        frames: list[str] = []
        for _ in range(limit):
            try:
                request = next(self.requests)
            except StopIteration:
                break
            self.seen.append(request)
            frames.append(request.WhichOneof("frame"))
        return frames


class FailingSessionStream:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.cancel_called = False

    def __iter__(self) -> "FailingSessionStream":
        return self

    def __next__(self) -> Any:
        raise self.error

    def cancel(self) -> None:
        self.cancel_called = True


class SessionStreamThenFail:
    """Yields exactly one response, then raises on every subsequent pull --
    for proving mid-stream errors are never treated as refresh triggers."""

    def __init__(self, first_response: Any, error: BaseException) -> None:
        self.first_response = first_response
        self.error = error
        self.index = 0
        self.cancel_called = False

    def __iter__(self) -> "SessionStreamThenFail":
        return self

    def __next__(self) -> Any:
        self.index += 1
        if self.index == 1:
            return self.first_response
        raise self.error

    def cancel(self) -> None:
        self.cancel_called = True


class FakeSessionGuest:
    def __init__(self) -> None:
        self.create_errors: queue.Queue[BaseException] = queue.Queue()
        self.list_errors: queue.Queue[BaseException] = queue.Queue()
        self.kill_errors: queue.Queue[BaseException] = queue.Queue()
        self.attach_errors: queue.Queue[BaseException] = queue.Queue()
        self.create_requests: list[Any] = []
        self.list_requests: list[Any] = []
        self.kill_requests: list[Any] = []
        self.metadata: list[Any] = []
        self.list_response: Any = None
        self.attach_responses: list[Any] = [
            _accepted_response(),
            guest_pb2.AttachSessionResponse(output=guest_pb2.StdoutData(data=b"hello")),
            guest_pb2.AttachSessionResponse(exit=guest_pb2.ExecExit(exit_code=0)),
        ]
        self.next_attach_stream: Any = None
        self.attach_stream: Any = None

    def CreateSession(self, request: Any, timeout: float | None = None, metadata: Any = None) -> Any:
        self.create_requests.append(request)
        self.metadata.append(metadata)
        if not self.create_errors.empty():
            raise self.create_errors.get()
        info = guest_pb2.SessionInfo(
            name=request.name,
            command=list(request.command),
            working_dir=request.working_dir,
            status=guest_pb2.SESSION_STATUS_IDLE,
            attached=False,
            started_at_unix_nanos=1_700_000_000_000_000_000,
        )
        return guest_pb2.CreateSessionResponse(session=info)

    def ListSessions(self, request: Any, timeout: float | None = None, metadata: Any = None) -> Any:
        self.list_requests.append(request)
        self.metadata.append(metadata)
        if not self.list_errors.empty():
            raise self.list_errors.get()
        if self.list_response is not None:
            return self.list_response
        return guest_pb2.ListSessionsResponse()

    def KillSession(self, request: Any, timeout: float | None = None, metadata: Any = None) -> Any:
        self.kill_requests.append(request)
        self.metadata.append(metadata)
        if not self.kill_errors.empty():
            raise self.kill_errors.get()
        info = guest_pb2.SessionInfo(
            name=request.name,
            status=guest_pb2.SESSION_STATUS_KILLED,
            ended_at_unix_nanos=1_700_000_001_000_000_000,
            exit=guest_pb2.ExecExit(exit_code=0, signaled=True, signal=15),
        )
        return guest_pb2.KillSessionResponse(session=info)

    def AttachSession(self, requests: Iterator[Any], timeout: float | None = None, metadata: Any = None) -> Any:
        self.metadata.append(metadata)
        if self.next_attach_stream is not None:
            stream = self.next_attach_stream
            self.next_attach_stream = None
            self.attach_stream = stream
            return stream
        if not self.attach_errors.empty():
            self.attach_stream = FailingSessionStream(self.attach_errors.get())
            return self.attach_stream
        self.attach_stream = FakeSessionStream(requests, list(self.attach_responses))
        return self.attach_stream


def make_sessions_client(monkeypatch: pytest.MonkeyPatch, guest: FakeSessionGuest) -> tuple[Tyto, FakeTransport]:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = guest  # type: ignore[assignment]
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    client = Tyto(
        endpoint="https://api.example.test/",
        timeout=2,
        max_retries=2,
        _channel_factory=transport.channel_factory,
        _tapi_stub_factory=transport.tapi_stub,
        _guest_stub_factory=transport.guest_stub,
    )
    return client, transport


# --- Typed objects / happy path -----------------------------------------


def test_create_list_kill_return_typed_session_info(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.list_response = guest_pb2.ListSessionsResponse(
        sessions=[
            guest_pb2.SessionInfo(name="server", status=guest_pb2.SESSION_STATUS_IDLE, command=["bash"]),
        ],
        sandbox_suspended=False,
    )
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    created = sandbox.sessions.create("server", ["bash"], cols=120, rows=40)
    assert isinstance(created, SessionInfo)
    assert created.name == "server"
    assert created.command == ("bash",)
    assert created.status is SessionStatus.IDLE
    assert guest.create_requests[0].cols == 120
    assert guest.create_requests[0].rows == 40

    listed = sandbox.sessions.list()
    assert listed.sandbox_suspended is False
    assert [s.name for s in listed] == ["server"]
    assert len(listed) == 1

    killed = sandbox.sessions.kill("server", signal="KILL", grace_ms=1000)
    assert killed.status is SessionStatus.KILLED
    assert killed.exit == Exit(exit_code=0, signaled=True, signal=15)
    assert killed.ended_at is not None
    assert guest.kill_requests[0].signal == "KILL"
    assert guest.kill_requests[0].grace_ms == 1000


def test_list_reports_sandbox_suspended_without_local_blocking(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.list_response = guest_pb2.ListSessionsResponse(
        sessions=[guest_pb2.SessionInfo(name="server", status=guest_pb2.SESSION_STATUS_IDLE)],
        sandbox_suspended=True,
    )
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    result = sandbox.sessions.list()

    assert result.sandbox_suspended is True
    assert [s.name for s in result] == ["server"]


def test_non_terminal_session_info_has_no_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    info = sandbox.sessions.create("server", ["bash"])

    assert info.exit is None
    assert info.ended_at is None


def test_unspecified_session_status_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    # SESSION_STATUS_UNSPECIFIED (0) is a real, compatible response: Roxy
    # falls back to it for a suspended snapshot with an unrecognized status
    # string, and a live SessionInfo with the status field left unset also
    # decodes to 0. Neither should raise.
    guest = FakeSessionGuest()
    guest.list_response = guest_pb2.ListSessionsResponse(
        sessions=[guest_pb2.SessionInfo(name="unknown-status")],
        sandbox_suspended=True,
    )
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    result = sandbox.sessions.list()

    assert result.sessions[0].status is SessionStatus.UNSPECIFIED


# --- Distinct errors ------------------------------------------------------


def test_create_over_existing_raises_session_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.create_errors.put(RpcFailure(grpc.StatusCode.ALREADY_EXISTS, "session \"server\" already exists"))
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(SessionExists):
        sandbox.sessions.create("server", ["bash"])


def test_kill_unknown_name_raises_session_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.kill_errors.put(RpcFailure(grpc.StatusCode.NOT_FOUND, "session \"ghost\" not found"))
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(SessionNotFoundError):
        sandbox.sessions.kill("ghost")


def test_attach_unknown_name_raises_session_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.attach_errors.put(RpcFailure(grpc.StatusCode.NOT_FOUND, "session \"ghost\" not found"))
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(SessionNotFoundError):
        sandbox.sessions.attach("ghost")


def test_session_permission_denied_is_capability_rejected_and_not_refreshed(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.create_errors.put(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "session capability rejected"))
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(CapabilityRejectedError):
        sandbox.sessions.create("server", ["bash"])

    assert len(transport.tapi.reissue_requests) == 0


# --- Replay metadata --------------------------------------------------


def test_attach_surfaces_replay_metadata_on_the_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.attach_responses = [
        _accepted_response(replayed_bytes=4096, history_dropped=True, cols=100, rows=30),
        guest_pb2.AttachSessionResponse(exit=guest_pb2.ExecExit(exit_code=0)),
    ]
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.sessions.attach("server")

    assert isinstance(session, SessionStream)
    assert session.replayed_bytes == 4096
    assert session.history_dropped is True
    assert session.cols == 100
    assert session.rows == 30
    assert session.info.name == "server"


# --- Streaming mechanics -----------------------------------------------


def test_attach_iterates_output_then_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with sandbox.sessions.attach("server") as session:
        events = list(session)

    assert events == [Stdout(b"hello"), Exit(exit_code=0)]


def test_attach_output_dropped_does_not_end_the_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.attach_responses = [
        _accepted_response(),
        guest_pb2.AttachSessionResponse(output_dropped=guest_pb2.OutputDropped(dropped_bytes=128)),
        guest_pb2.AttachSessionResponse(output=guest_pb2.StdoutData(data=b"still here")),
        guest_pb2.AttachSessionResponse(exit=guest_pb2.ExecExit(exit_code=0)),
    ]
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with sandbox.sessions.attach("server") as session:
        events = list(session)

    assert events == [
        SessionOutputDropped(128),
        Stdout(b"still here"),
        Exit(exit_code=0),
    ]


def test_attach_ended_reports_reason_and_terminates(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.attach_responses = [
        _accepted_response(),
        guest_pb2.AttachSessionResponse(ended=guest_pb2.AttachEnded(reason=guest_pb2.AttachEnded.REASON_TAKEOVER)),
    ]
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with sandbox.sessions.attach("server") as session:
        events = list(session)

    assert events == [SessionEnded(SessionEndedReason.TAKEOVER)]


def test_write_resize_and_detach_forward_expected_frames(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.attach_responses = [_accepted_response()]
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.sessions.attach("server")
    session.write(b"npm run dev\n")
    session.resize(cols=140, rows=45)
    session.detach()

    frames = guest.attach_stream.collect_requests()
    assert frames == ["start", "stdin", "resize", "detach"]
    assert guest.attach_stream.seen[1].stdin.data == b"npm run dev\n"
    assert guest.attach_stream.seen[2].resize.cols == 140
    assert guest.attach_stream.seen[2].resize.rows == 45


# --- Capability refresh policy (S14.7 contract) -------------------------


@pytest.mark.parametrize("method", ["create", "list", "kill"])
def test_unary_methods_refresh_once_on_unauthenticated(monkeypatch: pytest.MonkeyPatch, method: str) -> None:
    guest = FakeSessionGuest()
    error = RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "session capability expired")
    if method == "create":
        guest.create_errors.put(error)
    elif method == "list":
        guest.list_errors.put(error)
    else:
        guest.kill_errors.put(error)
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    if method == "create":
        result = sandbox.sessions.create("server", ["bash"])
        assert result.name == "server"
    elif method == "list":
        result = sandbox.sessions.list()
        assert list(result) == []
    else:
        result = sandbox.sessions.kill("server")
        assert result.name == "server"

    assert len(transport.tapi.reissue_requests) == 1
    assert sandbox._capability == transport.tapi.reissue_capability_value


def test_unary_methods_never_refresh_on_permission_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.list_errors.put(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "session capability rejected"))
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(CapabilityRejectedError):
        sandbox.sessions.list()

    assert len(transport.tapi.reissue_requests) == 0


def test_unary_refresh_retries_only_once_then_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.list_errors.put(RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "session capability expired"))
    guest.list_errors.put(RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "session capability expired (again)"))
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(AuthenticationError):
        sandbox.sessions.list()

    assert len(transport.tapi.reissue_requests) == 1
    assert len(guest.list_requests) == 2


def test_attach_refreshes_on_unauthenticated_at_admission_and_uses_fresh_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guest = FakeSessionGuest()
    guest.attach_errors.put(RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "session capability expired"))
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.sessions.attach("server")

    assert session.info.name == "server"
    assert len(transport.tapi.reissue_requests) == 1
    assert sandbox._capability == transport.tapi.reissue_capability_value
    assert guest.metadata[-1][1] == ("bonya-exec-capability", transport.tapi.reissue_capability_value)


def test_attach_never_refreshes_mid_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.next_attach_stream = SessionStreamThenFail(
        _accepted_response(),
        RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "session capability expired"),
    )
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.sessions.attach("server")
    with pytest.raises(AuthenticationError):
        next(session)

    assert len(transport.tapi.reissue_requests) == 0


def test_attach_refresh_retries_only_once_then_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.attach_errors.put(RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "session capability expired"))
    guest.attach_errors.put(RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "session capability expired (again)"))
    client, transport = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(AuthenticationError):
        sandbox.sessions.attach("server")

    assert len(transport.tapi.reissue_requests) == 1


# --- Validation (TTY dimensions identical to Exec's, name rule) --------


def test_create_validates_name_command_and_dimensions(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(InvalidRequestError):
        sandbox.sessions.create("Server", ["bash"])
    with pytest.raises(InvalidRequestError):
        sandbox.sessions.create("", ["bash"])
    with pytest.raises(InvalidRequestError):
        sandbox.sessions.create("server", [])
    with pytest.raises(InvalidRequestError):
        sandbox.sessions.create("server", ["bash"], cols=513)
    with pytest.raises(InvalidRequestError):
        sandbox.sessions.create("server", ["bash"], cols=-1)


def test_resize_rejects_zero_like_exec(monkeypatch: pytest.MonkeyPatch) -> None:
    guest = FakeSessionGuest()
    guest.attach_responses = [_accepted_response()]
    client, _ = make_sessions_client(monkeypatch, guest)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    session = sandbox.sessions.attach("server")

    with pytest.raises(InvalidRequestError):
        session.resize(cols=0, rows=24)
    with pytest.raises(InvalidRequestError):
        session.resize(cols=80, rows=513)


# --- S2: transient exec/exec_stream untouched --------------------------


def test_exec_and_exec_stream_are_unaffected_by_sessions_module(monkeypatch: pytest.MonkeyPatch) -> None:
    from test_contract import FakeGuest, make_client

    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    result = sandbox.exec(["printf", "ready"])
    assert result.stdout == "ready"

    with sandbox.exec_stream(["printf", "ready"]) as session:
        events = list(session)
    assert events[-1].exit_code == 0
