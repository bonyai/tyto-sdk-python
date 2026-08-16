from __future__ import annotations

import queue
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Callable, cast

import grpc

from ._errors import InvalidRequestError, TimeoutError
from ._grpc_errors import map_rpc_error
from ._transport import Deadline
from ._types import ExecEvent, Exit, Stderr, Stdout
from ._proto.tyto.runtime.v1 import guest_pb2

_HALF_CLOSE = object()
_END_REQUESTS = object()
_guest_pb2: Any = guest_pb2


class ExecSession:
    def __init__(
        self,
        *,
        sandbox_id: str,
        operation_id: str,
        command: Sequence[str],
        env: Mapping[str, str] | None = None,
        cwd: str = "",
        tty: bool = False,
        cols: int = 0,
        rows: int = 0,
        stub: Any,
        capability: str,
        timeout: float,
        secrets: list[str],
        request_queue_size: int = 16,
        response_queue_size: int = 16,
        on_error: Callable[[BaseException], BaseException] | None = None,
    ) -> None:
        tty, cols, rows = _validate_start_tty_options(tty, cols, rows)
        self._sandbox_id = sandbox_id
        self._operation_id = operation_id
        self._tty = tty
        self._capability = capability
        self._secrets = secrets
        self._on_error = on_error
        self._deadline = Deadline.start(timeout)
        self._cleanup_timeout = min(5.0, max(0.5, timeout))
        self._request_queue_size = request_queue_size
        self._request_cv = threading.Condition()
        self._requests: list[object] = []
        self._lock = threading.Lock()
        self._closed = False
        self._stdin_closed = False
        self._cancelled = False
        self._request_ended = False
        self._responses: queue.Queue[ExecEvent | BaseException | object] = queue.Queue(maxsize=response_queue_size)
        start = _guest_pb2.ExecStart(
            command=list(command),
            env=dict(env) if env is not None else {},
            working_dir=cwd,
            tty=tty,
            cols=cols,
            rows=rows,
        )
        metadata = (
            ("bonya-sandbox-id", sandbox_id),
            ("bonya-exec-capability", capability),
        )
        self._requests.append(_guest_pb2.ExecRequest(start=start))
        self._stream = stub.Exec(
            self._request_iter(),
            timeout=self._deadline.remaining() + self._cleanup_timeout + 1.0,
            metadata=metadata,
        )
        self._reader = threading.Thread(target=self._read_responses, name=f"bonya-exec-{sandbox_id}", daemon=True)
        self._reader_started = False

    def __enter__(self) -> "ExecSession":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def __iter__(self) -> Iterator[ExecEvent]:
        return self

    def __next__(self) -> ExecEvent:
        self._ensure_reader()
        try:
            item = self._responses.get(timeout=self._deadline.remaining())
        except queue.Empty as exc:
            self.cancel()
            raise TimeoutError("Exec timed out", sandbox_id=self._sandbox_id, operation_id=self._operation_id) from exc
        except TimeoutError:
            self.cancel()
            raise
        if item is _END_REQUESTS:
            raise StopIteration
        if isinstance(item, BaseException):
            raise item
        return cast(ExecEvent, item)

    def _read_responses(self) -> None:
        try:
            for response in self._stream:
                event = self._response_event(response)
                if not self._put_response(event):
                    return
                if isinstance(event, Exit):
                    self._put_response(_END_REQUESTS)
                    self._mark_terminal()
                    return
            self._put_response(_END_REQUESTS)
        except BaseException as exc:
            if self._closed or self._cancelled:
                self._put_response(_END_REQUESTS)
                return
            if self._on_error is not None:
                mapped = self._on_error(exc)
            else:
                mapped = map_rpc_error(
                    exc,
                    secrets=self._secrets,
                    sandbox_id=self._sandbox_id,
                    operation_id=self._operation_id,
                    exec_rpc=True,
                )
            self._put_response(mapped)

    def _response_event(self, response: Any) -> ExecEvent:
        frame = response.WhichOneof("frame")
        if frame == "stdout":
            return Stdout(bytes(response.stdout.data))
        if frame == "stderr":
            return Stderr(bytes(response.stderr.data))
        if frame == "exit":
            return Exit(
                exit_code=int(response.exit.exit_code),
                signaled=bool(response.exit.signaled),
                signal=int(response.exit.signal),
            )
        raise InvalidRequestError("Exec response contained no frame", sandbox_id=self._sandbox_id)

    def write(self, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise InvalidRequestError("write() requires bytes")
        with self._lock:
            if self._closed or self._cancelled:
                raise InvalidRequestError("Exec session is closed", sandbox_id=self._sandbox_id)
            if self._stdin_closed:
                raise InvalidRequestError("stdin is closed", sandbox_id=self._sandbox_id)
        self._put(_guest_pb2.ExecRequest(stdin=_guest_pb2.StdinData(data=bytes(data))))

    def close_stdin(self) -> None:
        with self._lock:
            if self._stdin_closed:
                return
            self._stdin_closed = True
        self._put(_HALF_CLOSE)

    def resize(self, *, cols: int, rows: int) -> None:
        cols = _validate_resize_dimension("cols", cols)
        rows = _validate_resize_dimension("rows", rows)
        with self._lock:
            if not self._tty:
                raise InvalidRequestError("resize requires a tty Exec session", sandbox_id=self._sandbox_id)
            if self._closed or self._cancelled:
                raise InvalidRequestError("Exec session is closed", sandbox_id=self._sandbox_id)
            if self._stdin_closed:
                raise InvalidRequestError("stdin is closed", sandbox_id=self._sandbox_id)
        self._put(_guest_pb2.ExecRequest(resize=_guest_pb2.ExecResize(cols=cols, rows=rows)))

    def cancel(self) -> None:
        with self._lock:
            if self._closed or self._cancelled:
                return
            self._cancelled = True
            self._stdin_closed = True
        if not self._request_ended:
            self._put_cleanup(_guest_pb2.ExecRequest(cancel=_guest_pb2.ExecCancel()), cancel_pending_half_close=True)
            self._put_cleanup(_END_REQUESTS)
        else:
            self._cancel_rpc()
        self._wait_for_cleanup()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
        self.cancel()

    def _mark_terminal(self) -> None:
        with self._lock:
            self._closed = True
            self._stdin_closed = True
        self._put_cleanup(_END_REQUESTS)

    def _request_iter(self) -> Iterator[Any]:
        try:
            while True:
                item = self._take_request()
                if item is _HALF_CLOSE or item is _END_REQUESTS:
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
                    raise TimeoutError("Exec request queue did not drain before deadline", sandbox_id=self._sandbox_id)
                self._request_cv.wait(timeout=remaining)
            self._requests.append(item)
            self._request_cv.notify_all()

    def _put_cleanup(self, item: object, *, cancel_pending_half_close: bool = False) -> None:
        stop_at = time.monotonic() + self._cleanup_timeout
        with self._request_cv:
            if cancel_pending_half_close:
                self._requests = [request for request in self._requests if request is not _HALF_CLOSE]
            while len(self._requests) >= self._request_queue_size:
                remaining = stop_at - time.monotonic()
                if remaining <= 0:
                    return
                self._request_cv.wait(timeout=remaining)
            self._requests.append(item)
            self._request_cv.notify_all()

    def _put_response(self, item: ExecEvent | BaseException | object) -> bool:
        while True:
            with self._lock:
                if self._closed or self._cancelled:
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


def _validate_resize_dimension(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidRequestError(f"{name} must be a positive integer <= 512")
    if not 1 <= value <= 512:
        raise InvalidRequestError(f"{name} must be a positive integer <= 512")
    return value


def _validate_start_tty_options(tty: object, cols: object, rows: object) -> tuple[bool, int, int]:
    if not isinstance(tty, bool):
        raise InvalidRequestError("tty must be a boolean")
    if isinstance(cols, bool) or not isinstance(cols, int):
        raise InvalidRequestError("cols must be a positive integer <= 512")
    if isinstance(rows, bool) or not isinstance(rows, int):
        raise InvalidRequestError("rows must be a positive integer <= 512")
    if not tty:
        if cols != 0 or rows != 0:
            raise InvalidRequestError("tty dimensions require tty=True")
        return False, 0, 0
    if cols == 0 and rows == 0:
        return True, 0, 0
    return True, _validate_resize_dimension("cols", cols), _validate_resize_dimension("rows", rows)
