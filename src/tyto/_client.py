from __future__ import annotations

import os
import uuid
import builtins
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Protocol, cast

from . import _proto  # noqa: F401
from ._errors import InvalidRequestError, TimeoutError
from ._errors import SandboxCreationTimeoutError, SandboxNotFoundError
from ._grpc_errors import is_retryable_transport_error, map_rpc_error
from ._jobs import (
    JobRun,
    JobRunDetail,
    JobSchedule,
    JobSpec,
    ScheduleSpec,
    job_run_detail_from_proto,
    job_run_from_proto,
    job_schedule_from_proto,
    job_spec_to_proto,
    schedule_spec_to_proto,
)
from ._previews import Preview, PreviewAuth
from ._sandbox import DeleteResult, ResumeResult, Sandbox, Snapshot
from ._sessions import SessionInfo, SessionList, SessionStream
from ._transport import (
    DEFAULT_ENDPOINT,
    ChannelFactory,
    ChannelPool,
    Deadline,
    channel_credentials,
    normalize_endpoint,
    sleep_with_deadline,
)
from ._types import Status, Wait
from ._proto.tyto.runtime.v1 import common_pb2, tapi_pb2, tapi_pb2_grpc


WaitInput = Wait | Literal["ready", "none"]
_common_pb2: Any = common_pb2
_tapi_pb2: Any = tapi_pb2

#: gRPC carrier for org context. The REST surface names the same value
#: ``X-Bonya-Organization-ID``; omitting either resolves to the caller's
#: personal organization.
ORGANIZATION_METADATA_KEY = "bonya-organization-id"


class OrganizationContextNotEnforcedWarning(UserWarning):
    """Retained for source compatibility; organization context is enforced."""


class _OrgContextStub:
    """Attaches org context to every TApi RPC.

    Wrapping the stub rather than editing each call site is deliberate: the
    SDK makes eleven TApi calls across four modules, and a per-call-site
    approach would silently omit the header from whichever RPC a future
    change forgets. Here a new TApi method carries org context by
    construction.

    When no organization is configured the client hands out the bare stub
    instead of this wrapper, so an unconfigured call is byte-identical to
    what the SDK sent before org context existed -- no empty metadata, no
    extra keyword argument.
    """

    def __init__(self, stub: Any, metadata: tuple[tuple[str, str], ...]) -> None:
        self._stub = stub
        self._metadata = metadata

    def __getattr__(self, name: str) -> Any:
        method = getattr(self._stub, name)

        def call(request: Any, **kwargs: Any) -> Any:
            supplied = kwargs.pop("metadata", None) or ()
            return method(request, metadata=tuple(supplied) + self._metadata, **kwargs)

        return call


class TApiStub(Protocol):
    def Create(self, request: object, *, timeout: float) -> object: ...

    def GetSandbox(self, request: object, *, timeout: float) -> object: ...

    def ListSandboxes(self, request: object, *, timeout: float) -> object: ...

    def CreateSnapshot(self, request: object, *, timeout: float) -> object: ...

    def DeleteSnapshot(self, request: object, *, timeout: float) -> object: ...

    def DeleteSandbox(self, request: object, *, timeout: float) -> object: ...

    def ReissueCapability(self, request: object, *, timeout: float) -> object: ...

    def ResumeSandbox(self, request: object, *, timeout: float) -> object: ...

    def ListOrganizations(self, request: object, *, timeout: float) -> object: ...

    def ListTemplates(self, request: object, *, timeout: float) -> object: ...

    def RunJob(self, request: object, *, timeout: float) -> object: ...

    def StartJob(self, request: object, *, timeout: float) -> object: ...

    def GetJobRun(self, request: object, *, timeout: float) -> object: ...

    def ListJobRuns(self, request: object, *, timeout: float) -> object: ...

    def CancelJobRun(self, request: object, *, timeout: float) -> object: ...

    def CreateJobSchedule(self, request: object, *, timeout: float) -> object: ...

    def GetJobSchedule(self, request: object, *, timeout: float) -> object: ...

    def ListJobSchedules(self, request: object, *, timeout: float) -> object: ...

    def UpdateJobSchedule(self, request: object, *, timeout: float) -> object: ...

    def SetJobSchedulePaused(self, request: object, *, timeout: float) -> object: ...

    def TriggerJobSchedule(self, request: object, *, timeout: float) -> object: ...

    def DeleteJobSchedule(self, request: object, *, timeout: float) -> object: ...


class Tyto:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        endpoint: str | None = None,
        ca_bundle: str | None = None,
        organization_id: str | None = None,
        timeout: float = 30,
        max_retries: int = 2,
        filesystem_read_limit: int = 64 * 1024 * 1024,
        _channel_factory: ChannelFactory | None = None,
        _tapi_stub_factory: Callable[[object], object] | None = None,
        _guest_stub_factory: Callable[[object], object] | None = None,
    ) -> None:
        self._api_key = api_key if api_key is not None else os.environ.get("BONYA_API_KEY")
        if not self._api_key:
            raise InvalidRequestError("api_key is required")
        endpoint_value = endpoint if endpoint is not None else os.environ.get("BONYA_ENDPOINT", DEFAULT_ENDPOINT)
        ca_bundle_value = ca_bundle if ca_bundle is not None else os.environ.get("BONYA_CA_BUNDLE")
        self._endpoint = normalize_endpoint(endpoint_value)
        self._organization_id = _resolve_organization_id(organization_id)
        if timeout <= 0:
            raise InvalidRequestError("timeout must be positive")
        if max_retries < 0:
            raise InvalidRequestError("max_retries must be non-negative")
        if isinstance(filesystem_read_limit, bool) or not isinstance(filesystem_read_limit, int):
            raise InvalidRequestError("filesystem_read_limit must be a non-negative integer")
        if filesystem_read_limit < 0:
            raise InvalidRequestError("filesystem_read_limit must be a non-negative integer")
        self._timeout = float(timeout)
        self._max_retries = max_retries
        self._filesystem_read_limit = filesystem_read_limit
        self._pool = ChannelPool(channel_credentials(ca_bundle_value), _channel_factory)
        self._tapi_stub_factory = _tapi_stub_factory or tapi_pb2_grpc.TApiServiceStub
        self._guest_stub_factory = _guest_stub_factory

    def close(self) -> None:
        self._pool.close()

    def __enter__(self) -> "Tyto":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    @property
    def organization_id(self) -> str | None:
        """The org context this client sends, or None for the personal org."""
        return self._organization_id

    @organization_id.setter
    def organization_id(self, value: str) -> None:
        """Change which organization this client's calls act on.

        Takes effect immediately: _tapi_stub() reads self._organization_id
        fresh on every call rather than baking it into a stub built once, so
        there is no cached-channel staleness to worry about here the way the
        Go SDK's dial-time interceptor had to guard against.

        An empty value is an error rather than a silent fallback to the
        personal organization, matching the constructor's own
        BONYA_ORGANIZATION_ID handling.
        """
        self._organization_id = _resolve_organization_id(value)

    def list_organizations(self) -> builtins.list[Organization]:
        """List the organizations this client's api_key's user belongs to."""
        request = _tapi_pb2.TApiListOrganizationsRequest(api_key=self._api_key)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().ListOrganizations(request, timeout=deadline.remaining())
                return [_organization_from_proto(org) for org in getattr(response, "organizations", [])]
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets()) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def _tapi_stub(self) -> TApiStub:
        stub = self._tapi_stub_factory(self._pool.get(self._endpoint))
        if self._organization_id is None:
            return stub  # type: ignore[return-value]
        return _OrgContextStub(stub, ((ORGANIZATION_METADATA_KEY, self._organization_id),))

    def _exec_stub(self, endpoint: str) -> object:
        normalized = normalize_endpoint(endpoint)
        factory = self._guest_stub_factory
        if factory is None:
            from ._proto.tyto.runtime.v1 import guest_pb2_grpc

            factory = guest_pb2_grpc.GuestServiceStub
        return factory(self._pool.get(normalized))

    def _secrets(self, *extra: str | None) -> list[str]:
        return [value for value in (self._api_key, *extra) if value]

    # Flat, client-level sandbox CRUD -- create_sandbox, get_sandbox,
    # get_sandbox_by_name, list_sandboxes, delete_sandbox, resume_sandbox.
    # There is no sandbox-collection namespace to also offer these under;
    # this is the one implementation.
    #
    # File operations (read_file, write_file, upload_file, ...) and managed
    # sessions/previews are flat methods directly on Sandbox instead of
    # sandbox.files/sandbox.sessions/sandbox.previews namespaces -- see
    # _sandbox.py.

    def create_sandbox(
        self,
        *,
        template: str | None = None,
        version: str | None = None,
        wait: WaitInput = Wait.READY,
        idempotency_key: str | None = None,
        name: str | None = None,
    ) -> Sandbox:
        """template may be omitted to use the deployment's configured default
        template, if it has one; the server rejects the request if it does
        not."""
        wait_value = _normalize_wait(wait)
        key = idempotency_key or uuid.uuid4().hex + uuid.uuid4().hex
        request = _tapi_pb2.TApiServiceCreateRequest(
            api_key=self._api_key,
            idempotency_key=key,
            template=_common_pb2.TemplateBinding(template_id=template or "", version=version or ""),
            wait=(
                _tapi_pb2.CREATE_WAIT_READY
                if wait_value is Wait.READY
                else _tapi_pb2.CREATE_WAIT_NONE
            ),
            name=name or "",
        )
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        last_error: BaseException | None = None
        while True:
            try:
                response = self._tapi_stub().Create(request, timeout=deadline.remaining())
                return _sandbox_from_create(self, response, wait_value, key)
            except BaseException as exc:
                last_error = exc
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    if isinstance(exc, TimeoutError):
                        raise SandboxCreationTimeoutError(
                            exc.message,
                            idempotency_key=key,
                        ) from exc
                    raise map_rpc_error(
                        exc,
                        secrets=self._secrets(key),
                        idempotency_key=key,
                        create=True,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)
            if last_error is None:
                raise TimeoutError("operation deadline exhausted")

    def get_sandbox(self, sandbox_id: str) -> Sandbox:
        """Reconnect to an existing sandbox by id."""
        if not sandbox_id:
            raise InvalidRequestError("sandbox_id is required")
        request = _tapi_pb2.TApiGetSandboxRequest(api_key=self._api_key, sandbox_id=sandbox_id)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().GetSandbox(request, timeout=deadline.remaining())
                return _sandbox_from_get(self, response, requested_sandbox_id=sandbox_id)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(
                        exc,
                        secrets=self._secrets(),
                        sandbox_id=sandbox_id,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def get_sandbox_by_name(self, name: str) -> Sandbox:
        """Reconnect to an existing sandbox by name.

        Names are not unique. This resolves the name to a single sandbox and
        then fetches it by id, and raises rather than guessing when the name
        matches more than one: picking one silently would let a later delete
        destroy an arbitrary sandbox.
        """
        if not name:
            raise InvalidRequestError("name is required")
        # Two is enough to tell "one match" from "more than one" without
        # paging the whole organization.
        matches = builtins.list(self.list_sandboxes(name=name, limit=2))
        if not matches:
            raise SandboxNotFoundError(f"no sandbox is named {name}")
        if len(matches) > 1:
            raise InvalidRequestError(
                f"more than one sandbox is named {name}, including "
                f"{matches[0].id} and {matches[1].id}; use get_sandbox() with a sandbox id"
            )
        return self.get_sandbox(matches[0].id)

    def list_sandboxes(
        self,
        *,
        states: Iterable[Status] | None = None,
        limit: int | None = None,
        name: str | None = None,
    ) -> Iterator[SandboxSummary]:
        state_values = _normalize_state_filters(states)
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise InvalidRequestError("limit must be a non-negative integer")
            if limit < 0:
                raise InvalidRequestError("limit must be a non-negative integer")
            if limit == 0:
                return iter(())
        return self._list_sandbox_pages(state_values=state_values, limit=limit, name=name or "")

    def delete_sandbox(self, sandbox_id: str) -> DeleteResult:
        """Delete a sandbox by id in a single RPC, without first fetching it.

        Sandbox.delete() also calls through to this same RPC and additionally
        short-circuits locally when called twice on the same handle -- there
        is no handle here to remember that, so a second call here always
        makes a second RPC, and its already_deleted reports what the server
        observed rather than what this SDK remembers.
        """
        if not sandbox_id:
            raise InvalidRequestError("sandbox_id is required")
        request = _tapi_pb2.TApiDeleteSandboxRequest(api_key=self._api_key, sandbox_id=sandbox_id)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().DeleteSandbox(request, timeout=deadline.remaining())
                return DeleteResult(
                    sandbox_id=getattr(response, "sandbox_id", sandbox_id) or sandbox_id,
                    already_deleted=bool(getattr(response, "already_deleted", False)),
                )
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), sandbox_id=sandbox_id) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def resume_sandbox(self, sandbox_id: str, *, idempotency_key: str | None = None) -> ResumeResult:
        """Resume a sandbox by id in a single RPC, without first fetching it.

        Sandbox.resume() also calls through to the same RPC via
        _resume_sandbox(), additionally copying the refreshed capability and
        exec endpoint onto its own handle, since only a handle has those to
        update -- ResumeResult itself never carries them.
        """
        result, _response = self._resume_sandbox(sandbox_id, idempotency_key=idempotency_key)
        return result

    def _resume_sandbox(self, sandbox_id: str, *, idempotency_key: str | None = None) -> tuple[ResumeResult, Any]:
        """The one ResumeSandbox call site. Returns the raw response alongside
        the mapped ResumeResult so Sandbox.resume() can read the capability
        and exec endpoint fields ResumeResult does not expose, without a
        second implementation of the retry loop.

        Unlike Sandbox.resume(), this does not check for a locally known
        failed status first: there is no handle to check, so the server is
        always asked, and a failed sandbox's rejection comes back as an
        ordinary RPC error.
        """
        if not sandbox_id:
            raise InvalidRequestError("sandbox_id is required")
        key = idempotency_key or uuid.uuid4().hex + uuid.uuid4().hex
        request = _tapi_pb2.TApiResumeSandboxRequest(
            api_key=self._api_key,
            sandbox_id=sandbox_id,
            idempotency_key=key,
        )
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().ResumeSandbox(request, timeout=deadline.remaining())
                result = ResumeResult(
                    sandbox_id=getattr(response, "sandbox_id", sandbox_id) or sandbox_id,
                    lifecycle_operation_id=getattr(response, "lifecycle_operation_id", ""),
                    already_running=bool(getattr(response, "already_running", False)),
                )
                return result, response
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(
                        exc,
                        secrets=self._secrets(),
                        sandbox_id=sandbox_id,
                        idempotency_key=key,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def _list_sandbox_pages(
        self, *, state_values: builtins.list[int], limit: int | None, name: str = ""
    ) -> Iterator[SandboxSummary]:
        yielded = 0
        page_token = ""
        while True:
            page_size = 0 if limit is None else min(100, limit - yielded)
            request = _tapi_pb2.TApiListSandboxesRequest(
                api_key=self._api_key,
                states=state_values,
                page_size=page_size,
                page_token=page_token,
                name=name,
            )
            deadline = Deadline.start(self._timeout)
            attempts = 0
            backoff = 0.05
            while True:
                try:
                    response = self._tapi_stub().ListSandboxes(request, timeout=deadline.remaining())
                    break
                except BaseException as exc:
                    if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                        raise map_rpc_error(
                            exc,
                            secrets=self._secrets(page_token),
                        ) from exc
                    attempts += 1
                    sleep_with_deadline(backoff, deadline)
                    backoff = min(backoff * 2, 0.5)
            for sandbox in getattr(response, "sandboxes", []):
                if limit is not None and yielded >= limit:
                    return
                yield _summary_from_metadata(sandbox)
                yielded += 1
            page_token = getattr(response, "next_page_token", "")
            if not page_token or (limit is not None and yielded >= limit):
                return

    # Flat, client-level forms of sandbox.create_session/list_sessions/...,
    # sandbox.create_preview/list_previews/..., and sandbox.snapshot().
    # Unlike the sandbox CRUD methods above, each of these needs a resolved
    # Sandbox to call through -- sessions and previews are scoped to one
    # sandbox's RPC surface, and snapshot creation checks the sandbox's last
    # observed status -- so every method here does a get_sandbox() first and
    # then delegates, which costs one extra round trip compared to already
    # holding the handle. Call sandbox.create_session (or the equivalent)
    # directly instead when a Sandbox is already in hand, such as right
    # after create_sandbox().

    def create_session(
        self,
        sandbox_id: str,
        name: str,
        command: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        cols: int = 0,
        rows: int = 0,
        replace: bool = False,
    ) -> SessionInfo:
        """get_sandbox() followed by sandbox.create_session()."""
        sandbox = self.get_sandbox(sandbox_id)
        return sandbox.create_session(name, command, env=env, cwd=cwd, cols=cols, rows=rows, replace=replace)

    def list_sessions(self, sandbox_id: str) -> SessionList:
        """get_sandbox() followed by sandbox.list_sessions()."""
        sandbox = self.get_sandbox(sandbox_id)
        return sandbox.list_sessions()

    def kill_session(self, sandbox_id: str, name: str, *, signal: str = "TERM", grace_ms: int = 5000) -> SessionInfo:
        """get_sandbox() followed by sandbox.kill_session()."""
        sandbox = self.get_sandbox(sandbox_id)
        return sandbox.kill_session(name, signal=signal, grace_ms=grace_ms)

    def attach_session(
        self, sandbox_id: str, name: str, *, cols: int = 0, rows: int = 0, max_replay_bytes: int = 0
    ) -> SessionStream:
        """get_sandbox() followed by sandbox.attach_session()."""
        sandbox = self.get_sandbox(sandbox_id)
        return sandbox.attach_session(name, cols=cols, rows=rows, max_replay_bytes=max_replay_bytes)

    def create_preview(
        self,
        sandbox_id: str,
        port: int,
        *,
        auth: PreviewAuth = PreviewAuth.TOKEN,
        name: str | None = None,
        idempotency_key: str | None = None,
    ) -> Preview:
        """get_sandbox() followed by sandbox.create_preview()."""
        sandbox = self.get_sandbox(sandbox_id)
        return sandbox.create_preview(port, auth=auth, name=name, idempotency_key=idempotency_key)

    def list_previews(self, sandbox_id: str) -> builtins.list[Preview]:
        """get_sandbox() followed by sandbox.list_previews()."""
        sandbox = self.get_sandbox(sandbox_id)
        return sandbox.list_previews()

    def delete_preview(self, sandbox_id: str, preview_id: str) -> None:
        """get_sandbox() followed by sandbox.delete_preview()."""
        sandbox = self.get_sandbox(sandbox_id)
        sandbox.delete_preview(preview_id)

    def create_snapshot(self, sandbox_id: str, *, idempotency_key: str | None = None) -> Snapshot:
        """get_sandbox() followed by sandbox.snapshot()."""
        sandbox = self.get_sandbox(sandbox_id)
        return sandbox.snapshot(idempotency_key=idempotency_key)

    def delete_snapshot(self, sandbox_id: str, snapshot_id: str) -> None:
        """get_sandbox() followed by sandbox.snapshot()'s Snapshot.delete():
        there is no sandbox-level delete_snapshot() to call through to, since
        a snapshot's own delete() is what every language's SDK treats as
        canonical, so this constructs the same Snapshot handle and deletes
        it."""
        sandbox = self.get_sandbox(sandbox_id)
        snapshot = Snapshot(client=self, snapshot_id=snapshot_id, source_sandbox_id=sandbox.id)
        snapshot.delete()

    # Jobs: a managed run of a command or script, on a new or existing
    # sandbox. Client-level only, like list_organizations -- a job is not
    # scoped to a sandbox handle the way sessions/previews/snapshots are, so
    # there is no sandbox.jobs namespace to also offer.

    def run_job(self, spec: JobSpec, *, idempotency_key: str | None = None) -> JobRun:
        """Run a job and block until it finishes, bounded by the client's own
        timeout. Use start_job() instead for a job that may outlive one call."""
        request = _tapi_pb2.TApiRunJobRequest(
            api_key=self._api_key,
            idempotency_key=idempotency_key or _generate_idempotency_key(),
            spec=job_spec_to_proto(spec, tapi_pb2=_tapi_pb2, common_pb2=_common_pb2),
        )
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().RunJob(request, timeout=deadline.remaining())
                run = getattr(response, "run", None)
                if run is None:
                    raise InvalidRequestError("RunJob response is missing run")
                return job_run_from_proto(run)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(
                        exc,
                        secrets=self._secrets(request.idempotency_key),
                        idempotency_key=request.idempotency_key,
                        job_rpc=True,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def start_job(self, spec: JobSpec, *, idempotency_key: str | None = None) -> tuple[str, bool]:
        """Start a job durably and return immediately with its run id,
        without waiting for it to finish. Returns (run_id, already_running).
        Use get_job_run() to poll for the result."""
        request = _tapi_pb2.TApiStartJobRequest(
            api_key=self._api_key,
            idempotency_key=idempotency_key or _generate_idempotency_key(),
            spec=job_spec_to_proto(spec, tapi_pb2=_tapi_pb2, common_pb2=_common_pb2),
        )
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().StartJob(request, timeout=deadline.remaining())
                return getattr(response, "run_id", ""), bool(getattr(response, "already_running", False))
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(
                        exc,
                        secrets=self._secrets(request.idempotency_key),
                        idempotency_key=request.idempotency_key,
                        job_rpc=True,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def get_job_run(self, run_id: str) -> JobRunDetail:
        """Fetch a job run's full detail, including its stored spec and
        activity timeline."""
        if not run_id:
            raise InvalidRequestError("run_id is required")
        request = _tapi_pb2.TApiGetJobRunRequest(api_key=self._api_key, run_id=run_id)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().GetJobRun(request, timeout=deadline.remaining())
                detail = getattr(response, "detail", None)
                if detail is None:
                    raise InvalidRequestError("GetJobRun response is missing detail")
                return job_run_detail_from_proto(detail)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), job_rpc=True) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def list_job_runs(
        self, *, sandbox_id: str = "", schedule_id: str = "", limit: int | None = None
    ) -> Iterator[JobRun]:
        """List job runs, newest first, paging internally as needed.

        A limit of None returns every matching run.
        """
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise InvalidRequestError("limit must be a non-negative integer")
            if limit < 0:
                raise InvalidRequestError("limit must be a non-negative integer")
            if limit == 0:
                return
        yielded = 0
        page_token = ""
        while True:
            page_size = 0 if limit is None else min(100, limit - yielded)
            request = _tapi_pb2.TApiListJobRunsRequest(
                api_key=self._api_key,
                sandbox_id=sandbox_id,
                schedule_id=schedule_id,
                page_size=page_size,
                page_token=page_token,
            )
            deadline = Deadline.start(self._timeout)
            attempts = 0
            backoff = 0.05
            while True:
                try:
                    response = self._tapi_stub().ListJobRuns(request, timeout=deadline.remaining())
                    break
                except BaseException as exc:
                    if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                        raise map_rpc_error(exc, secrets=self._secrets(page_token), job_rpc=True) from exc
                    attempts += 1
                    sleep_with_deadline(backoff, deadline)
                    backoff = min(backoff * 2, 0.5)
            for run in getattr(response, "runs", []):
                if limit is not None and yielded >= limit:
                    return
                yield job_run_from_proto(run)
                yielded += 1
            page_token = getattr(response, "next_page_token", "")
            if not page_token or (limit is not None and yielded >= limit):
                return

    def cancel_job_run(self, run_id: str) -> None:
        """Request cancellation of a running job. This cancels rather than
        terminates, so the run's own cleanup (e.g. deleting a sandbox it
        created) still executes."""
        if not run_id:
            raise InvalidRequestError("run_id is required")
        request = _tapi_pb2.TApiCancelJobRunRequest(api_key=self._api_key, run_id=run_id)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                self._tapi_stub().CancelJobRun(request, timeout=deadline.remaining())
                return
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), job_rpc=True) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def create_job_schedule(
        self, schedule: ScheduleSpec, job: JobSpec, *, idempotency_key: str | None = None
    ) -> JobSchedule:
        """Create a durable cron, interval, or one-shot trigger for a job.

        Exactly one of schedule.cron_expressions, interval_seconds, and
        run_at_unix_nanos is required.
        """
        request = _tapi_pb2.TApiCreateJobScheduleRequest(
            api_key=self._api_key,
            idempotency_key=idempotency_key or _generate_idempotency_key(),
            schedule=schedule_spec_to_proto(schedule, tapi_pb2=_tapi_pb2),
            spec=job_spec_to_proto(job, tapi_pb2=_tapi_pb2, common_pb2=_common_pb2),
        )
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().CreateJobSchedule(request, timeout=deadline.remaining())
                schedule_result = getattr(response, "schedule", None)
                if schedule_result is None:
                    raise InvalidRequestError("response is missing schedule")
                return job_schedule_from_proto(schedule_result)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(
                        exc,
                        secrets=self._secrets(request.idempotency_key),
                        idempotency_key=request.idempotency_key,
                        job_schedule_rpc=True,
                    ) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def get_job_schedule(self, schedule_id: str) -> JobSchedule:
        """Fetch a job schedule by id."""
        if not schedule_id:
            raise InvalidRequestError("schedule_id is required")
        request = _tapi_pb2.TApiGetJobScheduleRequest(api_key=self._api_key, schedule_id=schedule_id)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().GetJobSchedule(request, timeout=deadline.remaining())
                schedule_result = getattr(response, "schedule", None)
                if schedule_result is None:
                    raise InvalidRequestError("response is missing schedule")
                return job_schedule_from_proto(schedule_result)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), job_schedule_rpc=True) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def list_job_schedules(self, *, limit: int | None = None) -> Iterator[JobSchedule]:
        """List job schedules, paging internally as needed.

        A limit of None returns every schedule.
        """
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise InvalidRequestError("limit must be a non-negative integer")
            if limit < 0:
                raise InvalidRequestError("limit must be a non-negative integer")
            if limit == 0:
                return
        yielded = 0
        page_token = ""
        while True:
            page_size = 0 if limit is None else min(100, limit - yielded)
            request = _tapi_pb2.TApiListJobSchedulesRequest(
                api_key=self._api_key, page_size=page_size, page_token=page_token
            )
            deadline = Deadline.start(self._timeout)
            attempts = 0
            backoff = 0.05
            while True:
                try:
                    response = self._tapi_stub().ListJobSchedules(request, timeout=deadline.remaining())
                    break
                except BaseException as exc:
                    if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                        raise map_rpc_error(exc, secrets=self._secrets(page_token), job_schedule_rpc=True) from exc
                    attempts += 1
                    sleep_with_deadline(backoff, deadline)
                    backoff = min(backoff * 2, 0.5)
            for schedule in getattr(response, "schedules", []):
                if limit is not None and yielded >= limit:
                    return
                yield job_schedule_from_proto(schedule)
                yielded += 1
            page_token = getattr(response, "next_page_token", "")
            if not page_token or (limit is not None and yielded >= limit):
                return

    def update_job_schedule(self, schedule_id: str, schedule: ScheduleSpec, job: JobSpec) -> JobSchedule:
        """Replace a job schedule's timing and job spec.

        Replaces the whole schedule, so pass every field you want to keep,
        not just the one you're changing.
        """
        if not schedule_id:
            raise InvalidRequestError("schedule_id is required")
        request = _tapi_pb2.TApiUpdateJobScheduleRequest(
            api_key=self._api_key,
            schedule_id=schedule_id,
            schedule=schedule_spec_to_proto(schedule, tapi_pb2=_tapi_pb2),
            spec=job_spec_to_proto(job, tapi_pb2=_tapi_pb2, common_pb2=_common_pb2),
        )
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().UpdateJobSchedule(request, timeout=deadline.remaining())
                schedule_result = getattr(response, "schedule", None)
                if schedule_result is None:
                    raise InvalidRequestError("response is missing schedule")
                return job_schedule_from_proto(schedule_result)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), job_schedule_rpc=True) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def set_job_schedule_paused(self, schedule_id: str, paused: bool, *, note: str = "") -> JobSchedule:
        """Pause or resume a job schedule, optionally recording a note."""
        if not schedule_id:
            raise InvalidRequestError("schedule_id is required")
        request = _tapi_pb2.TApiSetJobSchedulePausedRequest(
            api_key=self._api_key, schedule_id=schedule_id, paused=paused, note=note
        )
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().SetJobSchedulePaused(request, timeout=deadline.remaining())
                schedule_result = getattr(response, "schedule", None)
                if schedule_result is None:
                    raise InvalidRequestError("response is missing schedule")
                return job_schedule_from_proto(schedule_result)
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), job_schedule_rpc=True) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def trigger_job_schedule(self, schedule_id: str) -> None:
        """Fire one run of a job schedule immediately, overriding the
        schedule's own timing."""
        if not schedule_id:
            raise InvalidRequestError("schedule_id is required")
        request = _tapi_pb2.TApiTriggerJobScheduleRequest(api_key=self._api_key, schedule_id=schedule_id)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                self._tapi_stub().TriggerJobSchedule(request, timeout=deadline.remaining())
                return
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), job_schedule_rpc=True) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def delete_job_schedule(self, schedule_id: str) -> None:
        """Permanently delete a job schedule. Does not cancel any run
        currently in flight."""
        if not schedule_id:
            raise InvalidRequestError("schedule_id is required")
        request = _tapi_pb2.TApiDeleteJobScheduleRequest(api_key=self._api_key, schedule_id=schedule_id)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                self._tapi_stub().DeleteJobSchedule(request, timeout=deadline.remaining())
                return
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets(), job_schedule_rpc=True) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)

    def list_templates(self) -> builtins.list[Template]:
        """List the deployment's template catalog: every template_id and
        version create_sandbox/run_job will accept, and which version each
        template_id resolves to when a caller omits version. Same catalog
        for every caller; not paginated."""
        request = _tapi_pb2.TApiListTemplatesRequest(api_key=self._api_key)
        deadline = Deadline.start(self._timeout)
        attempts = 0
        backoff = 0.05
        while True:
            try:
                response = self._tapi_stub().ListTemplates(request, timeout=deadline.remaining())
                return [_template_from_proto(t) for t in getattr(response, "templates", [])]
            except BaseException as exc:
                if not is_retryable_transport_error(exc) or attempts >= self._max_retries:
                    raise map_rpc_error(exc, secrets=self._secrets()) from exc
                attempts += 1
                sleep_with_deadline(backoff, deadline)
                backoff = min(backoff * 2, 0.5)


# Bonya was this class's name in 1.0, from when the SDK's package and client
# were both named Bonya. The alias keeps existing code working -- it is the
# same class object, so `isinstance(x, Bonya)` still matches everything
# `isinstance(x, Tyto)` does -- and will be removed in 2.0.
Bonya = Tyto


@dataclass(frozen=True)
class Organization:
    """One organization the client's api_key's user belongs to."""

    id: str
    name: str
    #: Marks the deterministic tenant an omitted organization context
    #: resolves to. Every account has exactly one.
    personal: bool
    #: The caller's role in this organization: "owner" or "member".
    role: str
    created_at: datetime


@dataclass(frozen=True)
class SandboxSummary:
    id: str
    operation_id: str
    template: str
    version: str
    last_observed_status: Status
    failure_code: str | None
    failure_message: str | None
    # Defaulted so existing positional construction keeps working.
    name: str = ""


@dataclass(frozen=True)
class TemplateStack:
    """One language or runtime toolchain preinstalled in a template."""

    name: str
    version: str


@dataclass(frozen=True)
class TemplateMetadata:
    """The operating system and tools preinstalled in a Template.

    A catalog entry without annotations yields its zero value.
    """

    description: str = ""
    os: str = ""
    os_version: str = ""
    stacks: tuple[TemplateStack, ...] = ()
    agent_cli_support: tuple[str, ...] = ()


@dataclass(frozen=True)
class Template:
    """One template_id/version binding the deployment's catalog offers to
    create_sandbox and run_job.

    One entry per version, not one per template_id: a template_id with
    several published versions appears once per version, and is_default
    marks the one a caller resolving by template_id alone (version omitted)
    gets.
    """

    id: str
    version: str
    digest: str
    is_default: bool
    metadata: TemplateMetadata


def _template_from_proto(template: Any) -> Template:
    metadata = TemplateMetadata(
        description=template.metadata.description,
        os=template.metadata.os,
        os_version=template.metadata.os_version,
        stacks=tuple(TemplateStack(name=s.name, version=s.version) for s in template.metadata.stacks),
        agent_cli_support=tuple(template.metadata.agent_cli_support),
    )
    return Template(
        id=template.template_id,
        version=template.version,
        digest=template.digest,
        is_default=template.is_default,
        metadata=metadata,
    )


def _generate_idempotency_key() -> str:
    return uuid.uuid4().hex + uuid.uuid4().hex


def _resolve_organization_id(organization_id: str | None) -> str | None:
    """Resolve org context from the argument, then the environment.

    An explicitly supplied empty value is an error rather than a silent
    fallback to the personal organization. In CI the variable is usually
    written as an expansion of another variable, so an unset upstream value
    arrives here as the empty string -- and a client that quietly ran every
    job against someone's personal organization would be a much worse
    outcome than a startup failure naming the problem.

    The id's shape is not checked here. The server owns that shape and
    answers a malformed one with 400; a client-side pattern would only mean
    an older SDK refusing ids a newer server had begun issuing.
    """
    value = organization_id if organization_id is not None else os.environ.get("BONYA_ORGANIZATION_ID")
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise InvalidRequestError("organization_id must be a non-empty string")
    return value.strip()


def _normalize_wait(wait: WaitInput) -> Wait:
    if isinstance(wait, Wait):
        return wait
    try:
        return Wait(wait)
    except ValueError as exc:
        raise InvalidRequestError("wait must be 'ready' or 'none'") from exc


def _normalize_state_filters(states: Iterable[Status] | None) -> list[int]:
    if states is None:
        return []
    values: list[int] = []
    for state in states:
        if not isinstance(state, Status):
            raise InvalidRequestError("states must contain Status values")
        if state is Status.DELETED:
            raise InvalidRequestError("Status.DELETED is not a valid list filter")
        values.append(_status_to_terminal_state(state))
    return values


def _sandbox_from_create(client: Tyto, response: object, wait: Wait, key: str) -> Sandbox:
    exec_endpoint = getattr(response, "exec_endpoint", "")
    capability = getattr(response, "exec_capability_jws", "")
    sandbox_id = getattr(response, "sandbox_id", "")
    operation_id = getattr(response, "operation_id", "")
    if not sandbox_id or not operation_id:
        raise InvalidRequestError("Create response is missing sandbox identity", idempotency_key=key)
    if not exec_endpoint:
        raise InvalidRequestError(
            "Create response is missing exec_endpoint",
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=key,
        )
    if not capability:
        raise InvalidRequestError(
            "Create response is missing exec capability",
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=key,
        )
    terminal = getattr(response, "terminal", None)
    if terminal is not None and getattr(terminal, "state", 0) == _tapi_pb2.TERMINAL_STATE_FAILED:
        from ._errors import SandboxCreationFailedError

        raise SandboxCreationFailedError(
            getattr(terminal, "message", "") or "sandbox creation failed",
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=key,
        )
    return Sandbox(
        client=client,
        sandbox_id=sandbox_id,
        operation_id=operation_id,
        template=getattr(response, "resolved_template_id", ""),
        version=getattr(response, "resolved_template_version", ""),
        status=Status.RUNNING if wait is Wait.READY else Status.CREATING,
        exec_endpoint=exec_endpoint,
        capability=capability,
        name=getattr(response, "name", ""),
    )


def _sandbox_from_get(client: Tyto, response: object, *, requested_sandbox_id: str) -> Sandbox:
    metadata = getattr(response, "sandbox", None)
    if metadata is None:
        raise InvalidRequestError("GetSandbox response is missing sandbox metadata", sandbox_id=requested_sandbox_id)
    sandbox_id = getattr(metadata, "sandbox_id", "")
    operation_id = getattr(metadata, "operation_id", "")
    if not sandbox_id or not operation_id:
        raise InvalidRequestError("GetSandbox response is missing sandbox identity", sandbox_id=requested_sandbox_id)
    status = _status_from_metadata(metadata)
    exec_endpoint = getattr(response, "exec_endpoint", "")
    capability = getattr(response, "exec_capability_jws", "")
    if status is Status.FAILED:
        return Sandbox(
            client=client,
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            template=getattr(metadata, "resolved_template_id", ""),
            version=getattr(metadata, "resolved_template_version", ""),
            status=status,
            exec_endpoint="",
            capability="",
            failure_code=_failure_code(metadata),
            failure_message=_failure_message(metadata),
            name=getattr(metadata, "name", ""),
        )
    if not exec_endpoint:
        raise InvalidRequestError(
            "GetSandbox response is missing exec_endpoint",
            sandbox_id=sandbox_id,
            operation_id=operation_id,
        )
    if not capability:
        raise InvalidRequestError(
            "GetSandbox response is missing exec capability",
            sandbox_id=sandbox_id,
            operation_id=operation_id,
        )
    return Sandbox(
        client=client,
        sandbox_id=sandbox_id,
        operation_id=operation_id,
        template=getattr(metadata, "resolved_template_id", ""),
        version=getattr(metadata, "resolved_template_version", ""),
        status=status,
        exec_endpoint=exec_endpoint,
        capability=capability,
        name=getattr(metadata, "name", ""),
    )


def _summary_from_metadata(metadata: object) -> SandboxSummary:
    return SandboxSummary(
        id=getattr(metadata, "sandbox_id", ""),
        operation_id=getattr(metadata, "operation_id", ""),
        template=getattr(metadata, "resolved_template_id", ""),
        version=getattr(metadata, "resolved_template_version", ""),
        last_observed_status=_status_from_metadata(metadata),
        failure_code=_failure_code(metadata),
        failure_message=_failure_message(metadata),
        name=getattr(metadata, "name", ""),
    )


def _organization_from_proto(organization: object) -> Organization:
    return Organization(
        id=getattr(organization, "organization_id", ""),
        name=getattr(organization, "name", ""),
        personal=getattr(organization, "personal", False),
        role=getattr(organization, "role", ""),
        created_at=datetime.fromtimestamp(
            getattr(organization, "created_at_unix_nanos", 0) / 1e9, tz=timezone.utc
        ),
    )


def _status_from_metadata(metadata: object) -> Status:
    observed = getattr(metadata, "observed", None)
    state = getattr(observed, "state", 0)
    return _terminal_state_to_status(state)


def _failure_code(metadata: object) -> str | None:
    observed = getattr(metadata, "observed", None)
    code = getattr(observed, "code", "")
    return code or None


def _failure_message(metadata: object) -> str | None:
    observed = getattr(metadata, "observed", None)
    message = getattr(observed, "message", "")
    return message or None


def _terminal_state_to_status(state: int) -> Status:
    mapping = {
        _tapi_pb2.TERMINAL_STATE_CREATING: Status.CREATING,
        _tapi_pb2.TERMINAL_STATE_RUNNING: Status.RUNNING,
        _tapi_pb2.TERMINAL_STATE_SUSPENDING: Status.SUSPENDING,
        _tapi_pb2.TERMINAL_STATE_SUSPENDED: Status.SUSPENDED,
        _tapi_pb2.TERMINAL_STATE_RESUMING: Status.RESUMING,
        _tapi_pb2.TERMINAL_STATE_FAILED: Status.FAILED,
        _tapi_pb2.TERMINAL_STATE_DELETED: Status.DELETED,
    }
    try:
        return mapping[state]
    except KeyError as exc:
        raise InvalidRequestError("sandbox metadata contained an unsupported state") from exc


def _status_to_terminal_state(status: Status) -> int:
    mapping = {
        Status.CREATING: _tapi_pb2.TERMINAL_STATE_CREATING,
        Status.RUNNING: _tapi_pb2.TERMINAL_STATE_RUNNING,
        Status.SUSPENDING: _tapi_pb2.TERMINAL_STATE_SUSPENDING,
        Status.SUSPENDED: _tapi_pb2.TERMINAL_STATE_SUSPENDED,
        Status.RESUMING: _tapi_pb2.TERMINAL_STATE_RESUMING,
        Status.FAILED: _tapi_pb2.TERMINAL_STATE_FAILED,
    }
    return cast(int, mapping[status])
