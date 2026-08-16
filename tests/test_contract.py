from __future__ import annotations

import os
import base64
import builtins
import hashlib
import json
import pathlib
import queue
import subprocess
import sys
import time
import tomllib
from collections.abc import Iterator

import grpc
import pytest

from tyto import (
    CapabilityRejectedError,
    ConnectionError,
    ExecFailedError,
    ExecSession,
    Exit,
    InvalidRequestError,
    SandboxDeletedError,
    SandboxSuspendedError,
    SandboxCreationTimeoutError,
    SandboxNotFoundError,
    Sandbox,
    SandboxFailedError,
    SandboxSummary,
    Snapshot,
    Status,
    Stderr,
    Stdout,
    TimeoutError,
    Tyto,
    Wait,
)
from tyto._proto.tyto.runtime.v1 import guest_pb2, host_pb2_grpc, preview_pb2, tapi_pb2, tapi_pb2_grpc


class RpcFailure(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str = "failed") -> None:
        super().__init__()
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def make_test_capability(*, exp: int) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def make_metadata(
    sandbox_id: str,
    *,
    operation_id: str | None = None,
    status: Status = Status.RUNNING,
    code: str = "",
    message: str = "",
    name: str = "",
) -> tapi_pb2.TApiSandboxMetadata:
    return tapi_pb2.TApiSandboxMetadata(
        sandbox_id=sandbox_id,
        operation_id=operation_id or "op-" + sandbox_id,
        resolved_template_id="ubuntu-24.04",
        resolved_template_version="dev",
        observed=tapi_pb2.TerminalStatus(
            state=status_to_terminal_state(status),
            code=code,
            message=message,
        ),
        name=name,
    )


def status_to_terminal_state(status: Status) -> int:
    return {
        Status.CREATING: tapi_pb2.TERMINAL_STATE_CREATING,
        Status.RUNNING: tapi_pb2.TERMINAL_STATE_RUNNING,
        Status.SUSPENDING: tapi_pb2.TERMINAL_STATE_SUSPENDING,
        Status.SUSPENDED: tapi_pb2.TERMINAL_STATE_SUSPENDED,
        Status.RESUMING: tapi_pb2.TERMINAL_STATE_RESUMING,
        Status.FAILED: tapi_pb2.TERMINAL_STATE_FAILED,
        Status.DELETED: tapi_pb2.TERMINAL_STATE_DELETED,
    }[status]


class FakeChannel:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeTransport:
    def __init__(self) -> None:
        self.channels: list[FakeChannel] = []
        self.tapi: FakeTapi | None = None
        self.guest: FakeGuest | None = None

    def channel_factory(self, endpoint, credentials):  # type: ignore[no-untyped-def]
        channel = FakeChannel(endpoint.url)
        self.channels.append(channel)
        return channel

    def tapi_stub(self, channel: object) -> "FakeTapi":
        assert self.tapi is not None
        return self.tapi

    def guest_stub(self, channel: object) -> "FakeGuest":
        assert self.guest is not None
        return self.guest


class FakeTapi:
    def __init__(self) -> None:
        self.create_errors: queue.Queue[BaseException] = queue.Queue()
        self.delete_errors: queue.Queue[BaseException] = queue.Queue()
        self.get_errors: queue.Queue[BaseException] = queue.Queue()
        self.list_errors: queue.Queue[BaseException] = queue.Queue()
        self.resume_errors: queue.Queue[BaseException] = queue.Queue()
        self.snapshot_create_errors: queue.Queue[BaseException] = queue.Queue()
        self.snapshot_delete_errors: queue.Queue[BaseException] = queue.Queue()
        self.reissue_errors: queue.Queue[BaseException] = queue.Queue()
        self.preview_create_errors: queue.Queue[BaseException] = queue.Queue()
        self.preview_delete_errors: queue.Queue[BaseException] = queue.Queue()
        self.preview_list_errors: queue.Queue[BaseException] = queue.Queue()
        self.list_organizations_errors: queue.Queue[BaseException] = queue.Queue()
        self.create_requests: list[tapi_pb2.TApiServiceCreateRequest] = []
        self.delete_requests: list[tapi_pb2.TApiDeleteSandboxRequest] = []
        self.get_requests: list[tapi_pb2.TApiGetSandboxRequest] = []
        self.list_requests: list[tapi_pb2.TApiListSandboxesRequest] = []
        self.resume_requests: list[tapi_pb2.TApiResumeSandboxRequest] = []
        self.snapshot_create_requests: list[tapi_pb2.TApiCreateSnapshotRequest] = []
        self.snapshot_delete_requests: list[tapi_pb2.TApiDeleteSnapshotRequest] = []
        self.reissue_requests: list[tapi_pb2.TApiReissueCapabilityRequest] = []
        self.preview_create_requests: list[tapi_pb2.TApiCreatePreviewRequest] = []
        self.preview_delete_requests: list[tapi_pb2.TApiDeletePreviewRequest] = []
        self.preview_list_requests: list[tapi_pb2.TApiListPreviewsRequest] = []
        self.list_organizations_requests: list[tapi_pb2.TApiListOrganizationsRequest] = []
        # None means ListOrganizations returns its single-personal-org
        # default; set to override with a specific list, including empty.
        self.organizations: list[tapi_pb2.TApiOrganization] | None = None
        self.source_tenants: dict[str, str] = {"sbx-1": "tenant-a"}
        self.source_statuses: dict[str, Status] = {"sbx-1": Status.RUNNING}
        self.api_key_tenants: dict[str, str] = {"secret-api": "tenant-a"}
        self.get_capability = "fresh-get-cap"
        self.get_endpoint = "https://exec.example.test/edge"
        self.reissue_capability_value = "fresh-reissue-cap"
        self.list_pages: list[tapi_pb2.TApiListSandboxesResponse] = []
        self.snapshots: dict[str, str] = {}
        self.preview_domain = ".preview.example.test"
        self.preview_capability_value = "fresh-preview-cap"
        self.previews: dict[str, preview_pb2.PreviewRecord] = {}
        self.next_preview_id = "pv-aaaaaaaaaaaaaaaaaaaaaaaaaa"
        # Stands in for the name the service generates when a create request
        # leaves the name blank.
        self.generated_name = "brave-cedar-6268"

    def Create(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.create_requests.append(request)
        if not self.create_errors.empty():
            raise self.create_errors.get()
        return tapi_pb2.TApiServiceCreateResponse(
            operation_id="op-1",
            sandbox_id="sbx-1",
            exec_capability_jws="secret-cap",
            exec_endpoint="https://exec.example.test/edge",
            resolved_template_id="ubuntu-24.04",
            resolved_template_version="dev",
            name=request.name or self.generated_name,
        )

    def DeleteSandbox(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.delete_requests.append(request)
        if not self.delete_errors.empty():
            raise self.delete_errors.get()
        self.source_statuses[request.sandbox_id] = Status.DELETED
        return tapi_pb2.TApiDeleteSandboxResponse(sandbox_id=request.sandbox_id, already_deleted=False)

    def GetSandbox(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.get_requests.append(request)
        if not self.get_errors.empty():
            raise self.get_errors.get()
        tenant = self.api_key_tenants.get(request.api_key)
        if tenant is None:
            raise RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "bad api key")
        if self.source_tenants.get(request.sandbox_id) != tenant:
            raise RpcFailure(grpc.StatusCode.NOT_FOUND, "sandbox missing")
        status = self.source_statuses.get(request.sandbox_id, Status.RUNNING)
        if status is Status.DELETED:
            raise RpcFailure(grpc.StatusCode.NOT_FOUND, "sandbox missing")
        response = tapi_pb2.TApiGetSandboxResponse(sandbox=make_metadata(request.sandbox_id, status=status))
        if status is not Status.FAILED:
            response.exec_capability_jws = self.get_capability
            response.exec_endpoint = self.get_endpoint
        return response

    def ListSandboxes(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.list_requests.append(request)
        if not self.list_errors.empty():
            raise self.list_errors.get()
        if self.list_pages:
            return self.list_pages.pop(0)
        return tapi_pb2.TApiListSandboxesResponse(
            sandboxes=[
                make_metadata(sandbox_id, status=status)
                for sandbox_id, status in self.source_statuses.items()
                if status is not Status.DELETED
            ]
        )

    def ResumeSandbox(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.resume_requests.append(request)
        if not self.resume_errors.empty():
            raise self.resume_errors.get()
        return tapi_pb2.TApiResumeSandboxResponse(
            sandbox_id=request.sandbox_id,
            lifecycle_operation_id="lco-resume",
            already_running=False,
            exec_capability_jws="fresh-cap",
            exec_endpoint="https://exec.example.test/edge",
        )

    def CreateSnapshot(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.snapshot_create_requests.append(request)
        if not self.snapshot_create_errors.empty():
            raise self.snapshot_create_errors.get()
        tenant = self.api_key_tenants.get(request.api_key)
        if tenant is None:
            raise RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "bad api key")
        if self.source_tenants.get(request.sandbox_id) != tenant:
            raise RpcFailure(grpc.StatusCode.NOT_FOUND, "sandbox missing")
        status = self.source_statuses.get(request.sandbox_id)
        if status is Status.DELETED:
            raise RpcFailure(grpc.StatusCode.FAILED_PRECONDITION, "sandbox_deleted")
        if status is Status.SUSPENDED:
            raise RpcFailure(grpc.StatusCode.FAILED_PRECONDITION, "sandbox_suspended")
        if status is not Status.RUNNING:
            raise RpcFailure(grpc.StatusCode.FAILED_PRECONDITION, "sandbox_failed")
        digest = hashlib.sha256(f"{tenant}\0{request.sandbox_id}\0{request.idempotency_key}".encode()).hexdigest()
        snapshot_id = "snp-" + digest[:24]
        self.snapshots[snapshot_id] = tenant
        return tapi_pb2.TApiCreateSnapshotResponse(
            snapshot_id=snapshot_id,
            source_sandbox_id=request.sandbox_id,
            already_created=False,
        )

    def DeleteSnapshot(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.snapshot_delete_requests.append(request)
        if not self.snapshot_delete_errors.empty():
            raise self.snapshot_delete_errors.get()
        tenant = self.api_key_tenants.get(request.api_key)
        if tenant is not None and self.snapshots.get(request.snapshot_id) == tenant:
            del self.snapshots[request.snapshot_id]
        return tapi_pb2.TApiDeleteSnapshotResponse(snapshot_id=request.snapshot_id)

    def ReissueCapability(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.reissue_requests.append(request)
        if not self.reissue_errors.empty():
            raise self.reissue_errors.get()
        tenant = self.api_key_tenants.get(request.api_key)
        if tenant is None:
            raise RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "bad api key")
        if self.source_tenants.get(request.sandbox_id) != tenant:
            raise RpcFailure(grpc.StatusCode.NOT_FOUND, "sandbox not found")
        status = self.source_statuses.get(request.sandbox_id)
        if status is Status.DELETED:
            raise RpcFailure(grpc.StatusCode.FAILED_PRECONDITION, "sandbox_deleted")
        return tapi_pb2.TApiReissueCapabilityResponse(
            capability_jws=self.reissue_capability_value,
            expires_at_unix_nanos=1,
        )

    def _preview_tenant(self, request):  # type: ignore[no-untyped-def]
        tenant = self.api_key_tenants.get(request.api_key)
        if tenant is None:
            raise RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "bad api key")
        if self.source_tenants.get(request.sandbox_id) != tenant:
            raise RpcFailure(grpc.StatusCode.NOT_FOUND, "sandbox not found")
        return tenant

    def _preview_info(self, record):  # type: ignore[no-untyped-def]
        return tapi_pb2.PreviewInfo(
            record=record,
            url=f"https://{record.preview_id}{self.preview_domain}",
        )

    def CreatePreview(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.preview_create_requests.append(request)
        if not self.preview_create_errors.empty():
            raise self.preview_create_errors.get()
        self._preview_tenant(request)
        mode = request.auth_mode
        if mode == preview_pb2.PREVIEW_AUTH_MODE_UNSPECIFIED:
            mode = preview_pb2.PREVIEW_AUTH_MODE_TOKEN
        record = preview_pb2.PreviewRecord(
            preview_id=self.next_preview_id,
            sandbox_id=request.sandbox_id,
            port=request.port,
            auth_mode=mode,
            name=request.name,
            created_at_unix_nanos=1_700_000_000_000_000_000,
        )
        self.previews[record.preview_id] = record
        return tapi_pb2.TApiCreatePreviewResponse(
            preview=self._preview_info(record),
            capability_jws=self.preview_capability_value,
        )

    def DeletePreview(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.preview_delete_requests.append(request)
        if not self.preview_delete_errors.empty():
            raise self.preview_delete_errors.get()
        self._preview_tenant(request)
        already = self.previews.pop(request.preview_id, None) is None
        return tapi_pb2.TApiDeletePreviewResponse(preview_id=request.preview_id, already_deleted=already)

    def ListPreviews(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.preview_list_requests.append(request)
        if not self.preview_list_errors.empty():
            raise self.preview_list_errors.get()
        self._preview_tenant(request)
        return tapi_pb2.TApiListPreviewsResponse(
            previews=[self._preview_info(record) for record in self.previews.values()],
        )

    def ListOrganizations(self, request, timeout=None):  # type: ignore[no-untyped-def]
        self.list_organizations_requests.append(request)
        if not self.list_organizations_errors.empty():
            raise self.list_organizations_errors.get()
        if self.organizations is not None:
            return tapi_pb2.TApiListOrganizationsResponse(organizations=self.organizations)
        return tapi_pb2.TApiListOrganizationsResponse(
            organizations=[
                tapi_pb2.TApiOrganization(
                    organization_id="org-personal",
                    name="personal",
                    personal=True,
                    role="owner",
                    created_at_unix_nanos=1,
                ),
            ]
        )


class FakeStream:
    def __init__(self, requests: Iterator[guest_pb2.ExecRequest], responses: list[guest_pb2.ExecResponse]) -> None:
        self.requests = requests
        self.responses = responses
        self.index = 0
        self.seen: list[guest_pb2.ExecRequest] = []

    def __iter__(self) -> "FakeStream":
        return self

    def __next__(self) -> guest_pb2.ExecResponse:
        if self.index >= len(self.responses):
            raise StopIteration
        response = self.responses[self.index]
        self.index += 1
        return response

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


class LazyStream:
    def __init__(self, requests: Iterator[guest_pb2.ExecRequest]) -> None:
        self.requests = requests
        self.seen: list[guest_pb2.ExecRequest] = []
        self.cancel_called = False

    def __iter__(self) -> "LazyStream":
        return self

    def __next__(self) -> guest_pb2.ExecResponse:
        raise StopIteration

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

    def cancel(self) -> None:
        self.cancel_called = True


class FloodStream:
    def __init__(self) -> None:
        self.cancel_called = False
        self.index = 0

    def __iter__(self) -> "FloodStream":
        return self

    def __next__(self) -> guest_pb2.ExecResponse:
        self.index += 1
        return guest_pb2.ExecResponse(stdout=guest_pb2.StdoutData(data=b"x"))

    def cancel(self) -> None:
        self.cancel_called = True


class HangingStream:
    def __init__(self, requests: Iterator[guest_pb2.ExecRequest]) -> None:
        self.requests = requests
        self.seen: list[guest_pb2.ExecRequest] = []
        self.cancel_called = False

    def __iter__(self) -> "HangingStream":
        return self

    def __next__(self) -> guest_pb2.ExecResponse:
        time.sleep(10)
        raise StopIteration

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

    def cancel(self) -> None:
        self.cancel_called = True


class FailingStream:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error or RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "exec capability rejected secret-cap")

    def __iter__(self) -> "FailingStream":
        return self

    def __next__(self) -> guest_pb2.ExecResponse:
        raise self.error


class FakeGuest:
    def __init__(self) -> None:
        self.calls = 0
        self.last_stream: FakeStream | None = None
        self.lazy_stream: LazyStream | None = None
        self.flood_stream: FloodStream | None = None
        self.hanging_stream: HangingStream | None = None
        self.fail = False
        self.lazy = False
        self.flood = False
        self.hang = False
        self.failure: BaseException | None = None

    def Exec(self, requests, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        self.calls += 1
        self.metadata = metadata
        if self.fail:
            return FailingStream(self.failure)
        if self.lazy:
            self.lazy_stream = LazyStream(requests)
            return self.lazy_stream
        if self.flood:
            self.flood_stream = FloodStream()
            return self.flood_stream
        if self.hang:
            self.hanging_stream = HangingStream(requests)
            return self.hanging_stream
        self.last_stream = FakeStream(
            requests,
            [
                guest_pb2.ExecResponse(stdout=guest_pb2.StdoutData(data=b"ready")),
                guest_pb2.ExecResponse(stderr=guest_pb2.StderrData(data=b"warn\xff")),
                guest_pb2.ExecResponse(exit=guest_pb2.ExecExit(exit_code=0)),
            ],
        )
        return self.last_stream


def make_client(monkeypatch: pytest.MonkeyPatch, transport: FakeTransport) -> Tyto:
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    return Tyto(
        endpoint="https://api.example.test/",
        timeout=2,
        max_retries=2,
        _channel_factory=transport.channel_factory,
        _tapi_stub_factory=transport.tapi_stub,
        _guest_stub_factory=transport.guest_stub,
    )


def test_configuration_precedence_and_endpoint_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    monkeypatch.setenv("BONYA_API_KEY", "env-key")
    monkeypatch.setenv("BONYA_ENDPOINT", "https://env.example.test")
    client = Tyto(_channel_factory=transport.channel_factory, _tapi_stub_factory=transport.tapi_stub)
    assert client.sandboxes
    with pytest.raises(InvalidRequestError):
        Tyto(api_key="k", endpoint="http://example.test")
    with pytest.raises(InvalidRequestError):
        Tyto(api_key="k", endpoint="https://u:p@example.test")
    with pytest.raises(InvalidRequestError):
        Tyto(api_key="k", endpoint="https://example.test/path?q=1")
    with pytest.raises(InvalidRequestError):
        Tyto(api_key="k", endpoint="https://example.test:bad")


def test_create_request_mapping_retry_and_channel_pooling(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.create_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "try again"))
    client = make_client(monkeypatch, transport)

    sandbox = client.sandboxes.create(
        template="ubuntu-24.04",
        version=None,
        wait=Wait.NONE,
        idempotency_key="idem-1",
    )

    assert sandbox.id == "sbx-1"
    assert sandbox.last_observed_status is Status.CREATING
    assert len(transport.tapi.create_requests) == 2
    assert transport.tapi.create_requests[0].SerializeToString() == transport.tapi.create_requests[1].SerializeToString()
    request = transport.tapi.create_requests[0]
    assert request.api_key == "secret-api"
    assert request.idempotency_key == "idem-1"
    assert request.template.template_id == "ubuntu-24.04"
    assert request.template.version == ""
    assert request.template.digest == ""
    assert request.wait == tapi_pb2.CREATE_WAIT_NONE

    result = sandbox.exec("printf ready")
    assert result.stdout == "ready"
    assert result.stderr == "warn�"
    assert result.stdout_bytes == b"ready"
    assert result.ok
    assert transport.guest.calls == 1
    assert [channel.endpoint for channel in transport.channels] == [
        "https://api.example.test",
        "https://exec.example.test/edge",
    ]
    client.close()
    assert all(channel.closed for channel in transport.channels)
    with pytest.raises(InvalidRequestError):
        client.sandboxes.create(template="ubuntu-24.04")


def test_missing_endpoint_is_hard_response_error(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.tapi.Create = lambda request, timeout=None: tapi_pb2.TApiServiceCreateResponse(  # type: ignore[method-assign]
        operation_id="op", sandbox_id="sbx", exec_capability_jws="cap"
    )
    client = make_client(monkeypatch, transport)
    with pytest.raises(InvalidRequestError):
        client.sandboxes.create(template="ubuntu-24.04", idempotency_key="idem")


def test_get_returns_usable_sandbox_with_metadata_without_resume(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.source_statuses["sbx-1"] = Status.SUSPENDED
    client = make_client(monkeypatch, transport)

    sandbox = client.sandboxes.get("sbx-1")

    assert sandbox.id == "sbx-1"
    assert sandbox.operation_id == "op-sbx-1"
    assert sandbox.template == "ubuntu-24.04"
    assert sandbox.version == "dev"
    assert sandbox.last_observed_status is Status.SUSPENDED
    assert len(transport.tapi.get_requests) == 1
    assert len(transport.tapi.resume_requests) == 0
    assert sandbox.exec(["printf", "ready"]).stdout == "ready"


def test_get_missing_deleted_and_cross_tenant_map_to_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    client = make_client(monkeypatch, transport)
    transport.tapi.source_tenants["deleted"] = "tenant-a"
    transport.tapi.source_statuses["deleted"] = Status.DELETED
    transport.tapi.source_tenants["cross-tenant"] = "tenant-b"
    transport.tapi.source_statuses["cross-tenant"] = Status.RUNNING

    for sandbox_id in ["missing", "deleted", "cross-tenant"]:
        with pytest.raises(SandboxNotFoundError):
            client.sandboxes.get(sandbox_id)


def test_list_is_lazy_paginates_and_honors_total_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    client = make_client(monkeypatch, transport)
    transport.tapi.list_pages = [
        tapi_pb2.TApiListSandboxesResponse(
            sandboxes=[
                make_metadata("sbx-3", status=Status.RUNNING),
                make_metadata("sbx-2", status=Status.SUSPENDED),
            ],
            next_page_token="secret-token",
        ),
        tapi_pb2.TApiListSandboxesResponse(
            sandboxes=[
                make_metadata("sbx-1", status=Status.FAILED, code="create_failed", message="disk full"),
            ],
        ),
    ]

    iterator = client.sandboxes.list(limit=3)
    assert transport.tapi.list_requests == []

    summaries = list(iterator)

    assert [summary.id for summary in summaries] == ["sbx-3", "sbx-2", "sbx-1"]
    assert summaries[2] == SandboxSummary(
        id="sbx-1",
        operation_id="op-sbx-1",
        template="ubuntu-24.04",
        version="dev",
        last_observed_status=Status.FAILED,
        failure_code="create_failed",
        failure_message="disk full",
    )
    assert len(transport.tapi.list_requests) == 2
    assert transport.tapi.list_requests[0].page_size == 3
    assert transport.tapi.list_requests[0].page_token == ""
    assert transport.tapi.list_requests[1].page_size == 1
    assert transport.tapi.list_requests[1].page_token == "secret-token"
    assert not hasattr(summaries[0], "exec")
    assert not hasattr(summaries[0], "_capability")


def test_list_limit_zero_and_invalid_filters_fail_before_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    client = make_client(monkeypatch, transport)

    assert list(client.sandboxes.list(limit=0)) == []
    assert transport.tapi.list_requests == []

    with pytest.raises(InvalidRequestError):
        client.sandboxes.list(states=[Status.DELETED])
    with pytest.raises(InvalidRequestError):
        client.sandboxes.list(states=["running"])  # type: ignore[list-item]
    with pytest.raises(InvalidRequestError):
        client.sandboxes.list(limit=-1)
    assert transport.tapi.list_requests == []


def test_list_state_filters_serialize(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    client = make_client(monkeypatch, transport)

    list(client.sandboxes.list(states=[Status.RUNNING, Status.FAILED], limit=1))

    request = transport.tapi.list_requests[0]
    assert list(request.states) == [
        tapi_pb2.TERMINAL_STATE_RUNNING,
        tapi_pb2.TERMINAL_STATE_FAILED,
    ]


def test_failed_get_handle_rejects_exec_locally_but_deletes(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.source_statuses["sbx-1"] = Status.FAILED
    client = make_client(monkeypatch, transport)

    sandbox = client.sandboxes.get("sbx-1")

    assert sandbox.last_observed_status is Status.FAILED
    with pytest.raises(SandboxFailedError):
        sandbox.exec(["printf", "x"])
    with pytest.raises(SandboxFailedError):
        sandbox.exec_stream(["printf", "x"])
    with pytest.raises(SandboxFailedError):
        sandbox.snapshot(idempotency_key="snapshot-failed")
    with pytest.raises(SandboxFailedError):
        sandbox.resume(idempotency_key="resume-failed")
    assert transport.guest.calls == 0
    assert transport.tapi.snapshot_create_requests == []
    assert transport.tapi.resume_requests == []
    assert sandbox.delete().sandbox_id == "sbx-1"


def test_exec_never_retries_and_redacts_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.fail = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(CapabilityRejectedError) as caught:
        sandbox.exec(["false"])

    assert transport.guest.calls == 1
    assert "secret-cap" not in str(caught.value)
    assert "secret-api" not in repr(sandbox)


def test_exec_expired_capability_gets_once_and_retries_without_resume(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    expired_capability = make_test_capability(exp=int(time.time()) - 10)
    transport.tapi.Create = lambda request, timeout=None: tapi_pb2.TApiServiceCreateResponse(  # type: ignore[method-assign]
        operation_id="op-1",
        sandbox_id="sbx-1",
        exec_capability_jws=expired_capability,
        exec_endpoint="https://exec.example.test/edge",
        resolved_template_id="ubuntu-24.04",
        resolved_template_version="dev",
    )
    original_exec = transport.guest.Exec

    def exec_with_one_expired_rejection(requests, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        if transport.guest.calls == 0:
            transport.guest.calls += 1
            return FailingStream(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "exec capability rejected"))
        return original_exec(requests, timeout=timeout, metadata=metadata)

    transport.guest.Exec = exec_with_one_expired_rejection  # type: ignore[method-assign]
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    result = sandbox.exec(["cat"], env={"MODE": "development"}, cwd="/workspace", input="replayed\n")

    assert result.stdout == "ready"
    assert len(transport.tapi.get_requests) == 1
    assert len(transport.tapi.resume_requests) == 0
    assert transport.guest.calls == 2
    assert sandbox._capability == "fresh-get-cap"
    assert transport.guest.last_stream is not None
    assert transport.guest.last_stream.collect_requests(limit=3) == ["start", "stdin"]
    start = transport.guest.last_stream.seen[0].start
    assert dict(start.env) == {"MODE": "development"}
    assert start.working_dir == "/workspace"
    assert transport.guest.last_stream.seen[1].stdin.data == b"replayed\n"


def test_streaming_exec_expired_capability_gets_once_and_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    expired_capability = make_test_capability(exp=int(time.time()) - 10)
    transport.tapi.Create = lambda request, timeout=None: tapi_pb2.TApiServiceCreateResponse(  # type: ignore[method-assign]
        operation_id="op-1",
        sandbox_id="sbx-1",
        exec_capability_jws=expired_capability,
        exec_endpoint="https://exec.example.test/edge",
        resolved_template_id="ubuntu-24.04",
        resolved_template_version="dev",
    )
    original_exec = transport.guest.Exec

    def exec_with_one_expired_rejection(requests, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        if transport.guest.calls == 0:
            transport.guest.calls += 1
            return FailingStream(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "exec capability rejected"))
        return original_exec(requests, timeout=timeout, metadata=metadata)

    transport.guest.Exec = exec_with_one_expired_rejection  # type: ignore[method-assign]
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with sandbox.exec_stream(["printf", "ready"], env={"NODE_ENV": "development"}, cwd="/workspace") as session:
        events = list(session)

    assert isinstance(events[-1], Exit)
    assert len(transport.tapi.get_requests) == 1
    assert len(transport.tapi.resume_requests) == 0
    assert transport.guest.calls == 2
    assert transport.guest.last_stream is not None
    assert transport.guest.last_stream.collect_requests(limit=1) == ["start"]
    start = transport.guest.last_stream.seen[0].start
    assert dict(start.env) == {"NODE_ENV": "development"}
    assert start.working_dir == "/workspace"


def test_streaming_exec_expired_retry_replays_preread_input(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    expired_capability = make_test_capability(exp=int(time.time()) - 10)
    transport.tapi.Create = lambda request, timeout=None: tapi_pb2.TApiServiceCreateResponse(  # type: ignore[method-assign]
        operation_id="op-1",
        sandbox_id="sbx-1",
        exec_capability_jws=expired_capability,
        exec_endpoint="https://exec.example.test/edge",
        resolved_template_id="ubuntu-24.04",
        resolved_template_version="dev",
    )
    original_exec = transport.guest.Exec

    def exec_with_one_expired_rejection(requests, timeout=None, metadata=None):  # type: ignore[no-untyped-def]
        if transport.guest.calls == 0:
            transport.guest.calls += 1
            return FailingStream(RpcFailure(grpc.StatusCode.PERMISSION_DENIED, "exec capability rejected"))
        return original_exec(requests, timeout=timeout, metadata=metadata)

    transport.guest.Exec = exec_with_one_expired_rejection  # type: ignore[method-assign]
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with sandbox.exec_stream(["cat"]) as session:
        session.write(b"input\n")
        session.close_stdin()
        events = list(session)

    assert isinstance(events[-1], Exit)
    assert transport.guest.last_stream is not None
    assert transport.guest.last_stream.collect_requests(limit=3) == ["start", "stdin"]
    assert transport.guest.last_stream.seen[1].stdin.data == b"input\n"
    assert len(transport.tapi.get_requests) == 1
    assert len(transport.tapi.resume_requests) == 0
    assert transport.guest.calls == 2


def test_exec_suspended_failure_updates_local_status(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.fail = True
    transport.guest.failure = RpcFailure(grpc.StatusCode.FAILED_PRECONDITION, "sandbox_suspended")
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(SandboxSuspendedError):
        sandbox.exec(["printf", "x"])

    assert sandbox.last_observed_status is Status.SUSPENDED
    with pytest.raises(SandboxSuspendedError):
        sandbox.exec(["printf", "x"])
    assert transport.guest.calls == 2


def test_exec_result_check_and_repr() -> None:
    from tyto import ExecResult

    result = ExecResult(stdout_bytes=b"ok", stderr_bytes=b"err", exit_code=2, sandbox_id="sbx")
    assert str(result) == "ok"
    assert "exit_code=2" in repr(result)
    with pytest.raises(ExecFailedError) as caught:
        result.check()
    assert caught.value.result is result


def test_delete_idempotence_and_context_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    first = sandbox.delete()
    second = sandbox.delete()
    assert first.already_deleted is False
    assert second.already_deleted is True
    assert len(transport.tapi.delete_requests) == 1
    assert sandbox.last_observed_status is Status.DELETED
    with pytest.raises(SandboxDeletedError):
        sandbox.exec(["printf", "x"])

    sandbox2 = client.sandboxes.create(template="ubuntu-24.04")
    transport.tapi.delete_errors.put(RpcFailure(grpc.StatusCode.NOT_FOUND, "missing"))
    with pytest.raises(SandboxNotFoundError):
        with sandbox2:
            pass


def test_snapshot_create_derives_by_source_and_key(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.source_tenants["sbx-2"] = "tenant-a"
    transport.tapi.source_statuses["sbx-2"] = Status.RUNNING
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    other_source = Sandbox(
        client=client,
        sandbox_id="sbx-2",
        operation_id="op-2",
        template="ubuntu-24.04",
        version="dev",
        status=Status.RUNNING,
        exec_endpoint="https://exec.example.test/edge",
        capability="secret-cap",
    )

    first = sandbox.snapshot(idempotency_key="snapshot-key")
    replay = sandbox.snapshot(idempotency_key="snapshot-key")
    other_key = sandbox.snapshot(idempotency_key="snapshot-key-2")
    other_source_snapshot = other_source.snapshot(idempotency_key="snapshot-key")

    assert isinstance(first, Snapshot)
    assert first.id == replay.id
    assert first.source_sandbox_id == "sbx-1"
    assert first.id != other_key.id
    assert first.id != other_source_snapshot.id
    request = transport.tapi.snapshot_create_requests[0]
    assert request.api_key == "secret-api"
    assert request.sandbox_id == "sbx-1"
    assert request.idempotency_key == "snapshot-key"


def test_snapshot_retry_reuses_idempotency_key_and_snapshot_id(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.snapshot_create_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "try again"))
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    snapshot = sandbox.snapshot(idempotency_key="stable-key")

    assert snapshot.id
    assert len(transport.tapi.snapshot_create_requests) == 2
    assert (
        transport.tapi.snapshot_create_requests[0].SerializeToString()
        == transport.tapi.snapshot_create_requests[1].SerializeToString()
    )
    expected = hashlib.sha256(b"tenant-a\0sbx-1\0stable-key").hexdigest()
    assert snapshot.id == "snp-" + expected[:24]


def test_snapshot_create_requires_owning_running_source(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.source_tenants["other-tenant-source"] = "tenant-b"
    transport.tapi.source_statuses["other-tenant-source"] = Status.RUNNING
    transport.tapi.source_tenants["missing-source"] = "tenant-a"
    transport.tapi.source_statuses["missing-source"] = Status.DELETED
    transport.tapi.source_tenants["suspended-source"] = "tenant-a"
    transport.tapi.source_statuses["suspended-source"] = Status.SUSPENDED
    client = make_client(monkeypatch, transport)

    running = client.sandboxes.create(template="ubuntu-24.04")
    assert running.snapshot(idempotency_key="ok").source_sandbox_id == "sbx-1"

    for sandbox_id, status, error_cls in [
        ("other-tenant-source", Status.RUNNING, SandboxNotFoundError),
        ("missing-source", Status.RUNNING, SandboxDeletedError),
        ("suspended-source", Status.RUNNING, SandboxSuspendedError),
    ]:
        source = Sandbox(
            client=client,
            sandbox_id=sandbox_id,
            operation_id="op-" + sandbox_id,
            template="ubuntu-24.04",
            version="dev",
            status=status,
            exec_endpoint="https://exec.example.test/edge",
            capability="secret-cap",
        )
        with pytest.raises(error_cls):
            source.snapshot(idempotency_key="not-ok")


def test_snapshot_delete_after_source_delete_and_repeated_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    snapshot = sandbox.snapshot(idempotency_key="keep-after-source-delete")

    sandbox.delete()
    first = snapshot.delete()
    second = snapshot.delete()

    assert first is None
    assert second is None
    assert len(transport.tapi.snapshot_delete_requests) == 1
    request = transport.tapi.snapshot_delete_requests[0]
    assert request.api_key == "secret-api"
    assert request.source_sandbox_id == "sbx-1"
    assert request.snapshot_id == snapshot.id


def test_snapshot_delete_missing_and_cross_tenant_have_same_public_result(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.snapshots["snp-cross-tenant"] = "tenant-b"
    client = make_client(monkeypatch, transport)

    missing = Snapshot(client=client, snapshot_id="snp-missing", source_sandbox_id="sbx-1")
    cross_tenant = Snapshot(client=client, snapshot_id="snp-cross-tenant", source_sandbox_id="sbx-1")

    assert missing.delete() is None
    assert cross_tenant.delete() is None
    assert "snp-cross-tenant" in transport.tapi.snapshots
    assert len(transport.tapi.snapshot_delete_requests) == 2


def test_snapshot_errors_are_stable_and_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.snapshot_create_errors.put(
        RpcFailure(
            grpc.StatusCode.DEADLINE_EXCEEDED,
            "snapshot timed out for secret-api stable-key /var/lib/bonya/snapshots",
        )
    )
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(TimeoutError) as create_caught:
        sandbox.snapshot(idempotency_key="stable-key")

    create_message = str(create_caught.value)
    assert "secret-api" not in create_message
    assert "stable-key" not in create_message
    assert "/var/lib/bonya/snapshots" not in create_message

    snapshot = Snapshot(client=client, snapshot_id="snp-sensitive", source_sandbox_id="sbx-1")
    client._max_retries = 0
    transport.tapi.snapshot_delete_errors.put(
        RpcFailure(
            grpc.StatusCode.UNAVAILABLE,
            "recovery failed for secret-api snp-sensitive /run/bonya/host.sock",
        )
    )
    with pytest.raises(ConnectionError) as delete_caught:
        snapshot.delete()

    delete_message = str(delete_caught.value)
    assert "secret-api" not in delete_message
    assert "snp-sensitive" not in delete_message
    assert "/run/bonya/host.sock" not in delete_message


def test_public_suspend_is_not_exposed_by_sdk_or_tapi_proto(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    assert not hasattr(sandbox, "suspend")
    assert not hasattr(tapi_pb2, "TApiSuspendSandboxRequest")
    assert not hasattr(tapi_pb2, "TApiSuspendSandboxResponse")
    assert "SuspendSandbox" not in tapi_pb2.DESCRIPTOR.services_by_name["TApiService"].methods_by_name
    assert not hasattr(tapi_pb2_grpc.TApiService, "SuspendSandbox")
    assert hasattr(host_pb2_grpc.HostService, "SuspendSandbox")


def test_resume_replaces_private_capability_and_sets_status(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    old_capability = sandbox._capability

    result = sandbox.resume(idempotency_key="resume-key")

    assert result.sandbox_id == "sbx-1"
    assert result.lifecycle_operation_id == "lco-resume"
    assert result.already_running is False
    assert sandbox.last_observed_status is Status.RUNNING
    assert old_capability == "secret-cap"
    assert sandbox._capability == "fresh-cap"
    assert transport.tapi.resume_requests[0].idempotency_key == "resume-key"


def test_resume_preserves_idempotency_key_and_status_on_ambiguous_error(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.tapi.resume_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "try again"))
    transport.tapi.resume_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "still down secret-api"))
    client = make_client(monkeypatch, transport)
    client._max_retries = 1
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    sandbox.last_observed_status = Status.SUSPENDED

    with pytest.raises(ConnectionError) as caught:
        sandbox.resume(idempotency_key="resume-key")

    assert caught.value.idempotency_key == "resume-key"
    assert sandbox.last_observed_status is Status.SUSPENDED
    assert sandbox._capability == "secret-cap"
    assert len(transport.tapi.resume_requests) == 2
    assert transport.tapi.resume_requests[0].SerializeToString() == transport.tapi.resume_requests[1].SerializeToString()


def test_dual_failure_chains_cleanup_error(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    transport.tapi.delete_errors.put(RpcFailure(grpc.StatusCode.NOT_FOUND, "missing"))

    with pytest.raises(RuntimeError) as caught:
        with sandbox:
            raise RuntimeError("body")

    assert isinstance(caught.value.__context__, SandboxNotFoundError)


def test_streaming_stdin_half_close_and_events(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with sandbox.exec_stream(["cat"]) as session:
        session.write(b"input\n")
        session.close_stdin()
        events = list(session)

    assert isinstance(events[0], Stdout)
    assert isinstance(events[1], Stderr)
    assert isinstance(events[2], Exit)
    assert transport.guest.last_stream is not None


def test_buffered_exec_env_and_cwd_start_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    env = {"MODE": "development"}
    result = sandbox.exec(["python3", "worker.py"], env=env, cwd="/workspace")
    env["MODE"] = "mutated"

    assert result.ok
    assert transport.guest.last_stream is not None
    assert transport.guest.last_stream.collect_requests(limit=1) == ["start"]
    start = transport.guest.last_stream.seen[0].start
    assert list(start.command) == ["python3", "worker.py"]
    assert dict(start.env) == {"MODE": "development"}
    assert start.working_dir == "/workspace"


def test_streaming_exec_env_and_cwd_start_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["npm", "run", "dev"], env={"NODE_ENV": "development"}, cwd="/workspace")

    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests(limit=1) == ["start"]
    start = transport.guest.lazy_stream.seen[0].start
    assert list(start.command) == ["npm", "run", "dev"]
    assert dict(start.env) == {"NODE_ENV": "development"}
    assert start.working_dir == "/workspace"
    session.cancel()


@pytest.mark.parametrize(
    ("kwargs", "method"),
    [
        ({"env": []}, "exec"),
        ({"env": {"": "value"}}, "exec"),
        ({"env": {"A=B": "value"}}, "exec"),
        ({"env": {"A\0B": "value"}}, "exec"),
        ({"env": {1: "value"}}, "exec"),
        ({"env": {"KEY": b"value"}}, "exec"),
        ({"env": {"KEY": "bad\0value"}}, "exec"),
        ({"cwd": ""}, "exec"),
        ({"cwd": b"/workspace"}, "exec"),
        ({"cwd": "/bad\0path"}, "exec"),
        ({"env": {"KEY": b"value"}}, "exec_stream"),
        ({"cwd": ""}, "exec_stream"),
    ],
)
def test_exec_env_and_cwd_invalid_inputs_fail_before_rpc(
    monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, object], method: str
) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    transport.guest.calls = 0

    with pytest.raises(InvalidRequestError):
        if method == "exec":
            sandbox.exec(["true"], **kwargs)  # type: ignore[arg-type]
        else:
            sandbox.exec_stream(["true"], **kwargs)  # type: ignore[arg-type]

    assert transport.guest.calls == 0


def test_exec_buffered_string_input_writes_stdin_and_returns_result(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    result = sandbox.exec(["cat"], input="snowman: \u2603\n")

    assert result.stdout == "ready"
    assert result.stderr_bytes == b"warn\xff"
    assert transport.guest.last_stream is not None
    assert transport.guest.last_stream.collect_requests(limit=3) == ["start", "stdin"]
    assert transport.guest.last_stream.seen[1].stdin.data == "snowman: \u2603\n".encode()


@pytest.mark.parametrize("buffered_input", [b"bin\x00ary", b""])
def test_exec_buffered_binary_and_empty_input_write_stdin(
    monkeypatch: pytest.MonkeyPatch, buffered_input: bytes
) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    sandbox.exec(["cat"], input=buffered_input)

    assert transport.guest.last_stream is not None
    assert transport.guest.last_stream.collect_requests(limit=3) == ["start", "stdin"]
    assert transport.guest.last_stream.seen[1].stdin.data == buffered_input


@pytest.mark.parametrize("buffered_input", [bytearray(b"x"), memoryview(b"x"), True, object()])
def test_exec_buffered_input_invalid_inputs_fail_before_rpc(
    monkeypatch: pytest.MonkeyPatch, buffered_input: object
) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    transport.guest.calls = 0

    with pytest.raises(InvalidRequestError):
        sandbox.exec(["true"], input=buffered_input)  # type: ignore[arg-type]

    assert transport.guest.calls == 0


def test_exec_buffered_input_with_tty_fails_before_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    transport.guest.calls = 0

    with pytest.raises(InvalidRequestError):
        sandbox.exec(["sh"], tty=True, input="")

    assert transport.guest.calls == 0


def test_tty_start_serialization_and_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["sh"], tty=True)
    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests(limit=1) == ["start"]
    start = transport.guest.lazy_stream.seen[0].start
    assert start.tty is True
    assert start.cols == 0
    assert start.rows == 0
    session.cancel()

    transport.guest.lazy_stream = None
    session = sandbox.exec_stream(["sh"], tty=True, cols=120, rows=40)
    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests(limit=1) == ["start"]
    start = transport.guest.lazy_stream.seen[0].start
    assert start.tty is True
    assert start.cols == 120
    assert start.rows == 40
    session.cancel()

    invalid_options = [
        {"tty": True, "cols": 80},
        {"tty": True, "rows": 24},
        {"tty": True, "cols": 0, "rows": 24},
        {"tty": True, "cols": -1, "rows": 24},
        {"tty": True, "cols": True, "rows": 24},
        {"tty": True, "cols": 80.0, "rows": 24},
        {"tty": True, "cols": 513, "rows": 24},
        {"tty": False, "cols": 80, "rows": 24},
    ]
    for options in invalid_options:
        with pytest.raises(InvalidRequestError):
            sandbox.exec_stream(["sh"], **options)  # type: ignore[arg-type]


def test_tty_buffered_result_uses_stdout_only(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
    transport.guest.Exec = lambda requests, timeout=None, metadata=None: FakeStream(  # type: ignore[method-assign]
        requests,
        [
            guest_pb2.ExecResponse(stdout=guest_pb2.StdoutData(data=b"out")),
            guest_pb2.ExecResponse(exit=guest_pb2.ExecExit(exit_code=0)),
        ],
    )

    result = sandbox.exec(["sh"], tty=True)

    assert result.stdout_bytes == b"out"
    assert result.stderr_bytes == b""


def test_tty_resize_serialization_and_invalid_state(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["sh"], tty=True)
    session.resize(cols=100, rows=30)

    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests(limit=2) == ["start", "resize"]
    resize = transport.guest.lazy_stream.seen[1].resize
    assert resize.cols == 100
    assert resize.rows == 30
    for options in [
        {"cols": 0, "rows": 24},
        {"cols": -1, "rows": 24},
        {"cols": True, "rows": 24},
        {"cols": 80.0, "rows": 24},
        {"cols": 513, "rows": 24},
    ]:
        with pytest.raises(InvalidRequestError):
            session.resize(**options)  # type: ignore[arg-type]
    session.cancel()

    non_tty = sandbox.exec_stream(["sh"])
    with pytest.raises(InvalidRequestError):
        non_tty.resize(cols=100, rows=30)
    non_tty.cancel()

    transport.guest.lazy = False
    completed = sandbox.exec_stream(["sh"], tty=True)
    list(completed)
    with pytest.raises(InvalidRequestError):
        completed.resize(cols=100, rows=30)


def test_tty_resize_after_close_stdin_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["sh"], tty=True)
    session.close_stdin()
    with pytest.raises(InvalidRequestError, match="stdin is closed"):
        session.resize(cols=100, rows=30)

    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests() == ["start"]
    session.cancel()


def test_exported_exec_session_constructor_preserves_non_tty_defaults() -> None:
    guest = FakeGuest()
    guest.lazy = True

    session = ExecSession(
        sandbox_id="sandbox-direct",
        operation_id="operation-direct",
        command=["true"],
        stub=guest,
        capability="capability",
        timeout=2,
        secrets=[],
    )

    assert guest.lazy_stream is not None
    assert guest.lazy_stream.collect_requests(limit=1) == ["start"]
    start = guest.lazy_stream.seen[0].start
    assert start.tty is False
    assert start.cols == 0
    assert start.rows == 0
    session.cancel()


@pytest.mark.parametrize(
    ("tty", "cols", "rows"),
    [
        (True, 80, 0),
        (True, 0, 24),
        (True, 513, 24),
        (True, True, 24),
        (True, 80.0, 24),
        (False, 80, 24),
    ],
)
def test_exported_exec_session_constructor_rejects_invalid_tty_options(
    tty: object, cols: object, rows: object
) -> None:
    guest = FakeGuest()

    with pytest.raises(InvalidRequestError):
        ExecSession(
            sandbox_id="sandbox-direct",
            operation_id="operation-direct",
            command=["true"],
            tty=tty,  # type: ignore[arg-type]
            cols=cols,  # type: ignore[arg-type]
            rows=rows,  # type: ignore[arg-type]
            stub=guest,
            capability="capability",
            timeout=2,
            secrets=[],
        )

    assert guest.calls == 0


def test_error_builtin_compatibility_and_retry_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    assert issubclass(TimeoutError, builtins.TimeoutError)
    assert issubclass(ConnectionError, builtins.ConnectionError)

    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.tapi.create_errors.put(RpcFailure(grpc.StatusCode.UNAUTHENTICATED, "bad secret-api"))
    client = make_client(monkeypatch, transport)
    with pytest.raises(Exception) as caught:
        client.sandboxes.create(template="ubuntu-24.04", idempotency_key="idem")
    assert len(transport.tapi.create_requests) == 1
    assert "secret-api" not in str(caught.value)


def test_create_local_deadline_exhaustion_uses_creation_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.tapi.create_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "try again"))
    transport.tapi.create_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "try again"))
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    client = Tyto(
        endpoint="https://api.example.test/",
        timeout=0.001,
        max_retries=2,
        _channel_factory=transport.channel_factory,
        _tapi_stub_factory=transport.tapi_stub,
    )

    with pytest.raises(SandboxCreationTimeoutError) as caught:
        client.sandboxes.create(template="ubuntu-24.04", idempotency_key="idem-timeout")

    assert caught.value.idempotency_key == "idem-timeout"


def test_close_after_half_close_sends_cancel_before_ending_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["cat"])
    session.close_stdin()
    session.close()

    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests() == ["start", "cancel"]


def test_close_after_consumed_half_close_cancels_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["cat"])
    session.close_stdin()

    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests() == ["start"]

    session.close()

    assert transport.guest.lazy_stream.cancel_called is True


def test_explicit_cancel_is_idempotent_and_sends_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["sleep", "60"])
    session.cancel()
    session.cancel()

    assert transport.guest.lazy_stream is not None
    assert transport.guest.lazy_stream.collect_requests() == ["start", "cancel"]


def test_write_backpressure_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.lazy = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["cat"], timeout=0.01)
    with pytest.raises(TimeoutError):
        for _ in range(32):
            session.write(b"x")
    session.cancel()


def test_close_unblocks_reader_when_response_queue_is_full(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.flood = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    session = sandbox.exec_stream(["yes"])
    next(session)
    time.sleep(0.2)
    session.close()

    assert transport.guest.flood_stream is not None
    assert session._reader.is_alive() is False


def test_buffered_timeout_sends_cancel_before_request_stream_ends(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    transport.guest.hang = True
    client = make_client(monkeypatch, transport)
    sandbox = client.sandboxes.create(template="ubuntu-24.04")

    with pytest.raises(TimeoutError):
        sandbox.exec(["sleep", "60"], timeout=0.01)

    assert transport.guest.hanging_stream is not None
    assert transport.guest.hanging_stream.collect_requests() == ["start", "cancel"]


def test_packaging_type_and_dependency_contracts(tmp_path: pathlib.Path) -> None:
    root = pathlib.Path(__file__).parents[1]
    assert (root / "src" / "tyto" / "py.typed").is_file()
    data = tomllib.loads((root / "pyproject.toml").read_text())
    dependencies = data["project"]["dependencies"]
    assert "grpcio>=1.83,<2" in dependencies
    assert "protobuf>=7.35.1,<8" in dependencies
    subprocess.run([sys.executable, "-m", "mypy"], cwd=root, check=True)
    dist = tmp_path / "dist"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--wheel",
            "--outdir",
            str(dist),
        ],
        cwd=root,
        check=True,
    )

    # Distribution name "tyto.run" normalizes to "tyto_run" in the wheel
    # filename per PEP 503/427; the importable package inside it is "tyto".
    wheel = next(dist.glob("tyto_run-*.whl"))
    installed = tmp_path / "installed"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(installed),
            str(wheel),
        ],
        check=True,
    )

    # This SDK's package name and the proto's own package name are both
    # "tyto" -- unlike the prior "bonya" package name, there is no longer a
    # foreign-package collision to prove absent, because this SDK now *is*
    # the thing that would collide. What is still worth proving after the
    # rename: the vendored proto module identity is correctly namespaced
    # under this SDK's own "tyto._proto" (not a bare "tyto.runtime.v1...",
    # which would mean the generated code was importing itself as if it were
    # a real top-level "tyto" proto package rather than the vendored copy),
    # and that generated messages still pickle correctly through that
    # identity -- protobuf's module-based deserialization depends on it
    # matching exactly.
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(installed)
    subprocess.run(
        [
            sys.executable,
            "-c",
            "\n".join(
                [
                    "import pathlib",
                    "import pickle",
                    "import sys",
                    "before = list(sys.path)",
                    "import tyto",
                    "from tyto._proto.tyto.runtime.v1 import tapi_pb2",
                    "assert pathlib.Path(tyto.__file__).is_relative_to(pathlib.Path.cwd())",
                    "assert tapi_pb2.__name__ == 'tyto._proto.tyto.runtime.v1.tapi_pb2'",
                    "request = tapi_pb2.TApiGetSandboxRequest(api_key='key', sandbox_id='sbx')",
                    "assert pickle.loads(pickle.dumps(request)) == request",
                    "assert sys.path == before",
                ]
            ),
        ],
        cwd=installed,
        env=environment,
        check=True,
    )
