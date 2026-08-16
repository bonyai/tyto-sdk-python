from __future__ import annotations

import pytest

from tyto import InvalidRequestError, SandboxNotFoundError, Wait
from tyto._proto.tyto.runtime.v1 import tapi_pb2

from test_contract import FakeTapi, FakeTransport, make_metadata, make_client


def _client(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    return make_client(monkeypatch, transport), transport


def test_create_sends_the_requested_name(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    sandbox = client.sandboxes.create(
        template="ubuntu-24.04", wait=Wait.NONE, idempotency_key="idem-1", name="my-box"
    )

    assert transport.tapi.create_requests[0].name == "my-box"
    assert sandbox.name == "my-box"


def test_create_surfaces_a_generated_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no name given the service generates one, and the SDK has to
    surface it or the caller never learns what their sandbox is called."""
    client, transport = _client(monkeypatch)

    sandbox = client.sandboxes.create(template="ubuntu-24.04", wait=Wait.NONE, idempotency_key="idem-1")

    assert transport.tapi.create_requests[0].name == ""
    assert sandbox.name == "brave-cedar-6268"


def test_list_forwards_the_name_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.list_pages = [
        tapi_pb2.TApiListSandboxesResponse(sandboxes=[make_metadata("sbx-1", name="my-box")])
    ]

    summaries = list(client.sandboxes.list(name="my-box"))

    assert transport.tapi.list_requests[0].name == "my-box"
    assert [s.name for s in summaries] == ["my-box"]


def test_get_by_name_resolves_to_a_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.list_pages = [
        tapi_pb2.TApiListSandboxesResponse(sandboxes=[make_metadata("sbx-1", name="my-box")])
    ]

    sandbox = client.sandboxes.get_by_name("my-box")

    assert sandbox.id == "sbx-1"
    # The name is only used to find the id; the fetch itself is by id.
    assert transport.tapi.get_requests[0].sandbox_id == "sbx-1"


def test_get_by_name_reports_no_match(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.list_pages = [tapi_pb2.TApiListSandboxesResponse(sandboxes=[])]

    with pytest.raises(SandboxNotFoundError):
        client.sandboxes.get_by_name("absent")


def test_get_by_name_refuses_to_guess_between_duplicates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Names are not unique, and silently picking one would let a later
    delete destroy an arbitrary sandbox."""
    client, transport = _client(monkeypatch)
    transport.tapi.list_pages = [
        tapi_pb2.TApiListSandboxesResponse(
            sandboxes=[make_metadata("sbx-1", name="shared"), make_metadata("sbx-2", name="shared")]
        )
    ]

    with pytest.raises(InvalidRequestError):
        client.sandboxes.get_by_name("shared")

    assert transport.tapi.get_requests == []


def test_get_by_name_requires_a_name(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch)

    with pytest.raises(InvalidRequestError):
        client.sandboxes.get_by_name("")
