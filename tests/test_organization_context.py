from __future__ import annotations

import warnings
from typing import Any

import pytest

from tyto import Tyto, InvalidRequestError, OrganizationContextNotEnforcedWarning, Wait
from tyto._client import ORGANIZATION_METADATA_KEY

from test_contract import FakeGuest, FakeTapi, FakeTransport, make_client


# Org context is one optional constructor parameter that must reach every
# TApi RPC and no guest RPC. The tests below pin both halves plus the
# resolution rules, because "sends it exactly when configured" is a claim
# about the whole surface rather than about one call.


class RecordingTapi(FakeTapi):
    """FakeTapi that records the metadata each TApi RPC was called with.

    Recording ``None`` for a call that passed no ``metadata`` keyword at all
    is the point: an unconfigured client must not merely send empty
    metadata, it must leave the call shape exactly as it was before org
    context existed. That is also what keeps the pre-existing suite -- whose
    fakes accept no ``metadata`` argument -- passing untouched.
    """

    def __init__(self) -> None:
        super().__init__()
        self.metadata_by_method: dict[str, Any] = {}

    def __getattribute__(self, name: str) -> Any:
        attribute = super().__getattribute__(name)
        if name.startswith("_") or not callable(attribute) or not name[0].isupper():
            return attribute

        def recording(request: Any, **kwargs: Any) -> Any:
            metadata = kwargs.pop("metadata", None)
            super(RecordingTapi, self).__getattribute__("metadata_by_method")[name] = metadata
            return attribute(request, **kwargs)

        return recording


def make_org_client(
    monkeypatch: pytest.MonkeyPatch,
    transport: FakeTransport,
    organization_id: str | None,
) -> Tyto:
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    monkeypatch.delenv("BONYA_ORGANIZATION_ID", raising=False)
    # These tests are about the metadata, not the not-enforced warning; the
    # warning has its own two tests below.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", OrganizationContextNotEnforcedWarning)
        return Tyto(
            endpoint="https://api.example.test/",
            timeout=2,
            max_retries=2,
            organization_id=organization_id,
            _channel_factory=transport.channel_factory,
            _tapi_stub_factory=transport.tapi_stub,
            _guest_stub_factory=transport.guest_stub,
        )


def drive_every_tapi_rpc(client: Tyto) -> None:
    """Exercise every TApi RPC the SDK knows how to make."""
    sandbox = client.sandboxes.create(
        template="ubuntu-24.04", wait=Wait.NONE, idempotency_key="idem-1"
    )
    client.sandboxes.get("sbx-1")
    list(client.sandboxes.list())
    sandbox.previews.create(3000, name="web")
    list(sandbox.previews.list())
    sandbox.previews.delete("pv-aaaaaaaaaaaaaaaaaaaaaaaaaa")
    snapshot = sandbox.snapshot(idempotency_key="idem-snap")
    snapshot.delete()
    sandbox.reissue_capability()
    sandbox.resume(idempotency_key="idem-resume")
    sandbox.delete()
    client.list_organizations()


EVERY_TAPI_RPC = {
    "Create",
    "GetSandbox",
    "ListSandboxes",
    "CreatePreview",
    "ListPreviews",
    "DeletePreview",
    "CreateSnapshot",
    "DeleteSnapshot",
    "ReissueCapability",
    "ResumeSandbox",
    "DeleteSandbox",
    "ListOrganizations",
}


def test_configured_organization_reaches_every_tapi_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = RecordingTapi()
    transport.guest = FakeGuest()
    client = make_org_client(monkeypatch, transport, "org-1111")

    drive_every_tapi_rpc(client)

    observed = transport.tapi.metadata_by_method
    # Every RPC the SDK makes, not a sample of them: a future TApi call that
    # forgot org context would show up as a missing key here.
    assert set(observed) == EVERY_TAPI_RPC
    for method, metadata in observed.items():
        assert metadata is not None, f"{method} sent no metadata"
        assert (ORGANIZATION_METADATA_KEY, "org-1111") in tuple(metadata), method


def test_unconfigured_client_sends_no_organization_metadata_at_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = FakeTransport()
    transport.tapi = RecordingTapi()
    transport.guest = FakeGuest()
    client = make_org_client(monkeypatch, transport, None)

    drive_every_tapi_rpc(client)

    observed = transport.tapi.metadata_by_method
    assert set(observed) == EVERY_TAPI_RPC
    for method, metadata in observed.items():
        # Not "empty metadata" -- no metadata argument was passed at all.
        assert metadata is None, f"{method} sent {metadata!r}"
    assert client.organization_id is None


def test_organization_context_never_reaches_guest_rpcs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guest data plane is capability-authorized and does not go through
    TApi, so org context has no meaning there -- and the capability metadata
    it does carry must not be disturbed."""
    transport = FakeTransport()
    transport.tapi = RecordingTapi()
    transport.guest = FakeGuest()
    client = make_org_client(monkeypatch, transport, "org-2222")
    sandbox = client.sandboxes.create(template="ubuntu-24.04", wait=Wait.NONE)

    sandbox.exec(["true"])
    keys = [key for key, _ in transport.guest.metadata]
    assert ORGANIZATION_METADATA_KEY not in keys
    # The capability metadata the guest plane does need is untouched.
    assert "bonya-sandbox-id" in keys
    assert "bonya-exec-capability" in keys
    # And the TApi leg of the same session did carry it.
    assert (ORGANIZATION_METADATA_KEY, "org-2222") in tuple(
        transport.tapi.metadata_by_method["Create"]
    )


@pytest.mark.filterwarnings("ignore::tyto.OrganizationContextNotEnforcedWarning")
def test_organization_id_falls_back_to_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    transport.tapi = RecordingTapi()
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    monkeypatch.setenv("BONYA_ORGANIZATION_ID", "org-from-env")
    client = Tyto(
        endpoint="https://api.example.test/",
        _channel_factory=transport.channel_factory,
        _tapi_stub_factory=transport.tapi_stub,
    )
    assert client.organization_id == "org-from-env"
    client.sandboxes.get("sbx-1")
    assert (ORGANIZATION_METADATA_KEY, "org-from-env") in tuple(
        transport.tapi.metadata_by_method["GetSandbox"]
    )

    # An explicit argument wins over the environment, matching every other
    # option on this constructor.
    explicit = Tyto(
        endpoint="https://api.example.test/",
        organization_id="org-explicit",
        _channel_factory=transport.channel_factory,
        _tapi_stub_factory=transport.tapi_stub,
    )
    assert explicit.organization_id == "org-explicit"


def test_blank_organization_id_is_refused_rather_than_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI usually writes this variable as an expansion of another one, so an
    unset upstream value arrives as the empty string. Falling back to the
    personal organization there would run every job against the wrong
    tenant, silently."""
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    for blank in ("", "   ", "\t"):
        monkeypatch.delenv("BONYA_ORGANIZATION_ID", raising=False)
        with pytest.raises(InvalidRequestError):
            Tyto(endpoint="https://api.example.test/", organization_id=blank)
        monkeypatch.setenv("BONYA_ORGANIZATION_ID", blank)
        with pytest.raises(InvalidRequestError):
            Tyto(endpoint="https://api.example.test/")


@pytest.mark.filterwarnings("ignore::tyto.OrganizationContextNotEnforcedWarning")
def test_organization_id_is_trimmed_but_not_shape_checked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The server owns the id's shape and answers a malformed one with 400.
    A client-side pattern would only mean an older SDK refusing ids a newer
    server had begun issuing."""
    monkeypatch.setenv("BONYA_API_KEY", "secret-api")
    monkeypatch.delenv("BONYA_ORGANIZATION_ID", raising=False)
    client = Tyto(endpoint="https://api.example.test/", organization_id="  org-padded  ")
    assert client.organization_id == "org-padded"
    # Not the server's id shape, and deliberately still accepted here.
    assert Tyto(
        endpoint="https://api.example.test/", organization_id="anything"
    ).organization_id == "anything"


def test_organization_context_survives_a_retried_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retries rebuild the stub from the pool, so org context has to come
    from the client rather than from a value captured once at call time."""
    import grpc

    from test_contract import RpcFailure

    transport = FakeTransport()
    transport.tapi = RecordingTapi()
    client = make_org_client(monkeypatch, transport, "org-3333")
    transport.tapi.get_errors.put(RpcFailure(grpc.StatusCode.UNAVAILABLE, "try again"))

    client.sandboxes.get("sbx-1")

    assert (ORGANIZATION_METADATA_KEY, "org-3333") in tuple(
        transport.tapi.metadata_by_method["GetSandbox"]
    )


def test_organization_id_setter_affects_the_next_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """The setter must change what the *next* call sends, including for a
    client that has already made calls -- unlike the Go SDK, _tapi_stub()
    here reads self._organization_id fresh every call rather than baking it
    into a stub built once, so there is no cached-channel staleness to guard
    against, but the behavior itself is still worth pinning."""
    transport = FakeTransport()
    transport.tapi = RecordingTapi()
    client = make_org_client(monkeypatch, transport, "org-before")

    client.sandboxes.get("sbx-1")
    assert (ORGANIZATION_METADATA_KEY, "org-before") in tuple(
        transport.tapi.metadata_by_method["GetSandbox"]
    )

    client.organization_id = "org-after"
    assert client.organization_id == "org-after"

    client.sandboxes.get("sbx-1")
    assert (ORGANIZATION_METADATA_KEY, "org-after") in tuple(
        transport.tapi.metadata_by_method["GetSandbox"]
    )


def test_organization_id_setter_rejects_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport()
    client = make_org_client(monkeypatch, transport, "org-before")

    with pytest.raises(InvalidRequestError):
        client.organization_id = ""
    assert client.organization_id == "org-before"
