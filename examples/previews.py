"""Publish a guest port at an HTTPS URL a browser can open.

    export BONYA_API_KEY=byk_...
    python examples/previews.py
"""

from __future__ import annotations

import os

from tyto import Tyto, PreviewAuth


def main() -> None:
    api_key = os.environ["BONYA_API_KEY"]
    with Tyto(api_key) as client:
        with client.create_sandbox(template="ubuntu-24.04") as sandbox:
            client.create_session(
                sandbox.id,
                "web",
                ["python3", "-m", "http.server", "3000"],
            )

            # Ports must be 1024-65535; privileged ports are never previewable.
            # TOKEN is the default, and an omitted auth never yields a public
            # URL.
            preview = client.create_preview(sandbox.id, 3000, name="web")
            print(f"preview: {preview.url}")

            # A token-mode URL needs the sandbox's capability, and a URL is not
            # a safe place to leave one. browser_url mints a single-use entry
            # point: the gateway validates the token, swaps it for an HttpOnly
            # cookie, and redirects to the same address without it.
            #
            # Open it once and let the cookie carry the session. Do not share
            # it -- whoever holds it holds the sandbox's capability. There is
            # no flat form for this: it is a local computation, not an RPC.
            print(f"open once: {sandbox.previews.browser_url(preview)}")

            for existing in client.list_previews(sandbox.id):
                print(f"{existing.id} :{existing.port} {existing.auth.value}")

            # PUBLIC means exactly that: no credential at all.
            public = client.create_preview(sandbox.id, 8080, auth=PreviewAuth.PUBLIC)
            print(f"public (anyone with this URL): {public.url}")

            client.delete_preview(sandbox.id, preview.id)
            client.delete_preview(sandbox.id, public.id)


if __name__ == "__main__":
    main()
