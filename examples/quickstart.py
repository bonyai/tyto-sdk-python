"""Create a sandbox, run a command in it, and clean up.

Run it with:

    export BONYA_API_KEY=byk_...
    python examples/quickstart.py
"""

from __future__ import annotations

import os

from tyto import Tyto


def main() -> None:
    # api_key falls back to BONYA_API_KEY and endpoint to BONYA_ENDPOINT, so
    # neither has to be passed explicitly. Both are spelled out here to show
    # where they come from.
    with Tyto(
        api_key=os.environ["BONYA_API_KEY"],
        endpoint=os.environ.get("BONYA_ENDPOINT", "https://api.tyto.run"),
    ) as client:
        # `with` on the sandbox deletes it on the way out, including if the
        # body raises. Drop it for a sandbox meant to outlive the script.
        with client.create_sandbox(template="ubuntu-24.04") as sandbox:
            print(f"created {sandbox.name} ({sandbox.id})")

            result = sandbox.exec(["echo", "hello from tyto"], check=True)
            print(result.stdout, end="")


if __name__ == "__main__":
    main()
