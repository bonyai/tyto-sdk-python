"""Capture a running sandbox's state as a snapshot.

    export BONYA_API_KEY=byk_...
    python examples/snapshots.py
"""

from __future__ import annotations

import os

from tyto import Tyto


def main() -> None:
    api_key = os.environ["BONYA_API_KEY"]
    with Tyto(api_key) as client:
        # No `with` on the sandbox here: snapshot identities outlive their
        # source, so this one is deleted explicitly at the end to show the
        # ordering is a choice rather than a requirement.
        sandbox = client.create_sandbox(template="ubuntu-24.04", name="snapshot-source")
        try:
            sandbox.files.write("/workspace/state.txt", "captured\n")

            # Snapshot create requires a running source. Suspended, failed, and
            # deleted sandboxes each raise their own error rather than a
            # generic one.
            #
            # Passing an idempotency_key makes a retry return the same snapshot
            # instead of minting a second one.
            snapshot = client.create_snapshot(sandbox.id, idempotency_key="example-snapshot-1")
            print(f"snapshot {snapshot.id} from {snapshot.source_sandbox_id}")

            snapshot.delete()
            snapshot.delete()  # idempotent: a local no-op the second time
        finally:
            sandbox.delete()


if __name__ == "__main__":
    main()
