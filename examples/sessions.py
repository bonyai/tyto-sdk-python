"""Managed console sessions: terminals that outlive your connection.

A session keeps running after you detach, survives the sandbox suspending and
resuming, and replays what it produced while nobody was attached. This is the
difference from `exec_stream`, whose process dies when the stream closes.

    export BONYA_API_KEY=byk_...
    python examples/sessions.py
"""

from __future__ import annotations

import os
import time

from tyto import Tyto, Exit, SessionEnded, SessionOutputDropped, Stdout


def main() -> None:
    api_key = os.environ["BONYA_API_KEY"]
    with Tyto(api_key) as client:
        with client.create_sandbox(template="ubuntu-24.04") as sandbox:
            # Session names match ^[a-z][a-z0-9-]{0,31}$ and are the identity
            # you reattach with later.
            info = client.create_session(
                sandbox.id,
                "worker",
                ["bash", "-c", "for i in $(seq 1 10); do echo tick $i; sleep 1; done"],
                cols=120,
                rows=40,
            )
            print(f"started {info.name}: {info.status.value}")

            # Let it produce output with nobody attached, so the attach below
            # has something to replay.
            time.sleep(3)

            with client.attach_session(sandbox.id, "worker") as stream:
                # Populated before the first event: they describe the bounded
                # replay buffer, not live output.
                print(f"replaying {stream.replayed_bytes} bytes")
                if stream.history_dropped:
                    print("(some older output was dropped)")

                for event in stream:
                    if isinstance(event, Stdout):
                        print(event.data.decode(), end="")
                    elif isinstance(event, Exit):
                        print(f"process exited with {event.exit_code}")
                        break
                    elif isinstance(event, SessionEnded):
                        print(f"attach ended: {event.reason.value}")
                        break
                    elif isinstance(event, SessionOutputDropped):
                        # Reading too slowly. The attach is still live.
                        print(f"[dropped {event.dropped_bytes} bytes]")

            for session in client.list_sessions(sandbox.id).sessions:
                print(f"{session.name}: {session.status.value}")

            client.kill_session(sandbox.id, "worker")


if __name__ == "__main__":
    main()
