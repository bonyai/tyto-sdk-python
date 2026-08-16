"""Stream a command's output as it is produced, instead of buffering it.

Use this over `sandbox.exec` when output is large, the command is long-running,
or you want to react to output before the process finishes.

    export BONYA_API_KEY=byk_...
    python examples/exec_streaming.py
"""

from __future__ import annotations

import os

from tyto import Tyto, Exit, Stderr, Stdout


def main() -> None:
    api_key = os.environ["BONYA_API_KEY"]
    with Tyto(api_key) as client:
        with client.create_sandbox(template="ubuntu-24.04") as sandbox:
            command = ["bash", "-c", "for i in 1 2 3; do echo line $i; sleep 1; done"]

            # Events arrive as they happen: this prints one line per second
            # rather than three lines after three seconds.
            with sandbox.exec_stream(command) as session:
                for event in session:
                    if isinstance(event, Stdout):
                        print(event.data.decode(), end="")
                    elif isinstance(event, Stderr):
                        print(event.data.decode(), end="")
                    elif isinstance(event, Exit):
                        print(f"exited with {event.exit_code}")

            # Streaming stdin: write, then half-close so the process sees EOF.
            with sandbox.exec_stream(["cat"]) as session:
                session.write(b"piped through cat\n")
                session.close_stdin()
                for event in session:
                    if isinstance(event, Stdout):
                        print(event.data.decode(), end="")


if __name__ == "__main__":
    main()
