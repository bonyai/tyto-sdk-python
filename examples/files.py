"""Read and write files inside a sandbox.

    export BONYA_API_KEY=byk_...
    python examples/files.py
"""

from __future__ import annotations

import os

from tyto import Tyto, FileKind


def main() -> None:
    api_key = os.environ["BONYA_API_KEY"]
    with Tyto(api_key) as client:
        with client.create_sandbox(template="ubuntu-24.04") as sandbox:
            files = sandbox.files

            files.write("/workspace/greeting.txt", "hello\n")
            print(files.read("/workspace/greeting.txt").decode(), end="")

            files.mkdir("/workspace/output")
            files.move("/workspace/greeting.txt", "/workspace/output/greeting.txt")

            for entry in files.list("/workspace/output"):
                kind = "dir " if entry.kind is FileKind.DIRECTORY else "file"
                print(f"{kind} {entry.name} ({entry.size} bytes)")

            info = files.stat("/workspace/output/greeting.txt")
            print(f"mode {info.mode:04o}, modified {info.modified_at}")

            # Upload and download stream in chunks, so file size is bounded by
            # disk rather than by memory. `read` buffers, and is capped by the
            # client's filesystem_read_limit.
            files.upload(__file__, "/workspace/output/example.py")
            files.download("/workspace/output/example.py", "/tmp/roundtrip.py")

            files.remove("/workspace/output", recursive=True)


if __name__ == "__main__":
    main()
