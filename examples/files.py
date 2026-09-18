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
        with client.create_sandbox(template="bonya-dev") as sandbox:
            sandbox.write_file("/workspace/greeting.txt", "hello\n")
            print(sandbox.read_file("/workspace/greeting.txt").decode(), end="")

            sandbox.mkdir_file("/workspace/output")
            sandbox.move_file("/workspace/greeting.txt", "/workspace/output/greeting.txt")

            for entry in sandbox.list_files("/workspace/output"):
                kind = "dir " if entry.kind is FileKind.DIRECTORY else "file"
                print(f"{kind} {entry.name} ({entry.size} bytes)")

            info = sandbox.stat_file("/workspace/output/greeting.txt")
            print(f"mode {info.mode:04o}, modified {info.modified_at}")

            # upload_file and download_file stream in chunks, so file size is
            # bounded by disk rather than by memory. read_file buffers, and
            # is capped by the client's filesystem_read_limit.
            sandbox.upload_file(__file__, "/workspace/output/example.py")
            sandbox.download_file("/workspace/output/example.py", "/tmp/roundtrip.py")

            sandbox.remove_file("/workspace/output", recursive=True)


if __name__ == "__main__":
    main()
