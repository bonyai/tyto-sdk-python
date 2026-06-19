# tyto

Python SDK for the [Tyto API](https://api.tyto.run) — manage nests, sessions, files, previews, snapshots, and keepalive holds.

## Install

```bash
pip install tyto
```

## Auth

Export your API key:

```bash
export TYTO_API_KEY=your_key_here
```

Or pass it directly:

```python
from tyto import Tyto
tyto = Tyto(api_key="your_key_here")
```

## Usage

```python
import os
from tyto import Tyto

tyto = Tyto(api_key=os.environ["TYTO_API_KEY"])

# Who am I?
me = tyto.me()
print(me.email)

# Create a nest
nest = tyto.create(name="my-nest", template="ubuntu-24-dev")

# Upload / download a file or directory
nest.put("./hello.txt", "hello.txt")
nest.get("hello.txt", "./hello.downloaded.txt")

# Create a session and attach over WebSocket
session = nest.create_session(argv=["bash", "-lc", "echo hi"], tty=False)
with session.attach() as ws:
    for message in ws:
        print(message)

# Open raw WebSocket connections
with nest.exec() as ws: ...
with nest.console() as ws: ...

# Create a preview
preview = nest.create_preview(port=3000, auth="private")
print(preview.url)

# Snapshot, fork, restore
snap = nest.create_snapshot(name="v1")
fork = nest.fork(name="my-fork")
nest.delete_snapshot(snap.id)
# nest.restore(snap.id)

# Keepalive hold
nest.holds.put("ci", ttl="30m", reason="CI job")
nest.holds.heartbeat("ci", ttl="30m")
nest.holds.delete("ci")

# Lifecycle
nest.stop()
nest.start()
nest.wake(reason="wakeup")
nest.delete()
```

## Resources

| Resource | Description |
|---|---|
| `tyto.create()` | Create a nest |
| `tyto.nests` | Create / list / get nests |
| `tyto.previews` | Inspect / revoke previews by ID |
| `tyto.auth` | CLI browser auth flow |
| `nest.put(local, remote)` | Upload a file or directory |
| `nest.get(remote, local)` | Download a file or directory |
| `nest.create_session()` | Create a managed session |
| `nest.create_preview()` | Create a preview URL |
| `nest.create_snapshot()` | Create a snapshot |
| `nest.delete_snapshot()` | Delete a snapshot |
| `nest.fs` | Low-level file upload / download |
| `nest.sessions` | Managed sessions (create / list) |
| `nest.previews` | Previews scoped to the nest |
| `nest.snapshots` | Snapshots + fork/restore |
| `nest.holds` | Keepalive holds |
| `session.attach()` | WebSocket stream for a session |
| `nest.console()` | Interactive shell WebSocket |
| `nest.exec()` | Command WebSocket |

## Configuration

| Parameter | Env var | Default |
|---|---|---|
| `api_key` | `TYTO_API_KEY` | — (required) |
| `api_url` | `TYTO_API_URL` | `https://api.tyto.run` |

## Examples

```bash
python examples/quickstart.py
python examples/files.py
python examples/preview.py
python examples/snapshot_fork.py
python examples/websocket_exec.py
```

## Context manager

The `Tyto` client can be used as a context manager to ensure the underlying HTTP connection pool is closed:

```python
with Tyto() as tyto:
    nests = tyto.nests.list()
```
