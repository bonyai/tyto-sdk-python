# Tyto Python SDK

Run code in a fast, isolated sandbox — from Python.

[![PyPI](https://img.shields.io/pypi/v/tyto.run)](https://pypi.org/project/tyto.run/)
[![Python](https://img.shields.io/pypi/pyversions/tyto.run)](https://pypi.org/project/tyto.run/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

```bash
python -m pip install tyto.run
```

```python
from tyto import Tyto

with Tyto() as client:                                     # reads BONYA_API_KEY
    with client.sandboxes.create(template="ubuntu-24.04") as sandbox:
        result = sandbox.exec(["echo", "hello"], check=True)
        print(result.stdout)                               # hello
```

That is a real VM: it boots in about a second, runs anything Linux runs, and is
gone when the `with` block exits.

The PyPI distribution is `tyto.run`; the importable package is `tyto`,
typed with `py.typed`. The public surface documented here is stable within
`1.x`.

## Contents

- [Install](#install)
- [Configuration](#configuration)
- [What you can do](#what-you-can-do)
- [Create sandboxes](#create-sandboxes)
- [Get and list](#get-and-list)
- [Delete and cleanup](#delete-and-cleanup)
- [Resume](#resume)
- [Buffered exec](#buffered-exec)
- [Streaming exec](#streaming-exec)
- [TTY exec](#tty-exec)
- [Managed console sessions](#managed-console-sessions)
- [Files](#files)
- [Preview URLs](#preview-urls)
- [Snapshots](#snapshots)
- [Organizations](#organizations-1)
- [Error model](#error-model)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)
- [Development](#development)

## What you can do

| I want to… | Call |
| --- | --- |
| Start a sandbox | `client.sandboxes.create(template=...)` |
| Reconnect to one | `client.sandboxes.get(id)` / `.get_by_name(name)` |
| Find my sandboxes | `client.sandboxes.list()` |
| Run a command | `sandbox.exec(cmd)` |
| Watch output as it happens | `sandbox.exec_stream(cmd)` |
| Keep a terminal alive across reconnects | `sandbox.sessions.create(...)` / `.attach(...)` |
| Read and write files | `sandbox.files.read/write/upload/download/...` |
| Expose a port to a browser | `sandbox.previews.create(port)` |
| Save state for later | `sandbox.snapshot()` |
| Pause and resume | suspend is automatic; `sandbox.resume()` is explicit |
| See which organizations I belong to | `client.list_organizations()` |
| Act in a specific organization | set `client.organization_id`, or pass `organization_id=` at construction |

Every sandbox operation on `client.sandboxes` also has a flat form directly on
`Client` — `client.create_sandbox(...)`, `client.get_sandbox(id)`,
`client.get_sandbox_by_name(name)`, `client.list_sandboxes()`,
`client.delete_sandbox(id)`, `client.resume_sandbox(id)` — for callers who
would rather call a verb than navigate an attribute. Both spellings are the
same implementation; use whichever reads better at the call site.

Sessions, previews, and snapshots have flat forms too —
`client.create_session(sandbox_id, name, cmd)`, `client.list_sessions(sandbox_id)`,
`client.kill_session(sandbox_id, name)`, `client.attach_session(sandbox_id, name)`,
`client.create_preview(sandbox_id, port)`, `client.list_previews(sandbox_id)`,
`client.delete_preview(sandbox_id, id)`, `client.create_snapshot(sandbox_id)`,
`client.delete_snapshot(sandbox_id, snapshot_id)` — but unlike the
sandbox-collection methods above, each of these needs a resolved `Sandbox` to
call through, so every one does a `get_sandbox()` first and then delegates:
one extra round trip compared to already holding the handle. Prefer
`sandbox.sessions.create(...)` (or the equivalent) when a `Sandbox` is
already in hand, such as right after `create_sandbox()`; reach for the flat
form when all you have is an id.

## Install

```bash
python -m pip install tyto.run
```

Or from a checkout of this repository:

```bash
python -m pip install -e .
```

Requires Python 3.10+, `grpcio>=1.83,<2`, and `protobuf>=7.35.1,<8`. For local
development checks:

```bash
python -m pip install -e '.[dev]'
```

## Configuration

Every setting has an environment-variable fallback, so the common case needs no
arguments at all:

```bash
export BONYA_API_KEY=byk_...
```

```python
from tyto import Tyto

client = Tyto()
```

`api_key` can also be passed as the first positional argument —
`Tyto("byk_...")` and `Tyto(api_key="byk_...")` both work.

| Argument | Environment variable | Default |
| --- | --- | --- |
| `api_key` | `BONYA_API_KEY` | *required* |
| `endpoint` | `BONYA_ENDPOINT` | `https://api.tyto.run` |
| `organization_id` | `BONYA_ORGANIZATION_ID` | your personal organization |
| `ca_bundle` | `BONYA_CA_BUNDLE` | system trust store |
| `timeout` | — | `30` seconds |
| `max_retries` | — | `2` |
| `filesystem_read_limit` | — | 64 MiB |

`api_key` is required. Pass it directly or set `BONYA_API_KEY`.

`endpoint` must be an HTTPS URL. The SDK rejects non-HTTPS URLs, URLs with
userinfo, query strings, fragments, malformed ports, or no host. Trailing
slashes are normalized. Point it at your own deployment if you self-host.

`ca_bundle` points to a PEM bundle used for private development CAs. If the file
cannot be read, the SDK raises `InvalidRequestError`.

`timeout` is the default per-operation deadline, in seconds. It must be
positive. Buffered and streaming exec calls can override it per call with
`timeout=...`.

`max_retries` controls SDK retries for retryable control-plane operations. It
must be non-negative. The SDK retries gRPC `UNAVAILABLE` for create, get, list,
delete, resume, snapshot create, and snapshot delete while preserving the same
request and idempotency key where one exists. Exec calls are not retried, except
for one capability refresh when the SDK can prove an exec capability token is
expired before responses start. Filesystem calls are not retried on transport
unavailability; they may refresh a rejected filesystem capability once.

`filesystem_read_limit` caps bytes buffered by `sandbox.files.read()`. It must
be a non-negative integer and defaults to 64 MiB.

Close clients when done — or use `with`, which does it for you:

```python
client = Tyto()
try:
    sandbox = client.sandboxes.create(template="ubuntu-24.04")
finally:
    client.close()
```

Calling SDK methods after `client.close()` raises `InvalidRequestError`.

### Organizations

`organization_id` selects which organization the client's calls act on. When it
is omitted, the server resolves the call against your **personal
organization** — the deterministic fallback every account has. An API key
belongs to a user, not to an organization, so one key works across every
organization you belong to; `organization_id` is how you say which one a given
call means.

`client.list_organizations()` returns every organization the caller belongs
to, including their personal one — see [Organizations](#organizations-1) for
the full method and the `Organization` fields it returns. `organization_id`
is also a settable property: assigning to it changes which organization
subsequent calls run against, effective immediately, and rejects an empty
value the same way the constructor argument does.

The REST equivalent is the `X-Bonya-Organization-ID` header. The SDK sends it as
`bonya-organization-id` metadata on control-plane calls only. Exec, filesystem,
and session calls go straight to the sandbox and are authorized by its
capability token, so they carry no organization context.

An empty value is an error rather than a silent fallback: `organization_id=""`,
or `BONYA_ORGANIZATION_ID` set to an empty string, raises `InvalidRequestError`.
In CI this variable is usually written as an expansion of another one, and
quietly running every job against someone's personal organization is a worse
outcome than failing at startup.

Naming an organization you do not belong to is a not-found error, identical to
naming one that does not exist. The server never confirms that someone else's
organization is real.

**In CI, always set it explicitly.** Relying on the personal-organization
fallback means the job's sandboxes land wherever the credential's owner happens
to have their personal tenant, which is rarely what a shared pipeline wants.

```yaml
# .github/workflows/integration.yml
env:
  BONYA_API_KEY: ${{ secrets.BONYA_API_KEY }}
  BONYA_ENDPOINT: https://api.tyto.run
  BONYA_ORGANIZATION_ID: ${{ vars.BONYA_ORGANIZATION_ID }}
```

```python
# Both values come from the environment; neither is defaulted away.
with Tyto() as client:
    with client.sandboxes.create(template="ubuntu-24.04") as sandbox:
        result = sandbox.exec(["pytest", "-q"], check=True)
        print(result.stdout, end="")
```

## Create Sandboxes

```python
from tyto import Tyto, Wait

with Tyto(api_key="BONYA_API_KEY", endpoint="https://api.tyto.run") as client:
    sandbox = client.sandboxes.create(
        template="ubuntu-24.04",
        version=None,
        wait=Wait.READY,
        idempotency_key="create-job-123",
    )
```

`client.sandboxes.create(...)` returns a `Sandbox`.

Parameters:

- `template: str` is required and must be non-empty.
- `version: str | None = None` uses the server's default template version when
  omitted.
- `wait: Wait | "ready" | "none" = Wait.READY` controls when create returns.
- `idempotency_key: str | None = None` is sent to the service. If omitted, the
  SDK generates one and reuses it for create transport retries.

Wait modes:

- `Wait.READY` or `"ready"` asks the service to return a running sandbox. The
  returned handle has `last_observed_status == Status.RUNNING`.
- `Wait.NONE` or `"none"` returns after the service accepts the request. The
  returned handle has `last_observed_status == Status.CREATING`.

If create exhausts its deadline, the SDK raises
`SandboxCreationTimeoutError`. The error carries the create `idempotency_key` so
you can decide whether to retry or inspect server state.

Sandbox fields:

```python
print(sandbox.id)
print(sandbox.operation_id)
print(sandbox.template)
print(sandbox.version)
print(sandbox.last_observed_status.value)
```

## Get And List

Reconnect to an existing sandbox by ID:

```python
with Tyto(api_key="BONYA_API_KEY", endpoint="https://api.tyto.run") as client:
    sandbox = client.sandboxes.get("sbx_123")
    result = sandbox.exec("printf reconnected", check=True)
    print(result.stdout)
```

`get(sandbox_id)` requires a non-empty ID and returns a usable `Sandbox` handle.
It does not explicitly resume the sandbox. Exec and filesystem operations are
the user activity that wakes a suspended sandbox when the service route supports
automatic wake. If a capability is rejected because it expired, the SDK refreshes
the handle with `get()` once before retrying the operation.

List sandboxes lazily:

```python
from tyto import Status

with Tyto(api_key="BONYA_API_KEY", endpoint="https://api.tyto.run") as client:
    for summary in client.sandboxes.list(
        states=[Status.RUNNING, Status.SUSPENDED],
        limit=20,
    ):
        print(summary.id, summary.last_observed_status.value)
```

`list(...)` returns an iterator of immutable `SandboxSummary` values. It pages
as you iterate. `limit=0` returns an empty iterator without an RPC.

Supported state filters are:

- `Status.CREATING`
- `Status.RUNNING`
- `Status.SUSPENDING`
- `Status.SUSPENDED`
- `Status.RESUMING`
- `Status.FAILED`

`Status.DELETED` is not a valid list filter.

`SandboxSummary` contains `id`, `operation_id`, `template`, `version`,
`last_observed_status`, `failure_code`, and `failure_message`. Summaries do not
include Exec credentials and do not expose `exec`; call `get(summary.id)` for a
usable sandbox handle.

## Delete And Cleanup

```python
result = sandbox.delete()
print(result.sandbox_id)
print(result.already_deleted)
```

`sandbox.delete()` returns `DeleteResult(sandbox_id: str, already_deleted:
bool)`. Calling it again on the same `Sandbox` object is local and idempotent:
the second call returns `already_deleted=True` without another RPC.

Context-manager cleanup calls `delete()`:

```python
with client.sandboxes.create(template="ubuntu-24.04") as sandbox:
    sandbox.exec("printf work", check=True)
```

If both the `with` block and cleanup fail, the cleanup error is attached as the
body exception's `__context__`.

## Resume

Use `resume()` when you want to explicitly resume before running work:

```python
resume = sandbox.resume(idempotency_key="resume-job-123")
print(resume.sandbox_id)
print(resume.lifecycle_operation_id)
print(resume.already_running)

result = sandbox.exec(["printf", "running\n"], check=True)
```

`sandbox.resume(...)` returns `ResumeResult(sandbox_id: str,
lifecycle_operation_id: str, already_running: bool)`. It updates the sandbox's
private Exec endpoint and capability when the service returns fresh values, and
sets `last_observed_status` to `Status.RUNNING`.

`idempotency_key` is optional. If omitted, the SDK generates one and reuses it
for resume transport retries. On ambiguous connection failure, the raised error
carries the idempotency key and the sandbox's local status/capability are left
unchanged.

`resume()` on a failed sandbox raises `SandboxFailedError` locally before an RPC.

Automatic wake is different from explicit resume: `get()` does not call
`resume()`, and ordinary Exec/filesystem calls do not make a public
`ResumeSandbox` RPC from the SDK. They use the sandbox's guest endpoint; the
service may wake the sandbox behind that route.

## Buffered Exec

Use `exec()` for commands with bounded output:

```python
result = sandbox.exec(
    ["python3", "-c", "import os; print(os.environ['MODE'])"],
    env={"MODE": "development"},
    cwd="/workspace",
    timeout=10,
)

print(result.stdout)
print(result.stderr)
print(result.exit_code)
print(result.ok)
```

Signatures:

```text
sandbox.exec(
    command,
    *,
    env=None,
    cwd=None,
    tty=False,
    cols=None,
    rows=None,
    timeout=None,
    check=False,
    input=None,
)
```

Commands can be either:

- `str`: executed as `["/bin/sh", "-c", command]`; the string must be non-empty.
- `Sequence[str]`: executed directly; the sequence must be non-empty and cannot
  contain empty or non-string entries.

`env` overlays string environment variables. Keys must be non-empty strings and
cannot contain `=` or NUL. Values must be strings without NUL. The SDK copies the
mapping before sending.

`cwd` sets the remote working directory. It must be a non-empty string without
NUL. When omitted, the service uses its default working directory.

`input` can be `str`, `bytes`, or `None`. Strings are encoded as UTF-8. The SDK
writes the bytes to stdin and half-closes stdin before collecting output.
Buffered `input` requires `tty=False`.

`exec()` returns `ExecResult`:

```python
result.stdout_bytes      # bytes
result.stderr_bytes      # bytes
result.stdout            # UTF-8 text with replacement for invalid bytes
result.stderr            # UTF-8 text with replacement for invalid bytes
result.exit_code         # int
result.signaled          # bool
result.signal            # int
result.ok                # exit_code == 0 and not signaled
str(result)              # result.stdout
```

`check=True` calls `result.check()` before returning. If the command exits
non-zero or by signal, it raises `ExecFailedError`; the original result is
available as `error.result`.

```python
from tyto import ExecFailedError

try:
    sandbox.exec(["false"], check=True)
except ExecFailedError as error:
    print(error.result.exit_code)
```

`exec()` buffers stdout and stderr in client memory. Use `exec_stream()` for
large output, long-running commands, interactive stdin, or cancellation.

## Streaming Exec

Use `exec_stream()` when you need events as they arrive:

```python
from tyto import Exit, Stderr, Stdout

with sandbox.exec_stream(["bash", "-lc", "echo out; echo err >&2"]) as session:
    for event in session:
        if isinstance(event, Stdout):
            print("stdout:", event.data.decode("utf-8", errors="replace"), end="")
        elif isinstance(event, Stderr):
            print("stderr:", event.data.decode("utf-8", errors="replace"), end="")
        elif isinstance(event, Exit):
            print("exit:", event.exit_code)
```

Signature:

```text
sandbox.exec_stream(
    command,
    *,
    env=None,
    cwd=None,
    tty=False,
    cols=None,
    rows=None,
    timeout=None,
)
```

The command, `env`, `cwd`, `tty`, `cols`, `rows`, and `timeout` rules are the
same as buffered Exec. `exec_stream()` returns an `ExecSession`, which is an
iterator of `Stdout`, `Stderr`, and `Exit` events.

Write streaming stdin as bytes:

```python
with sandbox.exec_stream(["cat"]) as session:
    session.write(b"hello\n")
    session.close_stdin()

    for event in session:
        if isinstance(event, Stdout):
            print(event.data.decode("utf-8"), end="")
```

`session.write(data)` accepts bytes-like objects and raises
`InvalidRequestError` after the session or stdin is closed.
`session.close_stdin()` is idempotent. `session.cancel()` is idempotent and sends
a cancel frame when possible. `session.close()` cancels unfinished sessions;
using `with sandbox.exec_stream(...)` closes on block exit.

The SDK keeps bounded request and response queues. If stdin writes cannot drain
before the session deadline, `write()` raises `TimeoutError`. If the response
queue is full and you close the session, the reader is unblocked and the RPC is
cancelled.

If iteration reaches the session deadline before receiving the next event, the
SDK cancels the remote Exec and raises `TimeoutError`.

## TTY Exec

Set `tty=True` for terminal semantics:

```python
result = sandbox.exec(["bash", "-lc", "stty size; printf done"], tty=True, check=True)
print(result.stdout)
assert result.stderr_bytes == b""
```

In TTY mode stdout and stderr share the terminal stream. The SDK returns terminal
output as stdout and leaves stderr empty. Streaming TTY sessions emit `Stdout`
events for terminal output; they do not emit separate `Stderr` events for the
terminal stream.

Default TTY dimensions are 80 columns by 24 rows. On the wire the SDK sends
`cols=0, rows=0` when you omit both dimensions; the guest runtime interprets
that pair as 80x24.

Provide explicit dimensions by passing both `cols` and `rows`:

```python
with sandbox.exec_stream(["bash"], tty=True, cols=120, rows=40) as session:
    session.write(b"printf 'ready\\n'\n")
    session.resize(cols=100, rows=30)
    session.close_stdin()
    for event in session:
        ...
```

TTY rules:

- `cols` and `rows` must be provided together.
- Each dimension must be an integer from 1 through 512.
- Dimensions require `tty=True`.
- Buffered `input=` is not allowed with `tty=True`; use `exec_stream()` and
  `session.write(...)`.
- `session.resize(cols=..., rows=...)` requires a TTY session, open stdin, and
  an unfinished session.

## Managed Console Sessions

Every `Sandbox` has `sandbox.sessions`, a `SandboxSessions` object for named,
persistent command sessions that outlive the client connection. This is
different from `exec_stream()`: an Exec process dies when its stream closes,
but a managed session keeps running detached, and you can reattach later --
even after the sandbox warm-suspends and resumes -- and replay what it
produced while nobody was watching.

```python
info = sandbox.sessions.create("server", ["bash"], cols=120, rows=40)
print(info.name, info.status)

session = sandbox.sessions.attach("server")
session.write(b"npm run dev\n")
session.resize(cols=140, rows=45)
for event in session:
    ...
session.detach()

for info in sandbox.sessions.list():
    print(info.name, info.status)

sandbox.sessions.kill("server")
```

### Create

```text
sandbox.sessions.create(
    name,
    command,
    *,
    env=None,
    cwd=None,
    cols=0,
    rows=0,
    replace=False,
)
```

`name` must match `^[a-z][a-z0-9-]{0,31}$`. `command` is a non-empty sequence
of non-empty strings -- there is no shell-string convenience like buffered
`exec()`'s. `cols`/`rows` are `0` (server default) or an integer from `1`
through `512`.

Creating over an existing record raises `SessionExistsError` unless
`replace=True`, and even then only a terminal record (exited, killed, or
failed) is replaced. A running or attached session is never replaced by
`create()`; kill it first.

Returns a `SessionInfo`.

### List

```python
result = sandbox.sessions.list()
for info in result:
    print(info.name, info.status)
print(result.sandbox_suspended)
```

`sandbox.sessions.list()` returns a `SessionList`: an immutable sequence of
`SessionInfo` that also carries `sandbox_suspended: bool`. Listing works on a
suspended sandbox without waking it; `sandbox_suspended=True` marks a result
served from the suspend-time snapshot rather than the live guest.

### Attach

```python
session = sandbox.sessions.attach("server", cols=120, rows=40, max_replay_bytes=0)
print(session.info.name, session.replayed_bytes, session.history_dropped)

for event in session:
    ...
```

`attach(name, *, cols=0, rows=0, max_replay_bytes=0)` returns a
`SessionStream`. `session.info`, `session.replayed_bytes`, and
`session.history_dropped` are populated immediately when `attach()` returns,
before you iterate anything: they describe the bounded replay the session
accumulated while detached. `replayed_bytes > 0` means output produced while
nobody was attached is being replayed now; `history_dropped=True` means the
1 MiB replay ring dropped some of the oldest of it. Attaching to a suspended
sandbox's session wakes it, the same way `exec_stream()` does.

Attaching preempts any other attached client for that session: the previous
stream receives a `SessionEnded(SessionEndedReason.TAKEOVER)` event and ends.
A reconnect is never blocked by a half-dead previous connection.

Iterating a `SessionStream` yields:

- `Stdout(data: bytes)`: merged output. Sessions are TTY-only, so there is no
  separate stderr stream.
- `Exit(exit_code, signaled, signal)`: the process exited.
- `SessionEnded(reason: SessionEndedReason)`: the attach ended without the
  process exiting -- `DETACHED` (you called `detach()`) or `TAKEOVER`
  (another client attached instead).
- `SessionOutputDropped(dropped_bytes: int)`: live output was dropped because
  the client was reading too slowly. This does not end the attach.

`session.write(data: bytes)` sends stdin. `session.resize(cols=..., rows=...)`
takes an integer from `1` through `512` for each dimension, the same rule as
TTY Exec resize. `session.detach()` ends the attach gracefully without
touching the process. `session.close()` calls `detach()` if the stream is
still open; `with sandbox.sessions.attach(...) as session:` detaches on block
exit.

### Kill

```python
sandbox.sessions.kill("server", signal="TERM", grace_ms=5000)
```

Signals the session's process group (default `TERM`), escalating to
`SIGKILL` after `grace_ms` if it has not exited. Returns a `SessionInfo`, but
exit info is not guaranteed on that specific response: `kill()` signals and
returns without waiting for the guest to reap the process, so a `list()`
shortly afterward is the reliable way to observe the final exit code.
Killing an unknown name raises `SessionNotFoundError`.

### SessionInfo

```python
info.name                # str
info.command              # tuple[str, ...]
info.working_dir          # str
info.status               # SessionStatus
info.attached             # bool
info.started_at           # timezone-aware datetime
info.last_activity_at     # timezone-aware datetime
info.ended_at             # timezone-aware datetime | None
info.exit                 # Exit | None, set only once terminal
```

`SessionStatus` values are `UNSPECIFIED`, `STARTING`, `IDLE`, `ATTACHED`,
`EXITED`, `KILLED`, and `FAILED`.

### Suspend and resume

A session's process never blocks idle suspend by itself. Only an *attached*
stream does, for as long as it stays open; a quiet, detached session lets
the sandbox warm-suspend, and survives the resume with its process and
replay buffer intact -- the same session, not a new one. Output from a
detached session still counts as activity and defers idle suspend while it
keeps producing it.

### Capability refresh

Session calls transparently reissue an expired capability and retry once,
the same way `exec_stream()` and `sandbox.files` do. Call
`sandbox.reissue_capability()` directly only if you manage tokens yourself.

## Files

Every `Sandbox` has `sandbox.files`, a `SandboxFiles` object:

```python
sandbox.files.write("/workspace/message.txt", "hello\n")
payload = sandbox.files.read("/workspace/message.txt")
print(payload.decode("utf-8"))

sandbox.files.upload("local-input.bin", "/workspace/input.bin")
sandbox.files.download("/workspace/input.bin", "local-output.bin")

entries = sandbox.files.list("/workspace")
info = sandbox.files.stat("/workspace/message.txt")

sandbox.files.mkdir("/workspace/output")
sandbox.files.move("/workspace/message.txt", "/workspace/output/message.txt")
sandbox.files.remove("/workspace/output", recursive=True)
```

Methods:

- `read(path: str) -> bytes`
- `write(path: str, data: bytes | str) -> None`
- `upload(local_path: str | os.PathLike[str], remote_path: str) -> None`
- `download(remote_path: str, local_path: str | os.PathLike[str]) -> None`
- `list(path: str) -> list[FileInfo]`
- `stat(path: str) -> FileInfo`
- `mkdir(path: str) -> None`
- `remove(path: str, recursive: bool = False) -> None`
- `move(source: str, destination: str) -> None`

Remote paths must be non-empty strings without NUL. The SDK accepts absolute or
relative remote paths and leaves interpretation to the guest runtime.

`read()` buffers the entire remote file in memory and returns bytes. It raises
`FilesystemLimitError` before exceeding `filesystem_read_limit`.

`write()` accepts bytes or a string. Strings are encoded as UTF-8. It streams
the payload in 64 KiB chunks, writes through a guest-side temporary file, and
publishes it by replacing the final directory entry. The final path is not
followed when it is a symlink.

`upload()` streams a local file to the remote path in 64 KiB chunks.
`download()` streams a remote file into a hidden temporary file in the
destination directory, fsyncs it, atomically replaces the destination with
`os.replace`, and fsyncs the parent directory where supported. If a read or
write error happens before replacement, the temporary file is removed and the
previous destination is left unchanged.

`list()` returns immediate children sorted by name. It returns a complete list
or raises; it does not return partial results after a remote listing error.

`stat()` returns lstat-style metadata. A final symlink is reported as a symlink
rather than followed.

`move()` is same-filesystem, atomic, and no-overwrite. Cross-filesystem moves
raise `CrossFilesystemMoveError`; destination-exists errors raise
`RemoteFileExistsError`.

`remove(recursive=True)` removes directories recursively. Recursive remove does
not follow symlinks and is not atomic.

`FileInfo` is immutable:

```python
from tyto import FileKind

info = sandbox.files.stat("/workspace/output/message.txt")
print(info.path)
print(info.name)
print(info.kind is FileKind.FILE)
print(info.size)
print(oct(info.mode))
print(info.modified_at)  # timezone-aware datetime
```

`FileKind` values are `FILE`, `DIRECTORY`, `SYMLINK`, and `OTHER`.

## Preview URLs

A preview publishes one guest port at an HTTPS URL a browser can open. The
server must bind a port in 1024-65535; privileged ports are never previewable,
so a guest's ssh can't be handed out by accident.

```python
preview = sandbox.previews.create(3000, name="web")
print(preview.url)          # https://pv-<26 chars>.preview.tyto.run

sandbox.previews.list()
sandbox.previews.delete(preview.id)
```

### Opening one in a browser

A token-mode preview needs the sandbox's capability, and a URL is not a safe
place to leave one. `browser_url()` produces a single-use entry point: the
gateway validates the token, trades it for a host-scoped `HttpOnly` cookie, and
redirects to the same address without it, so no page is ever rendered at a URL
containing the credential.

```python
import webbrowser
webbrowser.open(sandbox.previews.browser_url(preview))
```

Open it once and let the cookie carry the session. **Do not share that URL** —
anyone who receives it holds the sandbox's data-plane capability until it
expires. It raises on a public preview, which has no token to exchange.

### Public previews

```python
public = sandbox.previews.create(8080, auth=PreviewAuth.PUBLIC)
```

`PUBLIC` means exactly that: anyone with the URL reaches the service, with no
credential. The only thing protecting it is the 26 random characters in the
hostname, so treat it as published to the internet. `TOKEN` is the default and
an omitted `auth` never yields a public URL.

### Capability upgrade

`create()` returns a fresh capability and the SDK stores it on the sandbox
automatically, because the preview scope is newer than the token a sandbox is
created with. A token minted before previews existed is otherwise valid and
will be refused by the preview ingress with a permission error that is
deliberately *not* a refresh signal.

If you are holding a capability elsewhere — passed to another process, or kept
across a restart — refresh it explicitly:

```python
sandbox.reissue_capability()
```

### Suspend and wake

Traffic to a preview URL wakes a suspended sandbox and the request is served
once it is running. An idle sandbox therefore costs nothing until a visitor
arrives. The first request after a suspend pays the resume latency; if it takes
too long you get `503` with `Retry-After`, and retrying is the right move.

### Limitations

- **Bind to the interface, not localhost only.** A server listening solely on
  `127.0.0.1` inside the guest is reachable, but one bound to a specific
  non-loopback address may not be. `0.0.0.0` is the reliable choice.
- **Server-Sent Events reconnect.** An SSE stream is not an HTTP upgrade, so it
  is cut at the 120-second request cap. `EventSource` reconnects automatically;
  a long-lived stream that must not break should use a WebSocket.
- **WebSocket auth is cookie or bearer only.** WebSocket clients do not follow
  redirects, so the `?bonya_token=` exchange does not work for them. Open a
  normal page first to obtain the cookie, or send the capability as a bearer
  header.
- **A suspend cuts open connections.** Preview connections deliberately do not
  defer idle-suspend, so an open WebSocket does not keep a sandbox alive. The
  next request wakes it.

## Snapshots

Create a snapshot from a running sandbox:

```python
snapshot = sandbox.snapshot(idempotency_key="snapshot-job-123")
print(snapshot.id)
print(snapshot.source_sandbox_id)
```

`sandbox.snapshot(idempotency_key=None)` returns `Snapshot`. If
`idempotency_key` is omitted, the SDK generates one and reuses it for snapshot
create transport retries. Using the same key for the same source sandbox returns
the same snapshot identity when the service accepts idempotent replay.

Snapshot create requires a running source sandbox. Locally deleted or observed
deleted sandboxes raise `SandboxDeletedError`; failed sandboxes raise
`SandboxFailedError`; suspended sandboxes raise `SandboxSuspendedError`.

Delete snapshots when done:

```python
snapshot.delete()
snapshot.delete()  # local no-op
```

`snapshot.delete()` returns `None` and is idempotent on the same `Snapshot`
object. Snapshots can be deleted after deleting the source sandbox handle. A
snapshot has its own identifier and Python object lifetime does not control
remote snapshot retention.

## Organizations

An api key belongs to a user, not a single organization, so one key works
across every organization that user belongs to. Calls are scoped to whichever
organization is current on the client.

```python
organizations = client.list_organizations()
for org in organizations:
    print(org.id, org.name, org.personal, org.role)

client.organization_id = organizations[0].id
```

`list_organizations()` returns every organization the caller belongs to,
including their personal organization. `Organization.personal` marks that
one — it's the deterministic tenant an omitted organization context resolves
to, and every account has exactly one. TApi stores its name as the literal
string `"personal"`; render that however fits your UI rather than showing it
verbatim.

Assigning to `client.organization_id` changes which organization subsequent
calls run against, effective immediately. An empty value is rejected as an
`InvalidRequestError` rather than silently falling back to the personal
organization. To set it once at construction instead, pass
`organization_id=...` to `Tyto(...)`; reading `client.organization_id` back
returns whatever is currently in effect.

## Error Model

All SDK exceptions inherit from `TytoError`.

```python
from tyto import TytoError

try:
    sandbox = client.sandboxes.get("sbx_missing")
except TytoError as error:
    print(error.message)
    print(error.sandbox_id)
    print(error.operation_id)
    print(error.idempotency_key)
```

Public exceptions:

- `AuthenticationError`: invalid or rejected API key.
- `InvalidRequestError`: invalid local arguments or invalid service response.
- `SandboxNotFoundError`: sandbox missing, deleted, or not visible to the API
  key.
- `SandboxDeletedError`: operation cannot run because the sandbox is deleted.
- `SandboxSuspendedError`: operation reported a suspended sandbox.
- `SandboxBusyError`: service rejected a lifecycle operation as busy.
- `SandboxFailedError`: operation cannot run because the sandbox failed.
- `SandboxCreationFailedError`: create reached a failed terminal state.
- `SandboxCreationTimeoutError`: create deadline expired.
- `CapabilityRejectedError`: guest capability was rejected and could not be
  refreshed.
- `SessionExistsError`: `sessions.create()` targeted a name that already has a
  record and either no `replace=True` was given or the record is not
  terminal.
- `SessionNotFoundError`: `sessions.attach()` or `sessions.kill()` named a
  session that does not exist.
- `FilesystemError`: general filesystem failure.
- `RemoteFileNotFoundError`: remote file or directory missing.
- `RemoteFileExistsError`: remote destination already exists.
- `CrossFilesystemMoveError`: remote move crosses filesystems.
- `FilesystemLimitError`: client or service filesystem size/frame limit.
- `ExecFailedError`: `ExecResult.check()` or `check=True` saw a non-ok result.
- `TimeoutError`: operation deadline expired; also subclasses builtin
  `TimeoutError`.
- `ConnectionError`: retryable transport failure exhausted retries; also
  subclasses builtin `ConnectionError`.
- `ServiceError`: service or unexpected transport failure not covered above.

The SDK redacts API keys, capabilities, selected operation identifiers supplied
to the error mapper, and path-like internal details from mapped service
messages.

Examples:

```python
from tyto import AuthenticationError, SandboxNotFoundError

try:
    client.sandboxes.get("sbx_123")
except AuthenticationError:
    print("check BONYA_API_KEY")
except SandboxNotFoundError:
    print("sandbox does not exist or is not visible")
```

```python
from tyto import FilesystemError, RemoteFileNotFoundError

try:
    sandbox.files.read("/workspace/missing.txt")
except RemoteFileNotFoundError:
    print("missing")
except FilesystemError as error:
    print("filesystem failed:", error.message)
```

```python
from tyto import TimeoutError

try:
    sandbox.exec(["sleep", "60"], timeout=1)
except TimeoutError:
    print("command timed out and was cancelled")
```

## Resource Ownership

Use context managers for deterministic cleanup:

```python
with Tyto(api_key="BONYA_API_KEY", endpoint="https://api.tyto.run") as client:
    with client.sandboxes.create(template="ubuntu-24.04") as sandbox:
        with sandbox.exec_stream(["cat"]) as session:
            session.write(b"hello\n")
            session.close_stdin()
            for event in session:
                ...
```

Ownership rules:

- `Tyto.close()` closes cached channels and is idempotent.
- `Sandbox.__exit__` deletes the sandbox.
- `sandbox.delete()` affects the remote sandbox and updates the local handle to
  `Status.DELETED`.
- `ExecSession.__exit__` closes the session. Closing an unfinished session
  cancels the remote Exec.
- `SessionStream.__exit__` detaches. Closing an unfinished session detaches
  the guest process rather than killing it; the process keeps running.
- `Snapshot.delete()` deletes the remote snapshot identity and is a local no-op
  when repeated on the same object.

For intentionally persistent sandboxes, do not use `with sandbox:`. Store
`sandbox.id`, close the client, and reconnect later with `client.sandboxes.get`.

## Current Limitations

The current Python SDK intentionally exposes only the merged public surface:

- The SDK is synchronous only.
- There is no public `suspend()` method.
- There are no public networking, fork, template-engine, or multi-host APIs.
- Managed sessions are TTY-only; there is no non-TTY managed session mode.
- There is no `sandbox.console()` attach-or-create convenience yet, and no
  multi-attach or collaborative terminal mode -- a new attach always
  preempts the previous one.
- `SandboxSummary` values are metadata only and cannot run Exec.
- Buffered Exec stores stdout and stderr in memory.
- `sandbox.files.read()` stores the full file in memory up to
  `filesystem_read_limit`.
- Filesystem writes, uploads, moves, mkdir, and removes are not retried after
  ambiguous transport errors.
- Remote filesystem path normalization, permissions, symlink traversal inside
  parent directories, and service-side file size limits are guest-runtime
  behavior, not Python SDK behavior.

## Troubleshooting

**`InvalidRequestError: api_key is required`**
Nothing supplied a key. Set `BONYA_API_KEY`, or pass `api_key=...`. If you use
the `tyto` CLI, `tyto login` saves a key — but to a config file the SDK does not
read, so export it for Python:

```bash
export BONYA_API_KEY=byk_...
```

**`AuthenticationError`**
The key reached the server and was rejected. It may be revoked, or belong to a
different deployment than `BONYA_ENDPOINT` points at.

**`InvalidRequestError: endpoint must use https`**
The endpoint is validated before any connection is attempted. `http://` URLs,
bare hostnames, and URLs carrying userinfo, a query string, or a fragment are
all rejected. `https://api.tyto.run` is the shape to match.

**`InvalidRequestError: organization_id must be a non-empty string`**
`BONYA_ORGANIZATION_ID` is set but empty — usually an unset variable expanded in
CI. This is deliberately an error rather than a fallback to your personal
organization; see [Organizations](#organizations).

**`SandboxNotFoundError` on a sandbox you just created**
Most often an organization mismatch: the sandbox was created in one organization
and looked up in another. Sandboxes are not visible across organizations, and a
sandbox in an organization you cannot see is reported the same way as one that
does not exist.

**`SandboxCreationTimeoutError`**
Create did not reach a running state before the deadline. The error carries the
`idempotency_key` it used — retry `create()` with that same key to join the
original creation rather than starting a second sandbox.

**SSL/certificate errors against a private deployment**
Point `ca_bundle` (or `BONYA_CA_BUNDLE`) at the PEM bundle for your CA.

**`FilesystemLimitError` from `files.read()`**
The file is larger than `filesystem_read_limit` (64 MiB by default). Raise the
limit, or use `files.download()`, which streams to disk instead of buffering.

**A command hangs**
`exec()` buffers all output and returns only when the process exits, so a
server or REPL never returns. Use `exec_stream()`, or run it as a
[managed session](#managed-console-sessions).

## Examples

Runnable programs are in [`examples/`](examples):

| File | Shows |
| --- | --- |
| [`quickstart.py`](examples/quickstart.py) | Create, exec, clean up |
| [`exec_streaming.py`](examples/exec_streaming.py) | Streaming output and stdin |
| [`files.py`](examples/files.py) | Read, write, upload, download, list |
| [`sessions.py`](examples/sessions.py) | Persistent terminals and replay |
| [`previews.py`](examples/previews.py) | Publishing a port to a browser |
| [`snapshots.py`](examples/snapshots.py) | Capturing sandbox state |

```bash
export BONYA_API_KEY=byk_...
python examples/quickstart.py
```

## Development

```bash
make check      # mypy (strict) + pytest, the same checks CI runs
make test
make typecheck
```

Regenerate the protobuf/gRPC code. By default this exports the protos from the
Buf Schema Registry, so no checkout of the `compute` repository is needed:

```bash
make proto
```

To generate against unpublished proto changes instead, point at a local
checkout:

```bash
make proto PROTO_DIR=../../compute/proto
```

## See also

- [Go SDK](../go) · [TypeScript SDK](../typescript)
- [`tyto` CLI](../../cli) — the same API from a terminal
