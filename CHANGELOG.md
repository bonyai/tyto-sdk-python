# Changelog

All notable changes to `bonya-ai-tyto` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The default endpoint is now `https://api.tyto.run`. It was previously a
  private address that was only reachable from inside the development network,
  which meant the published package could not connect anywhere without an
  explicit endpoint. Set `BONYA_ENDPOINT` to point at a self-hosted
  deployment.

### Added

- `SessionExistsError`, replacing `SessionExists`. Every other error in this SDK ends in
  `Error`, and the old name was inconsistent with the other Bonya SDKs.
  `SessionExists` remains as a deprecated alias for the same class, so
  existing code continues to work unchanged; it will be removed in 2.0.
- An `examples/` directory with runnable programs for each capability:
  quickstart, streaming exec, files, managed sessions, previews, and snapshots.
- A `LICENSE` file (MIT), continuous integration, and a `make check` target
  that runs the same checks CI does.

## [1.0.0]

Initial release. The public surface documented in the README is stable within
`1.x`:

- **Sandboxes** — create (with an optional display name), get by id or name,
  list with state and name filters, delete, and resume.
- **Exec** — buffered and streaming, with TTY support and streaming stdin.
- **Managed sessions** — named TTY sessions that outlive the client connection,
  survive suspend/resume, and replay bounded output on reattach.
- **Filesystem** — read, write, upload, download, list, stat, mkdir, remove,
  and move.
- **Previews** — publish a guest port at an HTTPS URL, in token or public mode,
  with a single-use browser entry point for token mode.
- **Snapshots** — create from a running sandbox, and delete.
- **Organization context** — per-client selection of which organization a call
  acts in, defaulting to the caller's personal organization.
