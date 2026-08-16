#!/usr/bin/env python3
"""Rewrite generated Python protobuf imports to be relative.

protoc emits import paths that mirror the ``.proto`` package, so generated
modules import their siblings absolutely::

    from tyto.runtime.v1 import guest_pb2 as tyto_dot_runtime_dot_v1_dot_guest__pb2

The SDK vendors these modules privately under the ``tyto`` package's own
``_proto`` subpackage. The generated ``tyto.*`` these imports name is the
proto package path, not this SDK's top-level package, and is not itself
importable, so those absolute imports cannot resolve as generated. protoc
also records the top-level module name on generated message classes, which
breaks module-based serialization after vendoring.

This runs as part of `make proto`, immediately after `buf generate`, so the
generated tree is reproducible and importable in one step. It is a generation
step, not a manual edit of generated artifacts.

Only sibling imports within the same generated package are rewritten, which is
all protoc emits for this proto set. Anything unexpected is reported and fails
the run rather than being silently left broken.
"""

from __future__ import annotations

import pathlib
import re
import sys

# Matches, at the start of a line:  from <pkg.path> import <name> [as <alias>]
# where <pkg.path> is the proto package mirrored as a Python path.
_IMPORT = re.compile(
    r"^from (?P<package>tyto(?:\.[A-Za-z_][A-Za-z0-9_]*)+) import (?P<rest>.+)$",
    re.MULTILINE,
)
_ABSOLUTE_TYTO_IMPORT = re.compile(r"^(?:from|import) tyto(?:\.|\s)", re.MULTILINE)
_MODULE_IDENTITY = re.compile(
    r"(?P<prefix>_builder\.BuildTopDescriptorsAndMessages\(DESCRIPTOR, )"
    r"'(?P<module>tyto(?:\.[A-Za-z_][A-Za-z0-9_]*)+_pb2)'"
)
_MODULE_IDENTITY_CALL = re.compile(
    r"_builder\.BuildTopDescriptorsAndMessages\(DESCRIPTOR, '(?P<value>[^']*)'"
)
_VENDORED_PACKAGE = "tyto._proto"


def rewrite(root: pathlib.Path) -> tuple[int, int, int]:
    """Rewrite generated imports and module identities under root."""
    files_changed = 0
    imports_rewritten = 0
    identities_rewritten = 0
    problems: list[str] = []

    for path in sorted(root.rglob("*.py")) + sorted(root.rglob("*.pyi")):
        original = path.read_text()
        # The generated module's own package path, relative to the vendored root,
        # e.g. tyto/runtime/v1 -> tyto.runtime.v1
        own_package = ".".join(path.relative_to(root).parts[:-1])

        def replace(match: re.Match[str]) -> str:
            package = match.group("package")
            if package != own_package:
                problems.append(
                    f"{path.relative_to(root)}: import from {package!r} is not a "
                    f"sibling of {own_package!r}; this script only rewrites "
                    f"sibling imports"
                )
                return match.group(0)
            return f"from . import {match.group('rest')}"

        rewritten, import_count = _IMPORT.subn(replace, original)

        expected_module = f"{own_package}.{path.stem}"

        def replace_identity(match: re.Match[str]) -> str:
            module = match.group("module")
            if module != expected_module:
                problems.append(
                    f"{path.relative_to(root)}: generated module identity "
                    f"{module!r} does not match {expected_module!r}"
                )
                return match.group(0)
            return f"{match.group('prefix')}'{_VENDORED_PACKAGE}.{module}'"

        rewritten, identity_count = _MODULE_IDENTITY.subn(replace_identity, rewritten)
        if _ABSOLUTE_TYTO_IMPORT.search(rewritten):
            problems.append(
                f"{path.relative_to(root)}: generated absolute tyto import remains"
            )
        # Checked against the actual rewritten value, not a regex re-deriving
        # what "rewritten" should look like: a substring match on the literal
        # "tyto." would also match a correctly rewritten value whenever
        # _VENDORED_PACKAGE itself starts with "tyto" -- e.g.
        # 'tyto._proto.tyto.runtime.v1.tapi_pb2' still contains "tyto." in its
        # own second segment. Comparing the exact value against the expected
        # prefix has no such false positive, regardless of what
        # _VENDORED_PACKAGE is named.
        for match in _MODULE_IDENTITY_CALL.finditer(rewritten):
            value = match.group("value")
            if value.startswith("tyto.") and not value.startswith(f"{_VENDORED_PACKAGE}."):
                problems.append(
                    f"{path.relative_to(root)}: generated absolute module identity "
                    f"{value!r} remains unrewritten"
                )
        if rewritten != original:
            path.write_text(rewritten)
            files_changed += 1
            imports_rewritten += import_count
            identities_rewritten += identity_count

    if problems:
        for problem in problems:
            print(f"error: {problem}", file=sys.stderr)
        raise SystemExit(1)

    return files_changed, imports_rewritten, identities_rewritten


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: rewrite-python-proto-imports.py <generated-root>")
    root = pathlib.Path(sys.argv[1])
    if not root.is_dir():
        raise SystemExit(f"not a directory: {root}")
    files_changed, imports_rewritten, identities_rewritten = rewrite(root)
    print(
        f"rewrote {imports_rewritten} import(s) and {identities_rewritten} "
        f"module identity value(s) across {files_changed} file(s) in {root}"
    )


if __name__ == "__main__":
    main()
