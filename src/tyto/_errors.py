from __future__ import annotations

import builtins
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._sandbox import ExecResult


class TytoError(Exception):
    """Base class for SDK errors."""

    def __init__(
        self,
        message: str,
        *,
        sandbox_id: str | None = None,
        operation_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.sandbox_id = sandbox_id
        self.operation_id = operation_id
        self.idempotency_key = idempotency_key


# BonyaError was this class's name in 1.0, from when the SDK's package and
# client were both named Bonya. The alias keeps existing code working --
# it is the same class object, so `except BonyaError` still catches
# everything `except TytoError` does -- and will be removed in 2.0.
BonyaError = TytoError


class AuthenticationError(TytoError):
    pass


class InvalidRequestError(TytoError):
    pass


class SandboxNotFoundError(TytoError):
    pass


class SessionExistsError(TytoError):
    pass


# SessionExists was this class's original name, and is the one shipped in
# 1.0. It is the only error in this package that did not end in "Error",
# which made it the odd one out in every except-clause and inconsistent with
# the Go SDK's SessionExistsError. The alias keeps existing code working --
# it is the same class object, so `except SessionExists` still catches what
# `raise SessionExistsError` raises -- and will be removed in 2.0.
SessionExists = SessionExistsError


class SessionNotFoundError(TytoError):
    pass


class SandboxDeletedError(TytoError):
    pass


class SandboxSuspendedError(TytoError):
    pass


class SandboxBusyError(TytoError):
    pass


class SandboxFailedError(TytoError):
    pass


class SandboxCreationFailedError(TytoError):
    pass


class SandboxCreationTimeoutError(TytoError):
    pass


class CapabilityRejectedError(TytoError):
    pass


class FilesystemError(TytoError):
    pass


class RemoteFileNotFoundError(FilesystemError):
    pass


class RemoteFileExistsError(FilesystemError):
    pass


class CrossFilesystemMoveError(FilesystemError):
    pass


class FilesystemLimitError(FilesystemError):
    pass


class ExecFailedError(TytoError):
    def __init__(self, message: str, *, result: ExecResult) -> None:
        super().__init__(message, sandbox_id=result.sandbox_id)
        self.result = result


class TimeoutError(TytoError, builtins.TimeoutError):
    pass


class ConnectionError(TytoError, builtins.ConnectionError):
    pass


class ServiceError(TytoError):
    pass
