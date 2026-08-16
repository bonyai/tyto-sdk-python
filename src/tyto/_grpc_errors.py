from __future__ import annotations

from collections.abc import Sequence

import grpc

from ._errors import (
    AuthenticationError,
    TytoError,
    CapabilityRejectedError,
    ConnectionError,
    CrossFilesystemMoveError,
    FilesystemError,
    FilesystemLimitError,
    InvalidRequestError,
    RemoteFileExistsError,
    RemoteFileNotFoundError,
    SandboxCreationFailedError,
    SandboxCreationTimeoutError,
    SandboxBusyError,
    SandboxDeletedError,
    SandboxFailedError,
    SandboxSuspendedError,
    SandboxNotFoundError,
    ServiceError,
    SessionExistsError,
    SessionNotFoundError,
    TimeoutError,
)
from ._transport import sanitize_message

RETRYABLE_CODES = {grpc.StatusCode.UNAVAILABLE}
FILESYSTEM_CAPABILITY_REJECTION_MESSAGES = {
    "filesystem capability rejected",
    "filesystem capability sandbox binding rejected",
}


def is_retryable_transport_error(error: BaseException) -> bool:
    return isinstance(error, grpc.RpcError) and error.code() in RETRYABLE_CODES


def map_rpc_error(
    error: BaseException,
    *,
    secrets: Sequence[str],
    sandbox_id: str | None = None,
    operation_id: str | None = None,
    idempotency_key: str | None = None,
    create: bool = False,
    exec_rpc: bool = False,
    filesystem_rpc: bool = False,
    session_rpc: bool = False,
) -> TytoError:
    if isinstance(error, TytoError):
        return error
    if not isinstance(error, grpc.RpcError):
        return ServiceError(
            sanitize_message(error, list(secrets)),
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
        )

    code = error.code()
    details = sanitize_message(error.details() or code.name, list(secrets))

    if filesystem_rpc and code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return FilesystemError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if filesystem_rpc and code == grpc.StatusCode.UNAVAILABLE:
        return FilesystemError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        error_cls: type[TytoError]
        error_cls = SandboxCreationTimeoutError if create else TimeoutError
        return error_cls(
            details,
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
        )
    if code == grpc.StatusCode.UNAVAILABLE:
        return ConnectionError(
            details,
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
        )
    if code == grpc.StatusCode.UNAUTHENTICATED:
        return AuthenticationError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return InvalidRequestError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.NOT_FOUND and filesystem_rpc:
        return RemoteFileNotFoundError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.NOT_FOUND and session_rpc:
        return SessionNotFoundError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.NOT_FOUND:
        return SandboxNotFoundError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.ALREADY_EXISTS and filesystem_rpc:
        return RemoteFileExistsError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.ALREADY_EXISTS and session_rpc:
        return SessionExistsError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.PERMISSION_DENIED and exec_rpc:
        return CapabilityRejectedError(
            "exec capability was rejected; capability refresh/reconnect is unavailable in this SDK version",
            sandbox_id=sandbox_id,
            operation_id=operation_id,
        )
    if (
        code == grpc.StatusCode.PERMISSION_DENIED
        and filesystem_rpc
        and details in FILESYSTEM_CAPABILITY_REJECTION_MESSAGES
    ):
        return CapabilityRejectedError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.PERMISSION_DENIED and filesystem_rpc:
        return FilesystemError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.PERMISSION_DENIED and session_rpc:
        return CapabilityRejectedError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.FAILED_PRECONDITION and "sandbox_deleted" in details:
        return SandboxDeletedError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.FAILED_PRECONDITION and "sandbox_suspended" in details:
        return SandboxSuspendedError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.FAILED_PRECONDITION and "sandbox_failed" in details:
        return SandboxFailedError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.ABORTED and filesystem_rpc:
        return FilesystemError(
            details,
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
        )
    if code == grpc.StatusCode.ABORTED:
        return SandboxBusyError(
            details,
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
        )
    if code == grpc.StatusCode.FAILED_PRECONDITION and create:
        return SandboxCreationFailedError(
            details,
            sandbox_id=sandbox_id,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
        )
    if code == grpc.StatusCode.FAILED_PRECONDITION and exec_rpc:
        return ServiceError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.FAILED_PRECONDITION and filesystem_rpc and "cross_filesystem_move" in details:
        return CrossFilesystemMoveError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if code == grpc.StatusCode.RESOURCE_EXHAUSTED and filesystem_rpc:
        return FilesystemLimitError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    if filesystem_rpc:
        return FilesystemError(details, sandbox_id=sandbox_id, operation_id=operation_id)
    return ServiceError(
        details,
        sandbox_id=sandbox_id,
        operation_id=operation_id,
        idempotency_key=idempotency_key,
    )
