from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Wait(str, Enum):
    READY = "ready"
    NONE = "none"


class Status(str, Enum):
    CREATING = "creating"
    RUNNING = "running"
    SUSPENDING = "suspending"
    SUSPENDED = "suspended"
    RESUMING = "resuming"
    FAILED = "failed"
    DELETED = "deleted"


@dataclass(frozen=True)
class Stdout:
    data: bytes


@dataclass(frozen=True)
class Stderr:
    data: bytes


@dataclass(frozen=True)
class Exit:
    exit_code: int
    signaled: bool = False
    signal: int = 0

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.signaled


ExecEvent = Stdout | Stderr | Exit
