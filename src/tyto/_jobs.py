from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from ._errors import InvalidRequestError


class Disposition(str, Enum):
    """What happens to a sandbox a job created once the run ends.

    Not meaningful for a job given an existing sandbox.
    """

    DELETE = "delete"
    KEEP = "keep"


class JobRunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TIMED_OUT = "timed_out"
    TERMINATED = "terminated"


class JobRunAction(str, Enum):
    """Something the caller may do to a job run right now, computed server-side."""

    CANCEL = "cancel"
    RERUN = "rerun"
    RETRY = "retry"
    DELETE = "delete"
    DELETE_SANDBOX = "delete_sandbox"


class JobRunTimelineStatus(str, Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class ScheduleOverlap(str, Enum):
    """What a schedule fire does when the previous run is still going."""

    SKIP = "skip"
    BUFFER_ONE = "buffer_one"
    ALLOW_ALL = "allow_all"


class ScheduleAction(str, Enum):
    """Something the caller may do to a job schedule right now, computed server-side."""

    PAUSE = "pause"
    RESUME = "resume"
    TRIGGER = "trigger"
    UPDATE = "update"
    DELETE = "delete"


@dataclass(frozen=True)
class JobScriptSpec:
    """A script to run inline instead of cmd.

    The body travels inline and is size-capped; a script too large for that
    belongs in the template or should fetch itself.
    """

    body: bytes
    interpreter: str = ""
    args: tuple[str, ...] = ()
    filename: str = ""


@dataclass(frozen=True)
class JobSandboxSpec:
    """A new sandbox for a job to create, in place of an existing one."""

    template: str
    version: str = ""
    name: str = ""


@dataclass(frozen=True)
class JobSpec:
    """A job's definition: what to run, and where.

    Exactly one of existing_sandbox_id and new_sandbox is required, and
    exactly one of cmd and script is required.
    """

    existing_sandbox_id: str | None = None
    new_sandbox: JobSandboxSpec | None = None
    cmd: tuple[str, ...] | None = None
    script: JobScriptSpec | None = None
    #: Runs after the sandbox is ready and before cmd/script. A non-zero
    #: exit stops the job.
    pre_run_script: JobScriptSpec | None = None
    env: dict[str, str] | None = None
    path: str = ""
    stdin: bytes = b""
    command_timeout_seconds: int = 0
    run_deadline_seconds: int = 0
    max_output_bytes: int = 0
    disposition: Disposition = Disposition.DELETE
    #: Applies to existing_sandbox_id only. False (the default) makes a
    #: suspended target a failure rather than an implicit resume, which is a
    #: billable action the caller may not have intended.
    resume_if_suspended: bool = False
    name: str = ""


@dataclass(frozen=True)
class JobResult:
    exit_code: int
    signaled: bool
    signal: int
    timed_out: bool
    stdout: bytes
    stderr: bytes
    stdout_truncated: bool
    stderr_truncated: bool


@dataclass(frozen=True)
class JobRun:
    """A job run's status and outcome, without its stored spec or timeline.

    get_job_run returns the fuller JobRunDetail; list_job_runs returns this,
    since assembling a timeline is a history read per run.
    """

    run_id: str
    status: JobRunStatus
    sandbox_id: str
    #: Distinguishes whether this run created sandbox_id or was given it,
    #: which decides whether the caller still owns it after the run.
    created_sandbox: bool
    schedule_id: str
    started_at_unix_nanos: int
    finished_at_unix_nanos: int
    result: JobResult | None
    failure: str
    #: The run finished but could not delete the sandbox it created. That
    #: sandbox is still billed and still visible in list_sandboxes.
    cleanup_failed: bool
    available_actions: tuple[JobRunAction, ...]
    name: str = ""


@dataclass(frozen=True)
class JobRunTimelineEntry:
    """One activity attempt on a job run's timeline.

    E.g. CreateSandbox, WriteScript, ExecCommand, DeleteSandbox.
    """

    name: str
    status: JobRunTimelineStatus
    attempt: int
    started_at_unix_nanos: int
    finished_at_unix_nanos: int
    failure: str


@dataclass(frozen=True)
class JobRunDetail(JobRun):
    """Adds the stored spec and activity timeline to JobRun.

    Only get_job_run returns this.
    """

    spec: JobSpec | None = None
    timeline: tuple[JobRunTimelineEntry, ...] = ()


@dataclass(frozen=True)
class ScheduleSpec:
    """A job schedule's timing.

    Exactly one of cron_expressions, interval_seconds, and run_at_unix_nanos
    is required.
    """

    cron_expressions: tuple[str, ...] | None = None
    interval_seconds: int | None = None
    #: One-shot: a single future calendar time. Refused if in the past.
    run_at_unix_nanos: int | None = None
    #: IANA zone, e.g. "US/Pacific". Empty is UTC.
    time_zone: str = ""
    jitter_seconds: int = 0
    overlap: ScheduleOverlap = ScheduleOverlap.SKIP
    paused: bool = False


@dataclass(frozen=True)
class JobSchedule:
    """A durable cron, interval, or one-shot trigger for a job."""

    schedule_id: str
    schedule: ScheduleSpec | None
    spec: JobSpec | None
    paused: bool
    note: str
    next_run_at_unix_nanos: int
    #: True when this schedule fires exactly once (run_at_unix_nanos).
    one_shot: bool
    #: Present for one-shot schedules; 0 once spent.
    remaining_actions: int
    available_actions: tuple[ScheduleAction, ...]
    created_at_unix_nanos: int
    updated_at_unix_nanos: int
    num_actions: int
    num_actions_skipped_overlap: int
    num_actions_missed_catchup_window: int
    #: The last 10 fires' run ids, oldest first, including manual triggers.
    #: Each is a valid get_job_run id.
    recent_run_ids: tuple[str, ...]
    running_run_ids: tuple[str, ...]


_DISPOSITION_TO_PROTO = {
    Disposition.DELETE: 1,  # TAPI_SANDBOX_DISPOSITION_DELETE
    Disposition.KEEP: 2,  # TAPI_SANDBOX_DISPOSITION_KEEP
}
_DISPOSITION_FROM_PROTO = {v: k for k, v in _DISPOSITION_TO_PROTO.items()}

_JOB_RUN_STATUS_FROM_PROTO = {
    1: JobRunStatus.RUNNING,
    2: JobRunStatus.COMPLETED,
    3: JobRunStatus.FAILED,
    4: JobRunStatus.CANCELED,
    5: JobRunStatus.TIMED_OUT,
    6: JobRunStatus.TERMINATED,
}

_JOB_RUN_ACTION_FROM_PROTO = {
    1: JobRunAction.CANCEL,
    2: JobRunAction.RERUN,
    3: JobRunAction.RETRY,
    4: JobRunAction.DELETE,
    5: JobRunAction.DELETE_SANDBOX,
}

_JOB_RUN_TIMELINE_STATUS_FROM_PROTO = {
    1: JobRunTimelineStatus.SCHEDULED,
    2: JobRunTimelineStatus.RUNNING,
    3: JobRunTimelineStatus.COMPLETED,
    4: JobRunTimelineStatus.FAILED,
    5: JobRunTimelineStatus.CANCELED,
}

_SCHEDULE_OVERLAP_TO_PROTO = {
    ScheduleOverlap.SKIP: 1,  # TAPI_SCHEDULE_OVERLAP_SKIP
    ScheduleOverlap.BUFFER_ONE: 2,  # TAPI_SCHEDULE_OVERLAP_BUFFER_ONE
    ScheduleOverlap.ALLOW_ALL: 3,  # TAPI_SCHEDULE_OVERLAP_ALLOW_ALL
}
_SCHEDULE_OVERLAP_FROM_PROTO = {v: k for k, v in _SCHEDULE_OVERLAP_TO_PROTO.items()}

_SCHEDULE_ACTION_FROM_PROTO = {
    1: ScheduleAction.PAUSE,
    2: ScheduleAction.RESUME,
    3: ScheduleAction.TRIGGER,
    4: ScheduleAction.UPDATE,
    5: ScheduleAction.DELETE,
}


def job_spec_to_proto(spec: JobSpec, *, tapi_pb2: Any, common_pb2: Any) -> Any:
    has_existing = bool(spec.existing_sandbox_id)
    has_new = spec.new_sandbox is not None
    if has_existing and has_new:
        raise InvalidRequestError("exactly one of existing_sandbox_id and new_sandbox is required, not both")
    if not has_existing and not has_new:
        raise InvalidRequestError("exactly one of existing_sandbox_id and new_sandbox is required")

    has_cmd = bool(spec.cmd)
    has_script = spec.script is not None
    if has_cmd and has_script:
        raise InvalidRequestError("exactly one of cmd and script is required, not both")
    if not has_cmd and not has_script:
        raise InvalidRequestError("exactly one of cmd and script is required")

    try:
        disposition_value = _DISPOSITION_TO_PROTO[spec.disposition]
    except KeyError as exc:
        raise InvalidRequestError("disposition must be a valid Disposition value") from exc

    kwargs: dict[str, Any] = dict(
        existing_sandbox_id=spec.existing_sandbox_id or "",
        cmd=list(spec.cmd or []),
        env=dict(spec.env or {}),
        path=spec.path,
        stdin=spec.stdin,
        command_timeout_seconds=spec.command_timeout_seconds,
        run_deadline_seconds=spec.run_deadline_seconds,
        max_output_bytes=spec.max_output_bytes,
        disposition=disposition_value,
        resume_if_suspended=spec.resume_if_suspended,
        name=spec.name,
    )
    if spec.new_sandbox is not None:
        kwargs["new_sandbox"] = tapi_pb2.TApiJobSandboxSpec(
            template=common_pb2.TemplateBinding(
                template_id=spec.new_sandbox.template, version=spec.new_sandbox.version
            ),
            name=spec.new_sandbox.name,
        )
    if spec.script is not None:
        kwargs["script"] = _job_script_spec_to_proto(spec.script, tapi_pb2=tapi_pb2)
    if spec.pre_run_script is not None:
        kwargs["pre_run_script"] = _job_script_spec_to_proto(spec.pre_run_script, tapi_pb2=tapi_pb2)
    return tapi_pb2.TApiJobSpec(**kwargs)


def _job_script_spec_to_proto(spec: JobScriptSpec, *, tapi_pb2: Any) -> Any:
    return tapi_pb2.TApiJobScriptSpec(
        body=spec.body, interpreter=spec.interpreter, args=list(spec.args), filename=spec.filename
    )


def job_spec_from_proto(spec: Any) -> JobSpec | None:
    if spec is None:
        return None
    new_sandbox = None
    if spec.HasField("new_sandbox"):
        ns = spec.new_sandbox
        new_sandbox = JobSandboxSpec(template=ns.template.template_id, version=ns.template.version, name=ns.name)
    return JobSpec(
        existing_sandbox_id=spec.existing_sandbox_id or None,
        new_sandbox=new_sandbox,
        cmd=tuple(spec.cmd) or None,
        script=_job_script_spec_from_proto(spec.script) if spec.HasField("script") else None,
        pre_run_script=_job_script_spec_from_proto(spec.pre_run_script) if spec.HasField("pre_run_script") else None,
        env=dict(spec.env) or None,
        path=spec.path,
        stdin=spec.stdin,
        command_timeout_seconds=spec.command_timeout_seconds,
        run_deadline_seconds=spec.run_deadline_seconds,
        max_output_bytes=spec.max_output_bytes,
        disposition=_DISPOSITION_FROM_PROTO.get(spec.disposition, Disposition.DELETE),
        resume_if_suspended=spec.resume_if_suspended,
        name=spec.name,
    )


def _job_script_spec_from_proto(spec: Any) -> JobScriptSpec:
    return JobScriptSpec(body=spec.body, interpreter=spec.interpreter, args=tuple(spec.args), filename=spec.filename)


def job_result_from_proto(result: Any) -> JobResult | None:
    if result is None:
        return None
    return JobResult(
        exit_code=result.exit_code,
        signaled=result.signaled,
        signal=result.signal,
        timed_out=result.timed_out,
        stdout=result.stdout,
        stderr=result.stderr,
        stdout_truncated=result.stdout_truncated,
        stderr_truncated=result.stderr_truncated,
    )


def job_run_from_proto(run: Any) -> JobRun:
    return JobRun(
        run_id=run.run_id,
        status=_JOB_RUN_STATUS_FROM_PROTO.get(run.status, JobRunStatus.RUNNING),
        sandbox_id=run.sandbox_id,
        created_sandbox=run.created_sandbox,
        schedule_id=run.schedule_id,
        started_at_unix_nanos=run.started_at_unix_nanos,
        finished_at_unix_nanos=run.finished_at_unix_nanos,
        result=job_result_from_proto(run.result) if run.HasField("result") else None,
        failure=run.failure,
        cleanup_failed=run.cleanup_failed,
        available_actions=tuple(
            _JOB_RUN_ACTION_FROM_PROTO[a] for a in run.available_actions if a in _JOB_RUN_ACTION_FROM_PROTO
        ),
        name=run.name,
    )


def job_run_timeline_from_proto(entries: Any) -> tuple[JobRunTimelineEntry, ...]:
    return tuple(
        JobRunTimelineEntry(
            name=e.name,
            status=_JOB_RUN_TIMELINE_STATUS_FROM_PROTO.get(e.status, JobRunTimelineStatus.SCHEDULED),
            attempt=e.attempt,
            started_at_unix_nanos=e.started_at_unix_nanos,
            finished_at_unix_nanos=e.finished_at_unix_nanos,
            failure=e.failure,
        )
        for e in entries
    )


def job_run_detail_from_proto(detail: Any) -> JobRunDetail:
    run = job_run_from_proto(detail.run)
    return JobRunDetail(
        run_id=run.run_id,
        status=run.status,
        sandbox_id=run.sandbox_id,
        created_sandbox=run.created_sandbox,
        schedule_id=run.schedule_id,
        started_at_unix_nanos=run.started_at_unix_nanos,
        finished_at_unix_nanos=run.finished_at_unix_nanos,
        result=run.result,
        failure=run.failure,
        cleanup_failed=run.cleanup_failed,
        available_actions=run.available_actions,
        name=run.name,
        spec=job_spec_from_proto(detail.spec) if detail.HasField("spec") else None,
        timeline=job_run_timeline_from_proto(detail.timeline),
    )


def schedule_spec_to_proto(spec: ScheduleSpec, *, tapi_pb2: Any) -> Any:
    set_count = sum(
        1
        for value in (spec.cron_expressions, spec.interval_seconds, spec.run_at_unix_nanos)
        if value
    )
    if set_count != 1:
        raise InvalidRequestError(
            "exactly one of cron_expressions, interval_seconds, and run_at_unix_nanos is required"
        )
    try:
        overlap_value = _SCHEDULE_OVERLAP_TO_PROTO[spec.overlap]
    except KeyError as exc:
        raise InvalidRequestError("overlap must be a valid ScheduleOverlap value") from exc

    return tapi_pb2.TApiScheduleSpec(
        cron_expressions=list(spec.cron_expressions or []),
        interval_seconds=spec.interval_seconds or 0,
        run_at_unix_nanos=spec.run_at_unix_nanos or 0,
        time_zone=spec.time_zone,
        jitter_seconds=spec.jitter_seconds,
        overlap=overlap_value,
        paused=spec.paused,
    )


def schedule_spec_from_proto(spec: Any) -> ScheduleSpec | None:
    if spec is None:
        return None
    return ScheduleSpec(
        cron_expressions=tuple(spec.cron_expressions) or None,
        interval_seconds=spec.interval_seconds or None,
        run_at_unix_nanos=spec.run_at_unix_nanos or None,
        time_zone=spec.time_zone,
        jitter_seconds=spec.jitter_seconds,
        overlap=_SCHEDULE_OVERLAP_FROM_PROTO.get(spec.overlap, ScheduleOverlap.SKIP),
        paused=spec.paused,
    )


def job_schedule_from_proto(schedule: Any) -> JobSchedule:
    return JobSchedule(
        schedule_id=schedule.schedule_id,
        schedule=schedule_spec_from_proto(schedule.schedule) if schedule.HasField("schedule") else None,
        spec=job_spec_from_proto(schedule.spec) if schedule.HasField("spec") else None,
        paused=schedule.paused,
        note=schedule.note,
        next_run_at_unix_nanos=schedule.next_run_at_unix_nanos,
        one_shot=schedule.one_shot,
        remaining_actions=schedule.remaining_actions,
        available_actions=tuple(
            _SCHEDULE_ACTION_FROM_PROTO[a] for a in schedule.available_actions if a in _SCHEDULE_ACTION_FROM_PROTO
        ),
        created_at_unix_nanos=schedule.created_at_unix_nanos,
        updated_at_unix_nanos=schedule.updated_at_unix_nanos,
        num_actions=schedule.num_actions,
        num_actions_skipped_overlap=schedule.num_actions_skipped_overlap,
        num_actions_missed_catchup_window=schedule.num_actions_missed_catchup_window,
        recent_run_ids=tuple(schedule.recent_run_ids),
        running_run_ids=tuple(schedule.running_run_ids),
    )
