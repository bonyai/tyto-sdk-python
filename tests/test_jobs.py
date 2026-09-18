from __future__ import annotations

import pytest

from tyto import InvalidRequestError, JobRunNotFoundError, JobScheduleNotFoundError
from tyto._jobs import (
    Disposition,
    JobRunAction,
    JobRunStatus,
    JobSandboxSpec,
    JobScriptSpec,
    JobSpec,
    ScheduleSpec,
)
from tyto._proto.tyto.runtime.v1 import tapi_pb2

from test_contract import FakeGuest, FakeTapi, FakeTransport, RpcFailure, make_client
import grpc


def _client(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    transport = FakeTransport()
    transport.tapi = FakeTapi()
    transport.guest = FakeGuest()
    return make_client(monkeypatch, transport), transport


def test_run_job_requires_exactly_one_sandbox_target(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch)
    with pytest.raises(InvalidRequestError):
        client.run_job(JobSpec(cmd=("echo", "hi")))
    with pytest.raises(InvalidRequestError):
        client.run_job(
            JobSpec(existing_sandbox_id="sbx-1", new_sandbox=JobSandboxSpec(template="ubuntu"), cmd=("echo", "hi"))
        )


def test_run_job_requires_exactly_one_command_form(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch)
    with pytest.raises(InvalidRequestError):
        client.run_job(JobSpec(existing_sandbox_id="sbx-1"))
    with pytest.raises(InvalidRequestError):
        client.run_job(
            JobSpec(
                existing_sandbox_id="sbx-1",
                cmd=("echo", "hi"),
                script=JobScriptSpec(body=b"echo hi"),
            )
        )


def test_run_job_sends_spec_and_maps_result(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.job_run = tapi_pb2.TApiJobRun(
        run_id="run-42",
        status=tapi_pb2.TAPI_JOB_RUN_STATUS_COMPLETED,
        sandbox_id="sbx-1",
        created_sandbox=True,
        result=tapi_pb2.TApiJobResult(exit_code=0, stdout=b"hello"),
        available_actions=[tapi_pb2.TAPI_JOB_RUN_ACTION_RERUN],
    )

    run = client.run_job(JobSpec(new_sandbox=JobSandboxSpec(template="ubuntu"), cmd=("echo", "hello")))

    assert run.run_id == "run-42"
    assert run.status == JobRunStatus.COMPLETED
    assert run.sandbox_id == "sbx-1"
    assert run.created_sandbox is True
    assert run.result is not None and run.result.stdout == b"hello"
    assert run.available_actions == (JobRunAction.RERUN,)
    assert transport.tapi.run_job_requests[-1].spec.new_sandbox.template.template_id == "ubuntu"
    assert transport.tapi.run_job_requests[-1].idempotency_key


def test_start_job_returns_run_id_immediately(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    run_id, already_running = client.start_job(JobSpec(existing_sandbox_id="sbx-1", cmd=("sleep", "30")))

    assert run_id == "run-1"
    assert already_running is False
    assert len(transport.tapi.start_job_requests) == 1


def test_get_job_run_returns_detail_with_spec_and_timeline(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.job_run_detail = tapi_pb2.TApiJobRunDetail(
        run=tapi_pb2.TApiJobRun(run_id="run-1", status=tapi_pb2.TAPI_JOB_RUN_STATUS_FAILED),
        spec=tapi_pb2.TApiJobSpec(existing_sandbox_id="sbx-1", cmd=["false"]),
        timeline=[tapi_pb2.TApiJobRunTimelineEntry(name="ExecCommand", status=tapi_pb2.TAPI_JOB_RUN_TIMELINE_STATUS_FAILED)],
    )

    detail = client.get_job_run("run-1")

    assert detail.status == JobRunStatus.FAILED
    assert detail.spec is not None and detail.spec.existing_sandbox_id == "sbx-1"
    assert len(detail.timeline) == 1 and detail.timeline[0].name == "ExecCommand"


def test_get_job_run_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.get_job_run_errors.put(RpcFailure(grpc.StatusCode.NOT_FOUND, "run missing"))

    with pytest.raises(JobRunNotFoundError):
        client.get_job_run("run-missing")


def test_cancel_job_run(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    client.cancel_job_run("run-1")

    assert len(transport.tapi.cancel_job_run_requests) == 1


def test_list_job_runs_maps_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.job_runs = [
        tapi_pb2.TApiJobRun(run_id="run-1", status=tapi_pb2.TAPI_JOB_RUN_STATUS_RUNNING),
        tapi_pb2.TApiJobRun(run_id="run-2", status=tapi_pb2.TAPI_JOB_RUN_STATUS_COMPLETED),
    ]

    runs = list(client.list_job_runs())

    assert [r.run_id for r in runs] == ["run-1", "run-2"]
    assert runs[1].status == JobRunStatus.COMPLETED


def test_create_job_schedule_requires_exactly_one_timing(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch)
    job = JobSpec(existing_sandbox_id="sbx-1", cmd=("echo", "hi"))

    with pytest.raises(InvalidRequestError):
        client.create_job_schedule(ScheduleSpec(), job)
    with pytest.raises(InvalidRequestError):
        client.create_job_schedule(ScheduleSpec(cron_expressions=("* * * * *",), interval_seconds=60), job)


def test_create_job_schedule_sends_schedule_and_job(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    schedule = client.create_job_schedule(
        ScheduleSpec(interval_seconds=3600), JobSpec(existing_sandbox_id="sbx-1", cmd=("echo", "hi"))
    )

    assert schedule.schedule_id == "sched-1"
    assert transport.tapi.create_job_schedule_requests[-1].schedule.interval_seconds == 3600
    assert transport.tapi.create_job_schedule_requests[-1].idempotency_key


def test_get_job_schedule_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.get_job_schedule_errors.put(RpcFailure(grpc.StatusCode.NOT_FOUND, "schedule missing"))

    with pytest.raises(JobScheduleNotFoundError):
        client.get_job_schedule("sched-missing")


def test_update_job_schedule_sends_full_replacement(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    client.update_job_schedule(
        "sched-1", ScheduleSpec(interval_seconds=7200), JobSpec(existing_sandbox_id="sbx-1", cmd=("echo", "v2"))
    )

    assert transport.tapi.update_job_schedule_requests[-1].schedule.interval_seconds == 7200


def test_set_job_schedule_paused(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch)

    schedule = client.set_job_schedule_paused("sched-1", True, note="pausing for maintenance")

    assert schedule.paused is True
    assert schedule.note == "pausing for maintenance"


def test_trigger_and_delete_job_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch)

    client.trigger_job_schedule("sched-1")
    client.delete_job_schedule("sched-1")


def test_list_job_schedules_maps_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)
    transport.tapi.job_schedules = [
        tapi_pb2.TApiJobSchedule(schedule_id="sched-1", paused=False),
        tapi_pb2.TApiJobSchedule(schedule_id="sched-2", paused=True),
    ]

    schedules = list(client.list_job_schedules())

    assert [s.schedule_id for s in schedules] == ["sched-1", "sched-2"]
    assert schedules[1].paused is True


def test_run_job_disposition_defaults_to_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    client.run_job(JobSpec(existing_sandbox_id="sbx-1", cmd=("echo", "hi")))

    assert transport.tapi.run_job_requests[-1].spec.disposition == tapi_pb2.TAPI_SANDBOX_DISPOSITION_DELETE


def test_run_job_disposition_keep(monkeypatch: pytest.MonkeyPatch) -> None:
    client, transport = _client(monkeypatch)

    client.run_job(JobSpec(existing_sandbox_id="sbx-1", cmd=("echo", "hi"), disposition=Disposition.KEEP))

    assert transport.tapi.run_job_requests[-1].spec.disposition == tapi_pb2.TAPI_SANDBOX_DISPOSITION_KEEP
