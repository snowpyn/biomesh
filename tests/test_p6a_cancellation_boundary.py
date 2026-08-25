"""Deterministic P6A-002 cancellation/publication-boundary regressions."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import biomesh.local_queue as local_queue_module
import biomesh.local_queue_storage as queue_storage
import biomesh.project_campaign as campaign_module
from biomesh.local_queue import LocalQueueService
from biomesh.local_queue_runtime import (
    WorkerCancellation,
    process_start_ticks,
    terminate_worker_identity,
)
from biomesh.local_queue_storage import (
    LocalQueueStore,
    create_local_queue,
)
from biomesh.local_queue_types import AppliedResourceLimits, QueueItemStatus
from biomesh.project_campaign import (
    CampaignRecord,
    CampaignService,
    ExperimentRecord,
    ProjectCampaignError,
    ProjectDefinition,
    ProjectRecord,
    ProjectState,
    RunExecutionRequest,
    SeedPolicy,
    SweepPoint,
    accepted_core_execution_identity,
    create_project,
)

FIXTURE = Path("experiments/producer.yaml")
WORKER_MEMORY_BYTES = 8 * 1024**3


def _project_definition(*, runs: int = 2) -> ProjectDefinition:
    fixture_sha256 = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    return ProjectDefinition(
        schema_version=2,
        project=ProjectRecord(
            schema_version=1,
            project_id="p6a002-cancellation-project",
            title="P6A-002 cancellation-boundary project",
            description="Disposable deterministic software validation only.",
        ),
        experiments=[
            ExperimentRecord(
                schema_version=1,
                experiment_id="accepted-producer",
                title="Accepted producer fixture",
                fixture_file=str(FIXTURE),
                fixture_sha256=fixture_sha256,
                calibration_status="CALIBRATION_REQUIRED",
                notes="No calibration or biological claim.",
            )
        ],
        campaigns=[
            CampaignRecord(
                schema_version=1,
                campaign_id="campaign-a",
                experiment_id="accepted-producer",
                title="Cancellation boundary campaign",
                replicate_count=runs,
                seed_policy=SeedPolicy(kind="sequence", start=701, step=1),
                sweep_matrix=[
                    SweepPoint(point_id="point-0", condition_id="producer")
                ],
            )
        ],
        execution_identity=accepted_core_execution_identity(Path.cwd()),
    )


def _create_project(tmp_path: Path, *, runs: int = 2) -> Path:
    definition = _project_definition(runs=runs)
    definition_path = tmp_path / "definition.json"
    definition_path.write_text(
        definition.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    return create_project(definition_path, tmp_path / "project")


def _active_queue(
    tmp_path: Path,
) -> tuple[Path, Path, LocalQueueService, str, AppliedResourceLimits]:
    project = _create_project(tmp_path)
    queue = create_local_queue(
        tmp_path / "queue",
        cpu_cores=1,
        memory_limit_bytes=WORKER_MEMORY_BYTES,
    )
    service = LocalQueueService(queue)
    item = service.enqueue(project, "campaign-a", priority=3)
    applied = AppliedResourceLimits(
        cpu_ids=[min(os.sched_getaffinity(0))],
        memory_limit_bytes=WORKER_MEMORY_BYTES,
    )
    claimed = service._claim_next(applied)
    assert claimed is not None
    assert claimed.queue_id == item.queue_id
    return project, queue, service, item.queue_id, applied


def _tiny_executor(request: RunExecutionRequest, output: Path) -> None:
    (output / "result.json").write_text(
        json.dumps({"run_id": request.run.run_id}, sort_keys=True),
        encoding="utf-8",
    )


def _campaign_payload(project: Path) -> dict[str, object]:
    return json.loads((project / "campaign_state.json").read_text(encoding="utf-8"))


def _artifact_bytes(project: Path) -> dict[str, bytes]:
    return {
        path.relative_to(project).as_posix(): path.read_bytes()
        for path in sorted((project / "artifacts").rglob("*"))
        if path.is_file()
    }


@pytest.mark.parametrize(
    "window",
    [
        "after_queue_claim",
        "durably_running",
        "after_completed_publication",
        "campaign_state_before_replace",
    ],
)
def test_every_campaign_cancellation_window_is_durable_and_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    window: str,
) -> None:
    project, queue, service, queue_id, applied = _active_queue(tmp_path)

    with monkeypatch.context() as patch:
        if window == "after_queue_claim":
            def cancel_before_resume(
                _service: CampaignService, _campaign_id: str
            ) -> object:
                raise WorkerCancellation

            patch.setattr(CampaignService, "resume", cancel_before_resume)
        elif window == "durably_running":
            def cancel_running(
                _request: RunExecutionRequest, _output: Path
            ) -> None:
                raise WorkerCancellation

            patch.setattr(campaign_module, "execute_application_run", cancel_running)
        elif window == "after_completed_publication":
            patch.setattr(campaign_module, "execute_application_run", _tiny_executor)
            original_write = CampaignService._write_state
            injected = False

            def cancel_after_completed(
                campaign_service: CampaignService, state: ProjectState
            ) -> None:
                nonlocal injected
                original_write(campaign_service, state)
                if not injected and state.audit[-1].action == "run_completed":
                    injected = True
                    raise WorkerCancellation

            patch.setattr(CampaignService, "_write_state", cancel_after_completed)
        else:
            original_replace = campaign_module.os.replace
            injected = False

            def cancel_before_state_replace(source: object, target: object) -> None:
                nonlocal injected
                target_path = Path(target)  # type: ignore[arg-type]
                if not injected and target_path == project / "campaign_state.json":
                    injected = True
                    raise WorkerCancellation
                original_replace(source, target)

            patch.setattr(campaign_module.os, "replace", cancel_before_state_replace)

        claimed = service._current_item(queue_id)
        outcome = service._execute_claimed(claimed)

    assert outcome is QueueItemStatus.CANCELLED
    snapshot = service.status().items[0]
    assert snapshot.item.status is QueueItemStatus.CANCELLED
    assert snapshot.item.cancel_requested is True
    assert snapshot.campaign.failed == 1
    assert snapshot.campaign.running == 0

    cancelled_state = _campaign_payload(project)
    cancelled_runs = [
        run
        for run in cancelled_state["runs"]  # type: ignore[index]
        if run["status"] == "failed" and run["failure"]["kind"] == "cancelled"
    ]
    assert len(cancelled_runs) == 1
    cancelled_run_id = cancelled_runs[0]["run_id"]
    assert cancelled_runs[0]["attempt_count"] == 1
    assert sum(
        record["action"] == "run_failed"
        and record["run_id"] == cancelled_run_id
        for record in cancelled_state["audit"]  # type: ignore[index]
    ) == 1
    assert not list(project.glob(".campaign_state.json.*"))
    assert not list(queue.glob(".queue_state.json.*"))

    immutable_before_retry = _artifact_bytes(project)
    service.retry(queue_id)
    retried = service._claim_next(applied)
    assert retried is not None
    with monkeypatch.context() as patch:
        patch.setattr(campaign_module, "execute_application_run", _tiny_executor)
        assert service._execute_claimed(retried) is QueueItemStatus.COMPLETED

    final = service.status().items[0]
    assert final.item.status is QueueItemStatus.COMPLETED
    final_counts = (
        final.campaign.completed,
        final.campaign.failed,
        final.campaign.pending,
    )
    assert final_counts == (
        2,
        0,
        0,
    )
    final_state = _campaign_payload(project)
    final_by_id = {run["run_id"]: run for run in final_state["runs"]}  # type: ignore[index]
    assert final_by_id[cancelled_run_id]["attempt_count"] == 2
    assert all(
        path in _artifact_bytes(project)
        and _artifact_bytes(project)[path] == contents
        for path, contents in immutable_before_retry.items()
    )
    with pytest.raises(Exception, match="not retryable: completed"):
        service.retry(queue_id)


def test_interrupted_queue_terminal_publication_recovers_one_acknowledgement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, queue, service, queue_id, _applied = _active_queue(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(
            local_queue_module,
            "terminate_worker_identity",
            lambda _pid, _ticks: False,
        )
        service.cancel(queue_id)

    original_replace = queue_storage.os.replace
    injected = False

    def cancel_queue_terminal_replace(source: object, target: object) -> None:
        nonlocal injected
        target_path = Path(target)  # type: ignore[arg-type]
        if not injected and target_path == queue / "queue_state.json":
            injected = True
            raise WorkerCancellation
        original_replace(source, target)

    with monkeypatch.context() as patch:
        def cancel_running(
            _request: RunExecutionRequest, _output: Path
        ) -> None:
            raise WorkerCancellation

        patch.setattr(campaign_module, "execute_application_run", cancel_running)
        patch.setattr(queue_storage.os, "replace", cancel_queue_terminal_replace)
        with pytest.raises(WorkerCancellation):
            service._execute_claimed(service._current_item(queue_id))

    assert not list(queue.glob(".queue_state.json.*"))
    store = LocalQueueStore(queue)
    with store.lock():
        state = store.load()
        item = state.items[0].model_copy(
            update={"worker_pid": 999_999_999, "worker_start_ticks": 1}
        )
        store.write(state.model_copy(update={"items": [item]}))

    recovered = service.status().items[0]
    assert recovered.item.status is QueueItemStatus.CANCELLED
    campaign_state = _campaign_payload(project)
    cancelled = [
        run
        for run in campaign_state["runs"]  # type: ignore[index]
        if run["status"] == "failed" and run["failure"]["kind"] == "cancelled"
    ]
    assert len(cancelled) == 1
    assert sum(
        record["action"] == "run_failed"
        and record["run_id"] == cancelled[0]["run_id"]
        for record in campaign_state["audit"]  # type: ignore[index]
    ) == 1
    queue_state = LocalQueueStore(queue).load()
    cancellation_requests = sum(
        record.action == "cancellation_requested" for record in queue_state.audit
    )
    assert cancellation_requests == 1
    assert sum(record.action == "worker_recovered" for record in queue_state.audit) == 1


@pytest.mark.parametrize("shape", ["empty", "nonempty", "symlink", "malformed"])
def test_unowned_campaign_state_temporaries_fail_closed_without_deletion(
    tmp_path: Path,
    shape: str,
) -> None:
    project = _create_project(tmp_path)
    state_before = (project / "campaign_state.json").read_bytes()
    name = (
        ".campaign_state.json.abcdefgh"
        if shape != "malformed"
        else ".campaign_state.json.operator-owned"
    )
    candidate = project / name
    if shape == "symlink":
        candidate.symlink_to(project / "campaign_state.json")
    else:
        candidate.write_bytes(b"" if shape == "empty" else b"operator data")
    candidate_identity = candidate.lstat()

    with pytest.raises(
        ProjectCampaignError,
        match="unowned campaign-state atomic temporary siblings",
    ):
        CampaignService(project).persist_queue_cancellation("campaign-a")

    assert (project / "campaign_state.json").read_bytes() == state_before
    assert candidate.lstat().st_dev == candidate_identity.st_dev
    assert candidate.lstat().st_ino == candidate_identity.st_ino
    if shape == "nonempty":
        assert candidate.read_bytes() == b"operator data"
    if shape == "symlink":
        assert candidate.is_symlink()


def test_pidfd_signal_requires_exact_process_start_identity() -> None:
    worker = subprocess.Popen(
        [sys.executable, "-c", "import signal; signal.pause()"]
    )
    try:
        start_ticks = process_start_ticks(worker.pid)
        assert terminate_worker_identity(worker.pid, start_ticks + 1) is False
        assert worker.poll() is None
        assert terminate_worker_identity(worker.pid, start_ticks) is True
        assert worker.wait(timeout=10) == -15
    finally:
        if worker.poll() is None:
            worker.terminate()
            worker.wait(timeout=10)
