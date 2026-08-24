"""P6A-001 real-worker interruption, recovery, retry, and export regression."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from biomesh.local_queue import LocalQueueService
from biomesh.local_queue_runtime import process_start_ticks
from biomesh.local_queue_storage import create_local_queue
from biomesh.local_queue_types import LocalQueueState, QueueItemStatus
from biomesh.portable_project import (
    export_project_archive,
    import_project_archive,
    verify_project_archive,
)
from biomesh.portable_queue_activation import (
    activate_portable_queue_binding,
    load_portable_queue_binding,
)
from biomesh.portable_queue_import import (
    bind_portable_queue_intent,
    import_portable_queue_intent,
    parse_project_path_binding,
)
from biomesh.portable_queue_intent import export_portable_queue_intent
from biomesh.project_campaign import (
    COMPLETION_RECEIPT,
    CampaignRecord,
    CampaignService,
    ExperimentRecord,
    ProjectDefinition,
    ProjectRecord,
    SeedPolicy,
    SweepPoint,
    accepted_core_execution_identity,
    create_project,
)
from biomesh.project_reports import generate_campaign_report

FIXTURE = Path("experiments/producer.yaml")
MEMORY_LIMIT_BYTES = 8 * 1024**3


def _definition(
    *, project_id: str, campaign_id: str, replicate_count: int
) -> ProjectDefinition:
    return ProjectDefinition(
        schema_version=2,
        project=ProjectRecord(
            schema_version=1,
            project_id=project_id,
            title=f"P6A-001 manufactured project {project_id}",
            description="Software recovery validation only; no biological claim.",
        ),
        experiments=[
            ExperimentRecord(
                schema_version=1,
                experiment_id="accepted-producer",
                title="Accepted producer fixture",
                fixture_file=str(FIXTURE.resolve()),
                fixture_sha256=hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
                calibration_status="CALIBRATION_REQUIRED",
                notes="Preserves the accepted fixture and calibration boundary.",
            )
        ],
        campaigns=[
            CampaignRecord(
                schema_version=1,
                campaign_id=campaign_id,
                experiment_id="accepted-producer",
                title=f"P6A-001 campaign {campaign_id}",
                replicate_count=replicate_count,
                seed_policy=SeedPolicy(
                    kind="explicit",
                    seeds=[101 + index for index in range(replicate_count)],
                ),
                sweep_matrix=[
                    SweepPoint(point_id="producer-point", condition_id="producer")
                ],
            )
        ],
        execution_identity=accepted_core_execution_identity(Path.cwd()),
    )


def _create_project(
    root: Path, *, project_id: str, campaign_id: str, replicate_count: int
) -> Path:
    definition = _definition(
        project_id=project_id,
        campaign_id=campaign_id,
        replicate_count=replicate_count,
    )
    definition_path = root / f"{project_id}-definition.json"
    definition_path.write_text(
        definition.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    return create_project(definition_path, root / project_id)


def _json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_bytes())
    assert isinstance(value, dict)
    return value


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _queue_state(queue: Path) -> LocalQueueState:
    return LocalQueueState.model_validate_json(
        (queue / "queue_state.json").read_bytes()
    )


def _wait_for_sigstop(worker: subprocess.Popen[str], marker: Path) -> None:
    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        if marker.is_file():
            break
        if worker.poll() is not None:
            stdout, stderr = worker.communicate(timeout=5)
            raise AssertionError(
                "controlled queue worker exited before SIGSTOP marker: "
                f"returncode={worker.returncode}, stdout={stdout!r}, stderr={stderr!r}"
            )
        time.sleep(0.005)
    assert marker.is_file(), "controlled queue worker never reached its second run"

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        waited, status = os.waitpid(worker.pid, os.WUNTRACED | os.WNOHANG)
        if waited == worker.pid:
            assert os.WIFSTOPPED(status)
            assert os.WSTOPSIG(status) == signal.SIGSTOP
            return
        time.sleep(0.001)
    raise AssertionError("controlled queue worker did not enter SIGSTOP state")


def _wrapper_source() -> str:
    return textwrap.dedent(
        """
        from __future__ import annotations

        import os
        import signal
        import sys
        from pathlib import Path

        import biomesh.project_campaign as project_campaign

        real_executor = project_campaign.execute_application_run
        lower_project_id = os.environ["BIOMESH_P6A_LOWER_PROJECT_ID"]
        marker = Path(os.environ["BIOMESH_P6A_SIGSTOP_MARKER"])
        lower_run_count = 0

        def controlled_executor(request, output):
            global lower_run_count
            if request.project_id != lower_project_id:
                real_executor(request, output)
                return
            lower_run_count += 1
            if lower_run_count == 1:
                real_executor(request, output)
                return
            marker.write_text(request.run.run_id + "\\n", encoding="utf-8")
            os.kill(os.getpid(), signal.SIGSTOP)
            real_executor(request, output)

        project_campaign.execute_application_run = controlled_executor

        from biomesh.__main__ import main

        raise SystemExit(main(sys.argv[1:]))
        """
    )


def test_real_sigkill_recovery_retry_report_and_project_export(
    tmp_path: Path,
) -> None:
    original_root = tmp_path / "original"
    original_root.mkdir()
    original_a = _create_project(
        original_root,
        project_id="project-a",
        campaign_id="campaign-a",
        replicate_count=1,
    )
    original_b = _create_project(
        original_root,
        project_id="project-b",
        campaign_id="campaign-b",
        replicate_count=2,
    )

    transfer = tmp_path / "transfer"
    transfer.mkdir()
    archives = {
        "project-a": transfer / "project-a.biomesh",
        "project-b": transfer / "project-b.biomesh",
    }
    export_project_archive(original_a, archives["project-a"])
    export_project_archive(original_b, archives["project-b"])

    portable_source = transfer / "portable-source"
    destination = tmp_path / "destination"
    portable_source.mkdir()
    destination.mkdir()
    source_projects: dict[str, Path] = {}
    destination_projects: dict[str, Path] = {}
    for project_id in ("project-a", "project-b"):
        source_project = portable_source / project_id
        destination_project = destination / project_id
        import_project_archive(
            archives[project_id],
            source_project,
            allow_unauthenticated=True,
        )
        import_project_archive(
            archives[project_id],
            destination_project,
            allow_unauthenticated=True,
        )
        source_projects[project_id] = source_project
        destination_projects[project_id] = destination_project

    source_queue = create_local_queue(
        portable_source / "queue",
        cpu_cores=1,
        memory_limit_bytes=MEMORY_LIMIT_BYTES,
    )
    source_service = LocalQueueService(source_queue)
    source_service.enqueue(source_projects["project-a"], "campaign-a", priority=10)
    source_service.enqueue(source_projects["project-b"], "campaign-b", priority=5)

    intent = transfer / "intent.json"
    imported_intent = destination / "imported-intent.json"
    binding = destination / "binding.json"
    destination_queue = destination / "queue"
    export_portable_queue_intent(source_queue, intent)
    import_portable_queue_intent(intent, imported_intent)
    bind_portable_queue_intent(
        imported_intent,
        binding,
        project_bindings=[
            parse_project_path_binding(
                f"{project_id}={destination_projects[project_id].resolve()}"
            )
            for project_id in ("project-a", "project-b")
        ],
        cpu_cores=1,
        memory_limit_bytes=MEMORY_LIMIT_BYTES,
    )
    activation = activate_portable_queue_binding(binding, destination_queue)
    activated_by_project = {item.project_id: item for item in activation.items}
    assert [item.project_id for item in activation.items] == ["project-a", "project-b"]
    assert (
        activated_by_project["project-a"].local_project_directory
        != activated_by_project["project-b"].local_project_directory
    )

    immutable_portable_records = {
        intent: intent.read_bytes(),
        imported_intent: imported_intent.read_bytes(),
        binding: binding.read_bytes(),
        destination_queue / "portable_activation.json": (
            destination_queue / "portable_activation.json"
        ).read_bytes(),
    }

    wrapper = tmp_path / "controlled_worker.py"
    marker = tmp_path / "lower-project-second-run.marker"
    wrapper.write_text(_wrapper_source(), encoding="utf-8")
    worker_environment = os.environ.copy()
    worker_environment["BIOMESH_P6A_LOWER_PROJECT_ID"] = "project-b"
    worker_environment["BIOMESH_P6A_SIGSTOP_MARKER"] = str(marker)
    worker = subprocess.Popen(
        [
            sys.executable,
            str(wrapper),
            "queue",
            "run",
            str(destination_queue),
        ],
        cwd=Path.cwd(),
        env=worker_environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_sigstop(worker, marker)

        queue_before_kill = _queue_state(destination_queue)
        queue_items_before = {item.queue_id: item for item in queue_before_kill.items}
        queue_audit_before = [
            item.model_dump(mode="json") for item in queue_before_kill.audit
        ]
        queue_a_id = activated_by_project["project-a"].local_queue_id
        queue_b_id = activated_by_project["project-b"].local_queue_id
        queue_a_before = queue_items_before[queue_a_id]
        queue_b_before = queue_items_before[queue_b_id]
        assert queue_a_before.status is QueueItemStatus.COMPLETED
        assert queue_b_before.status is QueueItemStatus.RUNNING
        assert queue_b_before.worker_pid == worker.pid
        assert queue_b_before.worker_start_ticks == process_start_ticks(worker.pid)

        project_a = destination_projects["project-a"]
        project_b = destination_projects["project-b"]
        state_a_bytes_before = (project_a / "campaign_state.json").read_bytes()
        state_a_before = _json(project_a / "campaign_state.json")
        state_b_before = _json(project_b / "campaign_state.json")
        runs_a_before = state_a_before["runs"]
        runs_b_before = state_b_before["runs"]
        assert isinstance(runs_a_before, list)
        assert isinstance(runs_b_before, list)
        assert [run["status"] for run in runs_a_before] == ["completed"]
        assert sorted(run["status"] for run in runs_b_before) == [
            "completed",
            "running",
        ]
        completed_b_before = next(
            run for run in runs_b_before if run["status"] == "completed"
        )
        running_b_before = next(
            run for run in runs_b_before if run["status"] == "running"
        )
        assert marker.read_text(encoding="utf-8").strip() == running_b_before["run_id"]

        artifacts_b = project_b / "artifacts"
        staging_paths = [
            path for path in artifacts_b.iterdir() if path.name.startswith(".")
        ]
        assert len(staging_paths) == 1
        staging = staging_paths[0]
        run_id, separator, suffix = staging.name[1:].rpartition(".")
        assert separator == "."
        assert run_id == running_b_before["run_id"]
        assert len(suffix) == 8
        assert set(suffix) <= set("abcdefghijklmnopqrstuvwxyz0123456789_")
        assert staging.is_dir()
        assert not staging.is_symlink()
        assert not list(staging.iterdir())
        assert staging.parent == artifacts_b

        artifacts_a_before = _tree_bytes(project_a / "artifacts")
        completed_b_root = artifacts_b / str(completed_b_before["run_id"])
        completed_b_bytes_before = _tree_bytes(completed_b_root)
        completed_b_record_before = dict(completed_b_before)
        audit_b_before = list(state_b_before["audit"])

        os.kill(worker.pid, signal.SIGKILL)
        stdout, stderr = worker.communicate(timeout=15)
        assert worker.returncode == -signal.SIGKILL, (stdout, stderr)
    finally:
        if worker.poll() is None:
            os.kill(worker.pid, signal.SIGKILL)
            worker.communicate(timeout=15)

    assert staging.is_dir()
    assert not list(staging.iterdir())
    queue_b_id = activated_by_project["project-b"].local_queue_id
    recovered_snapshot = LocalQueueService(destination_queue).status()
    recovered_by_id = {item.item.queue_id: item for item in recovered_snapshot.items}
    assert recovered_by_id[queue_b_id].item.status is QueueItemStatus.FAILED
    assert recovered_by_id[queue_b_id].campaign.failed == 1
    assert recovered_by_id[queue_b_id].campaign.completed == 1
    assert not staging.exists()

    recovered_b_state = _json(project_b / "campaign_state.json")
    recovered_runs = recovered_b_state["runs"]
    assert isinstance(recovered_runs, list)
    interrupted = next(
        run for run in recovered_runs if run["run_id"] == running_b_before["run_id"]
    )
    assert interrupted["status"] == "failed"
    assert interrupted["attempt_count"] == 1
    assert interrupted["failure"]["kind"] == "interrupted"
    assert (
        next(
            run
            for run in recovered_runs
            if run["run_id"] == completed_b_before["run_id"]
        )
        == completed_b_record_before
    )
    assert recovered_b_state["audit"][: len(audit_b_before)] == audit_b_before

    retried = LocalQueueService(destination_queue).retry(queue_b_id)
    assert retried.status is QueueItemStatus.QUEUED
    fresh_worker = subprocess.run(
        [
            sys.executable,
            "-m",
            "biomesh",
            "queue",
            "run",
            str(destination_queue),
            "--once",
        ],
        cwd=Path.cwd(),
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert fresh_worker.returncode == 0, fresh_worker.stderr
    assert json.loads(fresh_worker.stdout) == {
        "cancelled": 0,
        "completed": 1,
        "executed": 1,
        "failed": 0,
    }

    final_snapshot = LocalQueueService(destination_queue).status()
    final_by_id = {item.item.queue_id: item for item in final_snapshot.items}
    queue_a_id = activated_by_project["project-a"].local_queue_id
    assert final_by_id[queue_a_id].item.status is QueueItemStatus.COMPLETED
    assert final_by_id[queue_b_id].item.status is QueueItemStatus.COMPLETED
    assert final_by_id[queue_b_id].campaign.completed == 2
    assert final_by_id[queue_b_id].campaign.failed == 0

    final_a_state = _json(project_a / "campaign_state.json")
    final_b_state = _json(project_b / "campaign_state.json")
    final_b_runs = final_b_state["runs"]
    assert isinstance(final_b_runs, list)
    final_b_by_id = {run["run_id"]: run for run in final_b_runs}
    assert final_b_by_id[completed_b_before["run_id"]] == completed_b_record_before
    assert final_b_by_id[running_b_before["run_id"]]["attempt_count"] == 2
    assert all(run["status"] == "completed" for run in final_b_runs)
    assert [run["attempt_count"] for run in final_a_state["runs"]] == [1]
    assert sorted(run["attempt_count"] for run in final_b_runs) == [1, 2]
    assert final_b_state["audit"][: len(audit_b_before)] == audit_b_before
    for run in final_b_runs:
        completions = [
            record
            for record in final_b_state["audit"]
            if record["action"] == "run_completed" and record["run_id"] == run["run_id"]
        ]
        assert len(completions) == 1
        receipt = _json(project_b / "artifacts" / run["run_id"] / COMPLETION_RECEIPT)
        assert receipt["attempt"] == run["attempt_count"]

    final_queue_state = _queue_state(destination_queue)
    final_queue_by_id = {item.queue_id: item for item in final_queue_state.items}
    assert final_queue_by_id[queue_a_id] == queue_a_before
    final_queue_audit = [
        item.model_dump(mode="json") for item in final_queue_state.audit
    ]
    assert final_queue_audit[: len(queue_audit_before)] == queue_audit_before
    for queue_id in (queue_a_id, queue_b_id):
        completions = [
            record
            for record in final_queue_state.audit
            if record.action == "campaign_completed" and record.queue_id == queue_id
        ]
        assert len(completions) == 1

    assert (project_a / "campaign_state.json").read_bytes() == state_a_bytes_before
    assert _tree_bytes(project_a / "artifacts") == artifacts_a_before
    assert _tree_bytes(completed_b_root) == completed_b_bytes_before
    for path, contents in immutable_portable_records.items():
        assert path.read_bytes() == contents

    run_ids_by_project: dict[str, set[str]] = {}
    for project_id, project in destination_projects.items():
        campaign_id = "campaign-a" if project_id == "project-a" else "campaign-b"
        state = _json(project / "campaign_state.json")
        runs = state["runs"]
        assert isinstance(runs, list)
        run_ids = {str(run["run_id"]) for run in runs}
        run_ids_by_project[project_id] = run_ids
        assert {path.name for path in (project / "artifacts").iterdir()} == run_ids
        for run_id in run_ids:
            request = _json(project / "artifacts" / run_id / "run_request.json")
            receipt = _json(project / "artifacts" / run_id / COMPLETION_RECEIPT)
            assert request["project_id"] == project_id
            trace = request["portable_trace"]
            assert trace["project_id"] == project_id
            assert trace["campaign_id"] == campaign_id
            assert trace["run_id"] == run_id
            assert receipt["portable_trace"] == trace
    assert run_ids_by_project["project-a"].isdisjoint(run_ids_by_project["project-b"])

    portable_binding = load_portable_queue_binding(binding)
    expected_runs = {"project-a": 1, "project-b": 2}
    campaign_ids = {"project-a": "campaign-a", "project-b": "campaign-b"}
    for project_id, project in destination_projects.items():
        report = tmp_path / f"{project_id}-report"
        result = generate_campaign_report(
            project,
            campaign_ids[project_id],
            report,
            portable_binding=portable_binding,
        )
        assert result.completed_runs == expected_runs[project_id]
        assert result.missing_runs == 0
        report_data = _json(report / "report_data.json")
        trace = report_data["portable_traceability"]
        assert trace["project_id"] == project_id
        assert trace["campaign_id"] == campaign_ids[project_id]
        assert {run["run_id"] for run in trace["runs"]} == run_ids_by_project[
            project_id
        ]

        archive = tmp_path / f"{project_id}-completed.biomesh"
        imported = tmp_path / f"{project_id}-completed-import"
        exported = export_project_archive(project, archive)
        verified = verify_project_archive(archive, allow_unauthenticated=True)
        assert verified.archive_sha256 == exported.archive_sha256
        imported_result = import_project_archive(
            archive,
            imported,
            allow_unauthenticated=True,
        )
        assert imported_result.completed_run_count == expected_runs[project_id]
        assert _tree_bytes(imported / "artifacts") == _tree_bytes(project / "artifacts")
        imported_status = CampaignService(imported).status(campaign_ids[project_id])
        assert imported_status.completed == expected_runs[project_id]
        assert imported_status.failed == 0

    assert (project_a / "campaign_state.json").read_bytes() == state_a_bytes_before
    assert _tree_bytes(project_a / "artifacts") == artifacts_a_before
    assert _tree_bytes(completed_b_root) == completed_b_bytes_before
