"""Focused P4-WP01 project/campaign model and application-path tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from biomesh.__main__ import main
from biomesh.project_campaign import (
    COMPLETION_RECEIPT,
    ArtifactRecord,
    CampaignRecord,
    CampaignRunStatus,
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
    execution_identity_sha256,
)

FIXTURE = Path("experiments/producer.yaml")


def _definition(*, replicate_count: int = 2, points: int = 2) -> ProjectDefinition:
    fixture_hash = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    return ProjectDefinition(
        schema_version=2,
        project=ProjectRecord(
            schema_version=1,
            project_id="research-project",
            title="Manufactured validation project",
            description="Software validation only; no biological conclusion.",
        ),
        experiments=[
            ExperimentRecord(
                schema_version=1,
                experiment_id="accepted-producer",
                title="Accepted producer fixture",
                fixture_file=str(FIXTURE),
                fixture_sha256=fixture_hash,
                calibration_status="CALIBRATION_REQUIRED",
                notes="Preserves the accepted P2/P3 fixture path.",
            )
        ],
        campaigns=[
            CampaignRecord(
                schema_version=1,
                campaign_id="campaign-a",
                experiment_id="accepted-producer",
                title="Replicate matrix",
                replicate_count=replicate_count,
                seed_policy=SeedPolicy(kind="sequence", start=10, step=5),
                sweep_matrix=[
                    SweepPoint(
                        point_id=f"point-{index}",
                        condition_id="producer",
                    )
                    for index in range(points)
                ],
            )
        ],
        execution_identity=accepted_core_execution_identity(Path.cwd()),
    )


def _write_definition(tmp_path: Path, definition: ProjectDefinition) -> Path:
    path = tmp_path / "definition.json"
    path.write_text(definition.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return path


def _create(tmp_path: Path, definition: ProjectDefinition | None = None) -> Path:
    definition_file = _write_definition(tmp_path, definition or _definition())
    return create_project(definition_file, tmp_path / "project")


def test_seed_policies_are_deterministic_and_strict() -> None:
    assert SeedPolicy(kind="explicit", seeds=[9, 4]).expand(2) == (9, 4)
    assert SeedPolicy(kind="sequence", start=7, step=3).expand(4) == (7, 10, 13, 16)

    with pytest.raises(ProjectCampaignError, match="must equal"):
        SeedPolicy(kind="explicit", seeds=[1]).expand(2)
    with pytest.raises(ValidationError, match="must be unique"):
        SeedPolicy(kind="explicit", seeds=[1, 1])
    with pytest.raises(ValidationError, match="requires start/step"):
        SeedPolicy(kind="sequence", start=1)


def test_project_creation_expands_stable_matrix_and_audit(tmp_path: Path) -> None:
    project = _create(tmp_path)
    state = json.loads((project / "campaign_state.json").read_text())

    assert [run["seed"] for run in state["runs"]] == [10, 15, 10, 15]
    assert len({run["run_id"] for run in state["runs"]}) == 4
    assert all(run["status"] == "pending" for run in state["runs"])
    assert state["audit"] == [
        {
            "action": "campaign_initialized",
            "attempt": 0,
            "campaign_id": "campaign-a",
            "detail": "planned 4 runs",
            "run_id": None,
            "sequence": 0,
        }
    ]
    assert CampaignService(project).status("campaign-a").as_dict() == {
        "campaign_id": "campaign-a",
        "completed": 0,
        "failed": 0,
        "pending": 4,
        "running": 0,
        "total": 4,
    }


def test_project_rejects_fixture_drift_and_sweep_provenance_mismatch(
    tmp_path: Path,
) -> None:
    definition = _definition()
    changed_hash = definition.experiments[0].model_copy(
        update={"fixture_sha256": "0" * 64}
    )
    mismatched = definition.model_copy(update={"experiments": [changed_hash]})
    with pytest.raises(ProjectCampaignError, match="fixture hash mismatch"):
        _create(tmp_path, mismatched)

    parameter_payload = {
        "name": "maximum_eps_allocation_fraction",
        "value": 0.2,
        "unit": "1",
        "level_id": "invented",
        "source": "manufactured software-validation fixture",
        "uncertainty": "not a biological uncertainty",
        "notes": "SI-labelled executable fixture; not calibration.",
        "calibration_status": "DERIVED",
    }
    point = SweepPoint.model_validate(
        {
            "point_id": "bad-point",
            "condition_id": "producer",
            "parameters": [parameter_payload],
        }
    )
    campaign = definition.campaigns[0].model_copy(update={"sweep_matrix": [point]})
    mismatch = definition.model_copy(update={"campaigns": [campaign]})
    second = tmp_path / "second"
    second.mkdir()
    with pytest.raises(ProjectCampaignError, match="parameters do not match"):
        _create(second, mismatch)


def test_partial_failure_is_explicit_and_retryable_without_rerunning_completion(
    tmp_path: Path,
) -> None:
    project = _create(tmp_path, _definition(replicate_count=2, points=1))
    attempts: dict[str, int] = {}

    def executor(request: RunExecutionRequest, output: Path) -> None:
        attempts[request.run.run_id] = attempts.get(request.run.run_id, 0) + 1
        if request.run.replicate_index == 1 and attempts[request.run.run_id] == 1:
            raise RuntimeError("synthetic partial failure")
        (output / "result.json").write_text(
            json.dumps({"seed": request.run.seed}), encoding="utf-8"
        )

    service = CampaignService(project, executor=executor)
    first = service.resume("campaign-a")
    assert (first.completed, first.failed, first.pending) == (1, 1, 0)

    first_state = json.loads((project / "campaign_state.json").read_text())
    failed = next(run for run in first_state["runs"] if run["status"] == "failed")
    completed = next(run for run in first_state["runs"] if run["status"] == "completed")
    assert failed["failure"] == {
        "kind": "runtime",
        "message": "synthetic partial failure",
        "retryable": True,
    }
    completed_bytes = (
        project / "artifacts" / completed["run_id"] / "result.json"
    ).read_bytes()

    final = service.retry("campaign-a", [failed["run_id"]])
    assert (final.completed, final.failed) == (2, 0)
    assert attempts[completed["run_id"]] == 1
    assert (
        project / "artifacts" / completed["run_id"] / "result.json"
    ).read_bytes() == completed_bytes

    state = json.loads((project / "campaign_state.json").read_text())
    retried = next(run for run in state["runs"] if run["run_id"] == failed["run_id"])
    assert retried["attempt_count"] == 2
    assert [record["sequence"] for record in state["audit"]] == list(
        range(len(state["audit"]))
    )


def test_interrupted_run_becomes_explicit_failure_and_other_work_resumes(
    tmp_path: Path,
) -> None:
    project = _create(tmp_path, _definition(replicate_count=2, points=1))
    interrupted = True

    def executor(request: RunExecutionRequest, output: Path) -> None:
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise KeyboardInterrupt
        (output / "result.txt").write_text(str(request.run.seed), encoding="utf-8")

    service = CampaignService(project, executor=executor)
    with pytest.raises(KeyboardInterrupt):
        service.resume("campaign-a")
    interrupted_state = json.loads((project / "campaign_state.json").read_text())
    running = next(
        run for run in interrupted_state["runs"] if run["status"] == "running"
    )
    staging = project / "artifacts" / f".{running['run_id']}.abcdefgh"
    staging.mkdir()

    with pytest.raises(ProjectCampaignError, match="unreconciled artifact staging"):
        service.status("campaign-a")

    resumed = service.resume("campaign-a")
    assert (resumed.completed, resumed.failed, resumed.running) == (1, 1, 0)
    assert not staging.exists()
    state = json.loads((project / "campaign_state.json").read_text())
    failure = next(run for run in state["runs"] if run["status"] == "failed")
    assert failure["failure"]["kind"] == "interrupted"
    assert any(record["action"] == "run_failed" for record in state["audit"])


@pytest.mark.parametrize(
    "unsafe_case",
    [
        "symlinked",
        "regular-file",
        "malformed",
        "ambiguous",
        "unknown-run",
        "nonempty-nested",
    ],
)
def test_interrupted_staging_recovery_rejects_unsafe_layout_without_mutation(
    tmp_path: Path,
    unsafe_case: str,
) -> None:
    case_root = tmp_path / unsafe_case
    case_root.mkdir()
    project = _create(case_root, _definition(replicate_count=1, points=1))

    def interrupt(_request: RunExecutionRequest, _output: Path) -> None:
        raise KeyboardInterrupt

    service = CampaignService(project, executor=interrupt)
    with pytest.raises(KeyboardInterrupt):
        service.resume("campaign-a")
    state_path = project / "campaign_state.json"
    state_before = state_path.read_bytes()
    state = json.loads(state_before)
    run_id = state["runs"][0]["run_id"]
    artifact_root = project / "artifacts"
    exact = artifact_root / f".{run_id}.abcdefgh"
    retained: list[Path] = []
    operator_file: Path | None = None
    symlink_target: Path | None = None

    if unsafe_case == "symlinked":
        symlink_target = case_root / "operator-target"
        symlink_target.mkdir()
        operator_file = symlink_target / "operator.txt"
        operator_file.write_text("retain this byte string", encoding="utf-8")
        exact.symlink_to(symlink_target, target_is_directory=True)
        retained.append(exact)
    elif unsafe_case == "regular-file":
        exact.write_text("operator-owned", encoding="utf-8")
        retained.append(exact)
    elif unsafe_case == "malformed":
        malformed = artifact_root / f".{run_id}.short"
        malformed.mkdir()
        retained.append(malformed)
    elif unsafe_case == "ambiguous":
        exact.mkdir()
        second = artifact_root / f".{run_id}.ijklmnop"
        second.mkdir()
        retained.extend((exact, second))
    elif unsafe_case == "unknown-run":
        exact.mkdir()
        unknown = artifact_root / ".zzzz-unknown-run.abcdefgh"
        unknown.mkdir()
        retained.extend((exact, unknown))
    else:
        exact.mkdir()
        nested = exact / "nested"
        nested.mkdir()
        operator_file = nested / "operator.txt"
        operator_file.write_text("retain this byte string", encoding="utf-8")
        retained.append(exact)

    with pytest.raises(ProjectCampaignError):
        service.recover_interrupted("campaign-a")

    assert state_path.read_bytes() == state_before
    for path in retained:
        assert path.exists() or path.is_symlink()
    if unsafe_case == "regular-file":
        assert exact.read_text(encoding="utf-8") == "operator-owned"
    if operator_file is not None:
        assert operator_file.read_bytes() == b"retain this byte string"
    if symlink_target is not None:
        assert exact.is_symlink()
        assert symlink_target.is_dir()


def test_completed_run_staging_lookalike_blocks_recovery_without_byte_changes(
    tmp_path: Path,
) -> None:
    project = _create(tmp_path, _definition(replicate_count=2, points=1))
    call_count = 0

    def interrupt_second(request: RunExecutionRequest, output: Path) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise KeyboardInterrupt
        (output / "immutable.txt").write_text(request.run.run_id, encoding="utf-8")

    service = CampaignService(project, executor=interrupt_second)
    with pytest.raises(KeyboardInterrupt):
        service.resume("campaign-a")
    state_path = project / "campaign_state.json"
    state_before = state_path.read_bytes()
    state = json.loads(state_before)
    completed = next(run for run in state["runs"] if run["status"] == "completed")
    completed_root = project / "artifacts" / completed["run_id"]
    completed_bytes = {
        path.relative_to(completed_root).as_posix(): path.read_bytes()
        for path in sorted(completed_root.rglob("*"))
        if path.is_file()
    }
    lookalike = project / "artifacts" / f".{completed['run_id']}.abcdefgh"
    lookalike.mkdir()

    with pytest.raises(ProjectCampaignError, match="not associated"):
        service.recover_interrupted("campaign-a")

    assert state_path.read_bytes() == state_before
    assert lookalike.is_dir()
    assert {
        path.relative_to(completed_root).as_posix(): path.read_bytes()
        for path in sorted(completed_root.rglob("*"))
        if path.is_file()
    } == completed_bytes


@pytest.mark.parametrize(
    "invalid_trace",
    [None, [], "not-a-portable-trace"],
    ids=["null", "list", "scalar"],
)
def test_completed_receipt_rejects_non_object_portable_trace(
    tmp_path: Path,
    invalid_trace: object,
) -> None:
    project = _create(tmp_path, _definition(replicate_count=1, points=1))

    def executor(request: RunExecutionRequest, output: Path) -> None:
        (output / "immutable.txt").write_text(request.run.run_id, encoding="utf-8")

    service = CampaignService(project, executor=executor)
    assert service.resume("campaign-a").completed == 1
    state_path = project / "campaign_state.json"
    state_before = state_path.read_bytes()
    run_id = json.loads(state_before)["runs"][0]["run_id"]
    receipt_path = project / "artifacts" / run_id / COMPLETION_RECEIPT
    receipt = json.loads(receipt_path.read_bytes())
    receipt["portable_trace"] = invalid_trace
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ProjectCampaignError, match="invalid portable completion trace"):
        service.status("campaign-a")

    assert state_path.read_bytes() == state_before


def test_completed_artifact_drift_blocks_resume_and_retry(tmp_path: Path) -> None:
    project = _create(tmp_path, _definition(replicate_count=1, points=1))

    def executor(_request: RunExecutionRequest, output: Path) -> None:
        (output / "immutable.txt").write_text("original", encoding="utf-8")

    service = CampaignService(project, executor=executor)
    assert service.resume("campaign-a").completed == 1
    state = json.loads((project / "campaign_state.json").read_text())
    run_id = state["runs"][0]["run_id"]
    (project / "artifacts" / run_id / "immutable.txt").write_text(
        "changed", encoding="utf-8"
    )

    with pytest.raises(ProjectCampaignError, match="artifact changed"):
        service.status("campaign-a")
    with pytest.raises(ProjectCampaignError, match="artifact changed"):
        service.resume("campaign-a")


def test_published_artifacts_recover_after_state_write_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = _create(tmp_path, _definition(replicate_count=1, points=1))

    def executor(_request: RunExecutionRequest, output: Path) -> None:
        (output / "complete.txt").write_text("complete", encoding="utf-8")

    service = CampaignService(project, executor=executor)
    original = service._write_state
    writes = 0

    def interrupt_completion(state: object) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("synthetic post-publication interruption")
        original(state)  # type: ignore[arg-type]

    monkeypatch.setattr(service, "_write_state", interrupt_completion)
    with pytest.raises(ProjectCampaignError, match="post-publication"):
        service.resume("campaign-a")
    monkeypatch.setattr(service, "_write_state", original)

    recovered = service.resume("campaign-a")
    assert (recovered.completed, recovered.failed, recovered.running) == (1, 0, 0)
    state = json.loads((project / "campaign_state.json").read_text())
    assert state["runs"][0]["attempt_count"] == 1
    assert state["audit"][-1]["action"] == "run_recovered"


def test_cli_create_status_resume_and_retry_application_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    definition_file = _write_definition(
        tmp_path, _definition(replicate_count=1, points=1)
    )
    project = tmp_path / "cli-project"
    assert main(["project", "create", str(definition_file), str(project)]) == 0
    created = json.loads(capsys.readouterr().out)
    assert created["project_directory"] == str(project)

    assert main(["campaign", "status", str(project), "campaign-a"]) == 0
    assert json.loads(capsys.readouterr().out)["pending"] == 1

    calls = 0

    def fail_once(request: RunExecutionRequest, output: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("retry probe")
        (output / "result.json").write_text(
            json.dumps({"run_id": request.run.run_id}), encoding="utf-8"
        )

    monkeypatch.setattr("biomesh.project_campaign.execute_application_run", fail_once)
    assert main(["campaign", "resume", str(project), "campaign-a"]) == 1
    assert json.loads(capsys.readouterr().out)["failed"] == 1
    assert main(["campaign", "retry", str(project), "campaign-a"]) == 0
    assert json.loads(capsys.readouterr().out)["completed"] == 1


def test_real_application_executor_preserves_raw_artifact_contract(
    tmp_path: Path,
) -> None:
    project = _create(tmp_path, _definition(replicate_count=1, points=1))
    status = CampaignService(project).resume("campaign-a")
    assert (status.completed, status.failed) == (1, 0)
    state = json.loads((project / "campaign_state.json").read_text())
    run = state["runs"][0]
    artifact_root = project / "artifacts" / run["run_id"]
    run_request = json.loads((artifact_root / "run_request.json").read_text())
    expected_identity = _definition(
        replicate_count=1, points=1
    ).execution_identity
    assert expected_identity is not None
    assert run_request["execution_identity"] == expected_identity.model_dump(
        mode="json"
    )
    assert run_request["execution_identity_sha256"] == execution_identity_sha256(
        expected_identity
    )
    receipt = json.loads((artifact_root / COMPLETION_RECEIPT).read_text())
    assert receipt["schema_version"] == 2
    assert receipt["execution_identity"] == run_request["execution_identity"]
    assert (
        receipt["execution_identity_sha256"]
        == run_request["execution_identity_sha256"]
    )
    assert (artifact_root / "raw" / "run_metadata.json").is_file()
    assert (artifact_root / "raw" / "summary.parquet").is_file()
    assert all(record["sha256"] for record in run["artifacts"])
    assert run["status"] == CampaignRunStatus.COMPLETED.value


def test_legacy_completed_project_is_read_only_and_not_backfilled(
    tmp_path: Path,
) -> None:
    current = _definition(replicate_count=1, points=1)
    legacy = current.model_copy(
        update={"schema_version": 1, "execution_identity": None}
    )
    project = _create(tmp_path, legacy)
    state_path = project / "campaign_state.json"
    state = ProjectState.model_validate_json(state_path.read_bytes())
    run = state.runs[0]
    run_root = project / "artifacts" / run.run_id
    run_root.mkdir()
    contents = b"historical completed bytes"
    (run_root / "result.bin").write_bytes(contents)
    artifact = ArtifactRecord(
        path="result.bin",
        sha256=hashlib.sha256(contents).hexdigest(),
        size_bytes=len(contents),
    )
    receipt = {
        "artifacts": [artifact.model_dump(mode="json")],
        "attempt": 1,
        "run_id": run.run_id,
        "schema_version": 1,
    }
    (run_root / COMPLETION_RECEIPT).write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    completed = run.model_copy(
        update={
            "status": CampaignRunStatus.COMPLETED,
            "attempt_count": 1,
            "artifacts": [artifact],
        }
    )
    historical_state = state.model_copy(update={"runs": [completed]})
    state_path.write_text(
        historical_state.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    before = {
        path.relative_to(project).as_posix(): path.read_bytes()
        for path in sorted(project.rglob("*"))
        if path.is_file()
    }

    assert CampaignService(project).status("campaign-a").completed == 1
    with pytest.raises(ProjectCampaignError, match="provenance backfilling"):
        CampaignService(project).resume("campaign-a")
    after = {
        path.relative_to(project).as_posix(): path.read_bytes()
        for path in sorted(project.rglob("*"))
        if path.is_file()
    }
    assert after == before
