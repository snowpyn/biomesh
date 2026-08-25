# P6-WP04 Operational Migration

This is the operator contract for BioMesh 0.6.0 portable operations. Migration
transfers path-free queued campaign intent and, separately, portable project
archives. It does not transfer a runnable queue, source scheduler identity,
credentials, trust, authorization, calibration status, process/lock state,
resource receipts, or completed artifacts through queue-intent records.

## Supported migration matrix

| Source or record | Version | Supported BioMesh 0.6.0 operation | Destination/restart behavior | Unsupported direction |
| --- | --- | --- | --- | --- |
| P4 local queue | queue schema 1 | `queue export-intent`; queued campaigns only. Clean installs first reproduce queued projects through archives in an explicit staging queue | Read-only stable snapshot; active or stale-running work must be resolved first | No direct queue copy/import, terminal-history migration, or P6-to-P4 downgrade |
| P6 portable intent | `biomesh-portable-queue-intent`, schema 1, BioMesh 0.6.0 | `queue migration-status`, `queue import-intent` | Deterministic `UNBOUND` record; retry failure with a new output | No unknown schema, noncanonical JSON, different BioMesh version, or older-runtime import |
| P6 import record | `biomesh-portable-queue-import`, schema 1 | `queue migration-status`, `queue bind-intent` | Deterministic complete `BOUND_NONRUNNABLE` record | No partial binding, implicit path, source-resource reuse, or reverse conversion |
| P6 local binding | `biomesh-portable-queue-local-binding`, schema 1 | `queue migration-status`, `queue activate-intent` | One fresh P4 queue plus schema-1 `portable_activation.json` | No duplicate activation, target reuse, automatic rebind, or downgrade to unbound |
| P6 activation | `biomesh-portable-queue-activation`, schema 1 | `queue migration-status` on its queue; P4 queue commands afterward | New destination scheduler IDs; restart uses P4 stale-worker reconciliation | No source PID, lock, lifecycle/failure state, counter, or resource receipt restoration |
| P4 portable project archive | archive/project schema 2 | Raw verification/import only with explicit `--allow-unauthenticated`; secure-envelope paths preferred | Pending campaigns resume after exact import; completed artifact bytes stay immutable | No archive-to-queue conversion, trust grant, credential transfer, or downgrade |
| Historical P4 archive/project | schema 1 | Verify/import completed historical results with explicit unauthenticated policy | Completed bytes remain readable and unchanged | Unfinished projects cannot be exported as executable or resumed; identity is never backfilled |
| P5 secure archive envelope | `biomesh-secure-project-archive`, envelope schema 1 around exact P4 bytes | `project verify-secure-archive` and `project import-secure-archive` with host policy/keys | Verification precedes publication; imported security status remains provenance | No key migration, archive-content trust, algorithm downgrade, or silent raw fallback |

P6 schema-1 records bind `biomesh_version` to `0.6.0`; mismatches fail closed.
There is no version negotiation, automatic upgrade/downgrade, or future-schema
support. P4 queue schema 1 remains the local execution substrate. Activation
creates a fresh instance instead of converting source queue state.

## Declare paths and destination policy

Record absolute, distinct paths for the source queue/projects; one new archive
per project; one new intent, import record, binding record, destination queue,
and source/destination report; and one new destination project path per source
`project_id`. Declare a positive CPU-core count and memory limit in bytes.

Every output must be new. Do not nest an intent in its queue/project, a binding
in a bound project, a destination queue in a bound project, or a report in raw
artifacts. Symlinked, relative, traversing, guessed, and reused paths fail.

## Source verify, portable staging, and export

`queue status` may reconcile an exact stale worker through accepted P4
recovery. Review its audit/campaign counts before export.

```bash
python -m biomesh --version
python -m biomesh queue status /srv/biomesh/source-queue
python -m biomesh campaign status /srv/biomesh/project-a campaign-a
python -m biomesh project export /srv/biomesh/project-a \
  --output /srv/biomesh/transfer/project-a.biomesh
python -m biomesh project verify-archive \
  /srv/biomesh/transfer/project-a.biomesh --allow-unauthenticated
```

Raw archives are explicitly `UNAUTHENTICATED`. When a secure envelope exists,
use destination-owned trust and optional decryption inputs:

```bash
python -m biomesh project verify-secure-archive project-a.secure.biomesh \
  --trust-policy /etc/biomesh/archive-trust.json \
  --recipient-id destination-a \
  --recipient-private-key /run/keys/destination-a.x25519 \
  --require-confidentiality
```

Keys/policies stay outside the transfer and repository. Source verification
does not grant destination trust.

For a clean destination, the queued project definition must first be converted
through the same archive import path on the source. Archive import deliberately
rewrites external fixture paths to contained portable paths and therefore has a
new definition hash. Import each archive to a new source staging project, then
construct a new staging queue containing only the queued campaigns. Copy the
integer priorities reported by the original queue. For equal priority, enqueue
in the original FIFO order. Do not enqueue terminal or active work.

```bash
python -m biomesh project import \
  /srv/biomesh/transfer/project-a.biomesh \
  /srv/biomesh/transfer/source-portable-project-a \
  --allow-unauthenticated
python -m biomesh queue create \
  /srv/biomesh/transfer/source-portable-queue \
  --cpu-cores 1 --memory-limit-bytes 8589934592
python -m biomesh queue enqueue \
  /srv/biomesh/transfer/source-portable-queue \
  /srv/biomesh/transfer/source-portable-project-a campaign-a --priority 7
python -m biomesh queue status \
  /srv/biomesh/transfer/source-portable-queue
```

For secure input, use `project import-secure-archive` for both source staging
and destination with their own host-owned policy/key arguments. The resulting
archive provenance must match exactly. Direct export from the original queue is
supported only when every destination project already has byte-identical
definition/state/dependency bytes and identical archive provenance. It is not
the clean-install path.

Preflight the staging queue, then publish to the same new intent path:

```bash
python -m biomesh queue export-intent \
  /srv/biomesh/transfer/source-portable-queue \
  --output /srv/biomesh/transfer/queue-intent.json --dry-run
test ! -e /srv/biomesh/transfer/queue-intent.json
python -m biomesh queue export-intent \
  /srv/biomesh/transfer/source-portable-queue \
  --output /srv/biomesh/transfer/queue-intent.json
python -m biomesh queue migration-status \
  /srv/biomesh/transfer/queue-intent.json
```

Dry-run computes the would-be canonical SHA-256/count but creates no output.
Export includes queued intent in priority-descending/FIFO order. Completed,
failed, and cancelled history is not migrated.

## Clean destination import, bind, and activate

Install BioMesh 0.6.0. Transfer archives and intent separately; never copy a
project directory, queue, lock, worker state, credential, or report as a
substitute for these paths. For explicitly accepted raw input:

```bash
python -m biomesh --version
python -m biomesh project verify-archive project-a.biomesh \
  --allow-unauthenticated
python -m biomesh project import project-a.biomesh \
  /srv/biomesh/destination/project-a --allow-unauthenticated
```

For a secure envelope, use `project import-secure-archive` with the same
destination-owned trust/decryption inputs used for secure verification.

```bash
python -m biomesh queue migration-status queue-intent.json
python -m biomesh queue import-intent queue-intent.json \
  --output /srv/biomesh/destination/unbound.json --dry-run
test ! -e /srv/biomesh/destination/unbound.json
python -m biomesh queue import-intent queue-intent.json \
  --output /srv/biomesh/destination/unbound.json
python -m biomesh queue migration-status \
  /srv/biomesh/destination/unbound.json
```

Bind every source project ID once. The example requests two available CPU
cores and 8 GiB address space as destination policy, not transported values or
a performance recommendation.

```bash
python -m biomesh queue bind-intent \
  /srv/biomesh/destination/unbound.json \
  --output /srv/biomesh/destination/bound.json \
  --project-binding project-a=/srv/biomesh/destination/project-a \
  --project-binding project-b=/srv/biomesh/destination/project-b \
  --cpu-cores 2 --memory-limit-bytes 8589934592 --dry-run
test ! -e /srv/biomesh/destination/bound.json
python -m biomesh queue bind-intent \
  /srv/biomesh/destination/unbound.json \
  --output /srv/biomesh/destination/bound.json \
  --project-binding project-a=/srv/biomesh/destination/project-a \
  --project-binding project-b=/srv/biomesh/destination/project-b \
  --cpu-cores 2 --memory-limit-bytes 8589934592
python -m biomesh queue migration-status \
  /srv/biomesh/destination/bound.json
```

The binding remains non-runnable. Destination archive/plugin/registry trust
and authorization are `NOT_GRANTED`; calibration is `CALIBRATION_REQUIRED`.

```bash
python -m biomesh queue activate-intent \
  /srv/biomesh/destination/bound.json \
  /srv/biomesh/destination/queue --dry-run
test ! -e /srv/biomesh/destination/queue
python -m biomesh queue activate-intent \
  /srv/biomesh/destination/bound.json \
  /srv/biomesh/destination/queue
python -m biomesh queue migration-status \
  /srv/biomesh/destination/queue
python -m biomesh queue status /srv/biomesh/destination/queue
```

`migration-status` canonical-verifies an intent, import, binding, or activation
and reports type, version, SHA-256, item count, and project count. On a queue it
also checks queue identity/resource policy against activation. It is read-only
and does not reconcile workers. `queue status` remains the P4 execution-status
and stale-worker recovery surface.

## Execute, cancel, retry, and recover

```bash
python -m biomesh queue run /srv/biomesh/destination/queue --once
python -m biomesh queue status /srv/biomesh/destination/queue
python -m biomesh queue cancel /srv/biomesh/destination/queue \
  queue-00000000-campaign-a
python -m biomesh queue status /srv/biomesh/destination/queue
```

Cancellation preserves completed publications. Interrupted unpublished work
becomes an explicit retryable failure. Terminal `cancelled` requires one
durable campaign cancellation acknowledgement: the persisted running attempt,
or the next pending attempt if SIGTERM lands after queue claim or between run
publications. The exact worker is signalled through a Linux pidfd only after
its persisted PID/process-start identity matches; there is no startup sleep.
After crash/restart, `queue status` verifies the dead worker identity and
reconciles completed/cancelled/interrupted state without duplicating the
acknowledgement. If `SIGKILL` left one exact empty direct-child artifact stage
for the persisted running run, this supported recovery validates all artifact
paths and completed bytes, removes only that inode-checked empty directory with
`rmdir`, and then records `interrupted`. It does not delete partial contents or
guess ownership.
Inspect, then retry only retained failed work:

```bash
python -m biomesh campaign status \
  /srv/biomesh/destination/project-a campaign-a
python -m biomesh queue retry /srv/biomesh/destination/queue \
  queue-00000000-campaign-a
python -m biomesh queue run /srv/biomesh/destination/queue --once
```

Completed queue items are never retryable. For non-queue P4 campaigns,
`campaign resume` and selected `campaign retry --run-id RUN_ID` remain
supported; never use them concurrently with an active queue worker.

## Report trace comparison

```bash
python -m biomesh campaign report /srv/biomesh/source-project campaign-a \
  --output /srv/biomesh/source-report \
  --portable-binding /srv/biomesh/destination/bound.json
python -m biomesh campaign report /srv/biomesh/destination/project-a campaign-a \
  --output /srv/biomesh/destination-report \
  --portable-binding /srv/biomesh/destination/bound.json
```

Compare `report_data.json` fields under `portable_traceability`: manifest/item,
project/archive, campaign, experiment/fixture, execution/model, parameter/
plugin, and corresponding run identities must agree. Review `environment` and
`portable_traceability.destination_environment` separately; host, platform,
Python, and destination project metadata may differ. Compare artifact hashes,
sizes, and immutable bytes, not environment-bearing report JSON bytes. Trace
agreement is reproducibility evidence, not calibration/scientific validation.

## Explicit failure examples

Failures return status 2 with an actionable diagnostic and must leave sources
and the declared new target unchanged.

| Failure | Reproduction | Recovery decision |
| --- | --- | --- |
| Schema incompatibility | `queue migration-status future-schema.json` or import an unknown schema | Obtain a canonical schema-1 BioMesh 0.6.0 export; never edit version fields |
| Changed inputs | Dry-run, then change fixture/parameter/project/archive/import/binding bytes | Stable revalidation rejects drift; restore exact bytes or restart export |
| Path/resource conflict | Existing/symlinked/nested target, unavailable CPUs, or nonpositive memory | Declare a new safe path and complete feasible destination policy |
| Incomplete binding | Omit one required `--project-binding` | Supply every source project ID exactly once |
| Duplicate activation | Activate again into the existing destination queue | `activation queue already exists`; inspect it and retain history |
| Trust/authorization non-transfer | Inspect a binding imported from an authenticated source | Source status is provenance; destination fields remain `NOT_GRANTED` |
| Calibration non-promotion | Inspect any imported/bound item | It remains `CALIBRATION_REQUIRED`; migration cannot promote it |
| Unsafe interrupted stage | `queue status` reports a symlink, non-directory, nonempty/nested, malformed, ambiguous, unknown/non-running/completed-run, or staging/canonical conflict | Preserve the project and operator files unchanged; do not manually rename/delete or retry. Resolve ownership externally, then rerun recovery from an exact safe state |
| Unsupported downgrade/migration | Use older/different runtime, future schema, or copy a P4 queue | Use matching 0.6.0 schema-1 commands and fresh destination state |

## Atomicity, artifact separation, and limitations

Export, record publication, project import, report publication, and activation
publish only after complete validation. Temporary siblings are removed on
failure; targets are never overwritten. Dry-run performs validation/stable
revalidation but skips publication.

Project archives carry project state and exact completed artifacts. Intent
carries queued identities only. Binding adds explicit paths/policy. Activation
adds fresh queue state/provenance. Completed artifacts stay in their separate
project roots and are never copied into queue records, rewritten, or rerun.

- Operation is Linux-local and operator-mediated; no remote/cloud scheduler,
  automatic transfer/path guessing, service, credential transfer, or UI exists.
- Host authentication, encryption, authorization, supervision, storage, and
  backup policy stay outside BioMesh.
- Live PID/lock/resource state and source cancellation/failure history do not
  transfer. Exact BioMesh 0.6.0/schema-1 compatibility is required.
- Resource limits are policy/feasibility constraints, not benchmark results.
- Only an exact empty stage has enough ownership evidence for automatic
  reconciliation. A stage containing any partial output remains untouched and
  blocks status/report/export until an operator resolves ownership; supporting
  authenticated nonempty-stage recovery would require a new governed journal
  and is outside P6A-001.
- Orderly cancellation can remove a state-write temporary only while the live
  write still proves the exact regular device/inode it created. A pre-existing
  or crash-left `.campaign_state.json.*` sibling has no authenticated durable
  ownership evidence, remains untouched, and blocks cancellation/recovery.
  Supporting automatic reconciliation of such state would require a new
  governed ownership journal and is outside P6A-002.
- All biology remains `CALIBRATION_REQUIRED`; no calibration is promoted.
- P6A is a separate independent audit. `v0.6.0` is its historical frozen
  baseline; after P6A-001, the fresh rerun prerequisite is the exact pushed
  remediation commit. Neither the tag nor remediation accepts Phase 6.

## Validation evidence

Python 3.14.4 passed 23 focused P6 documentation/CLI/migration tests, the
60-test P6/P4 queue/project/archive/report regression collection, and the full
suite with 375 passed and no failures or skips. The same gate passed host-level
Bubblewrap/libseccomp coverage, Ruff, strict mypy over 72 source files, module
help, documented CLI help/examples or automated equivalents, and diff checks.

A canonical provenance build from a disposable clean commit of the
implementation tree under test produced BioMesh 0.6.0 wheel/sdist/installer
artifacts. The freshly
installed wheel completed the documented archive staging, verify/import, all
four dry-run and publication paths, activation/status/execution, and source/
destination report comparison. Portable report content matched after the
separately recorded environment fields were removed, and project paths were
distinct. This is software portability evidence, not P6A acceptance,
biological calibration, scientific validation, or a performance benchmark.

### P6A-001 remediation rehearsal

The independent frozen-release reproduction built and clean-installed the
canonical `v0.6.0` wheel, killed the exact active destination worker with real
`SIGKILL` after completed and persisted-running work coexisted, and confirmed
the original failure: stale recovery and explicit retry completed 40/40 runs
with prior bytes unchanged and reporting successful, but the empty hidden
stage remained and `project export` rejected the unexpected artifact path.

The post-remediation replay built a new provenance-bound BioMesh 0.6.0 wheel
from a disposable clean commit containing exactly the current two production
remediation files. The installed package was loaded only from its fresh Python
3.14.4 environment with no checkout `PYTHONPATH`. Exact PID/process-start
identity and the empty direct stage were revalidated before `SIGKILL` returned
-9. `queue status` removed that stage and recorded `interrupted`; explicit
retry and a fresh worker preserved every earlier A/B completed byte, completed
only the interrupted run at attempt 2, and kept all project/run/portable
identities separated. The 40-run report completed with no missing runs.

Both completed projects then passed the supported archive closure. Project A
exported/verified/imported 1 completion and 25 files at archive SHA-256
`f3c53334eb59a230ecf6f5e6df14d10edb8ecd1e67b4e615360c5c45e37b3f12`;
Project B exported/verified/imported 40 completions and 688 files at
`15ff8048369ebfbfac3e550878dc6f2e170e722245a26007240a76e61f0d9097`.
Both imported artifact trees were byte-equal. The installed wheel SHA-256 was
`d9a4aea63707ecc2248a636563711aed7f9f0fcaa1e5e102e2f51e7852f6b3f3`.

The final source gate passed the 75-test P6/P4 remediation collection, the
unchanged 118-test P5 collection, the 366-test accepted P1-P5 collection, and
all 390 tests with no failures or skips, plus host-level sandbox checks, Ruff,
strict mypy over 72 source files, module help, documented examples or automated
equivalents, and diff checks. P6A remains `INCOMPLETE`; these records require a
fresh independent P6A rerun from the exact pushed remediation commit and do not
accept Phase 6 or authorize P7.

### P6A-002 remediation rehearsal

A deterministic barrier first reproduced terminal queue cancellation with a
campaign at `1 completed / 1 pending / 0 failed`, followed by the expected
retry rejection. The remediation tests then exercised every queue/campaign
publication window, including the campaign-state temporary write and queue
terminal replace. Each window produced exactly one explicit retryable
`cancelled` attempt, no partial state publication, no owned temporary sibling,
and completion of only remaining work at attempt 2 after explicit retry.
Unowned empty, nonempty, malformed, and symlinked campaign-state siblings
failed without deletion or state transition.

The focused deterministic suite passed 25 consecutive repetitions and the real
subprocess SIGTERM/exact-PID-start/retry/fresh-worker path passed 10 consecutive
repetitions. Python 3.14.4 passed 85 P6/P4 tests, the unchanged 118-test P5
collection, 366 accepted P1-P5 tests, and the 400-test complete suite with no
failures or skips. Bubblewrap 0.11.1, util-linux `prlimit` 2.41.3,
libseccomp 2.6.0, and all 22 sandbox tests passed. Runtime/package/generated
manifest version remains 0.6.0. This does not accept P6A or authorize P7.
