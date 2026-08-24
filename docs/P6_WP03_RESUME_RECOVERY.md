# P6-WP03 Resume, Retry, and Recovery Across Hosts

P6-WP03 activates only a complete P6-WP02 `BOUND_NONRUNNABLE` record. It does
not change the canonical P6-WP01 manifest or P6-WP02 import/binding bytes.
Activation is an explicit destination operation that creates a fresh local P4
queue; import and binding remain non-runnable.

## Versioned activation boundary

The schema-version 1 `biomesh-portable-queue-activation` record is published
as `portable_activation.json` inside the new queue. It embeds the exact
canonical binding and its import/manifest hashes, the verified destination
resource policy, and one ordered mapping from each portable intent sequence to
a newly allocated local queue ID. It contains no source PID, process-start
identity, lock, cancellation/running/failure state, old queue ID/counter,
source resource receipt, local source queue path, authorization, trust grant,
or calibration promotion.

`queue activate-intent BOUND.json DESTINATION_QUEUE` validates the complete
binding, every bound project/campaign/fixture/execution/parameter/archive
identity, pending eligibility, and stable source/bound inputs while holding
all bound project locks. It then stages the P4 queue state, queue locks, worker
lock, and activation record and publishes the new queue atomically. Existing
targets, unsafe paths, incompatible/canonicality failures, changed inputs, or
publication races fail without a target. An activated queue cannot accept an
unbound local enqueue or mix another project.

`queue activate-intent BOUND.json DESTINATION_QUEUE --dry-run` performs the
same complete validation and stable revalidation without creating the queue.
After publication, `queue migration-status DESTINATION_QUEUE` read-only
verifies the canonical activation record against local queue identity and
resource policy; it does not perform worker recovery.

## Execution, retry, and recovery

After activation, the existing P4 worker lock, queue-state lock, project
campaign lock, OS CPU/memory enforcement, campaign resume, immutable artifact
publication, stale-worker reconciliation, cancellation, and explicit failed
run records remain the only execution boundaries. The destination queue gets
new scheduler IDs and receipts; source scheduler identity is not reused.

`queue retry DESTINATION_QUEUE QUEUE_ID` only requeues an explicit failed or
cancelled queue item with retained failed campaign runs. The next worker
invocation calls the existing campaign retry boundary, which schedules failed
runs only and never rewrites or reruns completed artifacts. Terminal completed
items are not retryable. Stale workers produce deterministic queue and campaign
audit transitions through the existing recovery path.

A hard-killed worker can bypass its normal staging cleanup. Stale recovery now
removes only one exact empty direct-child stage mapped to the requested
campaign's known `running` run, after the complete artifact layout and all
completed bytes validate and after device/inode/emptiness revalidation. It
uses only `rmdir`, then records the retryable interruption; retry remains a
separate operator action. Symlinked, non-directory, nonempty/nested,
malformed, ambiguous, unknown-run, non-running/completed-run, or
canonical-directory lookalikes fail with no deletion or campaign/queue state
mutation.

New destination run requests and completion receipts carry a strict portable
trace containing the portable manifest/item, source project-definition and
archive provenance, project/campaign/experiment/fixture, execution/model/
parameter/plugin identities, and run identity. Optional report generation can
receive the canonical binding with `campaign report --portable-binding`; its
portable traceability section keeps those identities separate from destination
host/platform/Python/project environment metadata.

The completion receipt remains eligible for later P4 project export,
verification, and import. Campaign and archive verification share the exact
legacy/current receipt field sets: a current traced receipt permits only the
`portable_trace` object in addition to required fields, while invalid trace
types or unknown extensions are rejected.

## Validation evidence

On Python 3.14.4, the focused P6-WP03 collection passed 4 tests covering clean
CLI activation, atomic publication and conflicts, concurrent activation,
queue/project separation, completed-artifact retry immutability, tampered
binding failure atomicity, run receipt traceability, and report traceability.
The complete suite passed 372 tests with no failures, skips, or errors.
Ruff, strict mypy over 71 source files, module help, and `git diff --check`
passed in the isolated Python 3.14 environment. The P4 queue/project/report/
archive and P6-WP01/P6-WP02 regression tests are included in that full gate.

P6-WP04 operational migration documentation and P6A independent audit remain
incomplete. No cloud/remote scheduler, credential transfer, UI, scientific
change, calibration promotion, or 3D/acceleration behavior is included.

## P6A-001 production-remediation evidence

The frozen `v0.6.0` audit found that a real `SIGKILL` could leave one empty
campaign stage behind after stale recovery, explicit retry, and successful
reporting, causing the later strict project export to fail. The remediation's
focused subprocess regression uses two separately archived/imported projects,
fresh portable binding/activation and scheduler identity, exact PID plus Linux
process-start verification, and a real worker `SIGKILL`. Stale status removes
the exact empty stage and records attempt-1 `interrupted`; explicit retry plus
a fresh unpatched worker completes only that run at attempt 2. Prior project
state, completed artifacts/receipts, queue audit prefixes, and portable records
remain byte-equal; projects and trace identities remain disjoint; reports and
both project export/verify/import round trips pass.

A separate clean installed-wheel replay used BioMesh 0.6.0 from a disposable
clean commit containing exactly the two production remediation files. It killed
worker PID 696226 with return code -9 after exact process-start verification,
recovered Project B to 2 completed/1 interrupted/37 pending with the stage
absent, then completed 40/40 after explicit retry while Project A remained
1/1. All 17 prior A files and 34 prior B files retained their hashes; the
40-run report had no missing run. Project A and B archives verified/imported at
1 and 40 completions with byte-equal artifact trees. The wheel SHA-256 was
`d9a4aea63707ecc2248a636563711aed7f9f0fcaa1e5e102e2f51e7852f6b3f3`.

Python 3.14.4 passed the 75-test P6/P4 remediation collection and 390-test full
suite with no failures or skips, plus Ruff, strict mypy over 72 source files,
module help, and diff checks. This remediates P6A-001 only; P6A remains
`INCOMPLETE` and requires a fresh independent audit from the exact pushed
remediation commit.
