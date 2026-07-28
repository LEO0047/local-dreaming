# Exact pilot scope and rollback boundary

## Current safety boundary

`dream run-pilot` validates an exact allowlist of one to three queued Phase 1 jobs. Its
lease path is restricted to those jobs and to Phase 2 jobs created from newly eligible
candidates in the selected episodes. Historical candidates are excluded by the captured
baseline, and the Phase 2 group limit is at most five.

The current implementation is intentionally **clone-only**. `run-pilot --apply` rejects the
default production runtime. Run it only with `LOCAL_DREAMING_HOME` pointing at an isolated
snapshot clone. If the process fails or is interrupted, discard the clone and start again
from the snapshot; do not resume the partial clone.

This restriction exists because the current pilot scope is held in memory. It does not yet
provide durable job-to-candidate lineage, atomic Phase 2 cohort creation, or crash-resumable
result markers. A normal nightly run can therefore remain production-safe while exact pilot
experiments stay isolated.

## Production gate

Production pilot execution remains closed until all of the following exist:

- a durable Operations DB pilot checkpoint with exact Phase 1 and Phase 2 membership;
- candidate lineage bound to the originating Phase 1 job and model call;
- a hold that prevents generic nightly consolidation from consuming pilot candidates;
- atomic or fully idempotent exact Phase 2 cohort creation;
- durable Phase 2 result markers for persistence/completion crash recovery;
- regression coverage for interruptions at every persistence and queue boundary.

## Clone procedure

1. Create a managed production snapshot.
2. Copy both SQLite databases, `forgotten.jsonl`, and the current artifact bundle into an
   owner-only temporary runtime.
3. Set `LOCAL_DREAMING_HOME` to that temporary runtime.
4. Do not copy credentials into the clone. Pass
   `--worker-runtime-home "$HOME/Library/Application Support/Local-Dreaming"` so the
   clone borrows the certified production worker boundary and shared nightly lock while all
   data writes remain in the clone databases.
5. Reconcile and reactivate only the selected clone jobs using that worker-runtime option.
6. Run `dream run-pilot --apply` with the same worker-runtime option, exact job IDs, and a
   maximum of five Phase 2 groups.
7. Review only the clone output. Never transfer claims automatically; any accepted result
   must return through Leo's normal manual review path.
