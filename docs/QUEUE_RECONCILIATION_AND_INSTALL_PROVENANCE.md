# Queue reconciliation and same-version installation provenance

## Context

Phase 1 jobs created under an older Prompt or job-identity contract remain valid SQLite rows after
a wheel upgrade. Their payload can still be `{"episode_id": ...}` even though their opaque
`dedupe_key` no longer represents the current Prompt, Schema, model, or reasoning configuration.
Blindly retrying such rows mixes two execution contracts; blindly enqueueing again can create an
old and a new job for the same episode.

The package version is not sufficient installation evidence. During the P0 repair both the old and
remediated wheels report `0.1.0`, while their `orchestration.py`, `persistence.py`, and `pipeline.py`
hashes differ. An install command that only observes the version can therefore report success while
production still executes the previous code.

## Deterministic reconciliation contract

`queue_reconciliation.py` provides two local service boundaries:

- `reconcile_phase1_queue`: requires an exact non-empty job allowlist and defaults to dry-run. It
  rejects leased or changed rows, classifies inputs without model egress, preserves the old row as
  `cancelled`, and installs a current-identity terminal barrier in one operations transaction.
- `reactivate_phase1_barriers`: requires an exact barrier allowlist plus a successful live-doctor
  result from the caller. It can reactivate only `deferred_protocol` and `stale_prompt` barriers;
  permanent oversized or policy-ineligible barriers remain terminal.

The current-identity barrier is important: normal enqueue sees its `dedupe_key` and cannot silently
recreate the same unsafe work. Re-segmentation, policy changes, Prompt changes, or model changes
produce a new identity and must pass a fresh bounded gate.

## Same-version wheel gate

Production installation must use all of these checks:

1. Build one wheel and record its SHA-256 before installation.
2. Install that exact path with `uv pip install --reinstall`; a plain version-based install is not
   sufficient for `0.1.0` to `0.1.0` replacement.
3. Build an `InstallationProvenance` manifest from the wheel-smoke installation and another from
   production `site-packages`.
4. Require byte count and SHA-256 equality for the critical loader, persistence, pipeline, queue,
   worker, Prompt, and Schema files.
5. Only after the manifest matches may a live doctor gate authorize exact deferred barriers.

The wheel SHA and both manifests should be stored with the production snapshot checkpoint. Neither
the manifest nor queue reconciliation changes canonical memory or `memory_revision`.
