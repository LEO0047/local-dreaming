# P0 bounded pilot evaluation — 2026-07-20

## Summary

The schema 3 P0 implementation passed its technical invariants, but the single
allowlisted production pilot failed the proposal-precision gate. Production was
restored to the pre-pilot snapshot. Historical backfill must remain closed until
Phase 2 semantic deduplication and freshness handling are improved.

## Technical result

- Allowlisted episode: `ep_3b4529d137936055c4cd135e33b50627`
- Episode observations: 12 raw, 8 effective evidence families
- Loader input: 7,349 characters
- Run usage: 1 episode, 6 model calls, 53,271 input tokens, 3,618 output tokens
- Queue residue: 0
- Canonical state during pilot: revision 9, 8 claims, 8 versions
- Canonical SHA-256: `25ddf4e162d490caa2bb393f96c756b869e92f7f7ab39d14482d27e669d0b66d`
- Invariant result: pass

The evidence-family fix worked: every candidate stored only effective evidence,
and no confidence input contained the former adjacent `event_msg` / `response_item`
double observation.

## Precision result

Five pending proposals were produced. None was safe to approve as-is:

1. A Local-Dreaming goal proposal semantically duplicated an approved user-profile claim.
2. A timeline/local-state proposal blurred the memory versus operations-only boundary.
3. An AGENTS handoff proposal generalized the current bounded rule into an every-task rule.
4. An `outcome_unknown` implementation-status proposal was stale relative to current production.
5. A research-reference proposal semantically duplicated an approved project-state claim.

This is a Phase 2 quality issue, not an evidence-counting regression. Exact-slot
head comparison is insufficient when historical candidates use different subjects
or predicates for the same meaning, and historical status candidates need a
freshness check before proposal creation.

## Recovery evidence

- Pre-pilot restore point:
  `$HOME/Library/Application Support/Local-Dreaming/snapshots/<pre-pilot-snapshot>`
- Failed-pilot evidence snapshot:
  `$HOME/Library/Application Support/Local-Dreaming/snapshots/<failed-pilot-snapshot>`

After restore, production was verified at schema 3 / revision 9 with 8 canonical
claims, 14 suppressed legacy candidates, no pending proposals, no queued or leased
jobs, clean integrity and foreign-key checks, and unchanged LaunchAgent hashes.

## Required next gate

Before another production pilot:

1. Supply Phase 2 with bounded cross-slot canonical-neighbor context.
2. Add a freshness rule for historical project status and `outcome_unknown` candidates.
3. Preserve the operations-only boundary for local health and telemetry.
4. Add regression fixtures for semantic duplicate, stale status, and over-broad handoff rules.
5. Re-run the full offline gate and one new explicitly authorized bounded pilot.

## Offline remediation checkpoint

The required Phase 2 remediation is now implemented offline but is not installed in
production yet:

- Proposal evidence binding uses effective evidence-family fingerprints consistently in
  persistence, review, and canonical application. A full sink → approve → apply regression
  now covers the contract.
- Explicit candidate allowlists fail closed for unknown, suppressed, ineligible, or deferred
  candidates. The loader rechecks current eligibility before any model call, and the sink
  independently rejects out-of-job references.
- Phase 1 `outcome_unknown` candidates are atomically suppressed for the deterministic temporal
  engine instead of entering Phase 2 as new facts.
- Phase 2 receives a deterministic top-six, cross-slot canonical-neighbor context. Chinese
  2/3-character n-grams and ASCII tokens are used only for bounded recall, never for automatic
  approval or rejection. Secret claims are excluded; private claims require an approved
  `mcp_safe_summary`.
- The semantic context hash is checked at enqueue, load, persistence, review, and application.
  A cross-slot canonical change makes the old proposal stale without making every unrelated
  memory revision stale.
- Candidate evidence observation windows, canonical recorded times, operations-only rules,
  historical freshness rules, and bounded DREAMING_HANDOFF qualifiers are included in the
  Phase 2 contract.
- Model no-op decisions now preserve one auditable reason per skipped candidate:
  `semantic_duplicate`, `stale_status`, `operations_only`, `insufficient_evidence`, or
  `not_durable`.

Offline verification passed:

- `ruff format --check` and `ruff check`
- strict `mypy`
- 204 `pytest` tests
- non-editable Python 3.14.6 wheel smoke with `dream init` and `dream audit`
- isolated restore of the pre-pilot snapshot at schema 3 / revision 9 / 8 claims / 8 versions
- byte-stable SQL dump hash equality for production versus the isolated canonical clone

Production remains at schema 3 / revision 9 with eight canonical claims, no pending review
batches, no queued jobs, and a passing audit. The new wheel has not been installed and no second
production pilot has run. The next authorized step is a fresh production snapshot, wheel install,
and one bounded pilot that accumulates proposals for Leo's consolidated review rather than
requesting piecemeal decisions.

## Full deterministic history inventory

An isolated, ingest-only inventory completed all currently discovered Codex session cursors in
71 bounded batches. The final adapter response returned `truncated: false`.

- 437 JSONL session cursors reached EOF
- 8,316,379,444 raw bytes traversed while active session files could still grow
- 253,909 JSONL records traversed
- 47,163 stable, redacted events stored in the isolated clone
- 977 source descriptors: 344 task, 317 assistant-final, 314 tool-result, and 2 handoff
- 580 events contain 611 `[REDACTED_SECRET]` markers
- isolated runtime audit: pass
- isolated runtime size: 193 MiB

This inventory did not call a model, create episodes, create candidates, or modify production.
It establishes deterministic scan coverage only; it is not evidence that all history has received
semantic review.

## Remaining model gate

One isolated remediation replay reviewed only the five candidates from the failed pilot. It
correctly suppressed the insufficient-evidence completion claim and the duplicate research claim,
but three proposals still required deterministic guards. Those guards were added offline. The
verification replay then failed closed before model output because the Codex account returned:

```text
You've hit your usage limit. Visit https://chatgpt.com/codex/settings/usage to purchase more credits or try again at Jul 27th, 2026 12:31 AM.
```

Therefore the semantic fixes have complete static and regression coverage but do not yet have a
second live-model result. Full-history model extraction remains closed until the limit resets and
a freshly authorized production snapshot, wheel install, and bounded pilot pass. Canonical memory
remains manual-approval-only.

## Production operations recheck — 2026-07-22

A read-only production recheck found that canonical state is still intact, but the old production
wheel has continued to run scheduled nightly jobs:

- schema 3, memory revision 9, 8 claims, and 8 claim versions
- canonical SQL dump SHA-256 remains
  `4c0e3da14375fa2a634eb6b0d271d3300840c6dc61eff93ed62054e5a0c12c97`
- no pending review batches; audit passes
- latest nightly failed closed with `OversizedInputError`, zero episodes, and zero model calls
- eight Phase 1 jobs remain queued: three `OversizedInputError` and five `WorkerProtocolError`
- the previous nightly recorded five quarantined Phase 1 calls and created no candidates or
  proposals

Two queued episodes contain 12,341 text characters each. A third contains 11,425 characters but
still exceeds the 12,000-character loader gate after the deterministic envelope is added. This
confirms that the gate must cover the complete model input, not only event content.

Production backfill therefore remains closed. The recommended next operation, requiring fresh Leo
authorization, is to pause nightly retries, create a new snapshot, install the remediated wheel,
and reconcile the queued jobs through the new allowlist logic before one 1–3 episode pilot. No
LaunchAgent, queue row, LINE data, canonical memory, or Codex built-in memory was modified during
this recheck.
