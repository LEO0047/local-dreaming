from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from local_dreaming.config import ModelSettings
from local_dreaming.database import utc_now
from local_dreaming.errors import OversizedInputError, PrivacyBoundaryError
from local_dreaming.orchestration import (
    _PHASE1_JOB_IDENTITY_VERSION,
    DatabaseJobLoader,
)
from local_dreaming.pipeline import PHASE1_SCHEMA_VERSION, PROMPT_VERSION, PipelinePhase
from local_dreaming.storage import MemoryStore, OperationsStore, fingerprint

_RECONCILIATION_PREFIX = "queue-reconciliation-v1"


class Phase1ReconciliationDisposition(StrEnum):
    """Bounded reasons for retaining, deferring, or retiring one Phase 1 job."""

    CURRENT_READY = "current_ready"
    PERMANENT_OVERSIZED = "permanent_oversized"
    PERMANENT_INELIGIBLE = "permanent_ineligible"
    DEFERRED_PROTOCOL = "deferred_protocol"
    STALE_PROMPT = "stale_prompt"
    ALREADY_RECONCILED = "already_reconciled"


@dataclass(frozen=True, slots=True)
class Phase1ReconciliationItem:
    job_id: str
    episode_id: str
    status: str
    attempts: int
    max_attempts: int
    old_dedupe_key: str
    current_dedupe_key: str
    disposition: Phase1ReconciliationDisposition
    reason_code: str
    barrier_job_id: str | None
    row_fingerprint: str

    @property
    def requires_transition(self) -> bool:
        return self.disposition not in {
            Phase1ReconciliationDisposition.CURRENT_READY,
            Phase1ReconciliationDisposition.ALREADY_RECONCILED,
        }


@dataclass(frozen=True, slots=True)
class Phase1ReconciliationReport:
    items: tuple[Phase1ReconciliationItem, ...]
    dry_run: bool
    transitioned_job_ids: tuple[str, ...] = ()
    barrier_job_ids: tuple[str, ...] = ()


def _canonical_payload(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _row_fingerprint(row: Mapping[str, Any]) -> str:
    return fingerprint(
        "phase1-reconciliation-row-v1",
        row["job_id"],
        row["job_type"],
        row["dedupe_key"],
        row["payload_json"],
        row["status"],
        row["attempts"],
        row["max_attempts"],
        row["available_at"],
        row["lease_owner"],
        row["lease_expires_at"],
        row["last_error"],
    )


def _barrier_job_id(dedupe_key: str) -> str:
    return f"job_reconcile_{dedupe_key[:32]}"


def _bounded_reason(
    disposition: Phase1ReconciliationDisposition,
    *,
    reason_code: str,
    peer_job_id: str,
) -> str:
    return (
        f"{_RECONCILIATION_PREFIX}:{disposition.value}:reason={reason_code}:peer={peer_job_id}"
    )[:2000]


def _current_phase1_dedupe_key(
    memory: MemoryStore,
    episode_id: str,
    models: ModelSettings,
) -> str:
    with memory.connection() as connection:
        row = connection.execute(
            """
            SELECT ep.episode_id, ep.content_fingerprint, ep.segmenter_version,
                   s.policy_version
            FROM episodes AS ep
            JOIN sources AS s ON s.source_id = ep.source_id
            WHERE ep.episode_id = ?
            """,
            (episode_id,),
        ).fetchone()
    if row is None:
        raise ValueError(f"phase1 job references an unknown episode: {episode_id}")
    return fingerprint(
        _PHASE1_JOB_IDENTITY_VERSION,
        row["episode_id"],
        row["content_fingerprint"],
        row["segmenter_version"],
        row["policy_version"],
        PROMPT_VERSION,
        PHASE1_SCHEMA_VERSION,
        models.phase1_model,
        models.phase1_reasoning,
    )


def _load_exact_rows(
    operations: OperationsStore,
    job_allowlist: Sequence[str],
) -> list[dict[str, Any]]:
    requested = tuple(dict.fromkeys(str(job_id) for job_id in job_allowlist))
    if not requested:
        raise ValueError("phase1 reconciliation requires a non-empty exact job allowlist")
    if len(requested) != len(job_allowlist):
        raise ValueError("phase1 reconciliation job allowlist contains duplicates")
    placeholders = ",".join("?" for _ in requested)
    with operations.connection() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                f"SELECT * FROM jobs WHERE job_id IN ({placeholders}) ORDER BY job_id",
                requested,
            ).fetchall()
        ]
    found = {str(row["job_id"]) for row in rows}
    missing = sorted(set(requested) - found)
    if missing:
        raise ValueError(f"phase1 reconciliation allowlist contains unknown jobs: {missing}")
    return rows


def _classify_row(
    memory: MemoryStore,
    row: Mapping[str, Any],
    *,
    models: ModelSettings,
) -> Phase1ReconciliationItem:
    job_id = str(row["job_id"])
    if str(row["job_type"]) != PipelinePhase.PHASE1.value:
        raise ValueError(f"reconciliation job is not phase1: {job_id}")
    if str(row["status"]) == "leased" or row["lease_owner"] is not None:
        raise ValueError(f"reconciliation refuses a leased job: {job_id}")

    try:
        payload = json.loads(str(row["payload_json"]))
    except json.JSONDecodeError as exc:
        raise ValueError(f"phase1 job has invalid JSON payload: {job_id}") from exc
    if not isinstance(payload, dict) or set(payload) != {"episode_id"}:
        raise ValueError(f"phase1 job payload is not the exact legacy/current shape: {job_id}")
    episode_id = payload["episode_id"]
    if not isinstance(episode_id, str) or not episode_id:
        raise ValueError(f"phase1 job episode_id is invalid: {job_id}")

    current_key = _current_phase1_dedupe_key(memory, episode_id, models)
    old_key = str(row["dedupe_key"])
    status = str(row["status"])
    last_error = "" if row["last_error"] is None else str(row["last_error"])
    barrier_id = job_id if old_key == current_key else _barrier_job_id(current_key)

    if status == "cancelled" and last_error.startswith(_RECONCILIATION_PREFIX):
        return Phase1ReconciliationItem(
            job_id=job_id,
            episode_id=episode_id,
            status=status,
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            old_dedupe_key=old_key,
            current_dedupe_key=current_key,
            disposition=Phase1ReconciliationDisposition.ALREADY_RECONCILED,
            reason_code="already_reconciled",
            barrier_job_id=barrier_id,
            row_fingerprint=_row_fingerprint(row),
        )
    if status != "queued":
        raise ValueError(f"reconciliation requires queued or reconciled jobs: {job_id}={status}")

    leased_record = dict(row)
    leased_record["payload"] = payload
    try:
        DatabaseJobLoader(memory.path).load(leased_record)
    except OversizedInputError:
        disposition = Phase1ReconciliationDisposition.PERMANENT_OVERSIZED
        reason_code = "model_input_over_12000_chars"
    except PrivacyBoundaryError, ValueError, KeyError:
        disposition = Phase1ReconciliationDisposition.PERMANENT_INELIGIBLE
        reason_code = "loader_reference_or_policy_ineligible"
    else:
        if old_key == current_key and last_error != "WorkerProtocolError":
            disposition = Phase1ReconciliationDisposition.CURRENT_READY
            reason_code = "current_identity_and_loader_ready"
        elif last_error == "WorkerProtocolError":
            disposition = Phase1ReconciliationDisposition.DEFERRED_PROTOCOL
            reason_code = "live_doctor_and_exact_reactivation_required"
        else:
            disposition = Phase1ReconciliationDisposition.STALE_PROMPT
            reason_code = "stale_prompt_identity_requires_exact_reactivation"

    return Phase1ReconciliationItem(
        job_id=job_id,
        episode_id=episode_id,
        status=status,
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        old_dedupe_key=old_key,
        current_dedupe_key=current_key,
        disposition=disposition,
        reason_code=reason_code,
        barrier_job_id=barrier_id,
        row_fingerprint=_row_fingerprint(row),
    )


def plan_phase1_queue_reconciliation(
    memory: MemoryStore,
    operations: OperationsStore,
    *,
    job_allowlist: Sequence[str],
    models: ModelSettings | None = None,
) -> Phase1ReconciliationReport:
    """Build a content-free, deterministic plan without changing either database."""

    active_models = models or ModelSettings()
    rows = _load_exact_rows(operations, job_allowlist)
    items = tuple(_classify_row(memory, row, models=active_models) for row in rows)

    current_keys = [item.current_dedupe_key for item in items]
    if len(set(current_keys)) != len(current_keys):
        raise ValueError("phase1 reconciliation would merge distinct allowlisted jobs")
    requested_ids = {item.job_id for item in items}
    placeholders = ",".join("?" for _ in current_keys)
    with operations.connection() as connection:
        conflicts = connection.execute(
            f"SELECT * FROM jobs WHERE dedupe_key IN ({placeholders}) ORDER BY job_id",
            current_keys,
        ).fetchall()
    for conflict in conflicts:
        conflict_id = str(conflict["job_id"])
        if conflict_id in requested_ids:
            continue
        expected_barriers = {
            item.barrier_job_id
            for item in items
            if item.current_dedupe_key == str(conflict["dedupe_key"])
        }
        if (
            conflict_id not in expected_barriers
            or str(conflict["status"]) != "cancelled"
            or not str(conflict["last_error"] or "").startswith(_RECONCILIATION_PREFIX)
        ):
            raise ValueError(
                f"current phase1 identity already has a non-barrier job: {conflict_id}"
            )
    return Phase1ReconciliationReport(items=items, dry_run=True)


def reconcile_phase1_queue(
    memory: MemoryStore,
    operations: OperationsStore,
    *,
    job_allowlist: Sequence[str],
    models: ModelSettings | None = None,
    dry_run: bool = True,
) -> Phase1ReconciliationReport:
    """Atomically retire stale jobs and install current-identity terminal barriers."""

    active_models = models or ModelSettings()
    plan = plan_phase1_queue_reconciliation(
        memory,
        operations,
        job_allowlist=job_allowlist,
        models=active_models,
    )
    if dry_run:
        return plan

    transitioned: list[str] = []
    barriers: list[str] = []
    now = utc_now()
    with memory.maintenance_lock(), operations.transaction() as connection:
        for item in plan.items:
            current = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (item.job_id,)
            ).fetchone()
            if current is None or _row_fingerprint(current) != item.row_fingerprint:
                raise ValueError(
                    f"phase1 reconciliation job changed after preflight: {item.job_id}"
                )
            refreshed = _classify_row(memory, current, models=active_models)
            if (
                refreshed.current_dedupe_key != item.current_dedupe_key
                or refreshed.disposition is not item.disposition
                or refreshed.reason_code != item.reason_code
            ):
                raise ValueError(
                    f"phase1 reconciliation memory binding changed after preflight: {item.job_id}"
                )
        for item in plan.items:
            if not item.requires_transition:
                continue
            assert item.barrier_job_id is not None
            barrier_id = item.barrier_job_id
            old_reason = _bounded_reason(
                item.disposition,
                reason_code=item.reason_code,
                peer_job_id=barrier_id,
            )
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = 'cancelled', lease_owner = NULL, lease_expires_at = NULL,
                    last_error = ?, updated_at = ?
                WHERE job_id = ? AND status = 'queued' AND lease_owner IS NULL
                """,
                (old_reason, now, item.job_id),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"phase1 reconciliation transition raced: {item.job_id}")
            transitioned.append(item.job_id)

            if item.barrier_job_id == item.job_id:
                barriers.append(item.barrier_job_id)
                continue
            barrier_reason = _bounded_reason(
                item.disposition,
                reason_code=item.reason_code,
                peer_job_id=item.job_id,
            )
            connection.execute(
                """
                INSERT INTO jobs(
                    job_id, job_type, dedupe_key, payload_json, status, priority,
                    attempts, max_attempts, available_at, lease_owner,
                    lease_expires_at, last_error, created_at, updated_at
                ) VALUES (?, 'phase1', ?, ?, 'cancelled', 10, 0, 5, ?, NULL, NULL, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO NOTHING
                """,
                (
                    item.barrier_job_id,
                    item.current_dedupe_key,
                    _canonical_payload({"episode_id": item.episode_id}),
                    now,
                    barrier_reason,
                    now,
                    now,
                ),
            )
            barrier = connection.execute(
                "SELECT * FROM jobs WHERE dedupe_key = ?", (item.current_dedupe_key,)
            ).fetchone()
            if (
                barrier is None
                or str(barrier["job_id"]) != item.barrier_job_id
                or str(barrier["status"]) != "cancelled"
                or str(barrier["payload_json"])
                != _canonical_payload({"episode_id": item.episode_id})
                or not str(barrier["last_error"] or "").startswith(_RECONCILIATION_PREFIX)
            ):
                raise ValueError(f"phase1 reconciliation barrier conflict: {item.job_id}")
            barriers.append(item.barrier_job_id)

    return Phase1ReconciliationReport(
        items=plan.items,
        dry_run=False,
        transitioned_job_ids=tuple(transitioned),
        barrier_job_ids=tuple(barriers),
    )


def reactivate_phase1_barriers(
    operations: OperationsStore,
    *,
    barrier_job_allowlist: Sequence[str],
    live_doctor_verified: bool,
    dry_run: bool = True,
) -> tuple[str, ...]:
    """Explicitly reactivate only deferred/current-prompt barriers after a live doctor gate."""

    if not live_doctor_verified:
        raise ValueError("phase1 barrier reactivation requires a successful live doctor gate")
    rows = _load_exact_rows(operations, barrier_job_allowlist)
    allowed_reasons = {
        Phase1ReconciliationDisposition.DEFERRED_PROTOCOL.value,
        Phase1ReconciliationDisposition.STALE_PROMPT.value,
    }
    for row in rows:
        if str(row["job_type"]) != PipelinePhase.PHASE1.value:
            raise ValueError(f"reactivation job is not phase1: {row['job_id']}")
        if str(row["status"]) != "cancelled" or row["lease_owner"] is not None:
            raise ValueError(
                f"reactivation requires an unleased cancelled barrier: {row['job_id']}"
            )
        reason = str(row["last_error"] or "")
        if not reason.startswith(_RECONCILIATION_PREFIX) or not any(
            f":{allowed}:" in reason for allowed in allowed_reasons
        ):
            raise ValueError(f"barrier is permanent or not reconciler-owned: {row['job_id']}")
    if dry_run:
        return tuple(str(row["job_id"]) for row in rows)

    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    fingerprints = {str(row["job_id"]): _row_fingerprint(row) for row in rows}
    with operations.transaction() as connection:
        for row in rows:
            current = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (row["job_id"],)
            ).fetchone()
            if current is None or _row_fingerprint(current) != fingerprints[str(row["job_id"])]:
                raise ValueError(f"phase1 barrier changed after preflight: {row['job_id']}")
        for row in rows:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = 'queued', attempts = 0, available_at = ?,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE job_id = ? AND status = 'cancelled' AND lease_owner IS NULL
                """,
                (now, now, row["job_id"]),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"phase1 barrier reactivation raced: {row['job_id']}")
    return tuple(str(row["job_id"]) for row in rows)
