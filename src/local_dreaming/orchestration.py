from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from local_dreaming.config import ModelSettings, RuntimePaths, Settings
from local_dreaming.database import connect_memory
from local_dreaming.errors import OversizedInputError, PrivacyBoundaryError
from local_dreaming.evidence import MAX_MODEL_INPUT_CHARS, effective_evidence_rows
from local_dreaming.ingest import IngestService
from local_dreaming.models import (
    ClaimScope,
    PreparedEvent,
    Sensitivity,
    SourceKind,
    SourcePolicy,
)
from local_dreaming.nightly import DoctorCertificationGate, NightlyRunner
from local_dreaming.persistence import StorageOutputSink
from local_dreaming.pipeline import (
    PHASE1_SCHEMA_VERSION,
    PHASE2_SCHEMA_VERSION,
    PROMPT_VERSION,
    DreamingPipeline,
    PipelineJob,
    PipelineModels,
    PipelinePhase,
)
from local_dreaming.review import fingerprint_head_set
from local_dreaming.segmentation import SegmenterConfig, segment_events
from local_dreaming.semantic_neighbors import load_semantic_neighbor_context
from local_dreaming.storage import MemoryStore, OperationsStore, fingerprint
from local_dreaming.worker import DEFAULT_CODEX_BINARY, CodexWorker, WorkerRuntime

_PHASE1_JOB_IDENTITY_VERSION = "phase1-job-v3"
_PHASE2_JOB_IDENTITY_VERSION = "phase2-job-v4"


@dataclass(frozen=True, slots=True)
class QueueReport:
    phase: str
    eligible: int
    queued: int
    job_ids: tuple[str, ...]
    dry_run: bool = False
    skipped_advisory: int = 0


@dataclass(frozen=True, slots=True)
class SegmentationReport:
    eligible_events: int
    episodes: int
    persisted: int
    episode_ids: tuple[str, ...]
    deferred_episodes: int = 0
    blocked_events: int = 0
    diagnostics: tuple[dict[str, object], ...] = ()
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class PilotScope:
    """Exact Phase 1 scope and the candidate baseline captured before a pilot."""

    phase1_job_ids: tuple[str, ...]
    episode_ids: tuple[str, ...]
    baseline_candidate_ids: tuple[str, ...]


def _source_kind(value: str) -> SourceKind:
    try:
        return SourceKind(value)
    except ValueError as exc:
        raise ValueError(f"unsupported source type: {value}") from exc


def _sensitivity(value: str) -> Sensitivity:
    try:
        return Sensitivity(value)
    except ValueError as exc:
        raise ValueError(f"unsupported sensitivity: {value}") from exc


def _policy_from_row(row: Mapping[str, Any], *, opted_in: bool) -> SourcePolicy:
    sensitivity = _sensitivity(str(row["sensitivity"]))
    egress = bool(row["model_egress_allowed"])
    allow_private = bool(
        row.get("allow_private_model_egress", egress and sensitivity is Sensitivity.PRIVATE)
    )
    return SourcePolicy(
        source_id=str(row["source_id"]),
        source_kind=_source_kind(str(row["source_type"])),
        opted_in=opted_in,
        sensitivity=sensitivity,
        allow_model_egress=egress,
        allow_private_model_egress=allow_private,
    )


def _maximum_sensitivity(values: Sequence[str]) -> Sensitivity:
    sensitivities = {_sensitivity(value) for value in values}
    if Sensitivity.SECRET in sensitivities:
        return Sensitivity.SECRET
    if Sensitivity.PRIVATE in sensitivities:
        return Sensitivity.PRIVATE
    return Sensitivity.NORMAL


def _token_estimate(text: str) -> int:
    return max(1, (len(text.encode("utf-8")) + 3) // 4)


def _canonical_payload(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _require_bounded_model_input(model_input: str) -> None:
    if len(model_input) > MAX_MODEL_INPUT_CHARS:
        raise OversizedInputError(
            f"loader input exceeds the {MAX_MODEL_INPUT_CHARS}-character model boundary"
        )


def segment_pending_events(
    memory: MemoryStore,
    *,
    dry_run: bool = False,
    config: SegmenterConfig | None = None,
    limit: int = 40,
) -> SegmentationReport:
    """Deterministically segment redacted events that are not in any episode."""

    if limit < 1:
        raise ValueError("segment limit must be positive")

    with memory.connection() as connection:
        rows = connection.execute(
            """
            SELECT e.*, s.source_type, s.model_egress_allowed,
                   s.sensitivity AS source_sensitivity,
                   s.metadata_json AS source_metadata_json,
                   COALESCE(sp.opted_in, 1) AS partition_opted_in
            FROM events AS e
            JOIN sources AS s ON s.source_id = e.source_id
            LEFT JOIN source_partitions AS sp ON sp.partition_id = e.partition_id
            WHERE NOT EXISTS (
                SELECT 1 FROM episode_events AS ee WHERE ee.event_id = e.event_id
            )
            ORDER BY e.occurred_at, e.event_id
            """
        ).fetchall()

    prepared: list[PreparedEvent] = []
    diagnostics: list[dict[str, object]] = []
    active_config = config or SegmenterConfig()
    for row in rows:
        occurred_raw = row["occurred_at"] or row["captured_at"]
        occurred_at = datetime.fromisoformat(str(occurred_raw).replace("Z", "+00:00"))
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=UTC)
        source_metadata = json.loads(str(row["source_metadata_json"]))
        policy = _policy_from_row(
            {
                "source_id": row["source_id"],
                "source_type": row["source_type"],
                "model_egress_allowed": row["model_egress_allowed"],
                "sensitivity": row["source_sensitivity"],
                "allow_private_model_egress": source_metadata.get(
                    "allow_private_model_egress", False
                ),
            },
            opted_in=bool(row["partition_opted_in"]),
        )
        event_sensitivity = _sensitivity(str(row["sensitivity"]))
        prepared_event = PreparedEvent(
            event_id=str(row["event_id"]),
            source_id=str(row["source_id"]),
            partition_id=str(row["partition_id"] or row["source_id"]),
            source_kind=policy.source_kind,
            external_event_id=str(row["external_event_id"] or row["event_id"]),
            occurred_at=occurred_at,
            content=str(row["content_text"] or ""),
            content_fingerprint=str(row["content_fingerprint"]),
            sensitivity=event_sensitivity,
            redaction_count=str(row["content_text"] or "").count("[REDACTED_SECRET]"),
            eligible_for_model=policy.permits_model_egress(event_sensitivity),
            source_locator=row["source_locator"],
            metadata=json.loads(str(row["metadata_json"])),
        )
        if len(prepared_event.content) > active_config.max_chars:
            diagnostics.append(
                {
                    "code": "oversized_event_blocked",
                    "event_id": prepared_event.event_id,
                    "character_count": len(prepared_event.content),
                    "limit": active_config.max_chars,
                }
            )
            continue
        prepared.append(prepared_event)

    all_episodes = segment_events(prepared, active_config)
    episodes = all_episodes[:limit]
    service = IngestService(memory)
    persisted = 0
    for episode in episodes:
        if service.persist_episode(episode, dry_run=dry_run):
            persisted += 1
    return SegmentationReport(
        eligible_events=len(prepared),
        episodes=len(episodes),
        persisted=persisted,
        episode_ids=tuple(episode.episode_id for episode in episodes),
        deferred_episodes=max(0, len(all_episodes) - len(episodes)),
        blocked_events=len(diagnostics),
        diagnostics=tuple(diagnostics),
        dry_run=dry_run,
    )


def enqueue_phase1_jobs(
    memory: MemoryStore,
    operations: OperationsStore,
    *,
    limit: int = 40,
    dry_run: bool = False,
    models: ModelSettings | None = None,
    episode_allowlist: Sequence[str] | None = None,
) -> QueueReport:
    """Queue opaque episode references; raw content remains only in memory.sqlite3."""

    if limit < 1:
        raise ValueError("limit must be positive")
    with memory.connection() as connection:
        skipped_advisory = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM episodes AS ep
                JOIN sources AS s ON s.source_id = ep.source_id
                LEFT JOIN source_partitions AS sp ON sp.partition_id = ep.partition_id
                WHERE ep.sensitivity <> 'secret'
                  AND s.model_egress_allowed = 1
                  AND COALESCE(sp.opted_in, 1) = 1
                  AND s.source_type IN ('chronicle', 'codex_memory')
                """
            ).fetchone()[0]
        )
        all_rows = connection.execute(
            """
            SELECT ep.episode_id, ep.content_fingerprint, ep.sensitivity,
                   ep.segmenter_version, s.policy_version, s.model_egress_allowed,
                   COALESCE(sp.opted_in, 1) AS opted_in
            FROM episodes AS ep
            JOIN sources AS s ON s.source_id = ep.source_id
            LEFT JOIN source_partitions AS sp ON sp.partition_id = ep.partition_id
            WHERE ep.sensitivity <> 'secret'
              AND s.model_egress_allowed = 1
              AND COALESCE(sp.opted_in, 1) = 1
              AND s.source_type NOT IN ('chronicle', 'codex_memory')
            ORDER BY ep.occurred_from, ep.episode_id
            """,
        ).fetchall()
        allowed = None if episode_allowlist is None else set(episode_allowlist)
        rows = [row for row in all_rows if allowed is None or str(row["episode_id"]) in allowed][
            :limit
        ]
    with operations.connection() as connection:
        existing = {str(row[0]) for row in connection.execute("SELECT dedupe_key FROM jobs")}
    jobs: list[str] = []
    active_models = models or ModelSettings()
    for row in rows:
        dedupe_key = fingerprint(
            _PHASE1_JOB_IDENTITY_VERSION,
            row["episode_id"],
            row["content_fingerprint"],
            row["segmenter_version"],
            row["policy_version"],
            PROMPT_VERSION,
            PHASE1_SCHEMA_VERSION,
            active_models.phase1_model,
            active_models.phase1_reasoning,
        )
        if dedupe_key in existing:
            continue
        if dry_run:
            jobs.append(f"dry:{dedupe_key[:16]}")
        else:
            jobs.append(
                operations.enqueue_job(
                    job_type=PipelinePhase.PHASE1.value,
                    dedupe_key=dedupe_key,
                    payload={"episode_id": str(row["episode_id"])},
                    priority=10,
                )
            )
    return QueueReport(
        phase=PipelinePhase.PHASE1.value,
        eligible=len(rows),
        queued=len(set(jobs)),
        job_ids=tuple(dict.fromkeys(jobs)),
        dry_run=dry_run,
        skipped_advisory=skipped_advisory,
    )


def enqueue_phase2_jobs(
    memory: MemoryStore,
    operations: OperationsStore,
    *,
    limit: int = 40,
    dry_run: bool = False,
    models: ModelSettings | None = None,
    candidate_allowlist: Sequence[str] | None = None,
) -> QueueReport:
    """Queue candidate identity groups for review-proposal consolidation."""

    if limit < 1:
        raise ValueError("limit must be positive")
    with memory.connection() as connection:
        rows = connection.execute(
            """
            SELECT cc.candidate_id, cc.normalized_identity_fingerprint,
                   cc.extraction_fingerprint
            FROM candidate_claims AS cc
            JOIN episodes AS ep ON ep.episode_id = cc.episode_id
            JOIN sources AS s ON s.source_id = ep.source_id
            LEFT JOIN source_partitions AS sp ON sp.partition_id = ep.partition_id
            JOIN current_candidate_dispositions AS cd
              ON cd.candidate_id = cc.candidate_id AND cd.disposition = 'eligible'
            WHERE NOT EXISTS (
                SELECT 1 FROM review_proposals AS rp
                LEFT JOIN review_proposal_candidates AS rpc
                  ON rpc.proposal_id = rp.proposal_id
                WHERE (rp.candidate_id = cc.candidate_id OR rpc.candidate_id = cc.candidate_id)
                  AND rp.status IN ('pending', 'approved', 'corrected', 'applied')
            )
              AND cc.sensitivity <> 'secret'
              AND s.model_egress_allowed = 1
              AND COALESCE(sp.opted_in, 1) = 1
            ORDER BY cc.created_at, cc.candidate_id
            """
        ).fetchall()
        grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        allowed = (
            None
            if candidate_allowlist is None
            else set(dict.fromkeys(str(item) for item in candidate_allowlist))
        )
        if allowed is not None:
            eligible_ids = {str(row["candidate_id"]) for row in rows}
            unavailable = allowed - eligible_ids
            if unavailable:
                placeholders = ",".join("?" for _ in unavailable)
                known_rows = connection.execute(
                    f"""
                    SELECT cc.candidate_id, cd.disposition
                    FROM candidate_claims AS cc
                    LEFT JOIN current_candidate_dispositions AS cd
                      ON cd.candidate_id = cc.candidate_id
                    WHERE cc.candidate_id IN ({placeholders})
                    """,
                    tuple(sorted(unavailable)),
                ).fetchall()
                dispositions = {
                    str(row["candidate_id"]): (
                        None if row["disposition"] is None else str(row["disposition"])
                    )
                    for row in known_rows
                }
                unknown = sorted(unavailable - dispositions.keys())
                suppressed = sorted(
                    candidate_id
                    for candidate_id, disposition in dispositions.items()
                    if disposition == "suppressed"
                )
                ineligible = sorted(
                    candidate_id
                    for candidate_id in unavailable
                    if candidate_id not in unknown and candidate_id not in suppressed
                )
                details = "; ".join(
                    f"{label}={values}"
                    for label, values in (
                        ("unknown", unknown),
                        ("suppressed", suppressed),
                        ("ineligible", ineligible),
                    )
                    if values
                )
                raise ValueError(f"candidate_allowlist is not exact: {details}")
        for row in rows:
            if allowed is not None and str(row["candidate_id"]) not in allowed:
                continue
            grouped[str(row["normalized_identity_fingerprint"])].append(dict(row))
        ordered_groups = list(sorted(grouped.items()))
        if allowed is not None and len(ordered_groups) > limit:
            raise ValueError("candidate_allowlist exceeds the phase2 group limit")
        selected = ordered_groups[:limit]

    with operations.connection() as connection:
        existing = {str(row[0]) for row in connection.execute("SELECT dedupe_key FROM jobs")}
    jobs: list[str] = []
    active_models = models or ModelSettings()
    queued_as_of = datetime.now(UTC).isoformat()
    for identity, group in selected:
        candidate_ids = tuple(str(row["candidate_id"]) for row in group)
        head_hash = fingerprint_head_set(memory.load_heads(identity))
        with memory.connection() as connection:
            semantic_context = load_semantic_neighbor_context(connection, candidate_ids)
        dedupe_key = fingerprint(
            _PHASE2_JOB_IDENTITY_VERSION,
            identity,
            sorted(str(row["extraction_fingerprint"]) for row in group),
            head_hash,
            semantic_context.context_hash,
            PROMPT_VERSION,
            PHASE2_SCHEMA_VERSION,
            active_models.phase2_model,
            active_models.phase2_reasoning,
        )
        if dedupe_key in existing:
            continue
        if dry_run:
            jobs.append(f"dry:{dedupe_key[:16]}")
        else:
            jobs.append(
                operations.enqueue_job(
                    job_type=PipelinePhase.PHASE2.value,
                    dedupe_key=dedupe_key,
                    payload={
                        "as_of": queued_as_of,
                        "candidate_ids": list(candidate_ids),
                        "semantic_context_hash": semantic_context.context_hash,
                    },
                    priority=5,
                )
            )
    return QueueReport(
        phase=PipelinePhase.PHASE2.value,
        eligible=len(selected),
        queued=len(set(jobs)),
        job_ids=tuple(dict.fromkeys(jobs)),
        dry_run=dry_run,
    )


class DatabaseJobLoader:
    """Resolve opaque operation-queue references into one bounded model input."""

    def __init__(self, memory_path: Path) -> None:
        self.memory_path = Path(memory_path)

    def load(self, leased_record: Mapping[str, Any]) -> PipelineJob:
        try:
            phase = PipelinePhase(str(leased_record["job_type"]))
            payload = leased_record["payload"]
            if not isinstance(payload, Mapping):
                raise TypeError("job payload must be an object")
            if phase is PipelinePhase.PHASE1:
                return self._load_phase1(leased_record, payload)
            return self._load_phase2(leased_record, payload)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("invalid or stale pipeline job reference") from exc

    def _load_phase1(
        self,
        leased: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> PipelineJob:
        episode_id = str(payload["episode_id"])
        with connect_memory(self.memory_path) as connection:
            episode = connection.execute(
                """
                SELECT ep.*, s.source_type, s.policy_version,
                       s.model_egress_allowed, s.sensitivity AS source_sensitivity,
                       s.metadata_json AS source_metadata_json,
                       COALESCE(sp.opted_in, 1) AS partition_opted_in
                FROM episodes AS ep
                JOIN sources AS s ON s.source_id = ep.source_id
                LEFT JOIN source_partitions AS sp ON sp.partition_id = ep.partition_id
                WHERE ep.episode_id = ?
                """,
                (episode_id,),
            ).fetchone()
            if episode is None:
                raise KeyError("episode does not exist")
            events = connection.execute(
                """
                SELECT e.event_id, e.source_id, e.partition_id, e.event_type,
                       e.content_text, e.content_fingerprint, e.source_locator,
                       e.occurred_at, e.sensitivity, e.metadata_json, ee.ordinal
                FROM episode_events AS ee
                JOIN events AS e ON e.event_id = ee.event_id
                WHERE ee.episode_id = ? ORDER BY ee.ordinal
                """,
                (episode_id,),
            ).fetchall()
        if not events:
            raise ValueError("episode has no events")
        for row in events:
            if len(str(row["content_text"] or "")) > MAX_MODEL_INPUT_CHARS:
                raise OversizedInputError(
                    f"event {row['event_id']} exceeds the "
                    f"{MAX_MODEL_INPUT_CHARS}-character model boundary"
                )
        effective_events = effective_evidence_rows([dict(row) for row in events])
        source_metadata = json.loads(str(episode["source_metadata_json"]))
        policy = _policy_from_row(
            {
                "source_id": episode["source_id"],
                "source_type": episode["source_type"],
                "model_egress_allowed": episode["model_egress_allowed"],
                "sensitivity": episode["source_sensitivity"],
                "allow_private_model_egress": source_metadata.get(
                    "allow_private_model_egress", False
                ),
            },
            opted_in=bool(episode["partition_opted_in"]),
        )
        maximum = _maximum_sensitivity([str(row["sensitivity"]) for row in effective_events])
        if maximum is Sensitivity.SECRET:
            raise PrivacyBoundaryError("secret episode cannot be loaded for a model call")
        model_input = _canonical_payload(
            {
                "episode_id": episode_id,
                "events": [
                    {
                        "content": str(row["content_text"] or ""),
                        "event_id": str(row["event_id"]),
                        "occurred_at": row["occurred_at"],
                        "source_kind": str(row["event_type"]),
                    }
                    for row in effective_events
                ],
            }
        )
        _require_bounded_model_input(model_input)
        return PipelineJob(
            job_id=str(leased["job_id"]),
            phase=PipelinePhase.PHASE1,
            model_input=model_input,
            source_ids=(policy.source_id,),
            source_policies={policy.source_id: policy},
            source_policy_version=str(episode["policy_version"]),
            maximum_sensitivity=maximum,
            allowed_reference_ids=tuple(str(row["event_id"]) for row in effective_events),
            scan_bytes=len(model_input.encode("utf-8")),
            episode_count=1,
            estimated_input_tokens=_token_estimate(model_input),
            redaction_count=model_input.count("[REDACTED_SECRET]"),
            attempt=max(1, int(leased.get("attempts", 1))),
            persistence_context={"episode_id": episode_id},
        )

    def _load_phase2(
        self,
        leased: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> PipelineJob:
        raw_ids = payload["candidate_ids"]
        if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, str | bytes):
            raise TypeError("candidate_ids must be a list")
        candidate_ids = tuple(dict.fromkeys(str(item) for item in raw_ids))
        if not candidate_ids:
            raise ValueError("candidate_ids must not be empty")
        placeholders = ",".join("?" for _ in candidate_ids)
        with connect_memory(self.memory_path) as connection:
            rows = connection.execute(
                f"""
                SELECT cc.*, ep.source_id, s.source_type, s.policy_version,
                       s.model_egress_allowed, s.sensitivity AS source_sensitivity,
                       s.metadata_json AS source_metadata_json,
                       COALESCE(sp.opted_in, 1) AS partition_opted_in
                FROM candidate_claims AS cc
                JOIN episodes AS ep ON ep.episode_id = cc.episode_id
                JOIN sources AS s ON s.source_id = ep.source_id
                LEFT JOIN source_partitions AS sp ON sp.partition_id = ep.partition_id
                JOIN current_candidate_dispositions AS cd
                  ON cd.candidate_id = cc.candidate_id AND cd.disposition = 'eligible'
                WHERE cc.candidate_id IN ({placeholders})
                ORDER BY cc.candidate_id
                """,
                candidate_ids,
            ).fetchall()
            if len(rows) != len(candidate_ids):
                raise ValueError("phase2 candidate is missing or no longer eligible")
            expected_semantic_hash = payload.get("semantic_context_hash")
            if not isinstance(expected_semantic_hash, str) or not expected_semantic_hash:
                raise ValueError("phase2 job is missing its semantic context binding")
            semantic_context = load_semantic_neighbor_context(connection, candidate_ids)
            if semantic_context.context_hash != expected_semantic_hash:
                raise ValueError("phase2 semantic context changed after enqueue")
            identities = {str(row["normalized_identity_fingerprint"]) for row in rows}
            if len(identities) != 1:
                raise ValueError("phase2 job must contain one target slot")
            identity = identities.pop()
            heads = connection.execute(
                """
                SELECT cv.claim_version_id, cv.claim_id, cv.value_json, cv.summary,
                       cv.mcp_safe_summary, cv.value_fingerprint, cv.status,
                       cv.valid_from, cv.valid_to, cv.sensitivity,
                       cv.recorded_at, cv.recorded_revision
                FROM current_claim_versions AS cv
                JOIN claims AS c ON c.claim_id = cv.claim_id
                WHERE c.identity_fingerprint = ?
                ORDER BY cv.claim_version_id
                """,
                (identity,),
            ).fetchall()
            head_evidence_rows = connection.execute(
                """
                SELECT ce.claim_version_id, s.source_id, s.source_type,
                       s.policy_version, s.model_egress_allowed,
                       s.sensitivity AS source_sensitivity,
                       s.metadata_json AS source_metadata_json,
                       COALESCE(sp.opted_in, 1) AS partition_opted_in
                FROM claim_evidence AS ce
                JOIN events AS e ON e.event_id = ce.event_id
                JOIN sources AS s ON s.source_id = e.source_id
                LEFT JOIN source_partitions AS sp ON sp.partition_id = e.partition_id
                WHERE ce.claim_version_id IN (
                    SELECT cv.claim_version_id
                    FROM current_claim_versions AS cv
                    JOIN claims AS c ON c.claim_id = cv.claim_id
                    WHERE c.identity_fingerprint = ?
                )
                ORDER BY ce.claim_version_id, s.source_id
                """,
                (identity,),
            ).fetchall()
            evidence_by_candidate: dict[str, list[str]] = defaultdict(list)
            evidence_window_by_candidate: dict[str, dict[str, object]] = {}
            evidence_rows = connection.execute(
                f"""
                SELECT ce.candidate_id, ce.event_id, ce.ordinal,
                       e.source_id, e.partition_id, e.event_type,
                       e.content_fingerprint, e.source_locator, e.occurred_at,
                       e.metadata_json
                FROM candidate_evidence AS ce
                JOIN events AS e ON e.event_id = ce.event_id
                WHERE ce.candidate_id IN ({placeholders})
                ORDER BY ce.candidate_id, ce.ordinal
                """,
                candidate_ids,
            ).fetchall()
            raw_evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for evidence in evidence_rows:
                raw_evidence_by_candidate[str(evidence["candidate_id"])].append(dict(evidence))
            for candidate_id, candidate_rows in raw_evidence_by_candidate.items():
                effective_rows = effective_evidence_rows(candidate_rows)
                evidence_by_candidate[candidate_id] = [
                    str(item["event_id"]) for item in effective_rows
                ]
                occurred = sorted(
                    str(item["occurred_at"])
                    for item in effective_rows
                    if item.get("occurred_at") is not None
                )
                evidence_window_by_candidate[candidate_id] = {
                    "effective_evidence_count": len(effective_rows),
                    "evidence_occurred_from": occurred[0] if occurred else None,
                    "evidence_occurred_to": occurred[-1] if occurred else None,
                }
        policies: dict[str, SourcePolicy] = {}
        policy_versions: set[str] = set()
        for row in rows:
            source_id = str(row["source_id"])
            source_metadata = json.loads(str(row["source_metadata_json"]))
            policy = _policy_from_row(
                {
                    "source_id": source_id,
                    "source_type": row["source_type"],
                    "model_egress_allowed": row["model_egress_allowed"],
                    "sensitivity": row["source_sensitivity"],
                    "allow_private_model_egress": source_metadata.get(
                        "allow_private_model_egress", False
                    ),
                },
                opted_in=bool(row["partition_opted_in"]),
            )
            scope = ClaimScope(str(row["scope"]))
            if not policy.permits_claim_scope(scope):
                raise PrivacyBoundaryError("source policy cannot establish this claim scope")
            policies[source_id] = policy
            policy_versions.add(str(row["policy_version"]))
        head_evidence: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in head_evidence_rows:
            head_evidence[str(row["claim_version_id"])].append(dict(row))
        safe_heads: list[dict[str, Any]] = []
        transmitted_head_sensitivities: list[str] = []
        for head in heads:
            sensitivity = _sensitivity(str(head["sensitivity"]))
            if sensitivity is Sensitivity.SECRET:
                raise PrivacyBoundaryError("secret canonical head cannot enter Phase 2")
            summary = str(head["summary"])
            value: object = json.loads(str(head["value_json"]))
            content_policy = "canonical"
            if sensitivity is Sensitivity.PRIVATE:
                bounded_policies: list[SourcePolicy] = []
                for evidence in head_evidence[str(head["claim_version_id"])]:
                    source_metadata = json.loads(str(evidence["source_metadata_json"]))
                    evidence_policy = _policy_from_row(
                        {
                            "source_id": evidence["source_id"],
                            "source_type": evidence["source_type"],
                            "model_egress_allowed": evidence["model_egress_allowed"],
                            "sensitivity": evidence["source_sensitivity"],
                            "allow_private_model_egress": source_metadata.get(
                                "allow_private_model_egress", False
                            ),
                        },
                        opted_in=bool(evidence["partition_opted_in"]),
                    )
                    bounded_policies.append(evidence_policy)
                raw_allowed = bool(bounded_policies) and all(
                    policy.permits_model_egress(Sensitivity.PRIVATE) for policy in bounded_policies
                )
                if raw_allowed:
                    for evidence_policy in bounded_policies:
                        policies[evidence_policy.source_id] = evidence_policy
                    policy_versions.update(
                        str(item["policy_version"])
                        for item in head_evidence[str(head["claim_version_id"])]
                    )
                    content_policy = "private_source_policy"
                    transmitted_head_sensitivities.append(Sensitivity.PRIVATE.value)
                elif head["mcp_safe_summary"]:
                    summary = str(head["mcp_safe_summary"])
                    value = None
                    content_policy = "mcp_safe_summary"
                    transmitted_head_sensitivities.append(Sensitivity.NORMAL.value)
                else:
                    raise PrivacyBoundaryError(
                        "private canonical head lacks permitted egress or a safe summary"
                    )
            else:
                transmitted_head_sensitivities.append(sensitivity.value)
            safe_heads.append(
                {
                    "claim_id": str(head["claim_id"]),
                    "claim_version_id": str(head["claim_version_id"]),
                    "content_policy": content_policy,
                    "status": str(head["status"]),
                    "summary": summary,
                    "valid_from": head["valid_from"],
                    "valid_to": head["valid_to"],
                    "value": value,
                    "recorded_at": head["recorded_at"],
                    "recorded_revision": int(head["recorded_revision"]),
                }
            )
        maximum = _maximum_sensitivity(
            [str(row["sensitivity"]) for row in rows] + transmitted_head_sensitivities
        )
        if maximum is Sensitivity.SECRET:
            raise PrivacyBoundaryError("secret candidate cannot be loaded for a model call")
        head_hash = fingerprint_head_set(MemoryStore(self.memory_path).load_heads(identity))
        model_input = _canonical_payload(
            {
                "as_of": payload.get("as_of"),
                "candidates": [
                    {
                        "candidate_id": str(row["candidate_id"]),
                        "confidence": float(row["confidence"]),
                        "epistemic_status": str(row["epistemic_status"]),
                        "evidence_event_ids": evidence_by_candidate[str(row["candidate_id"])],
                        "predicate": str(row["predicate"]),
                        "scope": str(row["scope"]),
                        "subject": str(row["subject_text"]),
                        "valid_from": row["valid_from"],
                        "valid_to": row["valid_to"],
                        "value": json.loads(str(row["value_json"])),
                        **evidence_window_by_candidate[str(row["candidate_id"])],
                    }
                    for row in rows
                ],
                "current_heads": safe_heads,
                "expected_head_set_hash": head_hash,
                "related_canonical_heads": semantic_context.as_payload(),
                "target_slot_fingerprint": identity,
            }
        )
        _require_bounded_model_input(model_input)
        return PipelineJob(
            job_id=str(leased["job_id"]),
            phase=PipelinePhase.PHASE2,
            model_input=model_input,
            source_ids=tuple(sorted(policies)),
            source_policies=policies,
            source_policy_version="+".join(sorted(policy_versions)),
            maximum_sensitivity=maximum,
            allowed_reference_ids=candidate_ids,
            allowed_head_set_hashes=(head_hash,),
            allowed_claim_ids=tuple(sorted({str(row["claim_id"]) for row in heads})),
            scan_bytes=len(model_input.encode("utf-8")),
            episode_count=0,
            estimated_input_tokens=_token_estimate(model_input),
            redaction_count=model_input.count("[REDACTED_SECRET]"),
            attempt=max(1, int(leased.get("attempts", 1))),
            persistence_context={
                "candidate_ids": list(candidate_ids),
                "semantic_context_hash": semantic_context.context_hash,
            },
        )


class CoordinatingOperationsStore(OperationsStore):
    """Queue Phase 2 only after the final queued/leased Phase 1 job completes."""

    def __init__(
        self,
        path: Path,
        *,
        memory: MemoryStore,
        models: ModelSettings,
        phase2_limit: int,
    ) -> None:
        super().__init__(path)
        self.memory = memory
        self.models = models
        self.phase2_limit = phase2_limit

    def complete_job(self, job_id: str, *, owner: str) -> bool:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT job_type FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        completed = super().complete_job(job_id, owner=owner)
        if not completed or row is None or row["job_type"] != PipelinePhase.PHASE1.value:
            return completed
        with self.connection() as connection:
            remaining_phase1 = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM jobs
                    WHERE job_type = ? AND status IN ('queued', 'leased')
                    """,
                    (PipelinePhase.PHASE1.value,),
                ).fetchone()[0]
            )
        if remaining_phase1 == 0:
            enqueue_phase2_jobs(
                self.memory,
                self,
                limit=self.phase2_limit,
                models=self.models,
            )
        return completed


def plan_pilot_scope(
    memory: MemoryStore,
    operations: OperationsStore,
    *,
    phase1_job_allowlist: Sequence[str],
) -> PilotScope:
    """Validate a one-to-three job pilot without changing either database."""

    job_ids = tuple(dict.fromkeys(str(item) for item in phase1_job_allowlist))
    if not 1 <= len(job_ids) <= 3:
        raise ValueError("pilot requires an exact allowlist of one to three Phase 1 jobs")
    placeholders = ",".join("?" for _ in job_ids)
    with operations.connection() as connection:
        rows = connection.execute(
            f"""
            SELECT job_id, job_type, status, payload_json
            FROM jobs
            WHERE job_id IN ({placeholders})
            ORDER BY job_id
            """,
            job_ids,
        ).fetchall()
    by_id = {str(row["job_id"]): row for row in rows}
    missing = sorted(set(job_ids) - by_id.keys())
    if missing:
        raise ValueError(f"pilot job allowlist contains unknown jobs: {missing}")
    episode_ids: list[str] = []
    for job_id in job_ids:
        row = by_id[job_id]
        if str(row["job_type"]) != PipelinePhase.PHASE1.value:
            raise ValueError(f"pilot job is not Phase 1: {job_id}")
        if str(row["status"]) != "queued":
            raise ValueError(f"pilot job is not queued: {job_id}")
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict) or set(payload) != {"episode_id"}:
            raise ValueError(f"pilot job payload is not an opaque episode reference: {job_id}")
        episode_ids.append(str(payload["episode_id"]))
    if len(set(episode_ids)) != len(episode_ids):
        raise ValueError("pilot Phase 1 jobs must reference distinct episodes")

    episode_placeholders = ",".join("?" for _ in episode_ids)
    with memory.connection() as connection:
        episode_rows = connection.execute(
            f"""
            SELECT ep.episode_id, s.source_type
            FROM episodes AS ep
            JOIN sources AS s ON s.source_id = ep.source_id
            WHERE ep.episode_id IN ({episode_placeholders})
            """,
            tuple(episode_ids),
        ).fetchall()
        source_types = {str(row["episode_id"]): str(row["source_type"]) for row in episode_rows}
        missing_episodes = sorted(set(episode_ids) - source_types.keys())
        if missing_episodes:
            raise ValueError(f"pilot jobs reference missing episodes: {missing_episodes}")
        line_episodes = sorted(
            episode_id
            for episode_id, source_type in source_types.items()
            if source_type == SourceKind.LINE_SYNTHETIC.value
        )
        if line_episodes:
            raise ValueError(f"pilot does not permit LINE sources: {line_episodes}")
        baseline = tuple(
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT candidate_id
                FROM candidate_claims
                WHERE episode_id IN ({episode_placeholders})
                ORDER BY candidate_id
                """,
                tuple(episode_ids),
            )
        )
    return PilotScope(
        phase1_job_ids=job_ids,
        episode_ids=tuple(episode_ids),
        baseline_candidate_ids=baseline,
    )


class PilotOperationsStore(OperationsStore):
    """Lease and consolidate only work causally created by one exact pilot."""

    def __init__(
        self,
        path: Path,
        *,
        memory: MemoryStore,
        models: ModelSettings,
        scope: PilotScope,
        phase2_limit: int,
    ) -> None:
        super().__init__(path)
        self.memory = memory
        self.models = models
        self.scope = scope
        self.phase2_limit = phase2_limit
        self.allowed_job_ids = set(scope.phase1_job_ids)
        self.phase2_job_ids: set[str] = set()
        self._phase2_enqueued = False

    def lease_job(
        self,
        *,
        owner: str,
        lease_seconds: int = 300,
        job_types: Sequence[str] | None = None,
        job_ids: Sequence[str] | None = None,
    ) -> dict[str, Any] | None:
        permitted = set(self.allowed_job_ids)
        if job_ids is not None:
            permitted.intersection_update(str(item) for item in job_ids)
        return super().lease_job(
            owner=owner,
            lease_seconds=lease_seconds,
            job_types=job_types,
            job_ids=tuple(sorted(permitted)),
        )

    def complete_job(self, job_id: str, *, owner: str) -> bool:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT job_type FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        completed = super().complete_job(job_id, owner=owner)
        if not completed or row is None or row["job_type"] != PipelinePhase.PHASE1.value:
            return completed
        placeholders = ",".join("?" for _ in self.scope.phase1_job_ids)
        with self.connection() as connection:
            remaining_phase1 = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM jobs
                    WHERE job_id IN ({placeholders})
                      AND status IN ('queued', 'leased')
                    """,
                    self.scope.phase1_job_ids,
                ).fetchone()[0]
            )
        if remaining_phase1 == 0 and not self._phase2_enqueued:
            episode_placeholders = ",".join("?" for _ in self.scope.episode_ids)
            baseline = set(self.scope.baseline_candidate_ids)
            with self.memory.connection() as connection:
                candidate_ids = tuple(
                    str(row[0])
                    for row in connection.execute(
                        f"""
                        SELECT cc.candidate_id
                        FROM candidate_claims AS cc
                        JOIN current_candidate_dispositions AS cd
                          ON cd.candidate_id = cc.candidate_id
                         AND cd.disposition = 'eligible'
                        WHERE cc.episode_id IN ({episode_placeholders})
                        ORDER BY cc.candidate_id
                        """,
                        self.scope.episode_ids,
                    )
                    if str(row[0]) not in baseline
                )
            if candidate_ids:
                report = enqueue_phase2_jobs(
                    self.memory,
                    self,
                    limit=self.phase2_limit,
                    models=self.models,
                    candidate_allowlist=candidate_ids,
                )
                self.phase2_job_ids.update(report.job_ids)
                self.allowed_job_ids.update(report.job_ids)
            self._phase2_enqueued = True
        return completed


def build_nightly_runner(
    settings: Settings,
    *,
    codex_binary: Path = DEFAULT_CODEX_BINARY,
) -> NightlyRunner:
    runtime = WorkerRuntime(settings.paths, codex_binary=codex_binary)
    memory = MemoryStore(settings.paths.memory_db)
    operations = CoordinatingOperationsStore(
        settings.paths.operations_db,
        memory=memory,
        models=settings.models,
        phase2_limit=settings.nightly.max_episodes,
    )
    pipeline = DreamingPipeline(
        worker=CodexWorker(runtime),
        operations=operations,
        output_sink=StorageOutputSink(settings.paths.memory_db),
        models=PipelineModels.from_settings(settings.models),
    )
    return NightlyRunner(
        runtime=runtime,
        operations=operations,
        pipeline=pipeline,
        loader=DatabaseJobLoader(settings.paths.memory_db),
        certification=DoctorCertificationGate(
            runtime=runtime,
            models=settings.models,
            stamp_path=settings.paths.worker / "doctor-certification.json",
        ),
        limits=settings.nightly,
    )


def build_pilot_runner(
    settings: Settings,
    *,
    phase1_job_allowlist: Sequence[str],
    phase2_limit: int = 5,
    codex_binary: Path = DEFAULT_CODEX_BINARY,
    worker_paths: RuntimePaths | None = None,
) -> tuple[NightlyRunner, PilotScope]:
    """Build a clone-only fail-fast runner whose lease and Phase 2 scopes are exact."""

    if not 1 <= phase2_limit <= 5:
        raise ValueError("pilot Phase 2 group limit must be between one and five")
    production_home = (Path.home() / "Library" / "Application Support" / "Local-Dreaming").resolve()
    if settings.paths.home.resolve() == production_home:
        raise ValueError(
            "run-pilot --apply is clone-only until durable pilot checkpoints are implemented"
        )
    active_worker_paths = worker_paths or settings.paths
    runtime = WorkerRuntime(active_worker_paths, codex_binary=codex_binary)
    memory = MemoryStore(settings.paths.memory_db)
    base_operations = OperationsStore(settings.paths.operations_db)
    scope = plan_pilot_scope(
        memory,
        base_operations,
        phase1_job_allowlist=phase1_job_allowlist,
    )
    operations = PilotOperationsStore(
        settings.paths.operations_db,
        memory=memory,
        models=settings.models,
        scope=scope,
        phase2_limit=phase2_limit,
    )
    pipeline = DreamingPipeline(
        worker=CodexWorker(runtime),
        operations=operations,
        output_sink=StorageOutputSink(settings.paths.memory_db),
        models=PipelineModels.from_settings(settings.models),
    )
    return (
        NightlyRunner(
            runtime=runtime,
            operations=operations,
            pipeline=pipeline,
            loader=DatabaseJobLoader(settings.paths.memory_db),
            certification=DoctorCertificationGate(
                runtime=runtime,
                models=settings.models,
                stamp_path=active_worker_paths.worker / "doctor-certification.json",
            ),
            limits=settings.nightly,
            fail_fast=True,
        ),
        scope,
    )
