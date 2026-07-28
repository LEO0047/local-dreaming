from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from local_dreaming.evidence import effective_evidence_rows
from local_dreaming.forgetting import ForgottenLedger
from local_dreaming.models import ProposalType
from local_dreaming.pipeline import ModelProvenance, PipelineJob
from local_dreaming.redaction import has_secret_material
from local_dreaming.review import fingerprint_precondition
from local_dreaming.semantic_neighbors import load_semantic_neighbor_context
from local_dreaming.storage import (
    MemoryStore,
    ReviewProposalInput,
    fingerprint,
    identity_fingerprint,
    value_fingerprint,
)

_OPERATION_MAP = {
    "ADD": "add",
    "UPDATE": "update",
    "SUPERSEDE": "supersede",
    "DISPUTE": "dispute",
    "NARROW": "narrow",
}

_DISPOSITION_REASONS = {
    "semantic_duplicate",
    "stale_status",
    "operations_only",
    "insufficient_evidence",
    "not_durable",
}


def _reject_secret_output(value: object) -> None:
    if has_secret_material(value):
        raise ValueError("model output contains secret material")


def _rows_are_one_identity(rows: Sequence[Mapping[str, Any]]) -> str:
    identities = {str(row["normalized_identity_fingerprint"]) for row in rows}
    if len(identities) != 1:
        raise ValueError("one review proposal cannot merge different claim identities")
    return identities.pop()


def _deterministic_policy_suppression(raw: Mapping[str, Any]) -> str | None:
    subject = str(raw.get("subject", "")).casefold()
    value = str(raw.get("proposed_value", ""))
    summary = str(raw.get("proposed_summary", ""))
    combined = f"{value}\n{summary}".casefold()
    if "agents.md" in subject:
        bounded_handoff_qualifiers = (
            "程式碼",
            "檔案修改",
            "跨 session",
            "跨session",
            "尚未完成",
            "未完成",
        )
        if not any(qualifier in combined for qualifier in bounded_handoff_qualifiers):
            return "phase2_overbroad_governance"
    timeline_markers = ("時間軸", "timeline")
    local_state_markers = (
        "本機狀況",
        "local state",
        "local status",
        "git dirty",
        "磁碟",
        "automation",
        "launchd",
        "health",
    )
    separation_markers = (
        "operations",
        "telemetry",
        "隔離",
        "dream status",
        "status 查詢",
    )
    if (
        any(marker in combined for marker in timeline_markers)
        and any(marker in combined for marker in local_state_markers)
        and not any(marker in combined for marker in separation_markers)
    ):
        return "phase2_operations_only"
    return None


class StorageOutputSink:
    """Persist model outputs as candidates or review proposals, never canonical claims."""

    def __init__(self, memory_path: Path, forgotten_path: Path | None = None) -> None:
        self.store = MemoryStore(memory_path)
        self.forgotten = ForgottenLedger(
            forgotten_path or Path(memory_path).parent / "forgotten.jsonl"
        )

    def persist_candidates(
        self,
        job: PipelineJob,
        payload: Mapping[str, Any],
        provenance: ModelProvenance,
    ) -> int:
        context = dict(job.persistence_context or {})
        episode_id = context.get("episode_id")
        if not isinstance(episode_id, str) or not episode_id:
            raise ValueError("phase1 persistence requires an episode_id")
        candidates = payload.get("candidate_claims", [])
        if not isinstance(candidates, Sequence) or isinstance(candidates, str | bytes):
            raise ValueError("candidate_claims must be a list")
        persisted: set[str] = set()
        for raw in candidates:
            if not isinstance(raw, Mapping):
                raise ValueError("candidate must be an object")
            _reject_secret_output(raw)
            evidence = raw.get("evidence_event_ids", [])
            if not isinstance(evidence, Sequence) or isinstance(evidence, str | bytes):
                raise ValueError("candidate evidence_event_ids must be a list")
            proposed_supersedes = raw.get("possible_supersedes_claim_id")
            proposal_type = "supersede" if proposed_supersedes else "add"
            identity = identity_fingerprint(
                str(raw["subject"]), str(raw["predicate"]), str(raw["scope"])
            )
            value = value_fingerprint(raw["value"])
            if self.forgotten.is_claim_forgotten(identity, value):
                continue
            evidence_ids = tuple(dict.fromkeys(str(item) for item in evidence))
            if not evidence_ids:
                raise ValueError("candidate must reference effective evidence")
            placeholders = ",".join("?" for _ in evidence_ids)
            with self.store.connection() as connection:
                evidence_rows = connection.execute(
                    f"""
                    SELECT e.event_id, e.source_id, e.partition_id, e.event_type,
                           e.content_fingerprint, e.source_locator, e.occurred_at,
                           e.metadata_json, ee.ordinal
                    FROM events AS e
                    JOIN episode_events AS ee ON ee.event_id = e.event_id
                    WHERE ee.episode_id = ? AND e.event_id IN ({placeholders})
                    ORDER BY ee.ordinal
                    """,
                    (episode_id, *evidence_ids),
                ).fetchall()
            if len(evidence_rows) != len(evidence_ids):
                raise ValueError("candidate evidence is not part of its episode")
            effective_rows = effective_evidence_rows([dict(row) for row in evidence_rows])
            effective_ids = tuple(str(row["event_id"]) for row in effective_rows)
            evidence_families = tuple(
                str(row["evidence_family_fingerprint"]) for row in effective_rows
            )
            extraction_fp = fingerprint(
                "phase1-candidate-v1",
                episode_id,
                raw.get("subject"),
                raw.get("predicate"),
                raw.get("scope"),
                raw.get("value"),
                sorted(evidence_families),
                provenance.prompt_hash,
                provenance.schema_hash,
                provenance.model_id,
                provenance.reasoning_effort,
            )
            epistemic_status = str(raw.get("epistemic_status", "provisional"))
            initial_disposition = (
                "suppressed" if epistemic_status == "outcome_unknown" else "eligible"
            )
            initial_reason_code = (
                "phase1_outcome_unknown_requires_temporal_engine"
                if epistemic_status == "outcome_unknown"
                else "phase1_candidate_created"
            )
            candidate_id = self.store.create_candidate(
                episode_id=episode_id,
                subject_text=str(raw["subject"]),
                predicate=str(raw["predicate"]),
                scope=str(raw["scope"]),
                value=raw["value"],
                proposal_type=proposal_type,
                epistemic_status=epistemic_status,
                valid_from=raw.get("valid_from"),
                valid_to=raw.get("valid_to"),
                possible_supersedes_claim_id=(
                    str(proposed_supersedes) if proposed_supersedes else None
                ),
                confidence=float(raw.get("confidence", 0.5)),
                sensitivity=job.maximum_sensitivity.value,
                evidence_event_ids=effective_ids,
                extraction_fingerprint=extraction_fp,
                extractor_version=provenance.prompt_version,
                prompt_hash=provenance.prompt_hash,
                schema_version=provenance.schema_version,
                model_id=provenance.model_id,
                reasoning_effort=provenance.reasoning_effort,
                initial_disposition=initial_disposition,
                initial_reason_code=initial_reason_code,
            )
            persisted.add(candidate_id)
        return len(persisted)

    def persist_proposals(
        self,
        job: PipelineJob,
        payload: Mapping[str, Any],
        provenance: ModelProvenance,
    ) -> int:
        del provenance
        proposals = payload.get("proposals", [])
        if not isinstance(proposals, Sequence) or isinstance(proposals, str | bytes):
            raise ValueError("proposals must be a list")
        allowed_candidate_ids = set(job.allowed_reference_ids)
        persistence_context = dict(job.persistence_context or {})
        raw_evaluated = persistence_context.get("candidate_ids", job.allowed_reference_ids)
        if not isinstance(raw_evaluated, Sequence) or isinstance(raw_evaluated, str | bytes):
            raise ValueError("phase2 persistence requires candidate_ids")
        evaluated_candidate_ids = tuple(dict.fromkeys(str(item) for item in raw_evaluated))
        if not set(evaluated_candidate_ids).issubset(allowed_candidate_ids):
            raise ValueError("phase2 evaluated candidates exceed the job allowlist")
        evaluated = set(evaluated_candidate_ids)
        expected_semantic_hash = persistence_context.get("semantic_context_hash")
        if expected_semantic_hash is not None:
            if not isinstance(expected_semantic_hash, str) or not expected_semantic_hash:
                raise ValueError("phase2 semantic context binding is malformed")
            with self.store.connection() as connection:
                current_semantic_hash = load_semantic_neighbor_context(
                    connection, evaluated_candidate_ids
                ).context_hash
            if current_semantic_hash != expected_semantic_hash:
                raise ValueError("phase2 semantic context changed before persistence")
        raw_dispositions = payload.get("dispositions", [])
        if not isinstance(raw_dispositions, Sequence) or isinstance(raw_dispositions, str | bytes):
            raise ValueError("dispositions must be a list")
        disposition_reasons: dict[str, str] = {}
        for raw_disposition in raw_dispositions:
            if not isinstance(raw_disposition, Mapping):
                raise ValueError("disposition must be an object")
            _reject_secret_output(raw_disposition)
            candidate_id = raw_disposition.get("candidate_id")
            reason = raw_disposition.get("reason")
            if not isinstance(candidate_id, str) or candidate_id not in evaluated:
                raise ValueError("disposition references an unevaluated candidate")
            if candidate_id not in allowed_candidate_ids:
                raise ValueError("disposition candidate exceeds the job allowlist")
            if reason not in _DISPOSITION_REASONS:
                raise ValueError("unsupported phase2 disposition reason")
            if candidate_id in disposition_reasons:
                raise ValueError("candidate has duplicate dispositions")
            disposition_reasons[candidate_id] = f"phase2_{reason}"
        records: list[ReviewProposalInput] = []
        proposed_candidate_ids: set[str] = set()
        with self.store.connection() as connection:
            for raw in proposals:
                if not isinstance(raw, Mapping):
                    raise ValueError("proposal must be an object")
                _reject_secret_output(raw)
                candidate_ids = raw.get("candidate_ids", [])
                if not isinstance(candidate_ids, Sequence) or isinstance(
                    candidate_ids, str | bytes
                ):
                    raise ValueError("candidate_ids must be a list")
                identifiers = tuple(dict.fromkeys(str(item) for item in candidate_ids))
                if not identifiers:
                    raise ValueError("proposal must reference at least one candidate")
                referenced = set(identifiers)
                if not referenced.issubset(allowed_candidate_ids):
                    raise ValueError("phase2 proposal candidates exceed the job allowlist")
                if not referenced.issubset(evaluated):
                    raise ValueError("phase2 proposal references an unevaluated candidate")
                operation = str(raw.get("operation", "")).upper()
                proposal_type = _OPERATION_MAP.get(operation)
                if proposal_type is None:
                    raise ValueError(f"unsupported phase2 operation: {operation}")
                deterministic_reason = _deterministic_policy_suppression(raw)
                if deterministic_reason is not None:
                    for candidate_id in identifiers:
                        if candidate_id in disposition_reasons:
                            raise ValueError("candidate has conflicting deterministic disposition")
                        disposition_reasons[candidate_id] = deterministic_reason
                    continue
                proposed_candidate_ids.update(referenced)
                placeholders = ",".join("?" for _ in identifiers)
                candidate_rows = connection.execute(
                    f"""
                    SELECT * FROM candidate_claims
                    WHERE candidate_id IN ({placeholders})
                    ORDER BY candidate_id
                    """,
                    identifiers,
                ).fetchall()
                if len(candidate_rows) != len(identifiers):
                    raise ValueError("proposal references an unknown candidate")
                target_slot = _rows_are_one_identity(candidate_rows)
                for candidate in candidate_rows:
                    if self.forgotten.is_claim_forgotten(
                        target_slot, str(candidate["value_fingerprint"])
                    ):
                        raise ValueError("proposal references a forgotten fact")
                proposed_identity = identity_fingerprint(
                    str(raw["subject"]),
                    str(raw["predicate"]),
                    str(raw["scope"]),
                )
                if proposed_identity != target_slot:
                    raise ValueError("proposal identity differs from its candidate slot")
                evidence_rows = connection.execute(
                    f"""
                    SELECT ce.event_id, ce.ordinal, e.source_id, e.partition_id,
                           e.event_type, e.content_fingerprint, e.source_locator,
                           e.occurred_at, e.metadata_json
                    FROM candidate_evidence AS ce
                    JOIN events AS e ON e.event_id = ce.event_id
                    WHERE ce.candidate_id IN ({placeholders})
                    ORDER BY ce.candidate_id, ce.ordinal
                    """,
                    identifiers,
                ).fetchall()
                effective_rows = effective_evidence_rows([dict(row) for row in evidence_rows])
                evidence_ids = [str(row["event_id"]) for row in effective_rows]
                evidence_families = [
                    str(row["evidence_family_fingerprint"]) for row in effective_rows
                ]
                expected = raw.get("expected_head_set_hash")
                if not isinstance(expected, str) or not expected:
                    raise ValueError("proposal must bind expected_head_set_hash")
                if expected not in job.allowed_head_set_hashes:
                    raise ValueError("proposal expected_head_set_hash was not supplied to phase2")
                target_claim_id = str(raw["claim_id"]) if raw.get("claim_id") else None
                if operation == "ADD":
                    if target_claim_id is not None:
                        raise ValueError("ADD proposal cannot target an existing claim")
                else:
                    if target_claim_id is None:
                        raise ValueError(f"{operation} proposal requires claim_id")
                    target = connection.execute(
                        "SELECT identity_fingerprint FROM claims WHERE claim_id = ?",
                        (target_claim_id,),
                    ).fetchone()
                    if target is None or str(target[0]) != target_slot:
                        raise ValueError("proposal claim_id does not match its target slot")
                value = raw["proposed_value"]
                summary = str(raw["proposed_summary"])
                payload_record = {
                    "subject_text": str(raw["subject"]),
                    "predicate": str(raw["predicate"]),
                    "scope": str(raw["scope"]),
                    "value": value,
                    "summary": summary,
                    "valid_from": raw.get("valid_from"),
                    "valid_to": raw.get("valid_to"),
                    "confidence": min(float(row["confidence"]) for row in candidate_rows),
                    "epistemic_status": str(raw["epistemic_status"]),
                    "sensitivity": (
                        "private"
                        if any(row["sensitivity"] == "private" for row in candidate_rows)
                        else "normal"
                    ),
                    "candidate_ids": list(identifiers),
                    "evidence_event_ids": evidence_ids,
                    "evidence_family_fingerprints": evidence_families,
                    "semantic_context_hash": expected_semantic_hash,
                    "rationale": str(raw.get("rationale", "")),
                }
                enum_type = ProposalType(proposal_type)
                records.append(
                    ReviewProposalInput(
                        proposal_type=proposal_type,
                        target_slot_fingerprint=target_slot,
                        expected_head_set_hash=expected,
                        precondition_hash=fingerprint_precondition(
                            proposal_type=enum_type,
                            target_slot_fingerprint=target_slot,
                            expected_head_set_hash=expected,
                        ),
                        proposal_payload_fingerprint=fingerprint(payload_record),
                        evidence_set_fingerprint=fingerprint(evidence_families),
                        payload=payload_record,
                        candidate_id=identifiers[0],
                        candidate_ids=identifiers,
                        target_claim_id=target_claim_id,
                    )
                )
        if proposed_candidate_ids & disposition_reasons.keys():
            raise ValueError("candidate cannot be both proposed and dispositioned")
        if raw_dispositions and proposed_candidate_ids | disposition_reasons.keys() != evaluated:
            raise ValueError("phase2 output must account for every evaluated candidate")
        if not records:
            self.store.disposition_candidates(
                evaluated_candidate_ids,
                reason_code="phase2_no_proposal",
                suppressed_reason_codes=disposition_reasons,
            )
            return 0
        self.store.create_review_batch(
            records,
            consolidator_run_id=job.job_id,
            evaluated_candidate_ids=evaluated_candidate_ids,
            suppressed_reason_codes=disposition_reasons,
        )
        return len(records)
