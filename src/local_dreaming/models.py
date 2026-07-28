from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

type JsonScalar = str | int | float | bool | None


class Sensitivity(StrEnum):
    NORMAL = "normal"
    PRIVATE = "private"
    SECRET = "secret"


class SourceKind(StrEnum):
    USER_DIRECT = "user_direct"
    MANUAL = "manual"
    CODEX_TASK = "codex_task"
    DREAMING_HANDOFF = "dreaming_handoff"
    ASSISTANT_FINAL = "assistant_final"
    TOOL_RESULT = "tool_result"
    CHRONICLE = "chronicle"
    CODEX_MEMORY = "codex_memory"
    LINE_SYNTHETIC = "line_synthetic"


class ClaimScope(StrEnum):
    USER_PROFILE = "user_profile"
    PROJECT_STATE = "project_state"
    PERSONAL_RELATIONSHIP = "personal_relationship"
    OTHER = "other"


class ProposalType(StrEnum):
    ADD = "add"
    UPDATE = "update"
    SUPERSEDE = "supersede"
    DISPUTE = "dispute"
    NARROW = "narrow"


class ProposalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CORRECTED = "corrected"
    STALE = "stale"
    APPLIED = "applied"


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    """Explicit source-level privacy and trust policy.

    Opt-in is deliberately separate from model egress. A source can be ingested
    locally without allowing its contents to leave the machine.
    """

    source_id: str
    source_kind: SourceKind
    opted_in: bool
    sensitivity: Sensitivity = Sensitivity.NORMAL
    allow_model_egress: bool = False
    allow_private_model_egress: bool = False

    def permits_model_egress(self, sensitivity: Sensitivity) -> bool:
        if not self.opted_in or sensitivity is Sensitivity.SECRET:
            return False
        if sensitivity is Sensitivity.PRIVATE:
            return self.allow_private_model_egress
        return self.allow_model_egress

    def permits_claim_scope(self, scope: ClaimScope) -> bool:
        if self.source_kind in {SourceKind.CHRONICLE, SourceKind.CODEX_MEMORY}:
            return False
        if self.source_kind in {
            SourceKind.ASSISTANT_FINAL,
            SourceKind.DREAMING_HANDOFF,
            SourceKind.TOOL_RESULT,
        }:
            return scope is ClaimScope.PROJECT_STATE
        return True


@dataclass(frozen=True, slots=True)
class IngestEventInput:
    source_id: str
    partition_id: str
    source_kind: SourceKind
    external_event_id: str
    occurred_at: datetime
    content: str
    sensitivity: Sensitivity = Sensitivity.NORMAL
    source_locator: str | None = None
    metadata: dict[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")
        for field_name in ("source_id", "partition_id", "external_event_id"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")


@dataclass(frozen=True, slots=True)
class PreparedEvent:
    event_id: str
    source_id: str
    partition_id: str
    source_kind: SourceKind
    external_event_id: str
    occurred_at: datetime
    content: str
    content_fingerprint: str
    sensitivity: Sensitivity
    redaction_count: int
    eligible_for_model: bool
    source_locator: str | None = None
    metadata: dict[str, JsonScalar] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EpisodeDraft:
    episode_id: str
    source_id: str
    partition_id: str
    event_ids: tuple[str, ...]
    started_at: datetime
    ended_at: datetime
    content: str
    content_fingerprint: str
    sensitivity: Sensitivity
    segmentation_reason: str
    segmenter_version: str


@dataclass(frozen=True, slots=True)
class DreamingHandoff:
    workspace: str
    state: str
    completed: str
    verified: str
    leo_corrections: str
    pending: str
    redaction_count: int = 0
    classification: ClaimScope = ClaimScope.PROJECT_STATE


@dataclass(frozen=True, slots=True)
class ClaimHead:
    claim_version_id: str
    value_fingerprint: str
    status: str
    valid_from: str | None = None
    valid_to: str | None = None


@dataclass(frozen=True, slots=True)
class ReviewProposal:
    proposal_id: str
    batch_id: str
    proposal_type: ProposalType
    base_memory_revision: int
    target_slot_fingerprint: str
    expected_head_set_hash: str
    precondition_hash: str
    proposal_payload_fingerprint: str
    evidence_set_fingerprint: str
    status: ProposalStatus = ProposalStatus.PENDING


@dataclass(frozen=True, slots=True)
class ReviewValidation:
    proposal_id: str
    fresh: bool
    current_head_set_hash: str
    reason: str | None = None
