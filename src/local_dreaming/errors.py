class DreamingError(Exception):
    """Base error for expected Local-Dreaming failures."""


class ConfigurationError(DreamingError):
    """Raised when runtime configuration is invalid."""


class PrivacyBoundaryError(DreamingError):
    """Raised when content would cross a disallowed privacy boundary."""


class OversizedInputError(PrivacyBoundaryError):
    """Raised before model egress when a deterministic size boundary is exceeded."""


class StaleProposalError(DreamingError):
    """Raised when a review proposal no longer matches canonical state."""


class WorkerProtocolError(DreamingError):
    """Raised when Codex emits an unsafe or malformed event stream."""
