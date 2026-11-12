"""Human-readable source names for jurisdictions (OPEN-420; SYNC-157: from the manifest, no table).

These strings label the SOURCE of ingested documents (the knowledge base, and so VoteBot's citations), e.g.
"Florida Legislature". SYNC-157 deleted the per-state table this module kept: a state is labelled
"<manifest name> Legislature" from the shared list (ddp-open-states `jurisdictions.yaml`, `manifest_name`), which gives
exactly the strings already in the knowledge base for every tracked state (pinned in `tests/test_jurisdiction_names.py`).
The one exception is the federal jurisdiction, whose manifest name is "United States (Congress)": it keeps its
knowledge-base label "US Congress". When the manifest cannot be read the label falls back to "<CODE> Legislature"; the
knowledge-base embedding is gated on the same manifest (`enrollment.embedding`), so it writes nothing in that case.
"""

from __future__ import annotations

from ddp_sync.services.jurisdiction_enrollment import manifest_name

FEDERAL_SOURCE_NAME = "US Congress"  # the knowledge base's label for "us"; the manifest calls it "United States (Congress)"


def source_name(jurisdiction: str | None) -> str:
    """Human-readable source name for a jurisdiction code (e.g. 'fl' -> 'Florida Legislature'). A code the manifest
    cannot name falls back to '<CODE> Legislature'; an empty one to 'State Legislature'."""
    if not jurisdiction:
        return "State Legislature"
    code = jurisdiction.strip().lower()
    if code in ("us", "usa"):
        return FEDERAL_SOURCE_NAME
    from_manifest = manifest_name(code)
    return f"{from_manifest} Legislature" if from_manifest else f"{jurisdiction.upper()} Legislature"
