"""The one table of human-readable source names for jurisdictions (OPEN-420).

These strings label the SOURCE of ingested documents (the knowledge base, and so VoteBot's citations), e.g.
"Florida Legislature". The same table used to be copied into three classes (`pipelines/bill_sync.py`,
`pipelines/legislator_sync.py`, `ingestion/sources/openstates.py`), and none of the copies had `nc`, so NC would
have been labelled "NC Legislature" once it was enrolled in embedding (SYNC-95) instead of "North Carolina
Legislature" like the others.

The manifest (`jurisdictions.yaml`) cannot supply these strings: its `name` is "North Carolina", this label adds
"Legislature", and the federal entry reads "US Congress" here but "United States (Congress)" there. So this stays a
table of its own; `tests/test_jurisdiction_names.py` fails if a manifest jurisdiction is missing from it, so the next
state cannot forget it. `ca`, `ny` and `tx` are not tracked states; they stay for any non-enrolled caller.
"""

from __future__ import annotations

JURISDICTION_SOURCE_NAMES = {
    "us": "US Congress",
    "fl": "Florida Legislature",
    "wa": "Washington Legislature",
    "va": "Virginia Legislature",
    "mi": "Michigan Legislature",
    "ma": "Massachusetts Legislature",
    "ut": "Utah Legislature",
    "az": "Arizona Legislature",
    "al": "Alabama Legislature",
    "nc": "North Carolina Legislature",
    "ca": "California Legislature",
    "ny": "New York Legislature",
    "tx": "Texas Legislature",
}


def source_name(jurisdiction: str | None) -> str:
    """Human-readable source name for a jurisdiction code (e.g. 'fl' -> 'Florida Legislature'). An unknown code falls
    back to '<CODE> Legislature'; an empty one to 'State Legislature' (the behavior of the three copies this replaces)."""
    jurisdiction_lower = jurisdiction.lower() if jurisdiction else ""
    return JURISDICTION_SOURCE_NAMES.get(
        jurisdiction_lower,
        f"{jurisdiction.upper()} Legislature" if jurisdiction else "State Legislature",
    )
