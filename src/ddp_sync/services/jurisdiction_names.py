"""The one table of human-readable source names for jurisdictions (OPEN-420).

These strings label the SOURCE of ingested documents (the knowledge base, and so VoteBot's citations), e.g.
"Florida Legislature". The same table used to be copied into three classes (`pipelines/bill_sync.py`,
`pipelines/legislator_sync.py`, `ingestion/sources/openstates.py`), and none of the copies had `nc`, so NC would
have been labelled "NC Legislature" once it was enrolled in embedding (SYNC-95) instead of "North Carolina
Legislature" like the others.

The table keeps every name that is already in the knowledge base exactly as it is (pinned in
`tests/test_jurisdiction_names.py`), including the federal entry, which reads "US Congress" here but "United States
(Congress)" in the manifest. A state that is NOT in the table (every state added after Louisiana) is labelled "<manifest name> Legislature" from the shared
list (`jurisdictions.yaml`), so adding a state needs no edit here (SYNC-146); only when the manifest cannot be read does
it fall back to "<CODE> Legislature". `ca`, `ny` and `tx` are not tracked states; they stay for any non-enrolled caller.
"""

from __future__ import annotations

from ddp_sync.services.jurisdiction_enrollment import manifest_name

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
    "pa": "Pennsylvania Legislature",
    "ne": "Nebraska Legislature",
    "la": "Louisiana Legislature",
    "ca": "California Legislature",
    "ny": "New York Legislature",
    "tx": "Texas Legislature",
}


def source_name(jurisdiction: str | None) -> str:
    """Human-readable source name for a jurisdiction code (e.g. 'fl' -> 'Florida Legislature'). An unknown code falls
    back to '<CODE> Legislature'; an empty one to 'State Legislature' (the behavior of the three copies this replaces)."""
    jurisdiction_lower = jurisdiction.lower() if jurisdiction else ""
    if jurisdiction_lower in JURISDICTION_SOURCE_NAMES:
        return JURISDICTION_SOURCE_NAMES[jurisdiction_lower]
    from_manifest = manifest_name(jurisdiction_lower) if jurisdiction else None
    if from_manifest:
        return f"{from_manifest} Legislature"
    return f"{jurisdiction.upper()} Legislature" if jurisdiction else "State Legislature"
