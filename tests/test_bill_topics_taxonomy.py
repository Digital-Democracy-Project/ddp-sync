"""SYNC-124: bill_topics validation takes its list from the copied taxonomy file, not a hand-typed constant.

The file (src/ddp_sync/data/bill_topics_taxonomy.json) is a verbatim copy of the one ddp-broker-py generates
from its Topic table (BROKER-143). These tests make the file the single source for the validator, and catch
a hand edit to the copy: its version must still be the hash the broker's generator would give its content.
"""

import hashlib
import importlib.resources
import json

from ddp_sync.pipelines import bill_artifact_generation as gen


def _file() -> dict:
    return json.loads(
        importlib.resources.files("ddp_sync").joinpath("data", "bill_topics_taxonomy.json").read_text(encoding="utf-8")
    )


def test_the_validator_uses_exactly_the_files_names_cap_and_version():
    data = _file()
    assert gen._BILL_TOPICS_TAXONOMY == tuple(data["topics"])
    assert gen._BILL_TOPICS_MAX == data["max_topics"]
    assert gen._BILL_TOPICS_VERSION == data["version"]
    assert set(gen._BILL_TOPICS_CANONICAL_BY_FOLD.values()) == set(data["topics"])


def test_the_copy_is_unedited_its_version_is_the_brokers_hash_of_its_content():
    # The broker's rule (common/bill_topics_taxonomy.py): sha256 of {"max_topics", "topics"} as canonical JSON,
    # first 12 hex characters. Editing a name or the cap by hand without regenerating breaks this.
    data = _file()
    body = {"max_topics": data["max_topics"], "topics": data["topics"]}
    canonical = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert data["version"] == hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
    assert set(data) == {"version", "max_topics", "topics"}


def test_matching_uses_the_files_display_names():
    assert gen._canonicalize_bill_topic("military AND veterans ") == "Military and Veterans"
    assert gen._canonicalize_bill_topic("Not A Topic") is None
