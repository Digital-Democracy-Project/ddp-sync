"""Pure utility functions used across services."""

from __future__ import annotations

import re

from ddp_sync.webflow_cms.constants import KEY_FIELDS, MAP_BASE_URL


def item_label(item: dict) -> str:
    """Return a human-readable label for a CMS item (name, slug, or ID)."""
    data = item.get("fieldData", {}) or {}
    return data.get("name") or data.get("slug") or item.get("id", "(unknown)")


def parse_open_states_url(url: str | None) -> dict | None:
    """Parse an Open States URL and extract jurisdiction, session-code, bill-prefix, bill-number.

    URL pattern: https://openstates.org/{jurisdiction}/bills/{session-code}/{bill-identifier}/

    Returns a dict with keys: jurisdiction, session_code, bill_prefix, bill_number
    Returns None if URL cannot be parsed.
    """
    if not url:
        return None
    pattern = r"https?://openstates\.org/([^/]+)/bills/([^/]+)/([A-Za-z]+)(\d+)/?"
    match = re.match(pattern, url)
    if not match:
        return None
    return {
        "jurisdiction": match.group(1).upper(),
        "session_code": match.group(2),
        "bill_prefix": match.group(3).upper(),
        "bill_number": match.group(4),
    }


def build_map_url(parsed: dict) -> str:
    """Build the DDP map URL from parsed Open States URL components."""
    selected_bill = f"{parsed['bill_prefix']}{parsed['bill_number']}"
    return (
        f"{MAP_BASE_URL}?mapInteractionLevel=mini"
        f"&jurisdictionIso2={parsed['jurisdiction']}"
        f"&selectedBill={selected_bill}"
        f"&censusYear=2020"
        f"&sessionCode={parsed['session_code']}"
    )


def is_compliant_map_url(existing_url: str | None, expected_url: str) -> bool:
    """Check whether an existing map-url matches the expected format."""
    if not existing_url:
        return False
    return existing_url.strip().rstrip("/") == expected_url.strip().rstrip("/")


def normalize_title(title: str | None) -> str | None:
    """Normalize a bill title by removing spaces between prefix and number.

    Examples:
        "HB 123"    -> "HB123"
        "SB 5181"   -> "SB5181"
        "SJRES 83"  -> "SJRES83"
    """
    if not title:
        return None
    normalized = title.strip()
    # Remove spaces between letter sequences and numbers
    normalized = re.sub(r"([A-Za-z]+)\s+(\d+)", r"\1\2", normalized)
    return normalized.upper()


def has_random_slug_suffix(slug: str | None) -> bool:
    """Detect Webflow-generated random slug suffixes (e.g. '-9ef5a').

    Webflow appends random suffixes like '-9ef5a' to slugs when there's a
    naming conflict.  The pattern is: ends with hyphen followed by 4-6
    alphanumeric chars that look random.
    """
    if not slug:
        return False
    match = re.search(r"-([a-z0-9]{4,6})$", slug)
    if not match:
        return False
    suffix = match.group(1)
    has_letters = any(c.isalpha() for c in suffix)
    has_numbers = any(c.isdigit() for c in suffix)
    # Mixed letters and numbers = likely random
    if has_letters and has_numbers:
        return True
    # All letters: only flag if it looks random (mostly consonants)
    if has_letters and not has_numbers:
        legitimate_suffixes = {"bills", "draft", "final", "intro", "amend"}
        if suffix in legitimate_suffixes:
            return False
        vowels = set("aeiou")
        vowel_count = sum(1 for c in suffix if c in vowels)
        if len(suffix) >= 5 and vowel_count <= 1:
            return True
    return False


def analyze_field_completeness(field_data: dict) -> dict:
    """Analyze which KEY_FIELDS are populated vs empty.

    Returns a dict with populated_count, empty_count, total_fields,
    populated_fields, and empty_fields.
    """
    populated = []
    empty = []
    for f in KEY_FIELDS:
        value = field_data.get(f)
        if value and (not isinstance(value, list) or len(value) > 0):
            populated.append(f)
        else:
            empty.append(f)
    return {
        "populated_count": len(populated),
        "empty_count": len(empty),
        "total_fields": len(KEY_FIELDS),
        "populated_fields": populated,
        "empty_fields": empty,
    }


def parse_about_organization(text: str) -> dict[str, str]:
    """Parse the about-organization rich-text field into sub-sections.

    Recognized sections: Description, Type, Policies, Funding, Affiliates.
    Returns a dict mapping lowercase section name -> content.
    """
    if not text:
        return {}
    pattern = re.compile(
        r"(?ms)^(Description|Type|Policies|Funding|Affiliates):\s*"
        r"(.*?)\s*(?=^(?:Description|Type|Policies|Funding|Affiliates):|\Z)",
        re.MULTILINE,
    )
    return {name.lower(): val.strip() for name, val in pattern.findall(text)}
