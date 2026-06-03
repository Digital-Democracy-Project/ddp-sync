"""Structured return types for Webflow CMS operations."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class UpdateResult:
    """Result of updating one or more fields on a single CMS item."""

    item_id: str
    item_name: str
    fields_updated: dict[str, object]
    success: bool
    error: str = ""


@dataclass
class DeleteResult:
    """Result of deleting a CMS item (possibly after removing references)."""

    item_id: str
    item_name: str
    deleted: bool
    references_removed: int = 0
    references_failed: int = 0
    error: str = ""


@dataclass
class FillResult:
    """Aggregate result of a bulk fill operation (session-code, map-url, etc.)."""

    total_items: int = 0
    items_already_filled: int = 0
    items_updated: int = 0
    items_skipped: int = 0
    items_failed: int = 0
    updates: list[UpdateResult] = field(default_factory=list)


@dataclass
class SyncResult:
    """Result of a bill-org synchronization pass."""

    bills_processed: int = 0
    orgs_updated: int = 0
    bills_updated: int = 0
    references_added: int = 0
    about_fields_parsed: int = 0
    missing_field_hooks_sent: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class MergeResult:
    """Result of merging a duplicate org into a canonical org."""

    duplicate_id: str
    canonical_id: str
    duplicate_name: str
    canonical_name: str
    fields_migrated: list[str] = field(default_factory=list)
    bill_refs_repointed: int = 0
    deleted: bool = False
    error: str = ""


@dataclass
class DuplicateGroup:
    """A group of duplicate or companion bills."""

    label: str
    group_type: str  # "duplicate" or "companion"
    match_reasons: list[str]
    items: list[dict]  # item dicts with completeness info
