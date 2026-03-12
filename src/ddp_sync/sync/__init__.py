"""Unified sync service for VoteBot content ingestion."""

from ddp_sync.sync.handlers import (
    BillHandler,
    ContentHandler,
    LegislatorHandler,
    OrganizationHandler,
    TrainingHandler,
    WebpageHandler,
)
from ddp_sync.sync.service import UnifiedSyncService
from ddp_sync.sync.types import ContentType, SyncIdentifier, SyncMode, SyncOptions, SyncResult, SyncTarget

__all__ = [
    # Service
    "UnifiedSyncService",
    # Types
    "ContentType",
    "SyncMode",
    "SyncIdentifier",
    "SyncOptions",
    "SyncResult",
    "SyncTarget",
    # Handlers
    "ContentHandler",
    "BillHandler",
    "LegislatorHandler",
    "OrganizationHandler",
    "WebpageHandler",
    "TrainingHandler",
]
