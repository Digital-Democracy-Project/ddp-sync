"""Resolves the live RDS connection string from Secrets Manager at call time.

OPEN-260: every consumer of RDS_DATABASE_URL used to read it once from a rendered .env file
into this process's environment at container start, then reuse that cached value for the
container's whole lifetime -- Fargate archive launches, the local cloud_loader.py subprocess,
and (in ddp-open-states-dev's own tooling) the os-text-extract RDS backfill/dry-run commands.
RDS's own "manage master credentials in Secrets Manager" feature rotates that credential
automatically every 7 days (confirmed via the console: rotation enabled, 7-day schedule) --
a cached value goes stale on that cadence regardless of how recently a container restarted,
and nothing re-rendered .env to match, so every one of those consumers broke identically the
same afternoon (2026-09-09, see OPEN-192's ops-handoff thread) once the rotation fired.

This module fetches the current credential directly from Secrets Manager on every call
instead, so staleness can no longer accumulate between rotations. Deliberately called again
at each real point of use rather than resolved once and threaded through -- an hours-long
Fargate collection followed by a load step should pick up a rotation that happened mid-run,
not carry forward whatever was current when the run started (a single early resolution would
just shrink the staleness window, not close it).

RDS's managed secret is JSON (username/password/host/port/dbname), not a preformatted URL, so
this also assembles and URL-encodes the connection string -- a raw username or password can
contain characters (":", "/", "@", "%") that are valid in a JSON string but would silently
corrupt an unescaped URL.
"""

from __future__ import annotations

import json
import os
from urllib.parse import quote

import boto3
import structlog

from ddp_sync.config import AWS_REGION

logger = structlog.get_logger()

RDS_CREDENTIALS_SECRET_ARN = os.getenv("RDS_CREDENTIALS_SECRET_ARN")


def resolve_rds_database_url(secretsmanager_client=None) -> tuple[str | None, str]:
    """Fetch the current RDS credential from Secrets Manager and build a DSN.

    Returns (url, error): on success `error` is "" and `url` is a ready-to-use
    `postgresql://...` string; on any failure -- the secret ARN isn't configured, the API
    call fails, or the secret's shape isn't what's expected -- `url` is None and `error`
    describes what went wrong. Deliberately never falls back to a cached/stale value: a
    failure here should be loud and visible to the caller, not silently absorbed into
    "proceed anyway with whatever we had."

    `secretsmanager_client` is injectable for tests; production callers should leave it
    unset and get a real boto3 client.
    """
    if not RDS_CREDENTIALS_SECRET_ARN:
        return None, "RDS_CREDENTIALS_SECRET_ARN not set -- refusing to guess which secret to read"

    client = secretsmanager_client or boto3.client("secretsmanager", region_name=AWS_REGION)
    try:
        response = client.get_secret_value(SecretId=RDS_CREDENTIALS_SECRET_ARN)
    except Exception as e:  # noqa: BLE001 -- any boto3/network failure is equally "can't proceed"
        logger.error("rds_credentials: fetch failed", error=str(e))
        return None, f"could not fetch RDS credential from Secrets Manager: {e}"

    try:
        secret = json.loads(response["SecretString"])
        username = secret["username"]
        password = secret["password"]
        host = secret["host"]
        port = secret["port"]
        dbname = secret["dbname"]
    except (KeyError, ValueError, TypeError) as e:
        logger.error("rds_credentials: unexpected secret shape", error=str(e))
        return None, f"RDS credential secret has an unexpected shape: {e}"

    url = (
        f"postgresql://{quote(str(username), safe='')}:{quote(str(password), safe='')}"
        f"@{host}:{port}/{dbname}"
    )
    return url, ""
