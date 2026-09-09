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

RDS's managed secret is JSON, not a preformatted URL, so this also assembles and URL-encodes
the connection string -- a raw username or password can contain characters (":", "/", "@", "%")
that are valid in a JSON string but would silently corrupt an unescaped URL.

**Corrected 2026-09-09 (post-merge, found live on the ddp-sync host):** the actual secret only
carries `username`/`password` -- confirmed directly against the real secret's keys, not assumed.
No `host`/`port`/`dbname` fields exist in it at all. This matches `render-env.sh`'s own existing
behavior, which already hardcodes `RDS_HOST`/`RDS_PORT`/`RDS_DBNAME` as separate constants
rather than reading them from the secret -- this module now does the same via env vars, since
those three values are fixed per-database, not part of what actually rotates.
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

# 2026-09-09: the RDS-managed secret only carries username/password -- these three don't
# rotate and aren't part of the secret at all, so they come from their own env vars, matching
# render-env.sh's own existing RDS_HOST/RDS_PORT/RDS_DBNAME constants rather than inventing a
# second source of truth for the same three values.
RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT")
RDS_DBNAME = os.getenv("RDS_DBNAME")


def resolve_rds_database_url(secretsmanager_client=None) -> tuple[str | None, str]:
    """Fetch the current RDS credential from Secrets Manager and build a DSN.

    Returns (url, error): on success `error` is "" and `url` is a ready-to-use
    `postgresql://...` string; on any failure -- the secret ARN or connection details aren't
    configured, the API call fails, or the secret's shape isn't what's expected -- `url` is
    None and `error` describes what went wrong. Deliberately never falls back to a
    cached/stale value: a failure here should be loud and visible to the caller, not silently
    absorbed into "proceed anyway with whatever we had."

    `secretsmanager_client` is injectable for tests; production callers should leave it
    unset and get a real boto3 client.
    """
    if not RDS_CREDENTIALS_SECRET_ARN:
        return None, "RDS_CREDENTIALS_SECRET_ARN not set -- refusing to guess which secret to read"
    if not all([RDS_HOST, RDS_PORT, RDS_DBNAME]):
        return None, "RDS_HOST/RDS_PORT/RDS_DBNAME must all be set -- the secret itself only carries username/password"

    try:
        client = secretsmanager_client or boto3.client("secretsmanager", region_name=AWS_REGION)
        response = client.get_secret_value(SecretId=RDS_CREDENTIALS_SECRET_ARN)
    except Exception as e:  # noqa: BLE001 -- any boto3/network failure is equally "can't proceed"
        # pm-review: boto3.client() itself can raise (region/credential-provider/botocore
        # config problems), not just get_secret_value() -- both must land in this same tuple
        # contract, not let a client-construction failure escape uncaught past this function.
        logger.error("rds_credentials: fetch failed", error=str(e))
        return None, f"could not fetch RDS credential from Secrets Manager: {e}"

    try:
        secret = json.loads(response["SecretString"])
        username = secret["username"]
        password = secret["password"]
        # pm-review: a JSON null for either would otherwise stringify to the literal text
        # "None" and silently produce a garbage-but-well-formed DSN instead of an error.
        if not all([username, password]):
            raise ValueError("username or password is null or empty")
    except (KeyError, ValueError, TypeError) as e:
        logger.error("rds_credentials: unexpected secret shape", error=str(e))
        return None, f"RDS credential secret has an unexpected shape: {e}"

    url = (
        f"postgresql://{quote(str(username), safe='')}:{quote(str(password), safe='')}"
        f"@{RDS_HOST}:{RDS_PORT}/{quote(str(RDS_DBNAME), safe='')}"
    )
    return url, ""
