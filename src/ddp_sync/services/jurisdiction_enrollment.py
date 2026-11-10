"""Dual-read of ddp-open-states' jurisdiction manifest (OPEN-318 slice 3, OPEN-328).

`jurisdictions.yaml` in the ddp-open-states checkout says which jurisdictions are enrolled in which
feature. ddp-sync's own lists in config/sync_schedule.yaml are still the authority today:
PLAN-push-button-onboarding.md 4.1 requires ddp-sync to read BOTH and log every disagreement for at
least one full weekly cycle before the manifest becomes authoritative. This module is that dual-read:

    enrolled(feature, yaml_codes, root)   the answer a consumer uses; logs a disagreement once
    log_config_disagreements(config, root)  every list at once (startup, and the OPEN-330 daily check)

Mode is a per-host env var, `JURISDICTION_MANIFEST_MODE` (SYNC-51 style, never in the shared yaml):
"shadow" (default) returns the yaml's list and only logs; "authoritative" returns the manifest's. Flipping a
host is one env change plus a restart, after the soak.

Never raises. The manifest is only as fresh as the ddp-open-states checkout on this host (a bind mount on the
EC2 broker host that nothing here pulls), so a missing file, an invalid one, or an old one degrades to the
yaml's answer with a log line, and every disagreement log carries the file's mtime so a stale checkout is
not mistaken for real drift. The compare logic mirrors check-jurisdiction-drift.py's `project()` in
ddp-open-states, which is what CI runs against this repo's main. SYNC-152: who is scraped has no yaml list at
all any more; `enrolled_scrape_states` (pipelines/openstates_scrape.py) reads the `scrape` feature straight from the manifest.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timezone

import structlog
import yaml

logger = structlog.get_logger()

MODE_ENV = "JURISDICTION_MANIFEST_MODE"
MANIFEST_FILENAME = "jurisdictions.yaml"

# feature -> (does this manifest entry belong?, does ddp-sync spell the federal jurisdiction "usa" here?)
# The scrape tables and the cloud path say "usa"; archive, embedding and search refresh say "us".
_UNSCHEDULED_STATUSES = ("paused", "probing")

_FEATURES = {
    # OPEN-418: the RECURRING archive list is the manifest's archive.scheduled, not archive.enabled (which only means
    # "may be archived", the Stage 4 manual run). A pre-OPEN-418 manifest has no `scheduled`, which reads as "cannot say".
    "archive": (lambda e: e["archive"]["scheduled"], False),
    # SYNC-146 (2026-11-09): the archive that runs right after a state's own scrape (archive_after_scrape,
    # which then triggers LegBot on the Mac) follows archive.enabled, not `scheduled`: a new state archives after each
    # scrape from Stage 4 on, while its weekly cron (`scheduled`) stays off until it is live (OPEN-418). No yaml list of
    # its own, so it is not part of the drift comparison; its fallback is openstates_archive.jurisdictions.
    "archive_after_scrape": (lambda e: e["archive"]["enabled"], False),
    "embedding": (lambda e: e["enrollment"]["embedding"], False),
    "search_refresh": (lambda e: e["enrollment"]["search_refresh"], False),
    "cloud_path": (lambda e: e["enrollment"]["cloud_path"], True),
    # OPEN-330: the two env-var lists, spelled "us" like the env values themselves.
    "replica_routing": (lambda e: e["enrollment"]["replica_routing"], False),
    "legbot_replica": (lambda e: e["enrollment"]["legbot_replica"], False),
    # SYNC-152: ONE rule for "the states we scrape", read by every reader (the scheduler's per-state pipeline jobs, the
    # manual trigger, the cadence review, the drift check): status is not paused or probing. A state added at Stage 0 joins
    # when its status moves to `scraping` (mirrors check-jurisdiction-drift.py's UNSCHEDULED_STATUSES in ddp-open-states).
    # The manifest's `tier` is not read: WHEN a state is scraped is ddp-sync's `openstates_scrape.timing`, never WHO.
    "scrape": (lambda e: e["status"] not in _UNSCHEDULED_STATUSES, True),
}

_lock = threading.Lock()
_cache: dict[str, tuple[float, dict | None]] = {}  # path -> (mtime, parsed manifest or None)
_logged: set = set()  # de-duplicates per-call logs so a hot path does not repeat one line forever


def _log_once(key, event: str, **fields) -> None:
    with _lock:
        if key in _logged:
            return
        _logged.add(key)
    logger.warning(event, **fields)


def _mtime_iso(path: str) -> str | None:
    try:
        return datetime.fromtimestamp(os.stat(path).st_mtime, tz=timezone.utc).isoformat()
    except OSError:
        return None


def load_manifest(root: str) -> dict | None:
    """The parsed manifest under `root`, or None (logged once) when it cannot be used. Re-read only when
    the file's mtime changes, so a `git pull` of the checkout is picked up without a restart."""
    path = os.path.join(root, MANIFEST_FILENAME)
    try:
        mtime = os.stat(path).st_mtime
    except OSError as exc:
        _log_once(("missing", path), "jurisdiction_manifest_unavailable", path=path, error=str(exc))
        return None
    with _lock:
        cached = _cache.get(path)
    if cached and cached[0] == mtime:
        return cached[1]
    data = None
    try:
        with open(path) as f:
            parsed = yaml.safe_load(f)
        if isinstance(parsed, dict) and parsed and all(isinstance(v, dict) for v in parsed.values()):
            data = parsed
        else:
            _log_once(("shape", path, mtime), "jurisdiction_manifest_unusable", path=path, error="not a mapping of mappings")
    except (OSError, yaml.YAMLError) as exc:
        _log_once(("parse", path, mtime), "jurisdiction_manifest_unusable", path=path, error=str(exc))
    with _lock:
        _cache[path] = (mtime, data)
    return data


def manifest_codes(feature: str, root: str) -> list[str] | None:
    """The jurisdictions the manifest enrolls in `feature`, spelled the way ddp-sync spells them for that
    feature (sorted), or None when the manifest cannot answer."""
    predicate, uses_usa = _FEATURES[feature]
    manifest = load_manifest(root)
    if manifest is None:
        return None
    try:
        codes = [c for c, entry in manifest.items() if predicate(entry)]
    except (KeyError, TypeError) as exc:
        _log_once(("entry", root, feature), "jurisdiction_manifest_unusable", feature=feature, error=f"missing field {exc}")
        return None
    return sorted("usa" if (uses_usa and c == "us") else c for c in codes)


# Where a caller with no sync config in hand (the source-name labels) finds the ddp-open-states checkout: the same
# per-host env var the pipelines read, then their documented default. Mirrors openstates_scrape._get_root.
_DEFAULT_ROOT = "/Users/agentsmith/Developer/repos/ddp-open-states"


def default_root() -> str:
    return os.environ.get("OPENSTATES_ROOT") or _DEFAULT_ROOT


def manifest_name(code: str, root: str | None = None) -> str | None:
    """The manifest's `name` for a jurisdiction code (e.g. 'Pennsylvania'), or None when the manifest cannot say."""
    manifest = load_manifest(root or default_root())
    entry = (manifest or {}).get(str(code).strip().lower())
    name = entry.get("name") if isinstance(entry, dict) else None
    return name.strip() if isinstance(name, str) and name.strip() else None


def _mode() -> str:
    return "authoritative" if os.environ.get(MODE_ENV, "").strip().lower() == "authoritative" else "shadow"


def enrolled(feature: str, yaml_codes, root: str) -> list[str] | None:
    """The jurisdictions to use for `feature`. `yaml_codes` is ddp-sync's own list (None when its yaml has no
    such key). Shadow mode (default) returns the yaml's list and logs a disagreement with the manifest once
    per distinct difference; authoritative mode returns the manifest's. A yaml with no list falls back to the
    manifest in either mode (there is no yaml answer to protect); None is returned only when neither can answer,
    so the caller can fall back to its own last-resort default. An EMPTY list is an answer, never "absent":
    callers must test the result with `is None`, not truthiness. Pass `None` for yaml_codes only where the
    caller has such a fallback; where it does not (the hook gates), pass the yaml's list as it is, `[]` if the
    key is missing, so shadow mode keeps exactly the answer the code gave before OPEN-328."""
    manifest = manifest_codes(feature, root)
    if yaml_codes is None:
        return manifest
    have = [str(c).strip().lower() for c in yaml_codes]
    if manifest is None:
        return have
    only_yaml, only_manifest = sorted(set(have) - set(manifest)), sorted(set(manifest) - set(have))
    if only_yaml or only_manifest:
        path = os.path.join(root, MANIFEST_FILENAME)
        _log_once(("differs", feature, tuple(only_yaml), tuple(only_manifest)), "jurisdiction_manifest_disagrees",
                  feature=feature, only_in_yaml=only_yaml, only_in_manifest=only_manifest,
                  manifest_mtime=_mtime_iso(path), mode=_mode())
    return manifest if _mode() == "authoritative" else have


def _config_lists(config: dict) -> dict[str, list[str] | None]:
    """ddp-sync's own list for each feature, straight from the sync_schedule.yaml dict (None when absent)."""
    archive = config.get("openstates_archive") or {}
    scrape = config.get("openstates_scrape") or {}

    def listed(block, *keys):
        for k in keys:
            block = (block or {}).get(k) if isinstance(block, dict) else None
        return None if block is None else [str(c).strip().lower() for c in block]

    return {
        "archive": listed(archive, "jurisdictions"),
        "embedding": listed(archive, "knowledge_base_embedding", "jurisdictions"),
        "search_refresh": listed(archive, "bill_search_refresh", "jurisdictions"),
        "cloud_path": listed(scrape, "cloud_path", "jurisdictions"),
    }


def config_disagreements(config: dict, root: str) -> list[dict]:
    """Every feature whose yaml list differs from the manifest's. Empty when they agree or the manifest
    cannot answer (that case is logged by load_manifest, not counted as drift).

    SYNC-152: the yaml has no list of scraped states, only `openstates_scrape.timing` for some of them. A timing
    entry for a state the manifest does not scrape is drift (feature `scrape_timing`); a scraped state with no
    entry is not (it gets `openstates_scrape.defaults`)."""
    out = []
    for feature, have in _config_lists(config).items():
        want = manifest_codes(feature, root)
        if have is None or want is None or set(have) == set(want):
            continue
        out.append({"feature": feature, "only_in_yaml": sorted(set(have) - set(want)), "only_in_manifest": sorted(set(want) - set(have))})
    timing = ((config.get("openstates_scrape") or {}).get("timing"))
    scraped = manifest_codes("scrape", root)
    if isinstance(timing, dict) and scraped is not None:
        extra = sorted({str(k).strip().lower() for k in timing} - set(scraped))
        if extra:
            out.append({"feature": "scrape_timing", "only_in_yaml": extra, "only_in_manifest": []})
    return out


def log_config_disagreements(config: dict, root: str) -> int:
    """Compare every list in the sync config with the manifest and log each difference every time it is
    called (unlike `enrolled`, which de-duplicates): this is the line a weekly review counts. Returns the
    number of disagreements. Never raises."""
    try:
        path = os.path.join(root, MANIFEST_FILENAME)
        found = config_disagreements(config, root)
        for d in found:
            logger.warning("jurisdiction_manifest_disagrees", manifest_mtime=_mtime_iso(path), mode=_mode(), **d)
        if not found and load_manifest(root) is not None:
            logger.info("jurisdiction_manifest_agrees", manifest_mtime=_mtime_iso(path), mode=_mode())
        return len(found)
    except Exception as exc:  # noqa: BLE001 - a diagnostic must never take the scheduler down
        logger.warning("jurisdiction_manifest_compare_failed", error=str(exc))
        return 0
