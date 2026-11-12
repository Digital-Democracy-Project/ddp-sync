"""The reader of ddp-open-states' jurisdiction manifest (OPEN-318 slice 3, OPEN-328; SYNC-157).

`jurisdictions.yaml` in the ddp-open-states checkout is the ONE list of which jurisdictions are enrolled in what. SYNC-157
(2026-11-12) deleted every per-state list ddp-sync kept of its own, so every reader takes its answer from here:

    enrolled(feature, None, root)        the codes enrolled in a feature (None: the manifest cannot say)
    is_enrolled(feature, code, root)     one state (False when the manifest cannot say)
    manifest_value(code, path, root)     one per-state value (a timeout, a Fargate wait)
    check_removed_keys(config)           refuses a sync_schedule.yaml that still carries a deleted list

`enrolled` still accepts a yaml list (the OPEN-328 dual-read), but no caller passes one any more, and
`JURISDICTION_MANIFEST_MODE` no longer changes any answer. Never raises (except `check_removed_keys`, on purpose). The
manifest is only as fresh as the ddp-open-states checkout on this host (a bind mount on the EC2 broker host that nothing
here pulls), so a missing, invalid or old file answers None with a log line, and the daily drift check alerts. The
feature rules mirror check-jurisdiction-drift.py's `project()` in ddp-open-states. SYNC-152: who is scraped is the `scrape`
feature (status not paused or probing), read by `enrolled_scrape_states` (pipelines/openstates_scrape.py).
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
    # SYNC-153: who is archived is the manifest's archive.enabled, ONE list: a state's archive follows its own scrape
    # (and the manual archive trigger accepts the same states). There is no recurring archive schedule and no yaml list
    # any more, so `archive.scheduled` (OPEN-418) is not read and this is not part of the yaml comparison.
    "archive": (lambda e: e["archive"]["enabled"], False),
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
    # SYNC-157: per-state behaviour that sync_schedule.yaml used to list by state. The yaml keeps only the global switch
    # (`enabled`) and global numbers (interval_days, max_attempts, backoff_secs); WHO is the manifest. A manifest from
    # before SYNC-157 lacks these fields and answers None (logged once; the daily drift check alerts "unverifiable"),
    # which every reader treats as "not enrolled", never as a guess.
    "sweep_import": (lambda e: e["scrape"]["sweep_import"], True),
    "full_walk": (lambda e: e["scrape"]["full_walk"], True),
    "scrape_retry": (lambda e: e["scrape"]["retry"], True),
    "cadence_escalation": (lambda e: e["scrape"]["cadence_escalation"], True),
    "scrapebot_fallback": (lambda e: e["waf"]["scrapebot_fallback"], True),  # the scrape spells the federal one "usa"
    "scrapebot_fallback_archive": (lambda e: e["waf"]["scrapebot_fallback"], False),  # the archive spells it "us"
    "broker_bill_event_signal": (lambda e: e["enrollment"]["broker_bill_event_signal"], False),
}

# SYNC-157: sync_schedule.yaml keys that were copies of the manifest (or per-state lists that moved into it). A config
# that still carries one is refused at load (`check_removed_keys`), so a hand-edited host file cannot silently keep a
# second list. Path -> where the answer lives now.
REMOVED_CONFIG_KEYS = {
    ("active_jurisdictions",): "nothing reads it (the Redis set fills itself from bill syncs)",
    ("openstates_scrape", "cloud_path", "jurisdictions"): "manifest enrollment.cloud_path",
    ("openstates_scrape", "cloud_path", "memory_backend_jurisdictions"): "manifest enrollment.cloud_path",
    ("openstates_scrape", "cloud_path", "fargate", "max_wait_seconds_by_jurisdiction"): "manifest scrape.fargate_max_wait_s",
    ("openstates_scrape", "broker_bill_event_signal", "jurisdictions"): "manifest enrollment.broker_bill_event_signal",
    ("openstates_scrape", "sweep_import", "jurisdictions"): "manifest scrape.sweep_import",
    ("openstates_scrape", "full_walk", "jurisdictions"): "manifest scrape.full_walk",
    ("openstates_scrape", "scrape_retry", "jurisdictions"): "manifest scrape.retry",
    ("openstates_scrape", "scrape_retry", "jurisdictions_excluded"): "manifest scrape.retry (the validator refuses retry with a WAF)",
    ("openstates_scrape", "scrapebot_fallback", "jurisdictions"): "manifest waf.scrapebot_fallback",
    ("openstates_scrape", "dynamic_cadence", "jurisdictions_excluded"): "manifest scrape.cadence_escalation",
    ("openstates_archive", "scrapebot_fallback", "jurisdictions"): "manifest waf.scrapebot_fallback",
    ("openstates_archive", "knowledge_base_embedding", "jurisdictions"): "manifest enrollment.embedding",
    ("openstates_archive", "bill_search_refresh", "jurisdictions"): "manifest enrollment.search_refresh",
}


class RemovedConfigKey(ValueError):
    """sync_schedule.yaml still carries a per-state list SYNC-157 moved to the jurisdiction manifest."""


def check_removed_keys(config: dict) -> None:
    """Refuse a sync config that still has a key in REMOVED_CONFIG_KEYS (SYNC-157), naming each one."""
    found = []
    for path, now in REMOVED_CONFIG_KEYS.items():
        block = config
        for key in path[:-1]:
            block = block.get(key) if isinstance(block, dict) else None
        if isinstance(block, dict) and path[-1] in block:
            found.append(f"{'.'.join(path)} (now: {now})")
    if found:
        raise RemovedConfigKey("SYNC-157: sync_schedule.yaml still has per-state lists that moved to the jurisdiction "
                               "manifest (ddp-open-states jurisdictions.yaml); delete them: " + "; ".join(found))

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


def manifest_value(code: str, path: tuple[str, ...], root: str):
    """One per-state value from the manifest (e.g. ("scrape", "fargate_max_wait_s")), or None when the manifest, the
    state or the field is missing (logged once). `usa` is read as `us`."""
    manifest = load_manifest(root)
    key = "us" if str(code).strip().lower() == "usa" else str(code).strip().lower()
    value = (manifest or {}).get(key)
    for part in path:
        if not isinstance(value, dict) or part not in value:
            if manifest is not None and key in manifest:
                _log_once(("value", root, path), "jurisdiction_manifest_unusable", field=".".join(path),
                          error="missing field (SYNC-157)")
            return None
        value = value[part]
    return value


def is_enrolled(feature: str, code: str, root: str) -> bool:
    """Is `code` enrolled in `feature`? False when the manifest cannot answer (logged by manifest_codes)."""
    return str(code).strip().lower() in (manifest_codes(feature, root) or [])


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
    """ddp-sync's own per-feature lists. SYNC-157 deleted every one (the manifest is read directly, and
    `check_removed_keys` refuses a config that still has one), so there is nothing left to compare."""
    return {}


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
