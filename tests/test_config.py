"""Tests for ddp_sync.config's SESSION_PIPELINE_CONCURRENCY env-var handling
(AGENTS-37). config.py has two separate literals for this setting -- the
SyncSettings dataclass default and _load_from_env()'s os.getenv fallback --
that could silently drift apart; these tests exercise the actual env-parsing
path directly, not just the dataclass default already covered in
test_session_pipeline_runner.py.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ddp_sync.config import SyncSettings, _load_from_env, _parse_trigger_limit, get_settings


def test_load_from_env_defaults_session_pipeline_concurrency_to_one(monkeypatch):
    """AGENTS-37: the env-var fallback must also resolve to 1 when
    SESSION_PIPELINE_CONCURRENCY is unset -- this is the literal
    os.getenv("SESSION_PIPELINE_CONCURRENCY", "1") line PR AGENTS-37
    actually changed, not just the dataclass default."""
    monkeypatch.delenv("SESSION_PIPELINE_CONCURRENCY", raising=False)
    assert _load_from_env()["session_pipeline_concurrency"] == 1


def test_load_from_env_honors_explicit_session_pipeline_concurrency_override(monkeypatch):
    """AGENTS-37 lowers the DEFAULT only -- operators must still be able to
    opt into a higher value via the env var once real concurrent MLX-LM
    throughput on the target hardware is actually benchmarked and shown
    safe. This is not a removal of configurability."""
    monkeypatch.setenv("SESSION_PIPELINE_CONCURRENCY", "4")
    assert _load_from_env()["session_pipeline_concurrency"] == 4


def test_load_from_env_defaults_replica_freshness_allowlist_to_empty(monkeypatch):
    """OPEN-276: unset must resolve to an empty frozenset -- trusting no jurisdiction by default,
    not "everything," matching plan §3.6's own default (skip, don't fall back to trusting
    unverified data)."""
    monkeypatch.delenv("LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST", raising=False)
    assert _load_from_env()["legbot_rds_replica_jurisdiction_allowlist"] == frozenset()


def test_load_from_env_parses_replica_freshness_allowlist_normalizing_case_and_whitespace(
    monkeypatch,
):
    monkeypatch.setenv("LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST", "fl, VA ,ut")
    assert _load_from_env()["legbot_rds_replica_jurisdiction_allowlist"] == frozenset(
        {"FL", "VA", "UT"}
    )


def test_load_from_env_replica_freshness_allowlist_ignores_empty_entries(monkeypatch):
    """A trailing comma or accidental double-comma must not produce a bogus empty-string
    'jurisdiction' that could never legitimately match anything, but shouldn't be silently
    counted as a configured entry either."""
    monkeypatch.setenv("LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST", "fl,,va,")
    assert _load_from_env()["legbot_rds_replica_jurisdiction_allowlist"] == frozenset({"FL", "VA"})


def test_load_from_env_defaults_every_sync51_task_flag_to_true(monkeypatch):
    """SYNC-51: every per-task flag must default True -- merging this change alone must
    change nothing on any existing host (Mac, EC2-civic), since none of them will have
    these vars set in their .env yet."""
    flag_env_vars = [
        "BILL_SYNC_ENABLED",
        "LEGISLATOR_SYNC_ENABLED",
        "LEGISLATOR_BIO_SYNC_ENABLED",
        "ORGANIZATION_SYNC_ENABLED",
        "VOATZ_SYNC_ENABLED",
        "WEBFLOW_BATCH_ENABLED",
        "VOTEBOT_EVAL_ENABLED",
        "API_HEALTH_CHECK_ENABLED",
        "OPENSTATES_SCRAPE_ENABLED",
        "OPENSTATES_ARCHIVE_ENABLED",
        "MI_COOKIE_PUBLISH_ENABLED",
        "SESSION_PIPELINE_BATCH_ENABLED",
    ]
    for var in flag_env_vars:
        monkeypatch.delenv(var, raising=False)

    loaded = _load_from_env()
    for var in flag_env_vars:
        key = var.lower()
        assert loaded[key] is True, f"{key} must default True"


def test_load_from_env_honors_explicit_task_flag_opt_out(monkeypatch):
    """The actual SYNC-51 use case: a host's own .env opts a specific task out."""
    monkeypatch.setenv("VOATZ_SYNC_ENABLED", "false")
    monkeypatch.setenv("WEBFLOW_BATCH_ENABLED", "false")
    monkeypatch.delenv("OPENSTATES_SCRAPE_ENABLED", raising=False)

    loaded = _load_from_env()
    assert loaded["voatz_sync_enabled"] is False
    assert loaded["webflow_batch_enabled"] is False
    assert loaded["openstates_scrape_enabled"] is True


def test_task_flags_still_apply_when_secrets_manager_supplies_the_base_config(monkeypatch):
    """SYNC-51, found live on the EC2-broker host: get_settings() picks EITHER Secrets Manager
    OR .env for the whole config, never both -- so on any host where Secrets Manager succeeds
    (every EC2 host with an instance role that can reach it), _load_from_env() -- and every
    os.getenv() call inside it -- never ran, making the 12 per-task flags permanently inert
    exactly where they're needed most. Setting the env vars in the container's real
    environment had zero effect. This reproduces that exact scenario: a Secrets Manager
    payload that predates SYNC-51 (no flag keys in it at all, matching the real
    ddp-sync/credentials secret today) combined with real env vars set in the process."""
    monkeypatch.setenv("VOATZ_SYNC_ENABLED", "false")
    monkeypatch.setenv("OPENSTATES_SCRAPE_ENABLED", "true")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={"api_key": "from-secrets-manager"},  # no flag keys -- the real secret's shape
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"  # confirms Secrets Manager path was taken
        assert settings.voatz_sync_enabled is False
        assert settings.openstates_scrape_enabled is True
        # A flag with no env var set at all still falls back to the dataclass default (True),
        # not to whatever Secrets Manager omitted -- there's no flag key in that payload either.
        assert settings.webflow_batch_enabled is True
    finally:
        get_settings.cache_clear()


def test_redis_url_still_applies_when_secrets_manager_supplies_the_base_config(monkeypatch):
    """OPEN-193, found verifying PR #110 live on the EC2-broker host: same bug, different
    field. redis_url is host-specific the same way the 12 SYNC-51 flags are, but wasn't in
    that override list, so a real REDIS_URL set in the container's environment silently lost
    to the ddp-sync/credentials secret's own stored redis_url whenever Secrets Manager
    supplied the base config."""
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/3")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        # the real secret's shape: its own stale redis_url, distinct from the real env value
        return_value={"api_key": "from-secrets-manager", "redis_url": "redis://localhost:6379/0"},
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"  # confirms Secrets Manager path was taken
        assert settings.redis_url == "redis://redis:6379/3"
    finally:
        get_settings.cache_clear()


def test_mac_ddp_sync_base_url_and_rds_openstates_api_base_apply_over_secrets_manager(
    monkeypatch,
):
    """SYNC-59/SYNC-65, found live on the EC2-broker host verifying PR #148's Mac/EC2 archive-
    hook split (2026-09-13): third instance of the exact same bug. Both fields are per-host
    resource addresses set only via docker-compose.prod.yml's environment block, not part of
    the shared ddp-sync/credentials secret -- so a real value set in the container's
    environment was silently losing to whatever (or nothing) Secrets Manager supplied.
    Confirmed live: both settings came back "" despite being correctly set in the real
    container environment, causing resolve_touched_sessions(api_base="") to raise
    httpx.UnsupportedProtocol -- caught and logged by the archive-completion hook's own
    except Exception, so the hook appeared to run successfully while silently never
    triggering LegBot."""
    monkeypatch.setenv("MAC_DDP_SYNC_BASE_URL", "http://10.0.0.8:8001")
    monkeypatch.setenv("RDS_OPENSTATES_API_BASE", "http://10.0.0.11:8002")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        # the real secret's shape: neither field present at all, matching production today
        return_value={"api_key": "from-secrets-manager"},
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"  # confirms Secrets Manager path was taken
        assert settings.mac_ddp_sync_base_url == "http://10.0.0.8:8001"
        assert settings.rds_openstates_api_base == "http://10.0.0.11:8002"
    finally:
        get_settings.cache_clear()


def test_mac_ddp_sync_base_url_and_rds_openstates_api_base_env_wins_over_conflicting_secret(
    monkeypatch,
):
    """pm-review: the previous test's Secrets Manager fixture omits both fields entirely,
    which proves env values apply but not that they take PRECEDENCE over a real, conflicting
    secret-supplied value -- the actual claim this override loop makes. This pins that down
    directly, the same way redis_url's own test above does."""
    monkeypatch.setenv("MAC_DDP_SYNC_BASE_URL", "http://10.0.0.8:8001")
    monkeypatch.setenv("RDS_OPENSTATES_API_BASE", "http://10.0.0.11:8002")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={
            "api_key": "from-secrets-manager",
            "mac_ddp_sync_base_url": "http://stale-value-from-secret:9999",
            "rds_openstates_api_base": "http://stale-value-from-secret:9999",
        },
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"
        assert settings.mac_ddp_sync_base_url == "http://10.0.0.8:8001"
        assert settings.rds_openstates_api_base == "http://10.0.0.11:8002"
    finally:
        get_settings.cache_clear()


def test_legbot_scrape_completion_trigger_fields_apply_over_secrets_manager(monkeypatch):
    """OPEN-290, found live on the EC2-broker host verifying the WireGuard-relayed dispatch
    body OPEN-290 added (2026-09-14): fourth instance of the exact same bug. These four
    fields are per-host the same way mac_ddp_sync_base_url/rds_openstates_api_base are, but
    nothing on EC2 ever needed to resolve them locally until OPEN-290's WireGuard caller
    started building its own request body from them -- confirmed live: all four came back
    their dataclass defaults regardless of what was set in the container's real environment."""
    monkeypatch.setenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED", "true")
    monkeypatch.setenv(
        "LEGBOT_SCRAPE_COMPLETION_TRIGGER_ARTIFACT_TYPES", "bill_summary,bill_changelog"
    )
    monkeypatch.setenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_LIMIT", "25")
    monkeypatch.setenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_INCLUDE_CONCEPT_STATEMENTS", "false")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        # the real secret's shape: none of these four fields present, matching production today
        return_value={"api_key": "from-secrets-manager"},
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"  # confirms Secrets Manager path was taken
        assert settings.legbot_scrape_completion_trigger_enabled is True
        assert settings.legbot_scrape_completion_trigger_artifact_types == [
            "bill_summary", "bill_changelog",
        ]
        assert settings.legbot_scrape_completion_trigger_limit == 25
        assert settings.legbot_scrape_completion_trigger_include_concept_statements is False
    finally:
        get_settings.cache_clear()


def test_legbot_scrape_completion_trigger_fields_env_wins_over_conflicting_secret(monkeypatch):
    """pm-review pattern (see the mac_ddp_sync_base_url test above): the previous test's
    Secrets Manager fixture omits all four fields, which proves env values apply but not that
    they take PRECEDENCE over a real, conflicting secret-supplied value."""
    monkeypatch.setenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED", "true")
    monkeypatch.setenv(
        "LEGBOT_SCRAPE_COMPLETION_TRIGGER_ARTIFACT_TYPES", "bill_summary,bill_changelog"
    )
    monkeypatch.setenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_LIMIT", "25")
    monkeypatch.setenv("LEGBOT_SCRAPE_COMPLETION_TRIGGER_INCLUDE_CONCEPT_STATEMENTS", "false")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={
            "api_key": "from-secrets-manager",
            "legbot_scrape_completion_trigger_enabled": False,
            "legbot_scrape_completion_trigger_artifact_types": ["stale_from_secret"],
            "legbot_scrape_completion_trigger_limit": 999999,
            "legbot_scrape_completion_trigger_include_concept_statements": True,
        },
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"
        assert settings.legbot_scrape_completion_trigger_enabled is True
        assert settings.legbot_scrape_completion_trigger_artifact_types == [
            "bill_summary", "bill_changelog",
        ]
        assert settings.legbot_scrape_completion_trigger_limit == 25
        assert settings.legbot_scrape_completion_trigger_include_concept_statements is False
    finally:
        get_settings.cache_clear()


def test_legbot_scrape_completion_trigger_artifact_types_env_ignores_empty_entries(
    monkeypatch,
):
    """Same trailing/double-comma hazard the replica-freshness allowlist parser above already
    guards against."""
    monkeypatch.setenv(
        "LEGBOT_SCRAPE_COMPLETION_TRIGGER_ARTIFACT_TYPES", "bill_summary,,bill_changelog,"
    )
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={"api_key": "from-secrets-manager"},
    ):
        settings = get_settings()

    try:
        assert settings.legbot_scrape_completion_trigger_artifact_types == [
            "bill_summary", "bill_changelog",
        ]
    finally:
        get_settings.cache_clear()


# --- SYNC-89: knowledge_base_index_name ------------------------------------------------------


def test_knowledge_base_index_name_defaults_to_unset(monkeypatch):
    """SYNC-89: unset means the new embedding path is disabled; the legacy setting is untouched."""
    monkeypatch.delenv("KNOWLEDGE_BASE_INDEX_NAME", raising=False)
    raw = _load_from_env()
    assert raw["knowledge_base_index_name"] == ""
    assert raw["pinecone_index_name"] == "votebot-large"


def test_get_settings_env_override_wins_over_secrets_manager(monkeypatch):
    """SYNC-51 bug class: on a host where Secrets Manager succeeds, the process environment
    must still win for this per-host setting."""
    get_settings.cache_clear()
    monkeypatch.setenv("KNOWLEDGE_BASE_INDEX_NAME", " ddp-knowledge-base ")
    with patch("ddp_sync.config._load_from_secrets_manager", return_value={"pinecone_index_name": "votebot-large"}):
        settings = get_settings()
    get_settings.cache_clear()
    assert settings.knowledge_base_index_name == "ddp-knowledge-base"
    assert settings.pinecone_index_name == "votebot-large"


def test_knowledge_base_settings_swaps_only_the_index_name():
    import pytest

    from ddp_sync.config import SyncSettings, knowledge_base_settings

    base = SyncSettings(knowledge_base_index_name="ddp-knowledge-base")
    kb = knowledge_base_settings(base)
    assert kb.pinecone_index_name == "ddp-knowledge-base"
    assert base.pinecone_index_name == "votebot-large"  # original object untouched
    assert kb.pinecone_namespace == base.pinecone_namespace

    with pytest.raises(ValueError):
        knowledge_base_settings(SyncSettings())
    with pytest.raises(ValueError):
        knowledge_base_settings(SyncSettings(knowledge_base_index_name="votebot-large"))


def test_knowledge_base_settings_refuses_votebot_large_even_if_the_legacy_setting_moves():
    import pytest

    from ddp_sync.config import SyncSettings, knowledge_base_settings

    with pytest.raises(ValueError):
        knowledge_base_settings(
            SyncSettings(pinecone_index_name="something-else", knowledge_base_index_name="votebot-large")
        )


def test_secrets_manager_value_is_used_when_the_env_var_is_absent(monkeypatch):
    """The empty default _load_from_env() emits must not mask a Secrets Manager value."""
    get_settings.cache_clear()
    monkeypatch.delenv("KNOWLEDGE_BASE_INDEX_NAME", raising=False)
    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={"knowledge_base_index_name": "ddp-knowledge-base"},
    ):
        settings = get_settings()
    get_settings.cache_clear()
    assert settings.knowledge_base_index_name == "ddp-knowledge-base"


def test_load_from_env_reads_patch_refresh_opt_out(monkeypatch):
    """OPEN-320: defaults True (Mac unchanged); a host's own env opts out."""
    monkeypatch.delenv("OPENSTATES_PATCH_REFRESH_ENABLED", raising=False)
    assert _load_from_env()["openstates_patch_refresh_enabled"] is True

    monkeypatch.setenv("OPENSTATES_PATCH_REFRESH_ENABLED", "false")
    assert _load_from_env()["openstates_patch_refresh_enabled"] is False


def test_patch_refresh_opt_out_applies_when_secrets_manager_supplies_the_base_config(monkeypatch):
    """OPEN-320: the EC2-broker host gets its config from Secrets Manager, where SYNC-51's
    flags were once silently inert -- pin that this one takes effect on that same path."""
    monkeypatch.setenv("OPENSTATES_PATCH_REFRESH_ENABLED", "false")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={"api_key": "from-secrets-manager"},
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"  # Secrets Manager path was taken
        assert settings.openstates_patch_refresh_enabled is False
    finally:
        get_settings.cache_clear()


# --- SYNC-91: ddp_broker_api_base / ddp_broker_api_token ------------------------------------------------


def test_the_broker_base_and_token_come_from_the_environment_on_a_secrets_manager_host(monkeypatch):
    """SYNC-51 bug class, found live 2026-10-05: with Secrets Manager supplying the config the container's
    DDP_BROKER_API_BASE / DDP_BROKER_API_TOKEN were ignored and the base stayed http://localhost:8080."""
    monkeypatch.setenv("DDP_BROKER_API_BASE", "https://broker.example.org")
    monkeypatch.setenv("DDP_BROKER_API_TOKEN", "from-env")
    get_settings.cache_clear()
    with patch("ddp_sync.config._load_from_secrets_manager", return_value={"api_key": "from-secrets-manager"}):
        settings = get_settings()
    get_settings.cache_clear()
    assert settings.api_key == "from-secrets-manager"  # the Secrets Manager path really was taken
    assert settings.ddp_broker_api_base == "https://broker.example.org"
    assert settings.ddp_broker_api_token == "from-env"


def test_the_broker_environment_wins_over_a_conflicting_secret_value(monkeypatch):
    monkeypatch.setenv("DDP_BROKER_API_BASE", "https://broker.example.org")
    monkeypatch.setenv("DDP_BROKER_API_TOKEN", "from-env")
    get_settings.cache_clear()
    with patch("ddp_sync.config._load_from_secrets_manager", return_value={
        "ddp_broker_api_base": "http://stale:1", "ddp_broker_api_token": "stale"}):
        settings = get_settings()
    get_settings.cache_clear()
    assert (settings.ddp_broker_api_base, settings.ddp_broker_api_token) == ("https://broker.example.org", "from-env")


def test_a_host_that_sets_neither_variable_keeps_what_the_secret_or_the_default_gives(monkeypatch):
    """The Mac must be unchanged: no variable set means the secret's value, else the default, as before."""
    monkeypatch.delenv("DDP_BROKER_API_BASE", raising=False)
    monkeypatch.delenv("DDP_BROKER_API_TOKEN", raising=False)
    get_settings.cache_clear()
    with patch("ddp_sync.config._load_from_secrets_manager", return_value={
        "ddp_broker_api_base": "https://from-secret", "ddp_broker_api_token": "secret-token"}):
        from_secret = get_settings()
    get_settings.cache_clear()
    with patch("ddp_sync.config._load_from_secrets_manager", return_value={"api_key": "k"}):
        defaulted = get_settings()
    get_settings.cache_clear()
    assert (from_secret.ddp_broker_api_base, from_secret.ddp_broker_api_token) == ("https://from-secret", "secret-token")
    assert (defaulted.ddp_broker_api_base, defaulted.ddp_broker_api_token) == ("http://localhost:8080", "")


# --- SYNC-103: the scraper-trigger bill limit has no default ----------------------------------

_LIMIT_ENV = "LEGBOT_SCRAPE_COMPLETION_TRIGGER_LIMIT"


def test_trigger_limit_defaults_to_no_limit(monkeypatch):
    """Both literals that used to say 10000 (the dataclass default and _load_from_env's
    fallback) must now agree on None, or one host would be capped and another not."""
    monkeypatch.delenv(_LIMIT_ENV, raising=False)
    assert SyncSettings().legbot_scrape_completion_trigger_limit is None
    assert _load_from_env()["legbot_scrape_completion_trigger_limit"] is None


@pytest.mark.parametrize("raw", ["", "   "])
def test_blank_trigger_limit_means_no_limit(monkeypatch, raw):
    monkeypatch.setenv(_LIMIT_ENV, raw)
    assert _load_from_env()["legbot_scrape_completion_trigger_limit"] is None


def test_a_positive_trigger_limit_is_still_honored(monkeypatch):
    monkeypatch.setenv(_LIMIT_ENV, "2500")
    assert _load_from_env()["legbot_scrape_completion_trigger_limit"] == 2500


@pytest.mark.parametrize("raw", ["abc", "0", "-5", "1.5", "10k"])
def test_an_invalid_trigger_limit_fails_loudly_instead_of_meaning_unlimited(monkeypatch, raw):
    """A typo must not quietly turn a capped host into an unlimited one."""
    monkeypatch.setenv(_LIMIT_ENV, raw)
    with pytest.raises(ValueError, match=_LIMIT_ENV):
        _load_from_env()
    with pytest.raises(ValueError, match=_LIMIT_ENV):
        _parse_trigger_limit(raw)


def test_a_blank_env_var_overrides_a_secret_supplied_limit(monkeypatch):
    """The Secrets Manager override pass (EC2) uses the same parser: an explicitly blank
    variable on the host means no limit, whatever the shared secret says."""
    monkeypatch.setenv(_LIMIT_ENV, "")
    get_settings.cache_clear()
    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={"api_key": "from-secrets-manager", "legbot_scrape_completion_trigger_limit": 999999},
    ):
        settings = get_settings()
    try:
        assert settings.legbot_scrape_completion_trigger_limit is None
    finally:
        get_settings.cache_clear()

