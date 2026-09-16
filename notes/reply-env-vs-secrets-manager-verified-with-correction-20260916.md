# Reply: independent verification of the .env-is-dead-here claim -- correct, with one scope correction

**Date:** 2026-09-16
**Replying to:** `notes/preflight-unit-file-and-env-vs-secrets-manager-20260916.md`
**Audience:** Ramon / whoever picks this up next.

## Verdict: the recommendation holds. The blanket framing was slightly too broad.

A second, independent agent re-derived this from scratch (not just re-reading the prior note)
and ran an empirical test rather than trusting the code-reading alone. Summary:

**Confirmed correct, unchanged:**
- Only one `load_dotenv()` call site in all of `src/`, inside `_load_from_env()`
  (`config.py:598-599` on `origin/main`), which only runs when `_load_from_secrets_manager()`
  returns `None`. This host's `/health` genuinely reflects the real `_config_source` (not
  hardcoded) and a live `curl` right now confirms `"config_source":"secrets_manager"`.
- `local_openstates_api_base`, `ddp_broker_api_base`/`_token`, `ondemand_broker_*`, `cams_*`
  have **zero** override wiring in `origin/main`'s `get_settings()` -- they only appear inside
  `_load_from_env()`'s dict. For these, Secrets Manager is the only path in, exactly as step 4
  says. Nothing changes there.
- Empirically confirmed (python-dotenv 1.2.2, pinned `>=1.0.0` in `pyproject.toml`):
  `load_dotenv()` defaults to `override=False` (won't clobber a real env var that's already
  set) and does an upward directory search by default, not strict-cwd-only. Moot on this host
  since it never runs here at all, but good to have on record for local dev.

**What the original note got wrong:** it framed this as "nothing except Secrets Manager can
ever reach this app's config, full stop." `origin/main`'s `get_settings()` actually already
contains a targeted, intentional fix for exactly this problem (SYNC-51/OPEN-193/OPEN-290/
OPEN-292 commentary in `config.py` around lines 753-847): **after** `filtered` is built from
whichever loader won, it unconditionally re-reads `os.getenv()` for a specific list of fields
and overrides `filtered[...]` if the real process env var is set -- regardless of Secrets
Manager vs `.env`. That list is bigger than just the sibling doc's four job-toggle flags
(`_TASK_ENABLE_FLAG_ENV_VARS`, `config.py:579-591`, 12 fields total): it also includes
`redis_url`, `mac_ddp_sync_base_url`, `rds_openstates_api_base`, and six
`legbot_scrape_completion_trigger_*` fields.

**Why this doesn't actually change anything we need to do:** the override loop reads raw
`os.environ` via `os.getenv()`, not dotenv-loaded values -- and `load_dotenv()` still never
runs on the Secrets-Manager path either way. So a `.env` file still can't reach even this
broader list of 12 fields. The prior note's actual recommendation (systemd `Environment=` for
the four job-disable flags in step 3, Secrets Manager for step 4's settings, nothing in `.env`)
is still exactly right -- it was just right for a narrower reason (a specific, intentional
override list) than "everything but Secrets Manager is inert."

## Net effect on the checklist

No changes needed to steps 3-4 as already documented in the sibling docs. This closes the ask
raised in the prior note.
