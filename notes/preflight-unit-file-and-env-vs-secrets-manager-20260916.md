# Step-1 preflight results + a correction candidate for step 3's mechanism

**Date:** 2026-09-16
**Found by:** an agent walking the votebot/ddp-api EC2 upgrade with the user, doing the
sibling doc's own step 1 (preflight) before touching anything.
**Audience:** Ramon / whoever picks this up next -- please double-check the claim below before
relying on it, since getting step 3 wrong would silently leave this host running jobs it has no
business running (see the sibling doc's own warning about that).

## Step 1 preflight: no drift found

`systemctl cat ddp-sync` on the votebot/ddp-api EC2 host returns a unit at
`/etc/systemd/system/ddp-sync.service` that is byte-for-byte identical to this repo's checked-in
`infrastructure/ddp-sync.service`, on both the host's current pin (`6a97206`) and `origin/main`
(post-pull). No drift to reconcile -- the sibling doc's open item about this is resolved: the
path to edit for step 3 is confirmed as `/etc/systemd/system/ddp-sync.service`, `[Service]`
section, after the existing `Environment=PATH=...` line.

## Claim to double-check: `.env` cannot carry ANY of this host's new settings, by construction

While double-checking whether the four step-3 job-disable flags could instead go in a `.env`
file (simpler than editing the live unit file), traced `get_settings()` in `config.py`
(`origin/notes/ops-handoff`, i.e. post-pull `main`):

```python
def get_settings() -> SyncSettings:
    raw = _load_from_secrets_manager()
    if raw is None:
        raw = _load_from_env()
    ...
```

`_load_from_env()` is the **only** place `load_dotenv()` is called (`config.py:598-599`) --
and it only runs when `_load_from_secrets_manager()` returns `None`. This host's own `/health`
already confirms `"config_source": "secrets_manager"` (per the sibling doc), meaning Secrets
Manager succeeds here -- so `_load_from_env()` never runs, so `load_dotenv()` never runs, so a
`.env` file's contents never reach `os.environ` on this host **at all**, for anything.

This isn't a per-key merge (Secrets Manager missing key X falls through to `.env` for just X)
-- it's all-or-nothing at the whole-config level, decided once by whichever loader returns
first. That applies equally to:
- the four step-3 job-disable flags (`_TASK_ENABLE_FLAG_ENV_VARS` in `config.py:579`, read via
  plain `os.getenv()` against the real process environment -- which is exactly why step 3 uses
  systemd `Environment=` lines instead: that's the one mechanism that sets the real process
  environment regardless of which config loader wins), and
- the `local_openstates_*`/`ddp_broker_*` settings from step 4, which have no env-var override
  wiring at all and must go into the Secrets Manager secret itself (as step 4 already says).

**Net: nothing for this upgrade should be written to a `.env` file on this host.** If one gets
created, it will look like it's configuring the four flags or the new settings and silently do
nothing, which is a worse failure mode than not creating it (loud absence vs. quiet no-op).

## Ask

This is a chain-of-reasoning conclusion (reading `config.py`'s load order), not something
directly observed running against this host the way the step-1 result above is. Can someone
independently re-check that reasoning -- or, better, verify empirically (e.g. temporarily add
a throwaway var to a `.env` file, hit an endpoint/log line that would reveal it, confirm it does
NOT show up) -- before this is treated as settled? Getting this wrong in either direction
(assuming `.env` works when it doesn't, or missing some other code path that also calls
`load_dotenv()`) would matter for how step 3 and step 4 actually get executed.
