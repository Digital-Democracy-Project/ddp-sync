# Finding (read-only): the 06:20 UTC drift check reports drift in ddp-sync's OWN env list; and this morning's nightlies, 2026-11-12 14:20 UTC (prod agent)

Nothing changed. I changed no list, no secret and no container; this is for the dev (ddp-sync / OPEN-483 owners) to decide.

## The finding
`jurisdiction_drift_check` at **06:20:00 UTC today** logged `status=drift drift=1 alerted=False unverifiable=[] env_lists_not_set_on_this_host=[]`. It prints no detail, so I ran its own `collect_findings(sync_config, settings, root)` inside the running ddp-sync container, read-only, at 14:2x UTC. Result, exactly:
`{"drift": [{"feature": "replica_routing", "source": "env DDP_OPENSTATES_JURISDICTIONS", "only_in_yaml": ["ma"], "only_in_manifest": ["la", "ne", "pa"]}], "unverifiable": [], "skipped": []}`
Reading it (mine, not tested beyond this): **this container's own `DDP_OPENSTATES_JURISDICTIONS` setting contains `ma` and does not contain `la`, `ne`, `pa`**, while the manifest's `replica_routing` list has `la`, `ne`, `pa` and no `ma`. The broker's list on this host is different and was switched on for PA (11-08) and NE, LA (11-09) by me; I did **not** touch ddp-sync's own list at any switch-on (those requests named only the broker `.env`), so this gap is likely mine to have left open, but I have not checked where ddp-sync's value comes from (the secret store, per my earlier finding that ddp-sync's config source is Secrets Manager) or what ddp-sync uses it for.
**Why my earlier hand checks said OK:** `check-jurisdiction-drift.py --ddp-sync-root /opt/ddp-sync` printed `OK: every list compared agrees with jurisdictions.yaml (13 jurisdictions)` again at 14:2x UTC today, i.e. the hand script does not see this difference; the scheduled in-container check does. Which source each reads is not something I verified.
**Not investigated:** why `alerted=False` (cooldown, or the alert path), and the effect on LegBot's replica reads for PA, NE, LA (LegBot has been running for PA from the Mac, which uses its own settings).

## This morning's nightlies (all read from ddp-sync logs, 11-12)
- **Washington 02:30 UTC:** one job, `status=completed archived=True duration_seconds=502.0 failed=0`, archive on attempt 1, `archiver_triggered_legbot_no_sessions_touched`.
- **USA 03:00 UTC:** two scrapes (`usa-a70980e12c6c` 433.4 s, `usa-0fb1a51e9fab` 194.1 s), then the archive and `archiver_triggered_legbot_wireguard_trigger_failed` (empty error, the known 3,600 s timeout) for `US` `119` at 04:23:33; **`status=completed archived=True duration_seconds=5132.4 failed=0 sessions=2`**.
- **Pennsylvania 04:00 UTC:** `cloud_scrape: done` 11:30:25 (`pa-f61682fc4fca`, 27,024.7 s = 7 h 30 min), `archiver_triggered_legbot_no_sessions_touched`, `archive_after_scrape_succeeded attempt=1`, **`openstates_pipeline: done archived=True duration_seconds=27147.5 failed=0 status=completed`**. No `openstates_archive_*` job ran on its own; no `collection failed` this time.
