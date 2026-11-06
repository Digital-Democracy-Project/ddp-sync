# Reply (production change done): pull and restart for Pennsylvania's nightly scrape, EC2 host (OPEN-494, OPEN-478). 2026-11-06, 20:00-20:04 UTC

Reply to 0988ac8. Done on the EC2 host only; the Mac was not touched.

**The user's go (this session, verbatim pop-up answer):** "Yes, run it now, in order, with the stop conditions (Recommended)". I read your relayed go as information and waited for this one.

## Gates (read right before the pull)
1. **Nothing in flight** at 20:00 UTC: `list-tasks` on cluster ddp-scrapers answered none; no run-scrape, recompute, text-extract or backfill process; 0 "Running job" lines in the ddp-sync log in the last 12-15 min (rechecked 20:00:19). Clear of 02:00/02:30/03:00 UTC.
2. **Revisions I started from:** ddp-sync `28e8e0b`, ddp-open-states `05515505` (both clean, on main). Image revision visible: none newer than v43 / rev 46 (I still cannot read task definitions). ddp-sync image before: `0e233507c622`, kept as tag `ddp-sync:pre-open494`.
3. **Manifest diff `05515505..a2b4180fa -- jurisdictions.yaml`:** exactly three changes: the `advance-jurisdiction` comment block, `pa` tier secondary to primary, `ne` diff_cleaner none to clean_ne_text. Nothing else.
- **One difference to know:** ddp-open-states `main` is now `46065175`, two commits past your named SHA (#344, OPEN-495: `pa` diff_cleaner `clean_pa_text`, merged after your request). I pulled **exactly `a2b4180fa`**, not the tip, because that is what the go named, and `clean_pa_text` needs code the v43 image may not have. Pulling the tip is a separate request.

## Steps
1. ddp-sync fast-forwarded to `11cd64e`. 2. ddp-open-states fast-forwarded to `a2b4180f`.
3. Drift check by hand with the repo venv (`.venv/bin/python check-jurisdiction-drift.py --ddp-sync-root /opt/ddp-sync`, run without `-I` because `-I` hides its sibling module): "OK: every list compared agrees with jurisdictions.yaml (13 jurisdictions)", exit 0. It said openstates-core was not checked (no root given).
4. Log gzipped to ~/ops/log-archive/ddp-sync-pre-open494-20261106T2000Z.gz, then `sudo systemctl restart ddp-sync` (its unit runs `up -d --build`; `scheduler.py` is baked into the image, so a rebuild was needed, not just a restart). Ran 20:00:25 to 20:02:27.

## After the restart
- Container started **20:02:27 UTC**, Up and **healthy at 20:03:33**. New image `540a657e0c1f`. No errors in the first 5 minutes of the log.
- `GET /ddp-sync/v1/schedule`: **20 jobs** (was 19).
  - `openstates_primary_scrape_pa` "pa scrape (nightly, primary)" next run **2026-11-07 04:00:00 UTC**, cron 04:00 daily. Registered in the log as `openstates_primary_scrape: registered jurisdiction=pa sync_time=04:00`.
  - Sunday secondary job (`openstates_secondary_scrapes`, 02:00 Sunday): list is va, mi, ma, ut, az, nc, ne, la. **`pa` is absent.**
  - Unchanged: Washington 02:30 daily, USA 03:00 daily, Florida **Sunday 02:00 weekly** (its effective cadence is weekly, not daily; it was that way before this change).
- **Which host schedules scrapes:** on this host the scheduler registered and will run all the scrape jobs above; it launches them on Fargate. The README says the Mac Studio's `com.ddp.ddp-sync` also runs the scrapes and that EC2-broker scheduled jobs are "deliberately disabled", which is not what this host shows. I cannot see the Mac, so I cannot say whether it also schedules Pennsylvania. Please ask Ramon; if his Mac is still on the old yaml it will not have the 04:00 job, and if it has an older secondary list it would run `pa` on Sunday too.

## Next
I will watch tonight's 04:00 UTC run and report at launch (task id, image revision if I can read it), at about 3 hours, and at the end (exit code, bills and votes against 5,270 / 5,858 / 425,560, block signatures, RDS CPU and connections, and whether it coincides with another spike).

## Rollback (not needed)
`git -C /opt/ddp-open-states reset` is not used; the way back is both repos to `05515505` and `28e8e0b`, `ddp-sync:pre-open494` image, `sudo systemctl restart ddp-sync`, confirm `pa` is back in the Sunday list.
