# Checkpoint: ddp-sync cb5eade LIVE on EC2, released OUTSIDE the Saturday window on the user's word (reply to cfc8e5b)

Written 18:48 UTC (date -u at write time). From the production EC2 host. **The user told me to release now ("just go") after I explained the Saturday-window rule and its cost** (weekly states move mid-week; a state whose new day already passed this week waits).

## What ran (18:39-18:42 UTC, Tuesday 11-17)
- G1: /opt/ddp-sync at ce459b4, clean. G2: no pipeline, scrape, archive or LegBot line in the last 10 minutes (re-checked at 18:41), 0 running ddp-scrapers tasks. G0 (the window) was **not** met; that is the override.
- G3: running image tagged **ddp-sync:pre-cb5eade** (image c4bb07b269d0) for rollback.
- ff-only ce459b4 -> **cb5eade** (SYNC-170/171/172/158/159/156). Build on its own first: exit 0, new image ebdb4eaf724c. Old log gzipped to /home/bitnami/ops/log-archive/ddp-sync-pre-cb5eade-*. `sudo systemctl restart ddp-sync`: container healthy at 18:42:21 on image ebdb4eaf724c; 0 Tracebacks in its log since.

## Checks
- **GET /schedule, next runs after the restart:** wa 11-18 02:30; usa 11-18 03:00; pa 11-18 04:00; **az and nc 11-18 12:00 (Wed); la and ne 11-19 12:00 (Thu); al and ga 11-20 12:00 (Fri); nv and or 11-21 12:00 (Sat); fl and mi 11-22 02:00 (Sun); ma 11-23 12:00 (Mon); ut and va 11-24 12:00 (Tue)**. 21 jobs in total. (The Sunday 11-22 02:00 batch no longer carries the weekly states other than fl and mi.)
- **Effect of releasing mid-week, as I warned:** MA and VA/UT had their last weekly run on Sun 11-15 and are now next due Mon 11-23 and Tue 11-24 (8 and 9 days); AZ and NC run tomorrow (3 days after 11-15). No state runs twice or is skipped by the restart itself.
- **First cadence review** (18:46:19 UTC, about 5 minutes after the start): "openstates_cadence_review: completed ... changed=0 demotion_advice=[] rescheduled=False reviewed=16", cadences al, az, fl, ga, la, ma, mi, nc, ne, nv, or, ut, va weekly and pa, usa, wa nightly. **No `jobs_synced` line was logged** (0 matches), and nothing was added or removed, which is what you expected. The startup jobs already match the manifest.
- Mac half: not mine.

Rollback if wanted: retag ddp-sync:pre-cb5eade as ddp-sync:prod, then `sudo systemctl restart ddp-sync`; that brings back ce459b4's Sunday 02:00 batch for every weekly state.
