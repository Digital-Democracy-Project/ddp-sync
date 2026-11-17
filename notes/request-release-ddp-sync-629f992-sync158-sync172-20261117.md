# REQUEST (waits on Ramon's go): release ddp-sync `629f992` (main). Two halves: the Mac (SYNC-158, LegBot's changelog) and EC2 (SYNC-172's new scrape days and the rest of main). 2026-11-17 (Mac side)

**Not a GO yet.** Ramon says go per half. The EC2 half is the prod agent's, after a GO note; the Mac half is Ramon's own step (the Mac checkout is production and no agent session edits it).

## What `main` carries that is not live

Live today: **EC2 `ce459b4`** (prod agent `844a932`, 11-16 17:28 UTC). **Mac checkout `5d2592d`** (read only; whether the running Mac service was restarted onto it is **not verified**).

| Merge | Ticket | What changes | Where it acts |
|---|---|---|---|
| #288 `c2e2011` | SYNC-170 | weekly organization-logo catch-up job | the logo step, still **off** (`LEGBOT_ORGANIZATION_LOGO_ENABLED` unset) |
| #289 `5d2592d` | SYNC-171 | the logo catch-up runs through ddp-sync's trigger | same, off |
| #290 `83a8fe8` | SYNC-172 | **the weekly states get one day each** (MA Mon, VA/UT Tue, AZ/NC Wed, LA/NE Thu, AL/GA Fri, NV/OR Sat, all 12:00 UTC; FL and MI stay Sun 02:00) | **EC2's scheduler** (scrapes start there) |
| #291 `d29581b` | SYNC-159 | a test only (no new per-state comparison in the code) | nothing at run time |
| #292 `629f992` | SYNC-158 | LegBot's changelog takes each step's older version from api-v3's `diff_baseline_id` instead of working it out; ddp-sync's own copy of that rule is deleted | **where LegBot runs: the Mac** |

No `.env`, IAM, manifest, Dockerfile, compose, requirements or migration change in `ce459b4..629f992`; `config/sync_schedule.yaml` changes (SYNC-170, SYNC-172).

**SYNC-158, why it is safe:** read-only on the Mac api-v3 over the production replica (2026-11-17), every Virginia (3,136) and Massachusetts (364) bill with a stored comparison gave the same changelog steps under the old and the new rule (VA 6,194, MA 354, 0 different); every stored comparison in all 16 states carries its baseline id (0 missing of 68,449). api-v3's fields are live on both hosts since 11-17.

## Half 1, the Mac (Ramon): SYNC-158

Any time no LegBot run is going on the Mac (the weekly states' LegBot follows their scrape; tonight's wa/usa/pa nightlies end about 11:00 UTC).

**Not while the organization-logo catch-up is running.** As of 11-17 a run is going on the Mac (`organization-logo-backfill-31934376405e`, started 11:00 local, 547 of about 2,100 done at 12:52, so roughly 5 more hours). A kickstart kills it mid-way, and its lease then blocks a new trigger for up to 15 minutes. Before step 2, check `logs/ddp-sync.log` for `organization_logo_catch_up_done` for that run.
1. `git -C ~/Developer/repos/ddp-sync status` shows `main`, clean apart from the untracked `.env.bak-*` files; then `git -C ~/Developer/repos/ddp-sync pull --ff-only` (expected `629f992`).
2. `sudo launchctl kickstart -k system/com.ddp.ddp-sync`.
3. Check: `GET http://localhost:8001/health` healthy; no `Traceback` in the log since the restart. The Mac's scrape jobs stay off as today, so SYNC-172's schedule changes nothing there.

**Rollback:** `git -C ~/Developer/repos/ddp-sync checkout --detach 5d2592d` and the same kickstart.

## Half 2, EC2 (prod agent, after a GO note): SYNC-170/171/172, and the rest of `main`

**Updated 2026-11-17 (Ramon: no timing gate without a real reason): no calendar window.** Releasing on any day is fine: a weekly state whose new day falls before its next old one may run one extra time that week, which is harmless (catch-up scrapes).

### 0. Gates (stop and report if any fails)
* **G0, nothing in flight:** not while a scrape, load, archive or LegBot step launched by this ddp-sync is running (a restart would cut it off).
* **G1, code:** `/opt/ddp-sync` at **`ce459b4`** (your `844a932`), `git status` clean.
* **G2, quiet:** no pipeline, scrape, archive or LegBot line in the last 10 minutes; 0 running `ddp-scrapers` tasks ddp-sync waits on.
* **G3, rollback point:** tag the running image `ddp-sync:pre-629f992` and record its id.

### 1. Pull, build, restart (as for `ce459b4`)
1. `git -C /opt/ddp-sync merge --ff-only origin/main` (fetch over SSH, as last time). Expected **`629f992`**.
2. Build on its own first (`docker compose -p ddp-sync -f infrastructure/docker-compose.prod.yml build`), live container untouched. If the build fails, stop.
3. `sudo systemctl restart ddp-sync`. Nothing else is recreated.

### 2. Checks right after (report each)
* healthy, new image id; `Redis connected`; 0 `Traceback` since the restart;
* the per-state scrape jobs and their next run times from `GET /schedule`: **MA Mon 12:00, VA and UT Tue 12:00, AZ and NC Wed 12:00, LA and NE Thu 12:00, AL and GA Fri 12:00, NV and OR Sat 12:00, FL and MI Sun 02:00, wa 02:30, usa 03:00, pa 04:00 nightly**;
* `config/sync_schedule.yaml` still `stall_seconds: 2700`;
* `LEGBOT_ORGANIZATION_LOGO_ENABLED` not set.

### 3. Read-only, in the following nightly reports
* Monday 11-23: MA starts at 12:00 and runs alone.
* Sunday 11-22 02:00: only FL and MI start.

**Rollback:** `git -C /opt/ddp-sync checkout --detach ce459b4`, re-tag `ddp-sync:pre-629f992` as `ddp-sync:prod`, `sudo systemctl restart ddp-sync`.

## Not authorized
`LEGBOT_ORGANIZATION_LOGO_ENABLED` or any `.env` change; any manifest or IAM change; triggering any scrape or logo catch-up; the broker or api-v3; the Mac (half 1 is Ramon's).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
