# Reply: read-only evidence for SYNC-95, OPEN-320, OPEN-332 and BROKER-170 (prod agent, 2026-10-07 01:25 UTC)

Replies to `request-read-only-evidence-to-close-sync95-open320-open332-broker170-20261007.md`. **Item 1 is the only one still open: it has to wait for the archives.** The four ledger dry-runs (items 3 and 4) were the only production calls; they write nothing. Quiet window checked first (no locks, ddp-sync idle for 30 min). No secret values.

## SYNC-95
1. **Re-scan after the VA archive (10-07 05:00 UTC) and after MI (10-08): not done yet.** Scan at 21:30 UTC on 10-06 for reference: 66,488 records, 50,315 permanent, 16,173 expiring (FL 7,684, MI 4,107, VA 4,382; about 86.8 to 87.3 days left). I will re-scan after 05:30 and after the MI archive and report the totals, the pass line and the duration.
2. **FL sooner without a scrape: the manual reconcile route cannot do it.** `POST /trigger/knowledge-base-reconcile/{j}` has no `dry_run` parameter: its docstring says it "always plans and never writes". Only the post-archive hook's pass writes. The route that would run the real pass is **`POST /trigger/openstates-archive/fl`** (it archives FL, then the hook runs the ledger pass). **I did not run it.** Cost estimate (arithmetic from earlier passes, not measured for FL): US took 99 min for 38,572 bills (0.15 s per bill), WA 11 min for 3,411 (0.19 s), UT 6.5 min for 1,021 (0.38 s); FL has about 8,690 bills, so roughly **22 to 55 minutes for the ledger pass**, plus the archive step, whose duration for FL I have not measured (WA took about 92 s, AZ 92 s, US 694 s). The first pass restamps rather than embeds (0 written for UT, WA, AZ and US), so little OpenAI spend is expected. It takes the post-archive hook's lock, so it wants a quiet window.
3. **Ledger dry-runs, `POST /trigger/knowledge-base-reconcile/{j}?ledger=true` (plan only, writes nothing), logged as `knowledge_base_reconcile_plan`:**

| state | listed bills | missing | changed | orphaned | duration |
|---|---|---|---|---|---|
| UT | 1,021 | 0 | 0 | 0 | about 30 s |
| AZ | 2,190 | 0 | 0 | 0 | 31 s |
| WA | 3,411 | 0 | 0 | 0 | 42 s |
| US | 38,572 | 0 | 0 | 0 | 258 s |

   Each: `listing_complete=True unrecorded=0 sampled=0 sample_failed=0 status=ok`. 0 warnings or errors in ddp-sync's log; no locks left behind. **The real backlog on converged states is zero**, so a threshold of 100 would only fire on a genuine regression; I did not change `alert_backlog_over`.
4. **Orphans:** **0** for UT, AZ, WA and US (same runs), so `delete_orphans` would delete nothing today. Not enabled.
5. **`git status --short` in `/opt/ddp-sync`:** `M  infrastructure/ddp-sync.service` (staged), ` M infrastructure/docker-compose.prod.yml` (modified, +64/-11 against HEAD), `?? infrastructure/render-env.sh` (untracked). HEAD is `44e8b97`; `origin/main` is now `faa9630`, so the next pull needs a hand-merge of the compose file. Not committed or reset.

## OPEN-320
6. **No `patch_refresh` line at all** (`openstates_patch_refresh`, `patch refresh`, `patch_refresh`: count **0**) in the ddp-sync container log since its recreate on 2026-10-06 02:17, which covers the 01:00 UTC run on **10-07**. `OPENSTATES_PATCH_REFRESH_ENABLED=false` in the compose file and in the live container (the job is not registered). **The log does not reach back to 01:00 on 10-06** (it was lost at the recreate) and **I cannot read Slack from here**, so only 10-07 is checked, not 10-06.

## OPEN-332 (North Carolina has 0 current legislators)
7. **The cause is a hardcoded state list:** `run-people-refresh.sh` (in the ddp-open-states checkout, called by the weekly Sunday 10:00 UTC `openstates_people_refresh` job, enabled) loops `for state in fl wa us va mi ma ut az al` and runs `os-people to-database` for each. **`nc` is not in that list.** The people checkout `/opt/openstates-people/data/nc` is present with **170 legislator YAML files** in `legislature/` (plus 4 executive, 20 municipalities, 358 retired), so the source is not empty.
   - Database (read-only): NC has **4 organizations and 2,338 bills, 0 memberships and 0 people** (`current_jurisdiction_id` count 0; distinct people via membership 0). So it is "never imported", not "imported but marked not current".
   - People refresh recency: the six other states' newest `opencivicdata_person.updated_at` is **2026-10-04 10:06:06 UTC**, i.e. the Sunday 10:00 run finished at about 10:06. ddp-sync's own log lost that run's lines at the 10-06 recreate; the only people lines since are `openstates_people_refresh: registered sync_day=sunday sync_time=10:00`.
   - Note `ARCHIVE_ENABLED_STATES` in `activate.sh` includes `nc`, so NC is archived and scraped but not people-imported. Adding `nc` to the loop is the change; I made none.

## BROKER-170
8. **(a) Cloudflare / other proxy: none.** `curl -sI https://mapapp.digitaldemocracyproject.org/api/status/` returns `HTTP/2 200` and `server: nginx/1.28.3` with no `cf-ray`, `cf-*` or `via`; the hostname resolves to `44.210.190.195`, which is this host's own public address. **(b) Reaching `web:8000` directly:** `web` publishes no host port, but it is **not nginx-only**: the containers on `ddp-broker-py_default` are `ddp-broker-py-celery-1`, `-celery-beat-1`, `-nginx-1`, `-redis-1`, `-web-1`, `ddp-sync-ddp-sync-1` and `votebot-ddp-next`. Any of them could call `web:8000` directly (no `X-Forwarded-For`), though ddp-sync and VoteBot use the public hostname. The nginx published ports are 80, 443 and 8080; the `:8080` listener also goes through nginx.

## One more
9. **Real suggest traffic logs: none usable.** api-v3's log (since its 22:14 recreate) holds 595 `/ddp/search/suggest` calls, but all are my own tests and A/B loops, and the lines carry no duration. The broker's nginx log has 0 `/api/search/suggest` requests (search is off) and its log format has no `request_time`. So a realistic query mix would have to come from elsewhere.

Reply on this branch either way.
