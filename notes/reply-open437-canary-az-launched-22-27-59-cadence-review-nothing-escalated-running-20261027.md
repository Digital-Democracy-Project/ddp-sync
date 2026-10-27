# Reply (interim): OPEN-437 canary. Arizona launched 22:27:59 UTC and accepted; cadence review read; scrape still running

Prod agent, Tue 2026-10-27 about 22:35 UTC. The operator said "Option A, run step 1 and launch the Arizona canary" in my session. The one launch is the only write I made.

## 1. Cadence review (read-only, done before the launch)
* **No state is escalated to nightly.** None of the nine `ddp:scrape_cadence:<state>` keys exists in ddp-sync's Redis (fl, wa, usa, va, mi, ma, ut, az, nc all absent), so every state is on its configured cadence.
* Live scheduler (`/ddp-sync/v1/schedule`, next runs): cadence review **10-28 00:30**; **WA scrape 02:30**; **USA scrape 03:00** (both daily); people pull 03:00; archives VA and WA **05:00**; MI and NC archives 10-29; MA 10-30; AL 10-31. FL and the secondary batch scrape Sun 11-01 02:00; AZ archive 11-03 05:00. So **tonight's only scrape launches are WA 02:30 and USA 03:00**; the 00:30 review launches nothing itself.
* Nothing was running before the launch (the in-flight record was empty).
* **A risk to know:** the 00:30 review escalates a state after a productive run in the last 2 reviews (`escalate_window: 2`). I do not know whether this manual Arizona run counts. If it does, Arizona could be escalated to nightly tonight. I changed nothing and will report what the 00:30 review does to the `az` key.

## 2. Launch
`POST /ddp-sync/v1/trigger/openstates-scrape/az` sent **22:27:59 UTC**, answered `HTTP 200 {"status":"started","target":"az"}`. No `bill_no`, no full-rescrape flag, no other state.
* ddp-sync log: `openstates_scrape: cloud-owned -- triggering Fargate collection + RDS load (OPEN-193) jurisdiction=az`, then `cloud_scrape: triggering Fargate collection jurisdiction=az run_id=az-0f32cd69beb3`.
* **Run id `az-0f32cd69beb3`; Fargate task id `d5fd97ea9b824175835175ab750a550e`** (cluster `ddp-scrapers`).
* **Revision: I cannot see it.** ddp-sync's in-flight record names only the bare family `ddp-scrapers` (it launches the latest ACTIVE revision), and this host's role cannot describe ECS tasks or task definitions. The task's CloudWatch stream had no events yet at 22:28. Please check the revision from the Mac.

## 3. Report: still to come
The task is running (in-flight count 1). A watcher is waiting for it to finish; I will post the scrape log summary, the Arizona counts against the baseline (bills 2190, vote events 3443, person votes 75720, duplicate groups 249), the new working-tier run id, and the `not voting` versus `other` counts. Stop rules are in force: I will not retry, roll back, deregister anything or pause ddp-sync.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
