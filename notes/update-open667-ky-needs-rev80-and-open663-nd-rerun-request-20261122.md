# UPDATE + REQUEST (not a GO yet): Kentucky's first scrape needs revision 80 (v78), and North Dakota's rerun on revision 80. Both wait for Ramon's go. 2026-11-22 (Mac side)

## 1. Correction to Kentucky's request 30ae9df
That note says "ddp-scrapers:78 (image v76), or a later revision". That is now wrong. Since then, Ramon chose "Fix vote keys first" for Kentucky. The fix is OPEN-760: unique vote keys for KY, MT and IA, scrapers #216, merged 6176a590. It is in **v78 = ddp-scrapers:80** (verified in ddp-open-states ops-handoff e27e5b0) and not in revision 78.
* **For Kentucky's run, the task must be revision 80 or later.** Any earlier revision: stop and report.
* Everything else in 30ae9df stands: ddp-open-states #457 merges first, then one manual run on Ramon's go.

## 2. REQUEST: North Dakota's first scrape again, on revision 80 (on Ramon's go)
* **Why a rerun:** ND's first run, nd-4f564010e1a3, failed at the load because of a repeated vote key on HB 1015. Nothing was written (your reply a6d8a5b).
* **The fix:** OPEN-757, scrapers #213 (merge d657f665), is in v77 and v78.
* **Ramon's earlier call:** "stay on, fix by Sunday". ND is still at `scraping` on EC2, so no manifest change or rollout is needed.

**What I need from you (on Ramon's go):**
1. **Gates:**
    * new tasks run **ddp-scrapers:80 (v78)** or later; report the revision and tag;
    * nothing for `nd` is running.
2. **One call:** `POST /ddp-sync/v1/trigger/openstates-scrape/nd`, with no retry and without `skip_people_check`.
3. **Report the same gates as go-open663 (8e45b6f):**
    * the active sessions line (expect `69`, `69S1`, `69S2`);
    * vote events > 0;
    * `status=completed failed=0` and the archive finished;
    * the run id, the counts and `fetch_errors`.
    * If the load stops on a repeated vote key again, stop and send the full message.
4. **Hand back:** reply here with the run id. I'll do the Mac checks.

* **Timing:** ND's weekly Sunday 02:00 UTC run on 11-29 will also use revision 80. If Ramon prefers, that scheduled run can be the rerun instead of a manual one.
* **Not authorized:** LegBot, routing, search or embedding for ND; any other state; a second run.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
