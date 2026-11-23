# Checkpoint (2026-11-23 17:12 UTC): Ramon said "GO" in my session on the whole list I offered. Rollout done, people refreshed, 11 runs triggered

Notes re-fetched at 17:07 and 17:11 UTC (only a broker update `c9f64f25` landed, read later). Nothing withdrawn. **Not started (each needs its own go): the Kentucky and Oklahoma recompute commits, Nebraska 109 (waits for v83), the Idaho/RI/OK recompute follow-ups, Florida's archive (waits for its load).**

## One rollout (EC2; pull first)
`rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-open-states --skip-ecs-check` (tasks were running for other states), exit 0, "30 valid", "every list agrees", checkout `f0cfd9c7a` to **`f721770e3`** (#468 OPEN-777, #469 RI officers, #470 OK speaker, #471 to #477 MT IA MD HI ME MS MO), api-v3 copy md5 772f1b6edced86b929b9c02df0150566 in the container, healthy. **mt, ia, md, hi, me, ms, mo now scraping** (cloud_path, archive, people_refresh true; LegBot, routing, search, embedding false). No restart.

## People (Idaho step 1, RI step 4): people pulled `efc8618838` to **`d1acd229fc`** (fast-forward)
- **id:** safe and real `processed 345 person files, 17 created, 5 updated` (**expected 17 and 5: match**); 104 current legislators read back.
- **ri:** safe and real `processed 264 person files, 0 created, 1 updated` (**expected 0 and 1: match**); 112 current.

## Triggers (one POST each, no retry, no skip_people_check; all 202), 17:08:14 to 17:12:05 UTC
| State | Call | Answer |
|---|---|---|
| mt | `openstates-scrape/mt` (2027 only, decided in `12be6e8`) | 202, run **mt-9bbf779af061**, rev 84 |
| ia | `...ia?sessions=2025,2026,2026s1` | 202, sessions echoed, run **ia-a4202f577129**, rev 84 |
| md | `...md` | 202, run **md-9b5b1f118106**, rev 84 |
| hi, me, ms, mo | plain `openstates-scrape/<st>` | 202 each |
| nm | `...nm?full=true&sessions=2026` | 202, `"full": true` |
| id | `...id?full=true&sessions=2026` | 202, `"full": true` |
| ri | `...ri?full=true&sessions=2026` | 202, `"full": true` |
| ok | `...ok?full=true&sessions=2025,2026` | 202, both sessions echoed |
Gates: PRs merged; rev 84 (v82) for all (new tasks read from ECS); Missouri's `c3ec462d` gate via rev 84 per `0f2e655`; nothing running for these states; Monday, not Sunday.
**Only mt, ia and md have a Fargate task so far.** For hi, me, ms, mo, nm, id, ri and ok the pipelines started (their pipeline locks are held, `openstates_pipeline: starting`) but no run id or task appeared within about 4 minutes, while 8 tasks were running (az, fl, ia, la, ma, md, mt, ut). My **unverified** reading: ddp-sync launches each run from a worker thread, the pool looks capped at about 8, so they start as slots free up. I have not found the cap in the code and have not changed anything; I will report their run ids when they start and their results.

## BEFORE numbers for your pass/fail SQL (RDS, read only, 17:11 UTC)
- **ID 2026:** 45,990 votes, **48 unmatched**, **15 roll calls with a double vote**, **Don Hall 461** House votes.
- **RI 2026:** 78,652 votes, **2,212 unmatched**; 3,011 versions (stage coverage to be measured after).
- **OK 2025+2026:** 331,234 votes, **3,451 unmatched**; 17,556 versions.
- **LA 2026** (`9648a6e7`): 8,005 unmatched; **NE 109** waits for v83.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
