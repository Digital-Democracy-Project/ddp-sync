# Request: OPEN-437. Targeted Arizona rescrape of 24 bills on v38 (revision 41). First answer the route questions; run it only if every gate below is clean

2026-10-28 about 00:45 UTC. Ramon's go for **this list and this one run** was given in this session ("give the prod agent the list of bills to run"). It covers nothing else. **Stop and report, do not retry, if any gate fails. Do not roll back, deregister or pause anything.**

## Why
The Arizona canary (b181b8d) ran clean but scraped no votes, so the three Arizona fixes on `v38` never ran on a vote: #71 (per-voter `NV` becomes `not voting`, was `other`), #78 (excused count) and #79 (dated Engrossed names stored undated). This run exercises #71 and #79 on production data. I previewed the same 24 bills live against azleg with the production code (nothing written): 32 of 32 votes with a declared not-voting count carry exactly that many `not voting` rows; version names equal stored on 23 of 24 bills (SB1215 gains a real "Chaptered Version"); yes/no rows, other counts and results unchanged on every matched vote. Full detail on OPEN-437.

## The bills (session `57th-2nd-regular`)
`HB4049,SB1254,HCR2052,HCM2012,HB4030,SB1805,HCR2004,HCM2008,HB2875,SB1215,HCR2051,HCM2009,SB1787,HB4163,SCR1047,HCR2003,HCM2011,HR2004,SB1657,HB2153,HCR2064,SB1283,HB2593,HR2007`

## Part 1: answer first (read-only)
ddp-sync's `POST /trigger/openstates-scrape/{target}` takes no `bill_no` (checked in `main` `334f2df`; your 09-02 note says the same), so I do not know the route. Tell me:
1. **How you will run it.** The scraper inside the task already supports it (`python3 cloud_collector.py az session=57th-2nd-regular bill_no=<list>`, per its own header, and the Fargate command override passes key=value arguments). Is there something you already use (a script under `/home/bitnami/ops/`, a direct `ecs run-task` with a command override on `ddp-scrapers:41`, a ddp-sync option I missed)? Then the load into RDS (`cloud_loader.py az <run_id>`, as ddp-sync does after a collection): same command you use today.
2. **Does a `bill_no=` run advance Arizona's incremental marker** (`last-run`/start mark in the scraper-memory bucket, last value `2026-10-25T02:22:22`)? If it would, a 24-bill run could make the next incremental run skip bills. Read the marker before; the run must leave it unchanged (or you restore it only with Ramon's say). If you cannot tell, stop.
3. **Side effects after the load:** with ddp-sync's scrape-triggered chain, would these 24 bills start the archive and LegBot, and would LegBot process only the changed bills or the whole session `57th-2nd-regular` (about 2,190 bills)? Would the broker be written? **If a whole-session LegBot run or a large broker write would follow, do not launch; report.** If the chain is bounded to the changed bills, say how many LegBot would take.

## Part 2: gates, then run (write: one run)
Launch only if all of these hold, and say which you checked:
* Part 1 answers are clean (marker unchanged, side effects bounded to these bills).
* The in-flight record is empty and no scheduled launch is due within 30 minutes (WA 02:30, USA 03:00 UTC). Do not overlap the ddp-sync restart in the deploy request (e2a18fa); do it before or after it, not during.
* No other state is running.
Then run exactly the 24 bills above, in one run (about 24 bills; the preview took a few minutes), no other flags, **not** a full or incremental Arizona run. Record the task id, run id, start and end times.

## Part 3: report (read-only, after the load)
1. Scrape log: errors, warnings, WAF, duration, bills and vote events found (expected 24 bills, 33 vote events).
2. Arizona totals vs your baseline (81f191e): bills **2190**, vote events **3443**, person votes **75720**, duplicate groups **249**. Expected change: none. (SB1215's new "Chaptered Version" document is the only new document.)
3. For each row in the table below, from RDS: the person-vote rows by option (yes/no/other/not voting). Report any bill where a row differs, and the person-vote totals by option for these 24 bills (expected: not voting 102, other 12 from the preview; before the run: not voting 0, other 114).
4. Version notes for the 24 bills: count and any dated Engrossed note, and whether a new version document was archived (only SB1215 is expected).
5. The marker before and after, and what the archive/LegBot/broker did after the load.

## Expected values per vote (from the live preview with the production code; person-vote rows by option)

| Bill | Date | Motion | Org | yes | no | other | not voting (declared) | rows: yes/no/other/not voting |
|---|---|---|---|---|---|---|---|---|
| HB4049 | 2026-03-02 | Passed | House | 31 | 25 | 0 | 3 | 31/25/1/3 |
| HB4049 | 2026-06-09 | Passed | Senate | 16 | 13 | 0 | 1 | 16/13/0/1 |
| SB1254 | 2026-02-25 | Passed | Senate | 27 | 0 | 0 | 3 | 27/0/0/3 |
| SB1254 | 2026-04-20 | Passed | House | 50 | 0 | 0 | 9 | 50/0/1/9 |
| SB1254 | 2026-06-01 | Passed | Senate | 26 | 0 | 0 | 4 | 26/0/0/4 |
| HCR2052 | 2026-03-11 | failed to pass | House | 21 | 31 | 0 | 7 | 21/31/1/7 |
| HCM2012 | 2026-03-04 | Passed | House | 43 | 10 | 0 | 6 | 43/10/1/6 |
| HB4030 | 2026-03-11 | failed to pass | House | 21 | 31 | 0 | 7 | 21/31/1/7 |
| SB1805 | 2026-02-26 | Passed | Senate | 16 | 12 | 0 | 2 | 16/12/0/2 |
| SB1805 | 2026-04-22 | Passed | House | 52 | 0 | 0 | 7 | 52/0/1/7 |
| HCR2004 | 2026-03-03 | Passed | House | 37 | 19 | 0 | 3 | 37/19/1/3 |
| HCM2008 | 2026-03-02 | Passed | House | 31 | 25 | 0 | 3 | 31/25/1/3 |
| HB2875 | 2026-03-04 | Passed | House | 37 | 16 | 0 | 6 | 37/16/1/6 |
| HB2875 | 2026-04-07 | Passed | Senate | 29 | 0 | 0 | 1 | 29/0/0/1 |
| HB2875 | 2026-06-01 | Passed | House | 43 | 12 | 0 | 5 | 43/12/0/5 |
| SB1215 | 2026-03-02 | Passed | Senate | 27 | 2 | 0 | 1 | 27/2/0/1 |
| SB1215 | 2026-04-27 | Passed | House | 53 | 4 | 0 | 2 | 53/4/1/2 |
| SB1215 | 2026-06-11 | Passed | Senate | 26 | 1 | 0 | 3 | 26/1/0/3 |
| HCR2051 | 2026-03-03 | Passed | House | 32 | 24 | 0 | 3 | 32/24/1/3 |
| HCM2009 | 2026-03-17 | Passed | House | 31 | 22 | 0 | 6 | 31/22/1/6 |
| HCM2009 | 2026-04-20 | Passed | Senate | 17 | 12 | 0 | 1 | 17/12/0/1 |
| SB1787 | 2026-02-26 | Passed | Senate | 23 | 5 | 0 | 2 | 23/5/0/2 |
| SB1787 | 2026-04-01 | Passed | House | 31 | 24 | 0 | 5 | 31/24/0/5 |
| HB4163 | 2026-06-11 | Passed | House | 46 | 12 | 0 | 2 | 46/12/0/2 |
| HB4163 | 2026-06-11 | Passed | Senate | 23 | 5 | 0 | 2 | 23/5/0/2 |
| SCR1047 | 2026-03-10 | failed to pass | Senate | 12 | 17 | 0 | 1 | 12/17/0/1 |
| HCR2003 | 2026-02-23 | Passed | House | 32 | 25 | 0 | 3 | 32/25/0/3 |
| HCR2003 | 2026-06-12 | Passed | Senate | 16 | 12 | 0 | 2 | 16/12/0/2 |
| HCR2003 | 2026-06-12 | failed to pass | Senate | 12 | 16 | 0 | 2 | 12/16/0/2 |
| HCM2011 | 2026-02-26 | Passed | House | 31 | 23 | 0 | 6 | 31/23/0/6 |
| HCM2011 | 2026-04-16 | Passed | Senate | 17 | 10 | 0 | 3 | 17/10/0/3 |
| HR2004 | 2026-02-05 | Passed | House | 33 | 25 | 0 | 2 | 33/25/0/2 |
| SB1657 | 2026-03-03 | Passed | Senate | 17 | 13 | 0 | 0 | 17/13/0/0 |

## Expected version count per bill (all notes, undated Engrossed names)

| HB4049:3 | SB1254:7 | HCR2052:3 | HCM2012:2 | HB4030:3 | SB1805:4 | HCR2004:3 | HCM2008:2 | HB2875:7 | SB1215:6 | HCR2051:3 | HCM2009:3 | SB1787:3 | HB4163:4 | SCR1047:3 | HCR2003:4 | HCM2011:3 | HR2004:3 | SB1657:2 | HB2153:2 | HCR2064:3 | SB1283:2 | HB2593:2 | HR2007:3 |


## Stop rule
Stop and report at once if a bill's vote yes/no rows or any declared count changes from stored (other than the table), Arizona totals move, a dated Engrossed version name appears, an error/WAF line shows, or anything beyond these 24 bills is touched. Ramon decides about any repair.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
