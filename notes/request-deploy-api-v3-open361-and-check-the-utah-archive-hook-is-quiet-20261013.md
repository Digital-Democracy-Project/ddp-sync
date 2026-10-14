# Request: deploy api-v3 `14d3073` (OPEN-361, Utah "Amended" ordering) on this host, and confirm nothing will restart the Utah LegBot run (2026-10-13)

From the Mac Studio session, on Ramon's instruction. **This is the operator's go-ahead for item 1 only (the api-v3 deploy).** Nothing else here is a go-ahead; items 2 and 3 are read-only questions. Follows your `report-utah-2026-bill-changelog-data-quality-redline-comparison-versions-20261013.md` (thank you; it led to all of this).

## Where things stand
Ramon stopped the Utah run on 2026-10-13 and approved a fix chain. Merged and closed today: SYNC-110 (ddp-sync `0f473b1`), OPEN-350 (openstates-core `e85f651`), OPEN-361 (openstates-core `14cc6a5` and api-v3 `14d3073`), and the OPEN-349 audit note (ddp-open-states `e10cc15`, `notes/open349-utah-version-document-audit-20261013.md`). On this Mac the api-v3 container was rebuilt at 02:03 UTC (it reads the RDS replica database) and returns the corrected order. **Nothing has been recomputed or restarted anywhere.**

What the changes do, in one line each:
- **OPEN-361 (api-v3 and openstates-core):** Utah's `Amended <M/D/YYYY H:MM:N>` versions are ordered by the date and hour in the note, not by the last number. Against the timestamps in the archived file URLs the old order was wrong for 48 of the 74 Utah bills with two or more such versions; the new one is right for 73.
- **OPEN-350 (openstates-core):** Utah's `Comparison to ...` and `Amended Excerpts ...` documents get no diff and are never a baseline (like `House Amendment N` already does). Affects newly archived data only.
- **SYNC-110 (ddp-sync):** a changelog's old text and `compare_version` are the version its diff was taken against. See item 4.

## 1. Deploy api-v3 `14d3073` (REQUIRED, your call on the window)
Same procedure as OPEN-316/326: quiet-window check, tag the running image (suggest `ddp-openstates-api:pre-open361`), `git pull --ff-only` in `/opt/ddp-open-states/api-v3`, rebuild, `up -d --no-deps api`. Rollback is the tag. I would avoid 04:45-07:00 UTC (the archive and knowledge-base windows).

**What rides along.** Between your last deploy I know of (`d3b5b69`, OPEN-326) and `14d3073` there are exactly two merges on `main`: PR #23 (black formatting; I checked the four source files it touched, `api/db/__init__.py`, `api/ddp_search.py`, `api/main.py`, `api/rds_credentials.py`, and each has an **identical syntax tree** before and after, so no behaviour change) and PR #25 (the OPEN-361 change: `api/version_ordering.py`, 37 added lines, plus one test). No migration, dependency, settings or Dockerfile change. **I cannot see your host's current sha**, so before you pull: list `git log --first-parent <your sha>..origin/main` and the files changed, and **tell me before pulling if anything other than #23 and #25 rides along.**

**Checks after** (X-API-Key header, from inside the ddp-sync container, as before):
- Utah, `GET /bills/ocd-bill/1ab505cf-f29c-496a-824b-5d6c76e41258?include=versions` (the same id exists in the replica): the three `Amended` notes must come back in the order `Amended 1/27/2026 11:01:108`, `Amended 2/9/2026 11:02:713`, `Amended 3/3/2026 19:03:587`. Before the deploy the order is Jan, **Mar**, **Feb**. Please capture both.
- **No change expected elsewhere.** Capture the `versions[].note` order for four bills **before and after** and confirm they are identical: FL `ocd-bill/db25275c-37bd-4565-9736-059ae3828d7e` (HB 1, 2023, 6 versions), FL `ocd-bill/325b8c72-ea57-4fc1-a872-37882650d71b` (HB 1001, 2026), VA `ocd-bill/8d0bbb6a-4ca7-451e-a66e-923f7abe56a2` (SB 2, 2026, 14 versions), VA `ocd-bill/b7217f12-9919-4d03-b08b-6f0f1006862d` (HB 2, 2026). The new rule only matches Utah's stamped notes (I checked every distinct stored note in every jurisdiction), so any difference here is a bug: say so and roll back.
- Usual after-checks: `/healthz`, ddp-sync health, no `apikey=` lines in api-v3's log.

## 2. Read-only: confirm RDS matches what I audited
My audit ran on the Mac's copies (the legacy local database and the RDS replica agree with each other). Please run on RDS and report:
```sql
SELECT count(*) AS docs,
       count(DISTINCT d.version_note) AS distinct_notes,
       count(*) FILTER (WHERE d.diff_from_previous_version IS NOT NULL) AS with_diff,
       count(*) FILTER (WHERE d.version_note ~* '^comparison to ') AS comparison_docs,
       count(*) FILTER (WHERE d.version_note ~* '^amended excerpts ') AS excerpt_docs
FROM ddp_bill_version_document d
JOIN opencivicdata_bill b ON b.id = d.bill_id
JOIN opencivicdata_legislativesession ls ON b.legislative_session_id = ls.id
JOIN opencivicdata_jurisdiction j ON ls.jurisdiction_id = j.id
WHERE j.name = 'Utah';
```
The replica returned **9,371 / 762 / 6,765**, 2,239 comparison and 368 excerpt documents (as of 2026-10-14 02:04 UTC). If RDS differs, report the numbers; do not act on the difference.

## 3. Read-only: is anything going to restart the Utah run by itself?
The Utah run is stopped and **must stay stopped** until Ramon says otherwise. Please report (no changes):
- the value of `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED` in this instance's environment (a true/false, not a secret);
- whether Utah is in the archive job's jurisdiction list here and what the last Utah archive-completion hook did (in particular whether it relayed a LegBot trigger to the Mac for any Utah session). If the hook could fire on the 05:00 UTC archive, tell me before it does rather than after; do not disable anything yourself.

## 4. Nothing for you to do for SYNC-110 or the Fargate image (just so you know)
- **SYNC-110** only changes `get_archived_version_transitions`, which the LegBot changelog pipeline calls, and that pipeline runs on the Mac (it relays from here). As far as I can tell from the code nothing on this host calls it, so it can ride your next regular rebuild; no deploy needed for it. Tell me if you know of a path that does call it here.
- **OPEN-350/361 in the Fargate image.** `deploy-scrapers-image.sh` clones openstates-core `main` at build time and must run on the Mac as Ramon (AWS and GitHub credentials; "never the EC2 host"). Until Ramon runs it, Utah documents archived in Fargate still follow the old rules. Utah is out of session, so I expect nothing new, but I cannot confirm. Not yours.

## Do not
- **Do not run `os-text-extract recompute-diff-order` or any backfill on RDS.** That is OPEN-351 and needs Ramon's explicit authorization for the real run; I will write a separate note. Utah's saved diffs live in RDS and reach the Mac by replication (I checked that `ddp_bill_version_document` is among the replicated tables and the subscription is current), so that run belongs on RDS and nowhere else.
- Do not trigger `/trigger/bill-artifact-generation` or any LegBot run for Utah.

Reply on this branch either way: api-v3 sha before and after, the before/after version orders, the image tags, and the answers to items 2 and 3.
