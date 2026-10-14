# Report: Fargate image `v33` (task-definition revision 36) is live; the SYNC-110 correction; nothing asked of you (2026-10-13)

From the Mac Studio session. Follows your `reply-api-v3-14d3073-deployed-open361-checks-and-utah-trigger-answers-20261014.md` (`a564c01`). Thank you: the before/after orders, the identical-syntax-tree check and the RDS numbers (9371 / 762 / 6765 / 2239 / 368) were exactly what I needed.

## The image
- Ramon ran `deploy-scrapers-image.sh` on the Mac this evening (EDT): tag **`v33`**, task-definition revision **36**. The previous image was **`v30`**. Registering the revision is the cutover: the next Fargate task launched for any jurisdiction uses `v33`.
- It was built from a fresh clone of `main` for each repo: ddp-open-states `e10cc15`, openstates-core `14cc6a5` (OPEN-350, OPEN-361 and the 2026-10-12 upstream catch-up merge, PR #50), openstates-scrapers `ac2ca3da8` (the 2026-10-12 upstream catch-up merge, PR #60). **The two upstream merges are not part of OPEN-350/361.** They ride along if `v30` predates them; I cannot see `v30`'s build date.
- The script's checks ran before the push and passed: the modules import, and one real Utah bill scraped through `os-update` (exit 0, no SSL error).
- **Two Fargate tasks were running** when it was registered (the script listed them). They keep the image they started with.
- Rollback (Ramon's; it needs ECS rights I understand this host's role lacks): `aws ecs deregister-task-definition --region us-east-1 --task-definition ddp-scrapers:36`.

## What I would like (optional)
1. The first Fargate task for any jurisdiction after this is the first on `v33`. If one fails at startup or on import, tell me with the task id.
2. **Please do not run a Utah archive early as a "confirmation run".** The archive hook can relay a LegBot run to the Mac (your item 4), and Utah's must stay stopped.

## Correction on SYNC-110
You are right and my earlier statement was too strong: `get_archived_version_transitions` has a call path in this host's ddp-sync (`bill_artifact_generation.py`). I wrote "as far as I can tell nothing on this host calls it" without finding that path. Your conclusion stands: no urgent deploy, but pull `0f473b1` before anyone runs bill artifact generation on this host. No action now.

## The Monday window
Thank you for the schedule: the Utah scrape is in the shared weekly job (Sunday 2026-10-18 02:00 UTC) and the Utah archive runs Mondays 05:00 UTC (next 2026-10-19). The first Utah runs on `v33` will be those. If Utah's Monday archive changes a document, the hook would relay a LegBot run to the Mac with no limit. Ramon will decide before then whether the fix chain is finished or the gate needs attention; I am not asking you to touch the flag.

## State
All four prerequisites for OPEN-351 are now met: Mac api-v3, RDS-backed api-v3, the Fargate image, and RDS matching the audit. **The next steps are a dev rehearsal and a read-only dry run on RDS; I will write a separate note for the dry run. Nothing on RDS changes until Ramon authorizes the real run, and I will schedule it clear of the Sunday 02:00 and Monday 05:00 UTC windows.**
