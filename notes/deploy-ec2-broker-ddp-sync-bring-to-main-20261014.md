# DEPLOY: bring the EC2-broker host's ddp-sync up to main (dev side, 2026-10-14 EDT)

**STATUS: GO, conditional.** Ramon asked for this note. Do it only if every gate in section 2 passes; a failed gate means stop and report, not improvise. A HOLD note from Ramon or me on this branch overrides it. Another agent is writing the separate deployment note for the ddp-broker-py work done today; this note is only about ddp-sync and is independent of it (see section 1).

**This is the EC2-broker host's ddp-sync ONLY** (the one next to production ddp-broker-py, OPEN-193). **Do NOT touch the votebot/ddp-api EC2 instance**: it runs its own permanently diverged branch (`feat/rds-openstates-routing-standalone`) on purpose, and CLAUDE.md says never to pull it up to `main`.

## 1. Why, and what changes
That host's ddp-sync was at `fe04095` (2026-10-10, the SYNC-103 merge). `main` is now `7dbd216`, 46 commits later. The ones that matter for LegBot's bill changelogs:
- **SYNC-110** (`0f473b1`): a changelog's old side is the version its diff was taken against.
- **SYNC-115** (`7dbd216`): a rejected BillArtifact counts as missing and is written again. It needs ddp-broker-py's BROKER-217 (the status read reporting review_status) to do anything; against a broker without it, it behaves exactly as today, so **the order of the two deployments does not matter for safety**.
You said earlier that you believe this host only relays LegBot triggers to the Mac and does not run generation itself; the pull is still worth doing so the host is not left on a different generation path, and so SYNC-110/115 are in place if anyone ever runs generation there. Everything else in the range is below.

Verified from the diff (`fe04095..main`), all inert on this host unless an environment setting says otherwise:
- **No dependency changes** (no pyproject/requirements/Docker file in the range).
- **Fork upstream check** (`FORK_UPSTREAM_CHECK_ENABLED`): the yaml entry is on but the environment gate defaults OFF, "Mac Studio only"; it must stay unset here.
- **GrantBot similarity refresh**: yaml `enabled: false`; Mac-only; needs CAMS.
- **SYNC-107 organization About content** (`LEGBOT_ORGANIZATION_CONTENT_ENABLED`) and **SYNC-97 organization resolution** (`LEGBOT_ORGANIZATION_RESOLUTION_MODE`): both default off.
- **SYNC-108**: the people pull job is now registered even on a host with scrapes off, but still gated by its own default-off flag.
- **SYNC-100/106**: `/trigger/verify-org-citations` loses its 3-item cap and can run in the background. Manual only.
- The knowledge-base embedding yaml (`enabled: true` since 2026-10-05) is **unchanged** by this pull; this host already has it.
Nothing in the range changes a default that turns on spending or writes on this host.

## 2. Gates (all read-only until step 3; record each result)
- **G1. Starting state.** In the host's ddp-sync checkout: branch, `git rev-parse --short HEAD` (expect `fe04095`), and `git status --short`. If there are uncommitted changes or a different branch/commit, **stop and report**; do not stash, discard or reset anything.
- **G2. Quiet window.** No scrape, archive, load, LegBot relay or backfill in flight (backfill locks and the ddp-sync log); at least an hour clear of **Sunday 02:00 UTC** (the weekly job incl. Utah's scrape) and **Monday 05:00 UTC** (Utah's archive). The ddp-sync container recreate briefly stops the scheduler.
- **G3. Rollback point.** Record the exact commit and the running image/container identity you are replacing, so it can be restored: back to `fe04095` and the previous image.
- **G4. Environment unchanged and as expected.** Read (do not print values) that `FORK_UPSTREAM_CHECK_ENABLED`, `LEGBOT_ORGANIZATION_CONTENT_ENABLED`, `LEGBOT_ORGANIZATION_RESOLUTION_MODE` and `GRANTBOT_SIMILARITY_REFRESH_ENABLED` are unset/off here, and that `KNOWLEDGE_BASE_INDEX_NAME` is set here and only here. Report anything else.

## 3. The change
1. Fast-forward only: `git pull --ff-only origin main` in that checkout. If it is not a fast-forward, **stop and report**.
2. Recreate **only the ddp-sync container**, by the host's usual procedure (the one used for the 2026-10-05 yaml). Do not touch Redis, api-v3, the broker or any other container; do not change any environment setting.
3. Verify: `/health` healthy, Redis connected, scheduler running with its jobs; the container's code contains the fixes (e.g. import `ddp_sync.services.broker_client.artifact_counts_as_present`, which exists only from SYNC-115); log has no startup errors; `git rev-parse --short HEAD` equals `7dbd216`.

## 4. Not authorized
Any other host (especially the votebot/ddp-api instance), any environment or yaml change, enabling any flag listed above, any Utah scrape/archive/LegBot run or trigger, any broker change (that is the other note), restarting anything but ddp-sync.

## 5. Reply on this branch
G1 to G4 results, the commit before and after, the container recreate time, the verification results, and anything unexpected. If a gate stopped you, say which and what you saw.
