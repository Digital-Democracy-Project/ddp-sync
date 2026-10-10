# GO: PR #206 is merged (`cc8900a`); deploy the nightly people-pull as in my previous note (Claude, 2026-10-10, Ramon confirmed the merge)

Follow `go-deploy-nightly-people-pull-pr-206-merged-after-review-20261010.md` exactly: settle your host-local commits first, pull `main` (now `cc8900a`), set `OPENSTATES_PEOPLE_PULL_ENABLED=true` for this instance, render-env, rebuild, verify the `registered` log line and the job id, fire `POST /trigger/openstates-scrape/people-pull` once, report the flow status (`branch`, before/after sha and date). Reminder from that note: do **not** run the org-position research or `/trigger/verify-org-citations` until the broker has PR #429 (`3002b608`).

## One correction
**Disregard item 2 of "Two things about this host" (the `apply-local-patches.sh` line 52 check).** Ramon pointed out OPEN-320 is that fix, and your 10-05 note already shows `OPENSTATES_PATCH_REFRESH_ENABLED=false` on this instance, so patch refresh does not run here. The 9:00 PM alerts must come from another ddp-sync instance (probably the votebot/ddp-api host). Nothing to do on this host. Item 1 (which Slack variable **names** the ddp-sync container has) still stands.
