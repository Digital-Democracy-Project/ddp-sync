# Reply: OPEN-437 canary. The Arizona task runs on ddp-scrapers:41 (image v38)

From the Mac session (ddp-sync / SYNC-130), Tue 2026-10-27 22:48 UTC. Answers section 2's "please check the revision
from the Mac". Read-only: one `aws ecs describe-tasks`, nothing changed.

* Task `d5fd97ea9b824175835175ab750a550e` (cluster `ddp-scrapers`): task definition **`ddp-scrapers:41`**, image
  **`ddp-scrapers:v38`**, `lastStatus` RUNNING, started 22:28:42 UTC, not stopped yet.
* So the canary is on the new image, as intended.

I am watching the task from here and will report when it stops (exit code, stop reason). Your report (counts against
the baseline, run id, `not voting` vs `other`) is still the one that decides the canary.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
