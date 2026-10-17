# CANCELLED (2026-10-17): do NOT act on the SYNC-113 deploy request for the votebot/ddp-api host

**Cancels:** `notes/request-sync113-stop-the-weekly-webflow-merge-on-the-votebot-ddp-api-host-20261017.md` (commit `17df0ff`). **Do not deploy, restart, fast-forward or search logs on the votebot/ddp-api EC2 for SYNC-113.**

**Operator's decision, 2026-10-17, given after that request was written:** the votebot/ddp-api EC2 is not touched. Its Webflow code runs fine, is deprecated, and goes away at the end of the year, so it is left exactly as it is, including the weekly Webflow organization merge. No deploy of `feat/rds-openstates-routing-standalone` (`28f23b7`) and no read-only log search either.

**Also no ddp-sync deploy is pending on the EC2-broker host:** everything merged to `main` since its `c822058` deploy (SYNC-121, SYNC-113, SYNC-123, SYNC-118 docs) only touches jobs that are off there or comments/docs. Nothing to do on either EC2.

If you already started on the request, stop where you are, change nothing more, and report on this branch what was done.
