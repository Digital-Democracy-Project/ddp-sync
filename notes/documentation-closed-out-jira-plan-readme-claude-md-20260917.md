# Documentation closed out: Jira epic, PLAN doc, README, CLAUDE.md

**Date:** 2026-09-17
**Audience:** whoever picks this thread up next.

Following the deploy sign-off (`sign-off-dry-run-verified-session-closed-20260916.md`) and the
retirement decision (`decision-standalone-branch-is-the-right-call-instance-being-retired-
20260916.md`), the durable-documentation follow-ups both of those notes flagged as still
outstanding are now done:

- **`SYNC-69`** (Jira epic, `ddp-sync` project): tracks the retirement + Pinecone-into-
  EC2-broker consolidation decision. Linked (`relates to`) to `SYNC-67`, which now also has a
  comment explaining the timing relationship -- `SYNC-67`'s gap is real for as long as the
  current code runs, but whoever eventually builds a fix should treat it as a decision point
  (fix in place vs. build fresh in the eventual replacement) rather than a plain bug-fix task.
- **`ddp-infra` PR #165** (`PLAN-votebot-ddp-sync-retirement.md`, + a `PLANS-INDEX.md` row):
  the actual plan doc, recording the decision, what's built vs. not, and open questions (no
  firm date, no design for the EC2-broker Pinecone-ingestion step yet, no decision on the host
  itself once both jobs leave it).
- **`ddp-sync` PR #160** (`CLAUDE.md` + `README.md`): both docs previously described only two
  of the three real instances (from opposite gaps). Now both list all three, note the
  votebot/ddp-api instance's retirement plan, and `README.md`'s own EC2-civic deploy section
  carries the same "don't `git pull origin main` here" warning already present on
  `feat/rds-openstates-routing-standalone`'s own `README.md` copy -- so it's visible to anyone
  starting from `main`, not only to someone who already knows to check that branch.

None of tonight's actual deploy work changed as a result of this pass -- this was purely
making the already-made decisions and already-found gaps findable for whoever comes next,
per the standing rule that a decision without a durable home tends to get re-litigated or
silently drift.

## Still open, unchanged

- Whether to rotate `rds_openstates_api_key` given the confirmed (if brief, if local-only)
  production log exposure -- still a judgment call, not resolved by anything in this note.
