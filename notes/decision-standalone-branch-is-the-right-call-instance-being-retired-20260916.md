# Decision: merge the standalone branch. This host is being retired, not caught up.

**Date:** 2026-09-16
**Replying to:** `notes/request-review-of-standalone-openstates-routing-branch-20260916.md`'s open
question -- "is reimplementing SYNC-6/SYNC-8 by hand ... the right long-term call for this
host, or should this be treated as a stopgap?"
**Decided by:** Ramon, directly.
**Audience:** whoever finishes and deploys this.

## The answer: it's an intentional stopgap, and that's fine -- proceed with the merge.

New context that wasn't available earlier in this thread: **this whole votebot/ddp-api EC2
`ddp-sync` instance is expected to be retired once Webflow is fully removed in December 2026.**
The longer-term direction is to fold Pinecone/VoteBot-knowledge-base ingestion into the
**ddp-broker EC2's own `ddp-sync` instance** instead -- that instance already runs
scrape → archive → extract → LegBot-summarize for every bill in production, so adding "send to
Pinecone" as one more pipeline step there is the more natural long-term home, rather than
maintaining this separate third instance indefinitely.

Given that horizon, catching this specific host fully up to `main` (~350+ commits, most of it
LegBot/CAMS/Fargate/per-host-flag machinery this host will never use and won't exist much
longer to use it) isn't worth the investment. The standalone branch's narrower scope -- just the
OpenStates-routing capability this host actually needs today -- is the right-sized move, not a
compromise to eventually reconcile with a real `main` pull. **Do not treat this as
tech debt to clean up later by pulling main properly** -- the plan is for this host to go away
entirely, not to converge with `main` over time.

## Concretely, this also means: drop the ddp-broker-py / bill_version.py question from scope

The parallel `bill_version.py` upgrade question (the ddp-broker-py Flow 2 dependency covered in
`notes/ddp-broker-dependency-in-bill-sync-20260916.md` and its replies) is **not needed for this
host at all**. Skip it entirely, not just defer it -- this host's Flow 2 keeps running on its
existing local Redis version-tracking, unchanged, for the rest of this host's lifetime. The
confirmed `ddp_broker_api_base=http://10.0.0.11:8080` address is still correct and useful
information (worth keeping in these notes for whoever eventually builds the real Pinecone-step
addition to the ddp-broker EC2's own pipeline), just not something to configure or deploy on
*this* host.

## What still needs to happen on this host (unchanged, now fully unblocked)

Everything already confirmed in the sibling notes still applies -- these are settings, not tied
to which code is running:
- Merge `feat/rds-openstates-routing-standalone` (commit `77f93f7`).
- `ddp_openstates_jurisdictions`: `["FL", "WA", "US", "VA", "MI", "MA", "UT", "AZ", "NC"]`.
- `local_openstates_api_base`: `http://10.0.0.11:8002`.
- `local_openstates_api_key`: reuse this host's existing, already-confirmed-working
  `rds_openstates_api_key` value (see `notes/correction-local-vs-rds-openstates-settings-swapped-20260916.md`)
  -- no new key needed.
- Both `:8002` and `:8080` addresses are confirmed reachable directly from this host (see
  `notes/reachability-confirmed-from-votebot-ddp-api-host-20260916.md`) -- though `:8080` is now
  moot per the above.

## Separate follow-up, not blocking this deploy

The retirement + Pinecone-into-ddp-broker-EC2 consolidation plan is a real infrastructure
decision that deserves its own tracked ticket/plan doc at some point, so it doesn't get lost
between now and December -- flagging here, not resolving it in this note.
