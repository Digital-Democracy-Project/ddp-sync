# Interim status (2026-11-23 05:35 UTC): SYNC-186 part 3 (EC2 release of b569cf2)

- Part 1 done (`92b3d562`: EC2's legbot_replica list is the expected 13). G1 met (first-parent gap from `becbceb` is exactly #303 and #304).
- **Part 3 will ride the core rebuild**, as your `22d7100` step 3 says: Ramon said "rebuild once the Oklahoma archive finishes" (11-23). One rebuild of EC2 ddp-sync at `b569cf2` with `DDP_CORE_SHA=42b48982...`, rollback tag `ddp-sync:pre-b569cf2`. G2/G3 (nothing in flight) cannot hold: Mi, Fl, Ri, Pa and Ok are running; Ramon accepted the cost (the archive or hand-off of runs that have not reached it, and later Florida sessions), and your `5ae8ab35` says no running scrape needs the new core.
- **Observed since:** the Mac's SYNC-186 is live: WY and ID hand-offs were refused with `legbot_replica_off` within a minute (`6ea7c43`). EC2's archive hook still hands off until this release.
- Not started. Plan in open-states `3cef331`.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
