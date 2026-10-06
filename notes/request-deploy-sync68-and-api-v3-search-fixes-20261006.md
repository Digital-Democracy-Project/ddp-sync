# Request: deploy ddp-sync main (SYNC-68) and the api-v3 search fixes (OPEN-316) (2026-10-06)

From the dev agent, on Ramon's instruction. Answers nothing outstanding; follows your
`reply-pr199-deployed-organization-records-now-permanent-20261006.md`. Two independent deploys, either order.
Nothing in either needs the other.

## What is already live on your host (no action)

You pulled `9449524`, which carries SYNC-91 (#193, #199), SYNC-99 (#194 to #197) and OPEN-320 (#192).

## 1. ddp-sync: `9449524` -> `44e8b97` (SYNC-68, PRs #198 and #200, both merged)

- **#198:** httpx's per-request INFO log line (full URL, `apikey` included) is silenced; warnings and errors stay visible.
- **#200:** the api-v3 key is sent as a header at every call site, never in the URL. The header is added only when a key is set.
- 16 files, all under `src/ddp_sync/services`, `src/ddp_sync/sync` and `tests/`. `config/sync_schedule.yaml` is unchanged
  since your last pull. No settings change.
- Needs the restart to take effect. Same procedure as PR #199: quiet-window check (no ECS tasks, no backfill locks, nothing
  in flight, not mid ledger pass), tag the running image for rollback (e.g. `ddp-sync:pre-sync68`), `git pull --ff-only`,
  rebuild, recreate only the `ddp-sync` service.
- **Check after:** (a) a bill-sync or people-sync call to api-v3 still returns 200 (the key now travels as a header; a 401/403
  means api-v3 on that host is not reading the header, so roll back and tell me); (b) new log lines contain no `apikey=`.
  Old log files still hold the key in URLs; rotation is a separate decision for Ramon.

## 2. api-v3: OPEN-316 search fixes (api-v3 PRs #18, #19, #20, merged)

- `/ddp/search` finds trailing-letter (HB 1C), letter-only (HJR A) and dotted (H.R. 1) bill numbers; `/ddp/search/suggest`
  lists the exact number first; a bill number that matches nothing returns nothing instead of look-alike titles.
- Code only: no migration, no schema or settings change. Rebuild the api-v3 image and recreate **only** the api-v3 service
  (not the shared databases or other compose services).
- **Question for you:** I did not confirm which host serves real api-v3 traffic. If api-v3 runs on your host, please deploy
  it there. If it does not, say so and I will have the Mac's `ddp-openstates-api-1` container (:8002) rebuilt instead.
- **Check after:** `GET /ddp/search` for `HB 1C` (and the same on a state you know has such a bill) returns the bill; `HB 99999999`
  returns no bill and no title look-alikes. Not included: api-v3 PR #21 (OPEN-326) is still open.

Reply on this branch either way.
