# Request: deploy api-v3 `d3b5b69` (OPEN-326, transposed-letter surnames) and measure the real suggest p95 (2026-10-06)

From the Mac Studio session, on Ramon's instruction. **This is the operator's go-ahead to deploy**; he has also accepted the
latency cost below. Follows your `report-api-v3-open316-deployed-20261006.md` (thank you; the checks were what I needed).
Answers the earlier `request-read-only-timings-for-api-v3-pr21-...` thread: your RDS timings arrived and were used.

## What to deploy

- api-v3 `main` at **`d3b5b69`** (PR #21 merged). From your deployed `ae33226` the diff is two files only, `api/ddp_search.py` and
  `api/tests/test_ddp_search.py` (+204/-2). No migration, dependency, settings or Dockerfile change, so it is the same procedure as
  OPEN-316: quiet-window check, tag the running image (e.g. `ddp-openstates-api:pre-open326`), `git pull --ff-only` in
  `/opt/ddp-open-states/api-v3`, rebuild, `up -d --no-deps api`. Rollback is the tag.
- What it does: a one-word query of 4+ letters is also matched against surnames with two adjacent letters swapped, so `Smtih`,
  `Jhonson`, `Garica`, `Martniez` and `Pelsoi` find the right legislators. Hits score 0.9 so they outrank weak bill-title matches.

## Latency (accepted by Ramon; please measure it)

Your own `EXPLAIN ANALYZE` showed the people step going from about 26.5 to about 46-47 ms, so +19-20 ms on every one-word query of
4+ letters, projecting the idle suggest p95 at about 203-209 ms against BROKER-175's 200 ms bar. That is a projection. After the deploy:

1. Re-run the suggest loop you used for the 184/189 ms figures, with `smtih`, `garica`, `jhonson` added, and report p50 and p95.
2. If p95 is **above about 210 ms**, say so and do not roll back; tell me the number and I will open the precomputed-surname-column ticket.
   (Rollback only if something is broken, not for latency.)

## Checks after (X-API-Key header, from inside the ddp-sync container, as before; `jurisdiction` is required)

- `/ddp/search/suggest?q=Smtih`: Smith legislators (Adam, Adrian, Austin Smith ...), not an empty list. Same for `Pelsoi` (Nancy Pelosi).
- `Garica`: Garcia/García legislators first, not bills. `Jhonson`: Johnson legislators. `Martniez`: Martinez legislators.
- No new noise: `budget`, `housing`, `medicaid` return no legislators.
- Not tested last time, please include: `H.R. 1` (US), `H.J. Res. 1`, `HJR A` (shape checks from OPEN-316).
- NC will return no legislators for any name: production has 0 current NC people (already ticketed separately), not a regression.

Reply on this branch either way.
