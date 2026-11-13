# GO (addition to `502df30`): also update the EC2 api-v3 to `5c123ef`, as Part A2, between the manifest (Part A) and ddp-sync (Part B) (SYNC-157, BROKER-368, BROKER-482), 2026-11-13 about 03:55 UTC (Mac side)

**Ramon's go, 2026-11-13 (/onboard-state pop-up, his choice): "Add it to today's release".** It adds one part to the release you scheduled for after Pennsylvania's nightly (`3e67897`). Everything else in `502df30` stands, Part E still holds.

## Why
The broker deploy request posted tonight (ddp-broker-py `d7a584e3` / `e635fc00`, broker `9fd30ae3`, not cleared yet) makes the broker read each state's `actions.newest_first` and `voatz.test_precinct` from `/ddp/enrollment`. This host's api-v3 is `1cf251b` (your 11-10 deploy), which **does not serve those fields**: they came in api-v3 #57 (`c803d4c`, BROKER-368), and #58 (`5c123ef`, BROKER-482) drops any `voatz.creator_id`. So after Part A alone, the broker would still read "no setting" for US, NC and the others. The Mac api-v3 runs `5c123ef` with the `6c0372b` manifest since 03:31 UTC tonight; its `/ddp/enrollment` serves all 13 states with `actions` and `voatz` sections, no creator id.

## Part A2 (after Part A's manifest rollout, before Part B): your usual api-v3 pull-and-swap, as on 11-10
* **Gates:** `git -C /opt/ddp-open-states/api-v3 status` clean; report the current commit (`1cf251b` expected); tag a `pre-5c123ef` rollback image.
* **Change:** `git pull --ff-only` (expect **`5c123ef`**); `docker compose -p ddp-openstates-rds -f docker-compose.rds.yml build api`; `up -d --no-deps api`. **Only `api`; never an unscoped recreate.** If Part A's script already recreated `api`, this is a second recreate of the same service: fine.
* **Checks (read-only):**
  1. `/ddp/enrollment` answers 200 with 13 states; `replica_routing` still AZ FL LA MI NC NE PA US UT VA WA.
  2. `actions.newest_first` is present, and **true for `us` and `nc`** (and whatever else the manifest says; paste the list of states where it is true).
  3. **No entry carries `voatz.creator_id`** (count 0).
  4. `/bills?jurisdiction=pa&per_page=1` answers 200.
  5. The Postgres container's start time is unchanged.
* **Rollback:** the `pre-5c123ef` image, `up -d --no-deps api`.

## Not authorized
Anything beyond `502df30` Parts A to D and this Part A2: no broker deploy (that is `d7a584e3`, waiting for Ramon's own go), no Part E, no env or secret edit, no other container.
