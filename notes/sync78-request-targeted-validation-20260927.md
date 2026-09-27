# Please run a targeted validation for SYNC-78 specifically -- no need to wait for Sunday

**Re:** `sync76-77-78-all-deployed-live-20260927.md` (this branch). SYNC-76/77 already got a real
manual validation run against production data after deployment (`total_votes=3`, real RDS routing
confirmed). SYNC-78 hasn't had an equivalent check -- it landed *after* that validation run (it
was born from what that run exposed), and only its unit test suite (282/282) has run against it
since. The actual fix has never been exercised against real production data yet.

## What to run

Same mechanism as before, but this time it's worth specifically targeting the two legislators
whose sponsor-name lookup this fix addresses:

```python
import asyncio
from ddp_sync.scheduler import UpdateScheduler

async def main():
    scheduler = UpdateScheduler()
    result = await scheduler.trigger_legislator_bills_sync(
        limit=10,
        jurisdiction="us",
        include_votes=True,
    )
    print(result)

asyncio.run(main())
```

Same call as the earlier SYNC-76/77 validation. If James Gallagher and Darline Graham happen to
land in this sample again (CMS insertion order was stable last time -- likely, not guaranteed),
their `total_bills` should now be nonzero instead of hitting "Could not determine sponsor name" --
that's the direct, specific proof SYNC-78 works. If they don't land in the sample this time (CMS
data may have shifted), a quick single-legislator check is fine too:

```python
from ddp_sync.pipelines.legislator_sync import LegislatorSyncService
from ddp_sync.config import get_settings

async def check_one(openstates_id, jurisdiction="us"):
    service = LegislatorSyncService(get_settings())
    bills = await service.fetch_sponsored_bills(openstates_id, jurisdiction)
    print(f"{len(bills)} bills found")

# Gallagher: 2819c958-3cbe-4349-a2e9-6997054b8ea2
# Graham:    4c0ef839-19db-4ca7-844c-c863ff4963d2
asyncio.run(check_one("2819c958-3cbe-4349-a2e9-6997054b8ea2"))
```

## Why this matters

Before SYNC-78, both of these specifically failed at the sponsor-name-lookup step with zero
bills found, even though the replica already had their real sponsorship data. Confirming
`fetch_sponsored_bills` now returns real results for them is the actual proof the routing fix
works -- not just that tests pass, and not something we need to wait until Sunday's scheduled
run to see.

Reply here with the result -- happy to close out SYNC-76/77/78 on the strength of this rather than
waiting for the weekly cron if it comes back clean.
