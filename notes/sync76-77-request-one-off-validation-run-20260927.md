# Please run a one-off validation now instead of waiting for tonight's cron

**Re:** `sync76-77-deployed-live-20260927.md` (this branch). No need to wait for the 06:00/07:00
UTC scheduled jobs to confirm the fixes work -- there's already a manual entry point in the code
that can validate both fixes right now, on a small, scoped, low-risk sample.

## What to run

`UpdateScheduler.trigger_legislator_bills_sync()` (`scheduler.py:1923`) already exists for exactly
this -- it's just never been wired to an HTTP route (no `/trigger/...` endpoint calls it, unlike
`trigger_openstates_sync`/`trigger_bill_status_sync`, which do have one each). It builds its own
`LegislatorSyncService`/`WebflowSource` internally, so it doesn't need the live scheduler process
or a restart -- callable directly from a one-off script in the venv.

Suggested call, to exercise both fixes in a single small run:

```python
import asyncio
from ddp_sync.scheduler import UpdateScheduler

async def main():
    scheduler = UpdateScheduler()
    result = await scheduler.trigger_legislator_bills_sync(
        limit=10,
        jurisdiction="us",       # forces federal Congress members into scope --
                                  # exercises the SYNC-76 fix directly, since this
                                  # filter now runs against the corrected
                                  # _resolve_jurisdiction() output
        include_votes=True,      # exercises the SYNC-77 ocd-person/ fix
    )
    print(result)

asyncio.run(main())
```

## What a pass looks like

The returned dict has `total_bills`/`total_votes` directly. Before either fix, this would have
been `successful=N failed=0 total_bills=0 total_votes=0` no matter what (the exact silent-failure
shape from the original investigation). After both fixes, for 10 real federal legislators, expect
non-zero `total_bills` (most sitting members sponsor at least one bill) and likely non-zero
`total_votes` depending on session/vote data available for whichever 10 come up.

If `jurisdiction="us"` also worth spot-checking against `/health` or logs afterward: confirm those
10 legislators' `fetch_sponsored_bills`/`fetch_legislator_votes` calls actually routed to the RDS
replica (`rds_openstates_api_base`) rather than the public API, per SYNC-76.

If this comes back clean, no need to also wait for tonight's full cron run to close the loop --
happy to move both tickets to Done off this smaller result rather than waiting on the bigger
scheduled one, unless you'd rather have both data points first.

Reply here with the result.
