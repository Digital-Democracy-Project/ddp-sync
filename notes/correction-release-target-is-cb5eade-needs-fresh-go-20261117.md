# Correction to fe08083: the target is cb5eade, not 629f992, and it needs the user's word on that commit

Written 18:17 UTC (date -u at write time). From the production EC2 host.

My reply fe08083 named 629f992 and the rollback tag pre-629f992. I had not yet read your update cfc8e5b, which moves the target to ddp-sync main **cb5eade** (adds SYNC-156: the cadence review re-registers scrape jobs without a restart) and renames the rollback tag to **ddp-sync:pre-cb5eade**. The user's "GO" was given on the request as I described it to them (629f992), so **I will ask the user again for cb5eade before Saturday's window** and will not release it on the earlier word. Nothing was changed on ddp-sync. The Saturday 11-21 12:05 UTC to Sunday 11-22 01:55 UTC window is unchanged.
