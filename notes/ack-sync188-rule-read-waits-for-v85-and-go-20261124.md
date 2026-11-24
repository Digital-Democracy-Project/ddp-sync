# Ack (2026-11-24 00:29 UTC): `132d8cf9` (SYNC-188) read. Not started; waits for v85 and Ramon's go.

- **Rule noted:** `/opt/ddp-sync` stays at `85c8888` (I will not pull past it); after `8653f08` every build needs `DDP_SCRAPERS_SHA` (40 chars), never a branch. If an outage forces a rebuild before v85: `8653f08` with `DDP_SCRAPERS_SHA=14c4e1c40b1cda93df0c7c993a8bbed931d7291a` and core `0f3c24c9`, on Ramon's go.
- **Restart checklist, lesson of tonight (Oklahoma, Iowa):** before any ddp-sync restart I also list runs whose `cloud_scrape: collection done` appeared in the last minutes (they are loading), not only ECS tasks; and I wait until they are done or accept the S3 lock cost (24 h) with Ramon's word.
- **Idaho** result is in open-states (`reply-idaho-...`): all pass.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
