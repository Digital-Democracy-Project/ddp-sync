# Request (read-only): can the EC2 ddp-sync's scrape launches be paused without deploying code, and how fast? (OPEN-437)

2026-10-27. Ramon's go given in this session. **Read-only: look and describe only. Do not change the config, the secret or any service, and do not trigger a scrape.** If any step would need a write, stop and report.

## Why
The scrapers image (OPEN-401 catch-up) will go live on Fargate with a canary state first (plan on OPEN-437). ddp-sync launches the `ddp-scrapers` family with no revision number, so a new revision serves every state. If the canary shows a drop in quality, the only brake I know of is deregistering the new revision. I want to know whether there is a faster or safer one, and what the exact steps would be, before the deploy.

## Please report
1. **Pause switch:** is there a setting that stops ddp-sync launching *scrape* jobs (and, separately, archive jobs) for all jurisdictions or for one, without a code deploy? Name the exact key and where it lives (the `sync_schedule.yaml` on the host, the Secrets Manager config, an environment variable), whether changing it needs a restart of the service, and how long a restart takes (SYNC-131's was about 2 minutes).
2. **Per-jurisdiction hold:** can one jurisdiction be removed from the effective list (the manifest, `jurisdictions.yaml`, the Secrets Manager config) so only that state skips a launch? Note that the host's `/opt/ddp-open-states` checkout is 10 commits behind main, so say which copy is authoritative for this.
3. **Manual trigger:** what is the exact way to launch *one* jurisdiction's scrape by hand through ddp-sync (the canary), and does a manual launch use the same latest-revision resolution as a scheduled one?
4. **Next launches:** list the scrape and archive jobs due in the next 48 hours (jurisdiction, UTC time) so a quiet window can be chosen, and say whether any Fargate task is running now.
5. **Resume:** how a pause is undone, and whether jobs missed during a pause run late or are skipped.

Reply with the facts and the commands you ran (read-only). Nothing else changes.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
