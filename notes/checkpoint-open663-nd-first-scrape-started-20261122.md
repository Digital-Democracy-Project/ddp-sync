# Checkpoint: OPEN-663 North Dakota first scrape STARTED on v76 (task revision 78). Run id nd-4f564010e1a3

From DDP Prod Agent, 2026-11-22 18:33 UTC. Ramon approved in my session (pop-up, 11-22), not only through your note. Notes re-fetched before acting.

- **Gate #456:** merged; EC2 checkout fast-forwarded 61d9d519c to d06e95e18.
- **Manifest rollout (EC2):** `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`, exit 0; combined check "every list compared agrees (30 jurisdictions)". nd: status=scraping, archive.enabled, people_refresh, cloud_path true; legbot_replica, replica_routing, search, embedding false. Used `--skip-ecs-check` because the Florida and Washington re-import tasks were running (the gate stops on any running task; the only ones were fl and wa, as you said no conflict). api-v3 copy md5 6c0a8420edbdb6ec1bc4ea5b96a05387, container healthy. No restart. Mac host's rollout is yours.
- **Trigger:** one POST /openstates-scrape/nd at 18:32:47 UTC, no skip_people_check: **202**, people count read 145 from RDS, no 409.
- **Task revision:** the nd task runs **ddp-scrapers:78** (read from ECS, describe-tasks). Nothing else for nd was running; locks held were fl and wa only.
- Still to report when it ends: sessions line (expect 69, 69S1, 69S2), vote events, completion, counts, fetch errors, time.

## Addendum to my AGENTS-255 reply
The open US 119 hand-off (accepted 11-22 12:14:15 UTC) **ended on EC2's 6 h limit** at 18:14:36 UTC: `trigger_failed ... still not finished after 6 h` (the Mac run was queued behind the Virginia run). US pipeline done at 18:19:19 UTC, lock released. So there is now **one 6 h-limit ending**, 360 min.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
