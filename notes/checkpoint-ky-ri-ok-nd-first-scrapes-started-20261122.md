# Checkpoint: Kentucky, Rhode Island, Oklahoma first scrapes and the North Dakota rerun STARTED, all on task revision 80 (v78)

From DDP Prod Agent, 2026-11-22 20:12 UTC. Ramon said go for all four in my session (pop-up, 11-22), and to run them in parallel ("they are on fargate and don't conflict with each other"). Notes re-fetched before acting; nothing withdrawn.

- **Gates:** ddp-open-states #457, #462, #463 merged. EC2 checkout fast-forwarded d06e95e18 to `5a0eb41b2` by `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync` (exit 0, "every list compared agrees, 30 jurisdictions", api-v3 copy md5 9be18a13ccbc2163820a853ae70b8a6b, container healthy). `--skip-ecs-check` used again: the only running tasks were fl and wa. List now: scraping = al ga ky la nd ne nm nv ok or pa ri; probing = hi ia id md me mo ms mt wy. No restart. The Mac host's rollout is yours.
- **Triggers** (one POST each, no retry, no skip_people_check; all 202, no 409), 20:11:45 to 20:11:51 UTC:

| State | Run id | People read from RDS | Task revision (from ECS) |
|---|---|---|---|
| ky | ky-198bb9b87831 | 141 | ddp-scrapers:80 |
| ri | ri-8151f1638a5a | 116 | ddp-scrapers:80 |
| ok | ok-7192f1fe4328 | 149 | ddp-scrapers:80 |
| nd | nd-4afc42bc7ff5 | 145 | ddp-scrapers:80 |

I did not read the image tag (my role cannot read task definitions); revision 80 is what you named for v78.
- Locks before: fl and wa only. Florida and Washington re-imports are still running (revision 78).
- I will report each run's sessions line, votes, completion, counts, fetch errors and time in a reply per state. A load stopped by a repeated vote key: I stop and send the full message. No retry for any state.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
