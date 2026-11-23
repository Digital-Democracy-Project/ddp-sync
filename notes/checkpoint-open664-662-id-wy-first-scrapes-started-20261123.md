# Checkpoint: Idaho (OPEN-664) and Wyoming (OPEN-662) first scrapes STARTED on task revision 81 (v79); one rollout also put Pennsylvania live

From DDP Prod Agent, 2026-11-23 04:27 UTC. Ramon said go in my session ("launch all the scrapers now"; pop-up answers), including Pennsylvania going live with this rollout, and said not to queue them behind Pennsylvania's nightly.

- **Gates:** #461 and #467 merged (main `f0cfd9c7a`); mi, pa, ri, ok, fl locks held, none for id or wy; Monday 04:26 UTC, not Sunday.
- **PA-idle gate waived by Ramon:** your Pennsylvania request `945ce66` says pa's nightly must be finished first. Pa's 04:00 UTC nightly (task revision 81) was still running at 04:26 UTC; Ramon said to launch without waiting. I rolled out anyway; I have not seen what happens to that nightly run's embedding step now that pa's flags are on.
- **Rollout (EC2):** `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync --skip-ecs-check`, exit 0, "30 jurisdiction(s) valid", "every list compared agrees", checkout 5a0eb41b2 to `f0cfd9c7a` (e5fe4140 #458 PA, #461 ID, #467 WY, #464 docs), api-v3 copy md5 f4b6005a4d49b39d19190c366cfaea0a (also inside the container), container healthy. List: live = az fl ma mi nc pa us ut va wa; scraping = al ga id ky la nd ne nm nv ok or ri wy. **pa:** status live, search_refresh, embedding, api_smoke_test, qa_sweep true, replica_routing and legbot_replica true (unchanged), archive on. **id, wy:** scraping, cloud_path, archive, people_refresh true, LegBot, routing, search, embedding false. No restart. The check "/ddp/enrollment lists pa as live": not done (the broker's web has no such route and api-v3's needs an API key I did not use); the manifest read-back above is my evidence.
- **Triggers** (one POST each, no retry, no skip_people_check, no 409), all 202:

| State | Run id | Triggered (UTC) | People read | Task revision (ECS) |
|---|---|---|---|---|
| id | id-2a1021ebee6f | 04:26:54 | 109 | ddp-scrapers:81 |
| wy | wy-d64df4852faf | 04:26:57 | 96 | ddp-scrapers:81 |

- Michigan (`mi-b8b23c2b5df9`), Rhode Island (`ri-397bc21f0bff`), Florida (2024-2026), Oklahoma's archive and the Pennsylvania nightly are all running at the same time on Fargate.
- **Will report when each ends:** sessions line, load, votes, completion, counts, fetch errors, time; any LegBot hand-off for id or wy (SYNC-186 is not released on EC2). Then PA's first embedding, smoke test and QA sweep results.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
