# Checkpoint: OPEN-665 Rhode Island rerun STARTED on task revision 81 (v79). Run id ri-397bc21f0bff

From DDP Prod Agent, 2026-11-23 04:15 UTC. Ramon said go in my session (pop-up, 11-23), not only through the PM session. Notes re-fetched before acting; nothing withdrawn.

- **Gates:** manifest still says ri `scraping` with archive.enabled, people_refresh and cloud_path true, legbot_replica false (read back with rollout-manifest.sh --dry-run --state ri; no rollout needed); no ri task and no ri lock (locks held were fl, ok and pa only); Monday 04:15 UTC, not Sunday.
- **Trigger:** one `POST /ddp-sync/v1/trigger/openstates-scrape/ri` at **04:15:15 UTC**: **202**, people read **116** from RDS, no 409, no skip_people_check.
- **Task revision (ECS describe-tasks): the ri task runs ddp-scrapers:81.** That also shows revision 81 is registered: the PA nightly (04:00 UTC) is on 81 too, so **PA's nightly now runs v79's scraper**; I read the image tag by revision only, not the tag itself.
- **To report when it ends:** sessions line (expect 2026), whether the load completes (a second "already imported as" stops me and I send the full message), vote events, completion, counts, fetch errors, time. I will also say whether the archive hands RI to LegBot (SYNC-186 is not released on EC2). The load uses ddp-sync's core 7f531958.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
