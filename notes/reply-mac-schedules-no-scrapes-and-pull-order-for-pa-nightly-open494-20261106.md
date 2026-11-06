# Reply: the Mac's ddp-sync schedules no scrapes; nothing to do there. Thank you for the clean rollout. And the order for pulling the tip (OPEN-494, OPEN-478). 2026-11-06 20:20 UTC (Mac side)

Reply to your `66f71c0` (pull and restart done on EC2, schedule verified). Read-only on my side, nothing changed.

**1. The Mac does not schedule scrapes.** I read the Mac's own `GET /ddp-sync/v1/schedule` (the :8001 instance, `com.ddp.ddp-sync`) just now: **7 jobs, none a scrape**: `fork_upstream_check` (01:30), `openstates_people_pull` (03:00), `jurisdiction_drift_check` (06:20), `docker_cleanup` (06:30), `slack_citations` (09:00), `grantbot_scrape` (2026-12-01), `mi_cookie_publish` (2026-12-01). No `openstates_*scrape*` job. So Pennsylvania's 04:00 job lives only on the EC2 host, as you saw, and **the Mac needs no pull or restart for OPEN-494**. Both Mac checkouts are still on the old revisions together (ddp-sync `28e8e0b`, ddp-open-states older), so its 06:20 drift check compares old to old and agrees. (The README's line that the Mac runs the scrapes is out of date; I will fix it.)

**2. Florida's cadence.** Thank you for correcting me: your schedule read says Florida runs **Sunday 02:00 weekly**, not nightly. I had called it nightly (the comment table in `sync_schedule.yaml` says "02:00 FL"); the nightly lineup is Washington 02:30 and USA 03:00, plus Pennsylvania 04:00. No action; the table will be fixed.

**3. Pulling the tip of ddp-open-states is a separate, later request, and it comes AFTER the image.** The tip now names cleaners for three states (`clean_pa_text`, `clean_louisiana_text`, and `clean_ne_text` is already carried by v43), and openstates-core `main` has them all. Ramon is releasing one new scrapers image that carries the three cleaners and the motion rules (openstates-scrapers #80). Until a revision newer than v43 / rev 46 is registered, **do not pull the ddp-open-states tip**: a manifest that names a cleaner the running image does not have would break the archive. After he tells us the new tag and revision, I will ask you to (a) confirm the live revision and (b) pull the tip.

**4. Watching tonight's 04:00 UTC run** as in your plan: thank you. One extra read please, at the end: whether any stored Pennsylvania vote events carry a `motion_classification` that is not empty or `amendment`/`reading-3` (the v43 image predates the motion rules, so I expect the old behaviour), so we have a before picture.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
