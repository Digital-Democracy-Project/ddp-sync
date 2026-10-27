# REPLY (OPEN-437): can the EC2 ddp-sync's scrape launches be paused without deploying code? **Yes by a config edit plus a restart (about 2 minutes); there is no runtime switch, and no per-state task-definition setting** (read-only, 2026-10-27 15:00 to 15:20 UTC)

The operator said "Option A, run both now". **Nothing was changed, restarted or triggered.** What I ran: `git`/`grep`/`sed` on `/opt/ddp-sync` (HEAD `af6b098`, clean), `python3` reading `config/sync_schedule.yaml`, `printenv` of four booleans in the container, `GET /ddp-sync/v1/schedule`, and `aws ecs list-tasks`. Anything I could not read is said below.

## 1. Pause switch
* **There is no runtime pause.** The only schedule route is `GET /ddp-sync/v1/schedule` (read-only); jobs live in an in-memory APScheduler that is **built from the config at start-up**, so any pause needs a restart.
* **Scrape launches, all states:** the scheduler registers scrape jobs only if **both** `settings.openstates_scrape_enabled` **and** the yaml block's `enabled` are true (`scheduler.py:579`). The two places: (a) **`OPENSTATES_SCRAPE_ENABLED`** in `infrastructure/docker-compose.prod.yml`, `environment:` block (line 75; the container prints `true`); (b) **`openstates_scrape.enabled`** in `config/sync_schedule.yaml` (line 218; `true`). Both are tracked in git. **A value in `.env` cannot override (a)** (compose `environment:` beats `env_file:`). Neither is in the `ddp-sync/credentials` secret (37 keys, none of these).
* **Archive launches, separately:** **`OPENSTATES_ARCHIVE_ENABLED`** (compose line 83; `true`) and `openstates_archive.enabled` in the yaml (`scheduler.py:1212`). Pausing scrapes does not pause archives and the other way round. (Related flags: `OPENSTATES_PEOPLE_PULL_ENABLED=true`, `OPENSTATES_PATCH_REFRESH_ENABLED=false`.)
* **Restart:** `sudo systemctl restart ddp-sync` (re-renders `.env`, cached build, recreate). **Measured on this host: 1 min 59 s on 10-27 (13:44:23 to 13:46:22) and 2 min 9 s on 10-22.** Editing a tracked file leaves the host's git tree dirty until it is reverted, and a later `git pull --ff-only` can conflict if the same lines change.
* **Manual launches are not blocked by the flags:** the trigger route goes `run_single_scrape_job` to `_run_scrape` with no check of `OPENSTATES_SCRAPE_ENABLED` (that is how I started NC and FL by hand on 10-19 and 10-26). So a "paused" ddp-sync can still launch by hand, which is useful for a canary.

## 2. Per-jurisdiction hold
* **The authoritative copy for scheduling is `/opt/ddp-sync/config/sync_schedule.yaml`, not the manifest.** `JURISDICTION_MANIFEST_MODE` is unset (shadow): the manifest in `/opt/ddp-open-states` (10 commits behind main) only produces start-up log lines (`jurisdiction_manifest_agrees/disagrees/unusable`, `scheduler.py:94,142`); it decides nothing.
* **To hold one state:** take it out of the schedule list and restart: `openstates_scrape.secondary.jurisdictions` (now `va, mi, ma, ut, az, nc`), or `openstates_scrape.primary.<fl|wa|usa>.enabled: false`; and for archives out of `openstates_archive.jurisdictions` (now `fl, ut, az, wa, va, mi, ma, al, us, nc`; the weekly job and the scrape-triggered archive hook use it).
* **Do NOT remove a state from `openstates_scrape.cloud_path.jurisdictions` alone.** `_run_scrape` (`openstates_scrape.py:1123`) sends any state that is **not** in that list down the legacy on-host path (`run-scrape.sh`), which is the old Mac-owned path, not Fargate.
* Removing a state from `secondary.jurisdictions` also makes the manual route **404** for it (the route builds its accepted names from that list, `triggers.py:1135` to `1146`), so a held state cannot be launched by hand either until it is put back.
* The daily 00:30 UTC "scrape cadence review" job (dynamic cadence) adds and removes per-state jobs at run time (`scheduler.py` ~747, 804, 977). **I did not read its full logic**, so after any hold, check its log lines at the next 00:30.

## 3. Manual launch (the canary)
`POST /ddp-sync/v1/trigger/openstates-scrape/<target>` with this host's key. Targets: `fl` (all four FL sessions), `wa`, `usa`, `secondary` (all six), the single states in `secondary.jurisdictions` (**`va`, `mi`, `ma`, `ut`, `az`, `nc`**), plus `patches`, `people`, `people-pull`, `fork-check`, `drift-check`. It answers `{"status":"started","target":"nc"}` (HTTP 200 in practice) or 409 if one is running. **It resolves the task definition exactly as a scheduled launch does:** every path ends in `run_cloud_scrape` and `ecs.run_task(taskDefinition=fargate_cfg["task_definition"])`, and that value is the **single yaml string `'ddp-scrapers'`, the bare family name with no revision**, so ECS runs the **latest ACTIVE revision** for scheduled and manual launches alike. **There is no per-state task-definition setting** (one `cloud_path.fargate` block for all states), so a canary on a new revision for one state **is not possible through ddp-sync config**. The two revision-level brakes are: deregister the new revision (the family then resolves to the previous active one), or pin the whole fleet by setting `task_definition: ddp-scrapers:<revision>` in the yaml and restarting (about 2 minutes). **I cannot read which revision is latest:** this host's role is denied `ecs:ListTaskDefinitions` and `ecs:DescribeTaskDefinition`; the last revision I saw in use was `ddp-scrapers:39` (the 10-19 NC task).

## 4. Next 48 hours (UTC, from the live schedule; now Tuesday 15:03)
| when | job |
|---|---|
| Wed 10-28 00:30 | scrape cadence review (daily) |
| Wed 10-28 02:30 | WA scrape (daily) |
| Wed 10-28 03:00 | USA scrape (lower + upper) and people pull (daily) |
| Wed 10-28 05:00 | bill-document archives: VA, WA |
| Thu 10-29 00:30 / 02:30 / 03:00 | the same daily four |
| Thu 10-29 05:00 | archives: MI, NC |
Each successful scrape also starts its own scrape-triggered archive about 2 to 15 minutes later (the USA one about 12 minutes). The next weekly scrape batch is **Sunday 11-01 02:00** (FL, and the `secondary` six). **No Fargate task is running now** (`aws ecs list-tasks --cluster ddp-scrapers --desired-status RUNNING`: 0) and ddp-sync's in-flight record count is 0. A quiet window with no scrape or archive nearby is roughly **Tuesday and Wednesday 06:00 to 23:00 UTC** (the Wednesday 05:00 archives are the last before it).

## 5. Resume, and missed jobs
Undo = revert the edit (set the flag back to `true`, or put the state back in its list) and restart again (about 2 minutes). **Missed jobs are skipped, not run late:** the job store is in memory and rebuilt from the config at start-up, so a job whose time passes while paused, or during the 2-minute restart, waits for its next cron occurrence (daily jobs next day, weekly jobs next week). (`misfire_grace_time=3600` and `coalesce=True` only help when the process is running but late.) An **in-flight** Fargate run survives a restart: it is recorded in Redis and finished by `reconcile_inflight_fargate_jobs()` at start-up. For a gap, use the manual route.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
