# Request (read-only): why did the VA and MI scrapes fail on production ddp-sync with "cloud_path.fargate is missing required config"? 2026-11-03

**Ramon says this alert came from production ddp-sync (the EC2 broker host).** CodeBot relayed it in Slack at **3:25 PM Eastern (about 20:25 UTC)**: two alerts, one for `va` and one for `mi`, each *"Failure in ddp-sync (ScrapeTimeoutOrSubprocessError) ... scrape failed for <st>: cloud_path.fargate is missing required config: cluster, task_definition, subnets, security_groups (after 0s)"*. **Read-only: change, restart and trigger nothing. No keys or secret values in the reply.**

## What I know (from the code, not from the host)
* The error is raised by `_fargate_config()` in `pipelines/cloud_scrape_trigger.py` when the job's config has no `cloud_path.fargate` block or any of its four values is empty. "after 0s" means it refused **before launching anything**: no task, no scrape, no data touched.
* In the repo's `config/sync_schedule.yaml` those four values exist under `openstates_scrape.cloud_path.fargate` (cluster `ddp-scrapers`, task definition `ddp-scrapers`, subnets, security group). Pennsylvania, Nebraska and Louisiana launched from your host on 11-02 22:25 UTC with the same family, so the block was there then.
* **Timing:** ddp-sync PRs #263 and #264 merged at 20:31 UTC, **after** the alerts, so today's merges cannot be the cause unless a different revision was running. Your host's ddp-sync was last restarted 11-02 22:23 UTC (reply `1a3ca36`, image `d4a9b4edf9a4`, `b73983d`).
* **Not known:** which job started the VA and MI scrapes on a Tuesday (the weekly secondary batch is Sunday 02:00 UTC), and what config that job was holding.

## What I need (each with its value)
1. **The log lines around the failure:** from ddp-sync's container log, 20:15 to 20:45 UTC today, every line mentioning `va`, `mi`, `cloud_scrape`, `cloud_path`, `fargate`, `scrape failed`, `trigger` or a job name; and the **name of the scheduled job or the trigger that started the VA and MI scrapes** (say "manual trigger" if it was one and from where). Paste up to 30 lines (redacted).
2. **What was running:** the image id and `git rev-parse HEAD` of `/opt/ddp-sync`, the container's start time, and whether `/opt/ddp-sync` or `/opt/ddp-open-states` was pulled or ddp-sync restarted since 11-02 22:23 UTC (and when). Say whether **my request `7a94599` (pull ddp-open-states `0551550`) was carried out**, and at what time.
3. **The loaded config, key names and non-secret values only:** from the running app, `openstates_scrape.cloud_path.fargate`: for each of `cluster`, `task_definition`, `subnets`, `security_groups`, say **present or empty** (and the cluster and task definition names). Also `config_source` (your note of 10-26: Secrets Manager) and whether the schedule file the app loaded is the repo's `config/sync_schedule.yaml` or something else (path and modification time).
4. **Did anything else fail the same way** since 11-02 22:23 UTC: any other `scrape failed ... missing required config` or other `config_error` line (state and time), and whether any VA or MI scrape started at all in that window (the in-flight store should show none).
5. **Sunday check, read-only:** `openstates_scrape.cloud_path.fargate` as in item 3 is what Sunday's 02:00 UTC batch will use. If any of the four is empty now, say so first in your reply.

## Stop and tell me
If anything is empty or odd in item 3, or the log shows a scrape actually launched, stop and report before touching anything. Do not restart ddp-sync, edit config or trigger a scrape.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
