# Reply: OPEN-351 read-only preflight on the EC2-broker host: both `os-text-extract` installs are older than `14cc6a5`; nothing was changed

From: prod agent (EC2 broker host), 2026-10-14 UTC (checks 03:52 to 03:54). The operator asked me to run these. **Read-only.** The one thing that touches state: I ran `git fetch origin` (remote-tracking refs only) in `/opt/ddp-open-states/openstates-core`, which the note allowed. No pull, no install change, no `recompute-diff-order` in any mode, no flag or trigger changed.

## 1. Which installs exist
| | path | Python | openstates | how it is installed |
|---|---|---|---|---|
| A. host | `/opt/ddp-open-states/.venv/bin/os-text-extract` (also `os-update` in the same venv) | 3.9.2 | 6.25.5 | **editable**, pointing at `/opt/ddp-open-states/openstates-core` |
| B. ddp-sync container | `/opt/venv-openstates/bin/os-text-extract` (also `os-update`, `os-update-computed`, `os-update-wrapper`, `os-people`, `os-scrape`, ...) | 3.9.25 | 6.25.5 | **editable**, pointing at `/opt/openstates-core` **inside the image** |

- A's checkout: branch `main`, **HEAD `5c79b47d50688edd0d0a1b8c66c0c1ed78215deb`** (2026-09-08, "refresh-extraction: report the reason for each refused document (OPEN-259)"), remote `origin` = `Digital-Democracy-Project/openstates-core`, no local changes shown.
- B's `/opt/openstates-core` is **not a git checkout** (no `.git`); it was baked into the ddp-sync image (files dated 2026-09-13 17:47). The container mounts only `/opt/ddp-sync/config` and `/opt/ddp-open-states`, so B does **not** read A's checkout. I can only give its version (6.25.5, same as A), not a commit.

## 2. The code check, each install's own Python
Your snippet raises `AssertionError` on the first assert for **both** installs. I split it into four checks:
| check | A (host) | B (container) |
|---|---|---|
| `p("Utah","Comparison to Sub #1")` is true | **False** | **False** |
| `p("Utah","Amended Excerpts 2/12/2026 10:02:409")` is true | **False** | **False** |
| `not p("Utah","Amended 2/12/2026 10:02:409")` | True | True |
| `k("Amended 2/9/2026 11:02:713","") < k("Amended 3/3/2026 19:03:587","")` | **False** | **False** |

So both installs predate OPEN-350 (procedural documents) and OPEN-361 (stamped-Amended ordering). Run on either, `recompute-diff-order ut` would apply the old Utah rules. (The `version_ordering.py` module itself exists in both.)

## 3. What a pull would bring (nothing pulled)
- **A:** after `git fetch origin`: `HEAD 5c79b47d`, `origin/main 14cc6a51`, **44 commits behind, 0 ahead**. `14cc6a5` is an ancestor of `origin/main` and **not** of HEAD. The 44 include: OPEN-350 (`5e9ac5ec`, merge `e85f651f`), OPEN-361 (`1fe0b798`, `34b440d5`, merge `14cc6a51`), the 2026-10-11 upstream catch-up merge (PR #50, `460aa1e9`, upstream 6.26.x release bumps and their own changes, e.g. the CSV address-listing fix and the Python 3.13 / flake8 changes), and our own earlier work the host never got: OPEN-285 (`6dc52f88`, `os-people to-database` must never purge), OPEN-263 (`eecc84a5`), OPEN-323 (`3f55f944`), OPEN-334 (`41e7644d`), OPEN-260 (`79690f7b`, `6eef62f1`). `pyproject.toml` `python = "^3.9"` on both HEAD and `origin/main` (version 6.25.5 -> 6.26.3), so I see no sign that a pull would need a newer Python than the host's 3.9.2; I did not try an install.
- **B:** cannot be pulled; it is part of the image. A newer openstates-core there means rebuilding the ddp-sync image.
- **Which loader uses which install, as far as I can tell** (from the code, not from watching a run): the `os-update --import` loads for RDS (scrapes and archives) run in **Fargate**, from the ddp-scrapers image (`v33` now), not from A or B. `run-scrape.sh` and `run-archive.sh` use install A (`$SCRIPT_DIR/.venv`), but there is no cron entry, systemd timer or running process for them on this host (the only crontab line is the map freshness check; the schedulers run in ddp-sync). `run-people-refresh.sh` prefers B's `/opt/venv-openstates/bin/os-people` inside the ddp-sync container, so **the weekly people refresh/import runs on B**. So a change to B would change that job; a change to A would only matter for a manual run of the legacy scripts. Nothing else on this host that I found runs from A.
- **A pointer you may already know:** `src/ddp_sync/pipelines/openstates_backfill.py` has an endpoint that runs `recompute-diff-order` (allowed subcommands `reextract`, `refresh-extraction`, `recompute-diff-order`; modes `dry-run` and `commit`) as a **Fargate task from the currently deployed image**. With `v33` that image has the new Utah rules, so that path does not depend on A or B. I did not call it.

## 4. Rollback point (RDS instance `ddp-openstates`, us-east-1)
- Automated backups on, **retention 7 days**, **LatestRestorableTime 2026-10-14T03:49:19Z** (read at 03:53 UTC), preferred backup window **04:57 to 05:27 UTC daily**. Postgres, `available`.
- I could **not** read the earliest restorable time or the automated snapshot list: this host's role is read-only and gets `AccessDenied` on `rds:DescribeDBInstanceAutomatedBackups` and `rds:DescribeDBSnapshots` (and on `DescribeDBClusters`). So I have no snapshot timestamp; point-in-time recovery covers roughly the last 7 days (my reading of the retention setting).
- Timing note: the backup window overlaps the Monday 05:00 UTC Utah archive, and the 04:00 UTC chain runs about 4 hours from 04:00.

## State
Nothing changed on this host. Installs A and B are both on openstates-core 6.25.5 and both fail the new-rules check. I will not update either without a go from Ramon and your instructions.
