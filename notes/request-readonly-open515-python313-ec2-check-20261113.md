# Request (read-only now; release HOLDS for Ramon's go): OPEN-515, the OpenStates toolchain on EC2 moves to Python 3.13, 2026-11-13 (Mac side)

**Part A is read-only and needs no go. Part B holds:** do not start it until a separate GO note from Ramon names the commits.

## What I need from you
1. **Part A (read-only, now):** the host facts below, so the release is planned against what the host really has.
2. Reply: `reply-open515-python313-ec2-check-20261113.md`.

## Why
Upstream OpenStates and both DDP forks now require Python 3.13. Three PRs move every copy of the OpenStates toolchain to 3.13. None are merged yet:
* **ddp-open-states #364:** `requirements-openstates.txt` gets three pin changes (numpy 2.0.2 to 2.2.6, six 1.12.0 to 1.17.0, `legacy-cgi` 2.6.4 added). The Fargate scrapers image moves to `python:3.13-slim-bookworm`.
* **ddp-sync #273:** the `openstates-builder` stage of `infrastructure/Dockerfile` (which builds `/opt/venv-openstates`) moves from 3.9 to 3.13. ddp-sync's own app stays on 3.11.
* **openstates-core #95:** the fork's `pyproject.toml` / `poetry.lock` follow upstream (`python = "^3.13"`). It merges **last**.

The two EC2-facing PRs are coupled. ddp-sync's build reads `requirements-openstates.txt` from `/opt/ddp-open-states` (`additional_contexts`). With **only #364** pulled on the host, the 3.9 builder cannot install numpy 2.2.6. With **only #273**, the 3.13 builder cannot compile numpy 2.0.2. Either way the next ddp-sync **build** fails. The running container is unaffected.

Built and checked on the Mac: ddp-sync's image with both changes, for `linux/amd64` (this host's `m6a.xlarge`, under emulation) and `linux/arm64`. The toolchain runs Python 3.13.18. `os-initdb` plus `os-people to-database ut` loaded 296 people into a throwaway Postgres. `import ddp_sync.app` works on 3.11.

## Part A: read-only (report each value)
1. `uname -m` on the host. I expect `x86_64`.
2. Revisions: `git -C /opt/ddp-open-states rev-parse --short HEAD` and `git -C /opt/ddp-sync rev-parse --short HEAD`, and whether either has local edits (`git status --short`, especially `requirements-openstates.txt`).
3. Inside the running ddp-sync container: `readlink -f /opt/venv-openstates/bin/python` and `/opt/venv-openstates/bin/python --version`. I expect 3.9.
4. On the **bare host**: does `/opt/ddp-open-states/.venv` exist, and what is `.venv/bin/python --version`? Which Pythons does the host offer (`ls /usr/bin/python3*`, and `python3.13 --version` if present)?
5. Does **anything** run `/opt/ddp-open-states/.venv` directly on the bare host rather than inside ddp-sync's container (cron, systemd, a timer, a script)? `run-people-refresh.sh` prefers `/opt/venv-openstates` when it exists. I want to know whether the host venv is still used at all. If it is unused, it can stay on 3.9 and need not be rebuilt.
6. The last weekly `people_refresh`: when it ran, and whether it ran `/opt/venv-openstates/bin/os-people` (log line). Counts only.

## Part B: the release (HOLDS: needs Ramon's GO note with exact commits)
Planned order, for your comments now:
1. **Gates:** nothing in flight (no import, people refresh, archive or scrape), at least an hour clear of the scheduled runs, and both checkouts free of local edits. Record the rollback point: both checkouts' revisions, plus the current ddp-sync image ID and tag.
2. Pull **both** checkouts to the GO note's commits: `/opt/ddp-open-states`, then `/opt/ddp-sync`.
3. Rebuild and recreate **only the ddp-sync container**, by the host's usual procedure.
4. **Checks, read-only:**
   * `/health` healthy;
   * `/opt/venv-openstates/bin/python --version` is 3.13;
   * `/opt/venv-openstates/bin/os-update-wrapper --help` and `/opt/venv-openstates/bin/os-people --help` both run;
   * `/schedule` unchanged.
   
   Start no run. The next scheduled import or people refresh is the real test; report it afterwards.
5. **If the build fails:** leave the running container alone (it is still the old image), check both checkouts back out to the recorded revisions, and report. **If the new container fails its checks:** check both checkouts back out to the recorded revisions and rebuild, or re-tag the recorded old image and recreate ddp-sync, then report.

## Not authorized
Part B until Ramon's GO note. Any scrape, import, people or archive run. Any env, yaml, broker, api-v3, task-definition or Fargate image change. The scrapers image is Ramon's own `deploy-scrapers-image.sh` on the Mac and is not part of this request.

## What I check on the Mac side
That your Part A answers match the release plan. If anything still uses the bare-host venv, I'll add its rebuild to Part B before asking Ramon for the go.
