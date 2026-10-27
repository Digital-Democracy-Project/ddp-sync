# GO (SYNC-131): bring the EC2-broker host's ddp-sync to `main` `af6b098`, then confirm the CodeBot icon (Mac side, 2026-10-27)

**STATUS: GO.** This answers `reply-sync131-codebot-icon-gates-fail-on-this-host-nothing-changed-20261027.md`. Ramon chose option (a) (Mac session, 2026-10-27): deploy main on this host, then the icon. It **replaces the steps** of `request-codebot-icon-on-ec2-sync131-20261027.md`, whose gate 1 no longer applies. Do the parts in order, and do each one only if every gate passes. If a gate fails, stop and report; don't improvise. A later HOLD note from Ramon overrides this one.

**Scope: this host only.** **Do NOT touch the votebot/ddp-api instance.**

## 1. What the pull changes (`74d72fe..af6b098`, checked on the Mac)
Four merges, 15 files. There are no dependency, Dockerfile or compose changes, and no default is turned on.
- **PR #243 SYNC-131:** `render-env.sh` copies the optional `codebot_slack_icon_emoji` / `codebot_slack_username` secret keys. If a key is absent it is left out, so the code default applies.
- **PR #242 SYNC-122:** the Voatz-to-Brevo jobs skip people who opted out. This host sets `VOATZ_SYNC_ENABLED=false`, so it changes nothing here.
- **PR #241 OPEN-420:** the three copies of the state-name table become one. The only label that changes is `nc`, from "NC Legislature" to "North Carolina Legislature", in newly embedded documents.
- **PR #240 OPEN-418:** the config-drift check reads the manifest's `archive.scheduled` instead of `archive.enabled`. In the default `advisory` manifest mode this changes only warnings. See gate G4.

## 2. Gates (read-only)
- **G1. Starting state.** In `/opt/ddp-sync`: branch, `git rev-parse --short HEAD` (expect `74d72fe`) and `git status --short`. If there are uncommitted changes, another branch or another commit, **stop and report**. Do not stash, discard or reset.
- **G2. Quiet window.** Nothing is in flight: no scrape, archive, RDS load, LegBot relay or backfill. Stay at least an hour clear of **Sunday 02:00 UTC**, **Monday 05:00 UTC** and **Thursday 05:00 UTC**. Say which window you used.
- **G3. Rollback point.** Record the commit (`74d72fe`) and the running image and container you are replacing.
- **G4. Manifest mode.** Read the value but don't print anything secret: is `JURISDICTION_MANIFEST_MODE` set in this host's env? If it is `authoritative`, **stop and report**, because OPEN-418 would then change which states get archived, not just the warnings. If it is unset or `advisory`, continue.
- **G5. Secret key (names only).** Does `ddp-sync/credentials` list `codebot_slack_icon_emoji`? Use the names-only command from the earlier request. Either answer is fine; just report it (see Part 2).

## 3. Part 1: the deploy
1. `git -C /opt/ddp-sync pull --ff-only origin main` (expect `af6b098`). If it is not a fast-forward, **stop and report**.
2. `sudo systemctl restart ddp-sync`. Its `ExecStartPre` re-renders `.env`, then `up -d --build` recreates only the ddp-sync container. Do not touch any other container, and don't change any env setting by hand.
3. Verify:
   - `systemctl is-active ddp-sync` prints `active`.
   - `curl -s localhost:8001/health` is healthy, with Redis connected and the scheduler running.
   - The job count is the same before and after (none of these PRs adds or removes a job).
   - There are no startup errors.
   - `git rev-parse --short HEAD` prints `af6b098`.
   - `grep -c codebot_slack_icon_emoji infrastructure/render-env.sh` prints `1`.
   - List any new `jurisdiction_manifest_disagrees` warnings at startup. Expected and fine; just list them.

## 4. Part 2: the icon
The old `render-env.sh` ignores keys it doesn't know, so Ramon (or another person with write access) may add the key at any time, before or after Part 1. You can't write it, because your role is read-only.
- **If G5 found the key:** the Part 1 restart already picked it up. Go to step 2.
- **If not:** report after Part 1 and wait. When a note from Ramon says the key is added, re-check the names (names only), run `sudo systemctl restart ddp-sync` in a quiet window (G2), and repeat the Part 1 health checks.

Steps:
1. (Done by a person, not by you.) Add `codebot_slack_icon_emoji` = `:codebot:` to `ddp-sync/credentials`.
2. `docker exec ddp-sync-ddp-sync-1 printenv CODEBOT_SLACK_ICON_EMOJI` prints `:codebot:`.
3. Send one labelled test alert. This is test step 4 of the earlier request, unchanged (`post_alert('TEST (SYNC-131): EC2 ddp-sync alert identity check, please ignore', source='sync131.test')`). It should print `True` and show CodeBot with the :codebot: picture in #automation-errors.

## 5. Not authorized
- Any other host.
- Any env, yaml or secret change.
- Enabling any flag.
- Starting any scrape, archive, LegBot or backfill run.
- A second test alert.
- A rollback without authorization, unless the container fails to come up healthy: then restore `74d72fe` (`git checkout 74d72fe` plus a restart) and report.

## 6. Reply on this branch
One note covering:
- the results of gates G1 to G5 and the window you used;
- the commit and container before and after, and the time of the restart;
- the job counts and the startup warnings;
- then the Part 2 results, or "waiting for the key".

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
