# GO: finish the manifest rollout by pulling first, then the script (OPEN-521, OPEN-523). 2026-11-10 about 19:30 UTC (Mac side)

**Ramon's words, 2026-11-10** (pop-up answer to your `5503192`): "Pull first, then the script (Recommended)".

Your reading of `5503192` is right. `rollout-manifest.sh` validates the incoming file with the checkout's current `validate_jurisdictions.py`, which still requires `tier` and `archive.scheduled`. The new validator arrives only with the pull, so the script cannot pass a change that removes a required field. I will file a small ticket so the script validates with the incoming validator next time. I checked the new manifest on `main` (`a2ade4c`) myself: the new validator says `OK: 13`, and the drift check against ddp-sync `main` says `OK ... (13 jurisdictions)`.

## What I need from you
1. **Gates:** as in `7d66d82` (the running ddp-sync is `5919c8c0547d`, nothing in flight, clear of 02:00 to 04:00 UTC). Record the host checkout's current commit (`4f0ca727`) as the rollback point.
2. `git -C /opt/ddp-open-states pull --ff-only origin main`. Expect `a2ade4c` or later. Not a fast-forward: stop and report.
3. Right after: `.venv/bin/python validate_jurisdictions.py` (or the host's usual Python) in `/opt/ddp-open-states`. It must print `OK: 13`. If not: `git checkout --detach 4f0ca727` to restore the old manifest at once, then report.
4. `bash deploy/rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync --state al` (the real run; it now finds nothing to pull and runs validate, the drift check and api-v3's copy). Add `--recreate` only if it exits 3, as before.
5. Checks, step 5 if needed, and the reply: **exactly as `7d66d82` steps 4 to 6**:
   * api-v3 `/ddp/enrollment` shows Alabama `scraping` and no `tier`;
   * ddp-sync `GET /schedule` lists `openstates_scrape_al`, Sunday 11-15 02:00 UTC, 13 jobs in all;
   * if it is missing, restart only the ddp-sync container, outside 02:00 to 04:00 UTC.

   Reply: `reply-manifest-rollout-ec2-20261110.md`.

## Not authorized
Everything on `7d66d82`'s list, plus: any `--skip` flag; editing the validator or the manifest on the host.

**Rollback:** `git -C /opt/ddp-open-states checkout --detach 4f0ca727`, then rerun the script so api-v3's copy follows. The running ddp-sync and api-v3 accept both manifests.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
