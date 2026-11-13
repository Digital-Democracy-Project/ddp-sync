# GO: OPEN-515 Part B, release the Python 3.13 toolchain on EC2 (ddp-open-states `90e0db4` + ddp-sync `a04b40b`, together), 2026-11-13 (Mac side)

**Ramon's go, in this session: "write the GO note for the prod agent".** This clears **Part B** of `f8bdac2` (`request-readonly-open515-python313-ec2-check-20261113.md`), as planned there, with the exact commits below. Thank you for Part A (`283ccc5`); your answers are folded in.

## The exact commits (nothing newer)
| Checkout | From (your Part A) | To | What it brings |
|---|---|---|---|
| `/opt/ddp-open-states` | `6c0372b1` | **`90e0db44d5c9bc261de7f7b0f309bf95d14ab9ef`** | ddp-open-states #364 only (merged 2026-11-13 16:11 UTC): `requirements-openstates.txt` (numpy 2.2.6, six 1.17.0, `legacy-cgi` 2.6.4), the scrapers `Dockerfile`, docs, `activate.sh`/`run-people-refresh.sh` comments. **`jurisdictions.yaml` is not touched: no manifest rollout step.** |
| `/opt/ddp-sync` | `f1059b9` | **`a04b40b50d155a47c117acfab801993289608b07`** | ddp-sync #273 only (merged 16:11 UTC): `infrastructure/Dockerfile` alone, the `openstates-builder` stage 3.9 to 3.13. The app stays on 3.11. |

Move each checkout with `git fetch origin && git merge --ff-only <sha>` and confirm `git rev-parse HEAD` equals the sha. If `origin/main` has moved past either sha, **do not take the newer commits**; use the sha. If a fast-forward is refused, stop and tell me.

**openstates-core #95 is NOT merged and is not part of this release.** It merges after your checkpoint. Until then core `main` still allows Python 3.9, so the 3.13 build installs it as before.

## When
Today or tomorrow, in a quiet window:
* at least an hour clear of WA 02:30, USA 03:00 and PA 04:00 UTC (Pennsylvania's pipeline runs until about 11:45 UTC);
* **done before Saturday 2026-11-14 23:00 UTC.** Sunday 11-15 brings AL/LA/MA/NE at 02:00 UTC and the people refresh at 10:00 UTC, and the soak should not start on an untested switch.

Your choice of window within that. **Say in your checkpoint which nightly was the first real import on 3.13.**

## Steps (Part B of `f8bdac2`, unchanged except where marked)
1. **Gates:**
   * nothing in flight (no import, people refresh, archive, scrape or LegBot line in 20 min);
   * the windows above;
   * both checkouts at the From revisions with no local edits.

   Record the rollback point: both revisions, the current ddp-sync image id, and a tag on it, **`ddp-sync:pre-open515`**.
2. Fast-forward `/opt/ddp-open-states` to `90e0db44`, then `/opt/ddp-sync` to `a04b40b5`, **back to back** (either one alone makes the next ddp-sync build fail).
3. Rebuild and recreate **only the ddp-sync container**, by the host's usual procedure (as in today's SYNC-157 Part B). Nothing else is recreated: not api-v3, not Redis, not the broker.
4. **Checks, read-only:**
   * `/health` healthy and the scheduler running;
   * in the container: `/opt/venv-openstates/bin/python --version` is **3.13.x**, and `readlink -f` on it ends in `python3.13`;
   * the app's own `python3 --version` is still **3.11.x**;
   * `/opt/venv-openstates/bin/os-update-wrapper --help` and `/opt/venv-openstates/bin/os-people --help` both run;
   * `/opt/venv-openstates/bin/python -c "import openstates, django, numpy, cgi, fitz; from six.moves import urllib"` prints nothing and exits 0 (`fitz` is PyMuPDF);
   * `/schedule` lists the same 13 `openstates_scrape_*` jobs and times as after SYNC-157;
   * no Traceback or ERROR in the first 10 minutes of the log.

   **Start no run.**
5. **If the build fails:** leave the running container alone, `git checkout` both checkouts back to the From revisions, and report the error. **If the new container fails a check:** re-tag `ddp-sync:pre-open515` as the running tag and recreate ddp-sync, then return both checkouts to the From revisions and report.

## After (owed in your checkpoint or a follow-up)
* **The first real nightly import on 3.13:** its state, `status=completed failed=0`, and any Traceback naming `/opt/venv-openstates`. That is the real test; `--help` is not.
* **Sunday 11-15 10:00 UTC people refresh:** 0 ERROR lines, and created/updated counts per state. Fold it into your Sunday cycle report (`3b83a4f`).
* **If a nightly import fails on the new toolchain** with a Python or import error: roll back as in step 5 before the next scheduled job, then report. It's a version problem, not a data one, so the data can be left as it is.

## Not in this GO
* **The bare-host venv `/opt/ddp-open-states/.venv` (3.9.2), which you use by hand.** Leave it exactly as it is: no rebuild, no `pip install`.
  * Your flag in `283ccc5` is right: once core `main` requires 3.13 and later gains 3.10+ code, your manual runs from that venv could fail.
  * Where those runs move (the rebuilt `/opt/venv-openstates` in the ddp-sync container, the scrapers image, or a host 3.13 venv) is an open question for Ramon. Until it is answered, keep running them as you do now. #95 itself changes no code, only the dependency files.
* openstates-core #95, the Fargate scrapers image (Ramon's `deploy-scrapers-image.sh` on the Mac), the Mac's production venv, any env, yaml, manifest, broker, api-v3, Fargate or task-definition change, and SYNC-157 Part E.
* Any scrape, import, people or archive run started by hand.

## Reply
`checkpoint-release-open515-python313-ec2-<date>.md`. Include:
* the gates;
* the rollback point (revisions, image id, tag);
* both new HEADs;
* the new image id;
* each step-4 check with its value;
* and later, the first nightly and Sunday's people refresh.

Reviewed on the Mac before this GO (OPEN-515 comment of 2026-11-13, `/review-ticket`):
* the image built from exactly these two changes (arm64) runs the toolchain on 3.13.18 with the app on 3.11;
* openstates, Django 3.2.14 and numpy 2.2.6 import;
* `os-update` and `os-people` start;
* no token is left in the image.

The author reports the x86_64 build and a 296-person `os-people` load.
