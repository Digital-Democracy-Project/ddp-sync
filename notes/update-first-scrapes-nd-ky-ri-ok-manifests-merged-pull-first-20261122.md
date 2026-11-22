# UPDATE (not a GO by itself) to the first-scrape requests for ND, KY, RI and OK: their manifest PRs are merged, and the rollout must "pull first". 2026-11-22 (Mac side)

**Ramon said go for all four on v78, via the PM session (recorded on OPEN-663, 665, 666 and 667).** He brings each request to you himself, one at a time. This note changes only gate 1 and part 2 of these requests:
* `84dd514` (ND rerun, and the KY correction);
* `30ae9df` (KY);
* `d92a8ce` (RI);
* `3222a8d` (OK).

## Gate 1: the manifest PRs are merged
The KY, RI and OK manifest PRs are merged on ddp-open-states main:
* **KY:** #457, merge 2424fc3b;
* **RI:** #462, merge cd318855;
* **OK:** #463, merge 5a0eb41b.

The PM session test-merged all three onto main first: validator OK (30 valid), 164 tests pass. ND needs no manifest change; it is already at `scraping`.

## Part 2: roll out with "pull first, then the script"
ddp-open-states main also has **#460** (merge c25b56a1, OPEN-764). It adds a **new required manifest field**, `votes.senate_president_by_session`, to every entry.
* A host whose validator predates #460 will reject the new manifest (OPEN-532).
* So, as Ramon set on 2026-11-10: **pull ddp-open-states to main first, then run `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.**
* Report:
    * the commit, which must be at or after c25b56a1;
    * the combined check (30 jurisdictions agree);
    * `ky`, `ri` and `ok` at `scraping` with cloud_path, archive and people_refresh on, and LegBot, routing, search and embedding off.
* **One rollout covers all three states.** Each state still gets its own manual run, one at a time, as each request says.

## Unchanged
Everything else in each request stands:
* revision **80 (v78)** or later;
* one manual trigger, without `skip_people_check`;
* the gates and the reports;
* stop on a repeated vote key;
* nothing beyond that state's run.

**Order:** if it helps, ND first (no rollout needed), then KY, RI and OK after the one rollout. Please don't run two at the same time, and do all four before Sunday 11-29 02:00 UTC.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
