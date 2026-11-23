# UPDATE (not a GO): the open choices in the seven first-scrape requests are now decided. **The go itself still comes from Ramon in your session or through the PM session**, as each request says. 2026-11-23 (Mac side, Onboard New States)

Ramon answered each request's open question on 2026-11-23 (the PM session's /ask-me; recorded on each state's ticket, e.g. OPEN-673 comment 22774). This note only tells you which variant of each request to run when the go comes:

| State | Request | PR | Decided | Part 3 then is |
|---|---|---|---|---|
| Montana (OPEN-670) | `a362370` | #471 | 2027 session only, **no 2025** | the plain trigger; **part 4 is dropped** |
| Iowa (OPEN-673) | `aa0bf45` | #472 | **include the 2026 special session** | `…/openstates-scrape/ia?sessions=2025,2026,2026s1` |
| Maryland (OPEN-674) | `2672ac8` | #473 | no open question | as written |
| Hawaii (OPEN-668) | `5428971` | #474 | no open question | as written |
| Maine (OPEN-672) | `bea3eb0` | #475 | no open question | as written (stop and report if a phase passes 6 h) |
| Mississippi (OPEN-669) | `b75c260` | #476 | 20261E's end stays July 17 | as written |
| Missouri (OPEN-671) | `4ce63e0` | #477 | veto sessions stay under the regular session | as written, **only on an image containing scrapers `c3ec462d`** (v82 or later; v81 / rev 83 doesn't qualify) |

* **No request changes otherwise:** every gate, the PR-merged gate and "Not authorized" list stand.
* **One rollout** (pull first) can cover all seven once their PRs are merged.
* **Weekly side effect:** after the rollout, each state is in the Sunday 02:00 UTC weekly scrape. For Missouri, a weekly run on an image without `c3ec462d` would use the old session dates. Report it if that happens.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
