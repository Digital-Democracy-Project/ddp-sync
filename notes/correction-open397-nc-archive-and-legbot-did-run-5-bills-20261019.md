# CORRECTION to my OPEN-397 NC reply (`reply-open397-nc-rerun-done-clean-no-legbot-side-effect-20261019.md`): the post-scrape archive DID run, and LegBot ran on 5 NC bills

My reply said no archive or LegBot started and that the scrape-triggered archive hook did not fire. **That was wrong.** I checked about one to two minutes after `cloud_scrape: done` and concluded too early; I did not re-check until asked. This host's ddp-sync log shows:
- 05:32:07 `cloud_scrape: done` (NC), then the scrape-triggered archive: 05:34:09 `openstates_archive: fargate task done duration_seconds=121.3 jurisdiction=nc`.
- 05:36:22 `archiver_triggered_legbot_wireguard_result jurisdiction=NC`: **success, 5 bills considered, 5 processed**, 133 s, run_id `2ebc446e421b4adbb98e04c2b792f9f3`. These are the 5 bills of the session the load touched (`2025E1`): HJR 2, SR 1, HR 1, HB 3, SB 2. Concept statements dispatched.
- 05:36:22 `bill_search_refresh_run` (refreshed 6, with_text 6) and `scrape_triggered_archive_result success`.

**Per bill (artifacts generated / failed):** HJR 2: 5 / `bill_pros_cons`, `bill_supporting_orgs`, `bill_opposing_orgs`; SR 1: 7 / `bill_opposing_orgs`; HR 1: 7 / `bill_opposing_orgs`; HB 3: 8 / `bill_opposing_orgs`; SB 2: 7 / `bill_opposing_orgs`. `bill_opposing_orgs` failing on every bill matches the known dominant LegBot failure (WA: 46.1%). I did not read the cause for these five and did not check the broker rows beyond this log line.

**What it means for OPEN-390:** the big first NC batch (session `2025`, 2,338 bills, about 3,550 versions) was **not** started: the hook only processed the 5 touched `2025E1` bills. I started none of this; it was the normal scrape-triggered chain (the opt-in already configured on this host). The wording of my earlier note, "NC's first LegBot batch has not started", is right only for the large `2025` batch; 5 NC bills now exist in the broker with artifacts. I do not know whether Ramon wants those 5 reviewed first.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
