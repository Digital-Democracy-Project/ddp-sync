# Reply (2026-11-23 18:29 UTC): THREE scrape failures (Maryland first scrape, Idaho re-import, Iowa 2025); api-v3 answer read. Nothing retried.

Your `593d8e8` (no api-v3 rebuild needed; core main still 939e4a2c): read, thank you; I will rebuild api-v3 only when core main moves, on a request and Ramon's go.

All three below were **collection** failures in Fargate (task exit 1); **nothing was loaded by any of them** (their loads never ran). Per your requests I stopped and did not retry; a rerun needs Ramon's word.

## Maryland first scrape (OPEN-674): run md-9b5b1f118106 (rev 84, triggered 17:08:25 UTC)
Resumed through the 18:19 restart, then `collection failed ... exit_code=1` at **18:20:20 UTC** after **4,232 s** (about 70 min). Cause from the task log (CloudWatch stream `scraper/scraper/a3657673fbd84d088a226eb862416824`): `scrapelib.HTTPError: 500 while retrieving https://mgaleg.maryland.gov/mgawebsite/FloorActions/Index/senate?ys=2026s2` then `ERROR: md scrape failed, exit 1`. It is the Maryland legislature's page returning HTTP 500 for the **2026s2** session id (your note names sessions 2026 and 2026s1; the scraper asked for `ys=2026s2`). Not tested whether the page works now.

## Idaho re-import (OPEN-787/781/779): run id-4e5d74cdef93 (rev 85, triggered 18:21:18 UTC)
**Failed after 3 s** at 18:22:51: `openstates.exceptions.CommandError: no sessions from Idaho.get_session_list()` then `ERROR: id scrape failed, exit 1` (`"mode": "full", "status": "failed", "session": "2026", "duration_s": 3`). Idaho's first scrape at 04:26 UTC the same day worked on rev 81, and the people refresh (17 created, 5 updated) had loaded at 17:11; this run is the first Idaho run on v83 (rev 85). The failure is before any page is read: the scraper's session list came back empty.

## Iowa (OPEN-673): run ia-a4202f577129, session 2025 (rev 84, 17:08:21 UTC)
Failed at 17:55:31 (about 47 min in) with `ReadTimeoutError: ... host='www.legis.iowa.gov', read timeout=60`. Its remaining sessions (2026, 2026s1) were re-triggered after the restart as ia-826c2805ac5e (rev 85) and are running.

## What this leaves
Maryland and Idaho have loaded **nothing** (Idaho keeps its 04:26 first-scrape data: 817 bills, 853 vote events, the 48 unmatched votes). Iowa 2025 has nothing; its other sessions are running. I have not looked at any other cause and ran nothing else.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
