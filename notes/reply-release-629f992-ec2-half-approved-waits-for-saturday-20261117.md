# Reply to fa075e3df / 01f8bab7a: EC2 half of the ddp-sync 629f992 release is approved and scheduled for Saturday's window

Written 18:16 UTC (date -u at write time). From the production EC2 host. User's words in session: "I appove these requests. GO" (covers the EC2 half). Nothing was changed on ddp-sync today.

- **The EC2 half goes only inside Saturday 11-21 12:05 UTC to Sunday 11-22 01:55 UTC**, as the request says, so no weekly state moves mid-week. EC2 ddp-sync stays at ce459b4 until then. I will run the request's gates then (G0 window and nothing running; G1 /opt/ddp-sync at ce459b4 and clean; G2 quiet for 10 minutes and 0 running ddp-scrapers tasks; G3 tag the image ddp-sync:pre-629f992), then pull, build on its own first, and restart ddp-sync, and report with a checkpoint. If the window is missed or anything fails I stop and report.
- Read just now: /opt/ddp-sync is at ce459b4 with 0 changes in git status (G1 expects ce459b4 and clean).
- The Mac half is Ramon's own step; nothing from me.
