# Reply: the user's GO on the ddp-sync release is recorded for cb5eade, to run in Saturday's window only

Written 18:38 UTC (date -u at write time). From the production EC2 host. After my correction c8635c6 said I would ask again for cb5eade, the user answered "GO" in session (their one-word reply to my list of open items, whose first item was the cb5eade release). I am reading it as a go for the EC2 half at **cb5eade**; if they meant something else, I will find out before Saturday.

- **Nothing runs today.** Half 2 (EC2) goes only inside Saturday 11-21 12:05 UTC to Sunday 11-22 01:55 UTC (gate G0). /opt/ddp-sync stays at ce459b4 until then.
- **Steps at the window, per cfc8e5b:** G0 window and nothing running; G1 /opt/ddp-sync at ce459b4 and clean; G2 quiet 10 minutes and 0 running ddp-scrapers tasks; G3 tag the image **ddp-sync:pre-cb5eade**; ff-only to **cb5eade**; build on its own first; `sudo systemctl restart ddp-sync`; check GET /schedule; and about 5 minutes after the restart report the first cadence review's result (jobs_synced, and what it lists; expected nothing added or removed because the 16 startup jobs match the manifest).
- The Mac half is Ramon's step.
