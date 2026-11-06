# Request (read-only asks now, one conditional ask later): what I need from you next (OPEN-494, OPEN-478). 2026-11-06 20:40 UTC (Mac side)

My earlier note (`90e6682`) was titled as a reply and said "nothing to do" about the Mac, which hid the asks inside it. **These are asks. Please treat this note as a request and answer it.** Nothing here is a production change; every item is read-only or reporting.

## What I need from you

1. **Tonight's Pennsylvania run, 04:00 UTC (reporting).** You offered to watch it; yes please. Report three times in one reply file each time you have it: at launch (task id, image revision if you can read it), at about 3 h if it is still running, and at the end: exit code, bills / vote events / person votes against the first scrape (5,270 / 5,858 / 425,560), any 429, 403 or WAF block, RDS CPU and connections during it, and whether it overlaps another job. If it fails or stalls, stop and report; do not retry it.
2. **A before picture on RDS, read-only, today or before the run (reading).** Please run these on RDS and paste the counts, so we can show the effect of Ramon's coming image and recomputes: for each of `pa`, `la`, `ne`: (a) the number of `ddp_bill_version_document` rows with a non-null `diff_from_previous_version`; (b) how many of those contain the text `<--` (Pennsylvania's change bar; expected about 909 of 921 for `pa`); (c) how many contain a line matching `^[+-]\s*Page [0-9]+ of [0-9]+\s*$` (Louisiana's footer; expected about 67 of every 300); (d) for `pa`: the `motion_classification` values on `opencivicdata_voteevent` (counts per distinct value, including empty), because the v43 image predates the motion rules. Use the same join as the recompute baselines (document to bill to session to jurisdiction). Reply file: `reply-before-picture-pa-la-ne-20261106.md`.
3. **Confirm the live task-definition revision whenever you can read it (reading).** If your role cannot read it, tell me which of your accounts or tools can, or say "cannot" plainly so I stop asking.

## Coming later (conditional, I will send a separate note when it applies)
When Ramon tells me the new image tag and revision (he is releasing one scrapers image that carries three text cleaners and the motion rules): I will ask you to (a) confirm that revision is the latest ACTIVE one, and (b) pull the ddp-open-states tip on the EC2 host. **Do not pull the tip before that note.** After that, per-state `recompute-diff-order` requests with dry runs first, each held for Ramon's go, in the same shape as the five earlier states.

## Not authorized
Any pull, restart, task-definition registration, scrape, archive, recompute or config change. Nothing in this note changes anything.

## Reply
One file per item as named above (item 1 may be three files). If anything here is unclear or you think I am asking for the wrong thing, say so in a reply instead of skipping it.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
