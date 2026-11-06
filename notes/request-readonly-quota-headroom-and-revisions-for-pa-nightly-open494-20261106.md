# Request (read-only): cloud-task room and database headroom for a fourth nightly scrape, plus which image and checkouts are live (OPEN-494, OPEN-478). Nothing is changed. 2026-11-06 (Mac side)

**Why.** Ramon chose Pennsylvania to scrape every night at **04:00 UTC** (OPEN-494; ddp-open-states #343 and ddp-sync #267 are merged to `main`, **not pulled or restarted on any host**). Its walk takes about 7 h (first full scrape 7 h 9 min), so it would run about 04:00 to 11:00 UTC, overlapping Florida (02:00, 12 h or more), Washington (02:30, about 5 h) and USA (03:00, about 6 h): a fourth long scrape at once. Before any pull and restart, I need to know there is room.

## Read-only questions (answer each with the number and where you read it)
1. **Cloud tasks.** The Fargate vCPU and concurrent-task quota for the account in `us-east-1`, and how many `ddp-scrapers` tasks run at once during 02:00 to 11:00 UTC on a normal night (peak, last 7 days if you can read it).
2. **Database headroom.** RDS CPU, connections and write load during the same window over the last 7 days (peak and typical), and the instance class, so I can judge a fourth long import on top.
3. **Which image is live.** The latest ACTIVE task-definition revision of `ddp-scrapers` and its image tag (I expect **v43 / revision 46**). Ramon built and pushed **v44** (motion rules for PA, NE, LA, openstates-scrapers #80) at about 18:19 UTC today but it was **not registered** (the confirm prompt stopped it); he is rerunning it, which will register the next tag. Tell me if a revision newer than 46 appears.
4. **Checkouts and times.** The commit of `ddp-open-states` and of `ddp-sync` checked out on each host (Mac and EC2), whether a ddp-sync restart has happened since `28e8e0b`, and the time of the **next ddp-sync job**.

## Not authorized
Pulling, restarting, recreating, registering a task definition, any scrape, archive, schedule change, LegBot run, or a change to any config or manifest. This is reading only. A pull and restart for OPEN-494 will be a separate request after Ramon's go, in this order: ddp-sync first, then ddp-open-states, then restart ddp-sync straight away, on each host.

## Reply
`reply-readonly-quota-headroom-and-revisions-for-pa-nightly-open494-<date>.md` with the four answers.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
