# REPORT: the saved `RDS_DATABASE_URL` in this host's `/opt/ddp-sync/.env` stopped authenticating on 2026-10-21 (EC2-broker host). The live credential works. Not fixed, needs an owner.

## What I saw
* **Worked** until about 03:10 UTC on 2026-10-21: I used the saved URL read-only for the NC history query (about 03:08 UTC), and on 10-19 for the OPEN-399 Parts 1 and 2 and the NC checks.
* **Failed from about 03:35 UTC on 10-21:** `connection to server at "ddp-openstates...rds.amazonaws.com" port 5432 failed: FATAL: password authentication failed for user "openstates_admin"` (with sslmode `require` that is the only error; with the default `prefer` a second line says `no pg_hba.conf entry ... no encryption`, which is just the non-TLS fallback). Re-tested just now (about 05:40 UTC): same failure.
* **The live credential works:** reading the current secret from Secrets Manager with the ARN in the ddp-sync container's env (`RDS_CREDENTIALS_SECRET_ARN`, the same way `resolve_rds_database_url()` does) and connecting read-only succeeded at 04:53 UTC (OPEN-377 G4). Nothing was printed or written.

## What I do not know
* **Whether the password was rotated, and when.** The saved URL fails and the live secret works, which is what a rotation looks like, but I cannot prove it: this host's role (`EC2ServiceAccessReadOnlyRole`) is **not allowed `secretsmanager:DescribeSecret`**, so I cannot read the secret's last-changed or last-rotated date. If an automatic rotation exists, its schedule would explain the timing; I did not look for one.
* Whether the `.env` line is meant to be kept current by anything (render-env, a job) or is a leftover from before OPEN-103 (credentials resolved live).

## Impact, as far as I can tell
* **Not affected:** scheduled loads and anything else that resolves the credential live. Evidence: the USA scrape load at 03:05 to 03:06 UTC on 10-21 completed (`cloud_scrape: done`), and the NC load on 10-19 did too.
* **Affected:** only things that read the saved `RDS_DATABASE_URL` directly: my own read-only helper scripts on this host (I read the value from `/opt/ddp-sync/.env`, never printing it). I do not know whether any other script, runbook or cron on this host or another still uses the saved line.
* No data was touched by any of this.

## What I did and did not do
Nothing changed: I did not edit `.env`, did not rotate or write any secret, and did not try other credentials. For the OPEN-377 read I used the live secret for one query, as above. Ramon's OPEN-377 GO note listed "the saved `RDS_DATABASE_URL` problem" as a separate matter I must not touch, so I have not.

## What I need from you or Ramon
Who owns the fix, and which way: (a) refresh the saved line (and whatever keeps it current); (b) remove it and have runbooks and helper scripts resolve live (my scripts already can); or (c) grant this role `secretsmanager:DescribeSecret` on that one secret so the next change can be dated. Until then I will keep reading the live secret for any read-only database check and say so in each note.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
