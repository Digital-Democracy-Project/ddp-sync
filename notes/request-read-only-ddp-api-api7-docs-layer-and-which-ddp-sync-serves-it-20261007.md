# API-7 is deployed on the ddp-api host: nothing to do for ddp-sync, plus two read-only questions (dev agent, on Ramon's instruction, 2026-10-07)

From the dev agent (Claude Code, working in the ddp-api repo). Nothing here changes ddp-sync, and **no ddp-sync deploy or restart is requested.** It explains what the ddp-api API-7 deployment does with ddp-sync's routes, and asks two read-only questions that came out of checking it.

## What was deployed (ddp-api only)

ddp-api `main` is `da82158` on the ddp-api host (PRs #18-#24, tickets API-7 to API-11), restarted and verified there by the ddp-api prod agent (`notes/ops-handoff` on the ddp-api repo). It is a change to ddp-api's **docs page only**: the real API, auth and every request forwarded to ddp-sync are unchanged. ddp-sync code, config and secrets were not touched.

## What ddp-api's `/docs` now does with ddp-sync's routes

ddp-api fetches the ddp-sync instance's own `/openapi.json` when it builds its docs (cached 5 minutes), remounts the `/sync/*` and `/trigger/*` paths, then applies these on the public page only:

- **Safe defaults.** `dry_run` defaults to `true` on `POST /sync/unified/all` and `POST /trigger/legislator-bio-sync` (both default to false in ddp-sync itself; the real default is untouched, only the pre-filled value on the docs page). `POST /sync/unified` pre-fills a dry-run body.
- **Warnings** ("this starts a real job") on routes that take no input and have no dry-run mode: `user-sync`, `full-sync`, `bill-version-check`, `bill-status-sync`, `votebot-eval` today, and `grantbot-scrape-funders` if it is ever served.
- **Try it out is switched off** for every write operation, so nobody can start a ddp-sync job from the docs page with one click. Direct calls are unaffected.
- Safe example bodies, ready for when they are served, for `/trigger/bill-artifact-generation` and `/trigger/legbot-analyze-bill-full` (both pre-filled with `dry_run: true`) and `/trigger/legbot-analyze-bill` (no dry-run mode; pre-filled with an unknown artifact type and an all-zero bill id).

## What I noticed, and why I am asking

The live ddp-api `/docs` shows only **11** ddp-sync routes: `/sync/unified`, `/sync/unified/all`, `/sync/unified/status/{task_id}`, and under `/trigger`: `bill-status-sync`, `bill-version-check`, `full-sync`, `legislator-bio-sync`, `openstates-scrape/{target}`, `user-sync`, `votebot-eval`, `webflow/{job_name}`. ddp-sync `main` (`faa9630`) has more write routes than that. These are **not** served behind ddp-api today: `bill-artifact-generation`, `legbot-analyze-bill`, `legbot-analyze-bill-full`, `grantbot-scrape-funders`, `openstates-archive/{target}`, `openstates-backfill/{jurisdiction}`, `knowledge-base-backfill/{jurisdiction}`, `knowledge-base-reconcile/{jurisdiction}`, `knowledge-base-entities/{entity}`, `vote-person-backfill`, `open304-lis-identifiers`. So either the ddp-sync that ddp-api talks to runs older code than `main`, or those routes live on a different instance (for example the Mac Studio one, since `legbot-analyze-bill` is Mac-Studio-only by construction). I cannot tell which from here, and I do not want to guess.

## Questions (read-only; nothing changes on any host)

1. **Which ddp-sync does ddp-api talk to, and what is it running?** On the ddp-api host, what is `DDP_SYNC_SERVICE_URL` (host and port only; do not print any key)? For the checkout or image serving that URL: its commit (`git rev-parse --short HEAD`, or the image tag), and the number of paths in its own `/openapi.json`. Is it intentional that it is behind `main`? If it is, say which of the routes above, if any, are served from another instance, and which instance.
2. **A live confirmation I could only do by reading source.** In the ddp-api examples, `POST /trigger/legbot-analyze-bill` is pre-filled with an unknown `artifact_type` (`EXAMPLE-DO-NOT-USE`) and an all-zero bill id, on the grounds that ddp-sync rejects it before writing anything (400 for the artifact type, then 404 for the bill; 503 first if the host has no CAMS settings). If you have an instance that serves that route, please send exactly this one request **directly to that ddp-sync instance** and report only the status code:
   ```bash
   curl -s -o /dev/null -w '%{http_code}\n' -X POST "$SYNC/ddp-sync/v1/trigger/legbot-analyze-bill" \
     -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
     -d '{"bill_openstates_id":"00000000-0000-0000-0000-000000000000","jurisdiction":"FL","session_code":"2026F","bill_source":"https://example.invalid/","artifact_type":"EXAMPLE-DO-NOT-USE"}'
   ```
   Expect **400** (or **503** on a host without CAMS). A **202 would be a real problem**: it would mean the example is not inert, and I need to know before it ever appears on the public page. Use whatever auth header ddp-sync's `api_key_auth` expects on that host; do not paste a key. If no instance serves the route, say "not served" and skip it. Do not send the `bill-artifact-generation` or `legbot-analyze-bill-full` examples; they are dry runs by design but read real data and need no live check.

## For whoever changes ddp-sync routes (no action from ops)

ddp-api's tests hold a snapshot of ddp-sync `main` at `faa9630`. When a `/trigger` or `/sync` route is added or changed (a new JSON body, a `dry_run` that defaults to false, or a route with no input and no dry-run mode), the ddp-api suite will name what the docs page needs, but only once that snapshot is regenerated; it does not follow ddp-sync by itself. A line in the ddp-sync handoff notes when such a route is deployed is enough to trigger that.

Reply on this branch (or the ddp-api one) either way. Nothing here blocks anything.
