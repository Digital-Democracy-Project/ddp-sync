# SYNC-78 targeted validation results: sponsor-name fix confirmed, but a deeper `/bills` schema mismatch means `total_bills` is still 0 for everyone

**Re:** `sync78-request-targeted-validation-20260927.md` (this branch). Ran the read-only fallback
(`fetch_sponsored_bills`/`_get_sponsor_name` directly, no `trigger_legislator_bills_sync`, no
writes) against Gallagher and Graham, then widened to known-prolific sponsors once the result
looked suspicious. Mixed outcome -- SYNC-78 itself is confirmed correct, but it uncovered a
separate, broader bug.

## SYNC-78's actual fix: confirmed working

`_get_sponsor_name()` now resolves both names cleanly via the RDS replica:

```
Gallagher (ocd-person/2819c958-3cbe-4349-a2e9-6997054b8ea2): sponsor_name resolved: 'Gallagher'
Graham    (ocd-person/4c0ef839-19db-4ca7-844c-c863ff4963d2): sponsor_name resolved: 'Graham'
```

Neither hit the old `"Could not determine sponsor name"` warning. The routing fix
(`_get_sponsor_name(person_id, jurisdiction=...)` passing `jurisdiction` through to
`_get_api_base_and_key`) does exactly what SYNC-78 intended.

## But `total_bills` is still 0 -- for everyone, not just recently-seated members

Both Gallagher and Graham still came back with 0 bills from `fetch_sponsored_bills()`. Before
concluding that's just "too new to have sponsored anything," I ran the same read-only check
against five long-serving, known-prolific federal sponsors, looked up live via the replica's own
`/people?name=` search:

```
Grassley (ocd-person/259d896b-...):   0 bills
Pelosi   (ocd-person/8056c810-...):   0 bills
DeLauro  (ocd-person/30e732cb-...):   0 bills
McConnell(ocd-person/200bd7b3-...):   0 bills
Blumenthal(ocd-person/62fd9e8b-...):  0 bills
```

All five are current, long-serving members who've sponsored large numbers of bills. Zero across
the board rules out "not enough time in office" and points at the sponsor-fetch mechanism itself.

## Root cause: the replica's `/bills` schema doesn't match what this code expects

Traced it directly:

- `GET /bills?jurisdiction=us` (no sponsor filter): **38,564 total_items** -- the replica has
  plenty of federal bill data, this isn't an empty-dataset problem.
- `GET /bills?jurisdiction=us&sponsor=Grassley`: **0 total_items**, from the API's own
  pagination, before any of our code's local filtering runs.
- The raw bill records the replica returns carry sponsor info as
  `extras.sponsor_bioguides: ["M001231"]` (bioguide ID strings) -- there is no `sponsorships[]`
  array with nested `person.id` objects, which is what the public OpenStates v3 API returns and
  what `fetch_sponsored_bills()`'s post-filter (`legislator_sync.py:429-437`,
  `person.get("id") == person_id`) is written against.
- Confirmed the replica silently ignores unrecognized query params rather than erroring: passing
  a made-up `sponsor_id=<ocd-person-id>` param returned the identical unfiltered 38,564-item set,
  same bills in the same order as no filter at all.

So the `sponsor=<family_name>` filter this code sends isn't respected by the replica's `/bills`
endpoint the way it is by the public API, and even if it were, the downstream person-id match
would never succeed against this schema. The result is a **silent** empty list -- HTTP 200,
well-formed pagination, just zero matches -- indistinguishable from "this legislator sponsored
nothing," which is exactly why Gallagher/Graham's original zero looked like a targeted problem
instead of a systemic one.

## Net assessment

- SYNC-78 is correct and should stay -- it fixed a real, narrower bug (sponsor-name lookup
  routing) and doesn't need to be reverted or reworked.
- It just isn't sufficient on its own: `fetch_sponsored_bills()` returns 0 for any
  RDS-routed federal legislator, prolific or not. This is bigger than SYNC-76/77/78 and predates
  all three -- it's plausible bill-level sponsor attribution via this method has been silently
  broken since RDS routing for bills was first introduced, not just for jurisdiction `us`.
- I have not touched `fetch_legislator_votes` in this pass -- SYNC-76/77's validation reported
  `total_votes=3` working live, so votes may use a different endpoint/schema that isn't affected.
  Worth confirming rather than assuming, given how wrong the "replica has full data so lookups
  must work" assumption turned out to be here.
- Haven't opened a ticket for this yet -- wanted this written up first. Suggest something like
  "SYNC-79: `/bills` sponsor-filter schema mismatch against RDS replica makes
  `fetch_sponsored_bills()` silently return zero for all RDS-routed jurisdictions," scoped to
  figuring out the replica's actual supported bill-sponsor query (bioguide-based?) and either
  switching `fetch_sponsored_bills()` to match it or filtering client-side on
  `extras.sponsor_bioguides` instead of `sponsorships[].person.id`.

No code changes made, no writes to Webflow/Pinecone/CMS, nothing triggered beyond direct
read-only GETs against the replica's public `/people` and `/bills` endpoints. Reply here or open
SYNC-79 directly, whichever's easier on your end.
