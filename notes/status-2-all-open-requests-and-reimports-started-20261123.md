# STATUS 2 from the prod agent (2026-11-23 14:17 UTC): every open dev request, where it stands, and the re-imports I just started

**Started now (Ramon's overnight pre-approval, his own words in my session; notes re-fetched at 14:16 UTC, nothing withdrawn):** one full re-import each, triggered 14:16:47 to 14:16:51 UTC, all **202 with `"full": true`**, all on **ddp-scrapers:83** (v81, read from ECS describe-tasks; Massachusetts' scheduled run also shows 83):
| State | Request | Trigger | Run id |
|---|---|---|---|
| wy | `795a93b` (three fixes) | `openstates-scrape/wy?full=true&sessions=2026` | wy-37630822bc40 |
| ut | `d7ce0d8` (OPEN-775 coordination links) | `openstates-scrape/ut?full=true&sessions=2026` | ut-1777beb302dc |
| az | `753a793` (OPEN-649 Arizona links) | `openstates-scrape/az?full=true&sessions=57th-2nd-regular` | az-183293f2bebd |
Gates: revision 83 is at or above your 83 (WY, UT) and 82 (AZ); no wy, ut or az task or lock; flhouse.gov 200; Monday. **Gate-2 note (UT, AZ):** the live image v81 still carries core 42b48982 while the loader is at 939e4a2c, so a drift check against the image reads differently; the purpose (loader at 7f531958 or later, to keep link labels) holds, and `check-core-pin-drift.py --image-core 939e4a2c...` exits 0 for api-v3 and ddp-sync. I will report each result with your SQL.

## Runs resumed by the 07:39 rebuild (see `38e02fcd`)
pa: loaded 11:08 (pa-8f2bbe25fbb9, 12,561 s), manual archive 11:09 done in 121 s; its search/embedding/smoke/QA waits for tomorrow's 04:00 nightly. mi (mi-b8b23c2b5df9, 9 h 50 min, saving HB 6259 at 13:49) and fl 2024 (fl-adde183a9a34, 13 h 30 min): still collecting; on their load I trigger Michigan's archive and Florida's 2025 and 2026 (pre-approved). A resumed run holds no pipeline lock (observed). MA's scheduled run `ma-87867254a85b` (12:00) runs normally.

## Requests I have read and NOT started (each waits for Ramon's go in my session, and for its PR or image)
- **First scrapes:** MT `a362370` (PR #471; his question on 2025), IA `aa0bf45` (#472; his question on 2026s1), MD `2672ac8` (#473), HI `5428971` (#474), ME `bea3eb0` (#475), MS `b75c260` (#476), MO `4ce63e0` (#477; also needs an image with scrapers c3ec462d, i.e. v82 or later). One rollout would cover all of them.
- **Idaho acting senators** `7788700` / `9b4d8b9`: step 1 people refresh (expect created 7, updated 4), step 2 image v82, step 3 re-import of ID 2026. **RI fixes** `98e5dcb` steps 2-5 (step 1 core: done by the rebuild). **OK fixes** `8108de3` steps 2-5. **KY recompute** (`22d7100` step 4) and the OK recompute: dry runs wait for Ramon's go. The image v82 is not built yet (I see rev 83 as the newest).
- **NE and LA before go-live** `df508b7`: pre-approved overnight; parts A, B, C start after Wyoming, Utah and Arizona settle (one heavy load at a time per your note, so after their loads).
- **OPEN-611:** MA 8 and MI 9 done; WA moot; NC held until RDS NC 2025 end_date is 2026-12-18; US backfill commit after the NE/LA loads (not during a rebuild; none is planned).
- **Broker `1403f64e`** (broker `cc2e65d5`): needs Ramon's go and D1 to D5; interim in broker `6a1d4625`. **Florida reconcile:** Aristide HJR1F still missing from his legislator scorecard (`46c202fc`); not repaired.
- **Held for MLX:** AGENTS-225 and Virginia LegBot.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
