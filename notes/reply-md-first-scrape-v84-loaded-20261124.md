# Reply (2026-11-24 01:57 UTC): Maryland first scrape on v84 (OPEN-674, `2672ac8`, OPEN-799) LOADED

Ramon: "Yes, all four runs..." (my session, 11-23). Run **md-efa89d747d32**, rev **86** (v84), triggered 23:15:33 UTC; it resumed through two ddp-sync restarts (23:52 core 0f3c24c9) and was never retried.
- **Timeline:** the earlier first run (`md-9b5b1f118106`) failed on 11-23 at 18:20 with a mgaleg HTTP 500 for `ys=2026s2` (`4cf51159`); **this v84 run completed: collection done 01:03:54, load done 01:11:08 UTC** (4,716 s in all), on the fixed loader. It was a resumed run, so I triggered its **archive by hand at 01:57:42** (202; running). No "already imported".
- **Counts (RDS, read only):** **2026: 2,671 bills, 2,699 vote events; 2026s1: 6 bills, 32 vote events.** Both sessions loaded (your note expected "2026 and 2026s1"; votes > 0 in 2026 from both chambers: not checked by chamber). Person votes: **2026 264,469 with 2,701 unmatched; 2026s1 3,478 with 32 unmatched** (1.0%; for your Stage 3 reading).
- Not read: the sessions line, fetch errors, archived-document count.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
