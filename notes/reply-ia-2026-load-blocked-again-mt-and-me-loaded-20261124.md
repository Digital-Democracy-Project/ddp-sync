# Reply (2026-11-24 00:09 UTC): Iowa 2026 load was killed by my 23:52 restart (second time: orphan S3 import lock); Montana 2027 and Maine loaded

## Iowa (OPEN-673): `ia-826c2805ac5e` (`2026,2026s1`, rev 85): load NOT done, blocked by my restart
Collection ended 23:52:32 and the **old ddp-sync process was already loading** when my core `0f3c24c9` restart hit at 23:52:27. The resumed run logged at **23:52:35** `load failed ... cloud_loader.py exited 90: ERROR: another import of ia_2026 already holds the lock -- refusing to load ia/ia-826c2805ac5e (OPEN-203)`: the same orphan S3 import lock as Oklahoma's (`98fbc47e`), 24 h TTL, so a reload is refused until about **23:50 UTC on 11-24**. My in-flight list at 23:57 had Iowa as "resuming"; **it was in its load phase and I did not catch it**: I repeat the mistake I described for Oklahoma. Iowa 2026 is still the first-scrape data; 2026s1 did not start; Iowa 2025 (v84 run you asked for) has not been started either. Not retried; the lock is not mine to touch.

## Montana (OPEN-670/801): `mt-da06a3dfea66` (2027 full, rev 86, triggered 23:15:38)
Resumed through the 23:52 restart (it was still collecting), **load done 00:02:05 UTC** on the fixed loader (core 0f3c24c9). Resumed run: I triggered its archive by hand at **00:09:24** (202). **Counts (RDS):** MT 2027 **2,148 bills**, 0 vote events (pre-filing: as you expected). **Version names (your gate: was 294/294 named like a file):** **"Bill Draft" 240, "Introduced" 60, 1 named `LC0858.pdf`** (301 versions; **1 of 301 still named like a file**; before 100%). The Mac checks the stage coverage.

## Maine (OPEN-672): `me-3bf9a26aba24` (first scrape, rev 85, since 17:53)
Resumed through two restarts; **load done 00:06:32**. Archive triggered by hand at **00:09:28**. **Counts:** 132R1 **1,305** bills / 85 vote events; 132S1 **834** / 1,071; 132R2 **311** / 419. Failures: none seen.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
