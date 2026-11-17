# Addendum to 8e1dfc6: the remaining checks from 2eeea32f3 (window dropped), all pass

Written 18:50 UTC (date -u at write time). From the production EC2 host. Read only.

I had already released cb5eade at 18:41 UTC on the user's word before reading your 2eeea32f3 (13:49, "drop the Saturday window"). So the release was inside your updated rules (no calendar window; nothing in flight; ce459b4 clean; 0 tasks), and the schedule I read back matches your step-2 list exactly: MA Mon 11-23 12:00, VA and UT Tue 11-24 12:00, AZ and NC Wed 11-18 12:00, LA and NE Thu 11-19 12:00, AL and GA Fri 11-20 12:00, NV and OR Sat 11-21 12:00, FL and MI Sun 11-22 02:00, wa 02:30, usa 03:00, pa 04:00 nightly. The rollback tag is ddp-sync:pre-cb5eade (the target moved to cb5eade in cfc8e5b), image c4bb07b269d0.

Remaining step-2 checks (read at 18:49 UTC): healthy on image ebdb4eaf724c; "Redis connected url=redis://redis:6379/3" at 18:41:18; 0 Tracebacks since the restart; config/sync_schedule.yaml still stall_seconds: 2700; LEGBOT_ORGANIZATION_LOGO_ENABLED is not set (not in the container's environment, not in /opt/ddp-sync/.env, and ddp-sync's settings have no such field).

Step-3 reads I will report: Wed 11-18 AZ and NC start at 12:00 (the first weekly runs on the new days, on revision 66 if registered), Sunday 11-22 02:00 only FL and MI, Monday 11-23 MA alone at 12:00.
