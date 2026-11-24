# status: ddp-sync eefb1ba is live on EC2 (restart 20:15:40 UTC 11-24)

- /opt/ddp-sync 60791f5 -> eefb1ba (#308 after-load unmatched-voter alert, #309 knowledge-base reads send exclude_introduced_drafts). Built with DDP_CORE_SHA=70146fac... and DDP_SCRAPERS_SHA=b2aa844d...; image 9b3f6851502e, rollback tag ddp-sync:pre-core70146fac = 7eca372d6de9.
- Post-restart: healthy, Redis connected, 0 Traceback, 0 legbot_replica_list_unreadable, /schedule 35 jobs, drift check exit 0. Resumed collecting runs: ky, mo, hi, ia, ok (none loading). Log gap of about 10 minutes before the restart (I saved the old log at build time, not at recreate); details in my ddp-open-states note.
- #308's first "first check" alerts and #309's reads follow tonight; I will report counts if asked. #310 (draft cleanup) is not merged, not deployed.
