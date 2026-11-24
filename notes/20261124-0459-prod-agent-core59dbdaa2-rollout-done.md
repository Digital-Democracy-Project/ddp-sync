# status: core 59dbdaa2 rollout finished on EC2 (11-24 04:57 UTC)

- api-v3 at core 59dbdaa2 since ~04:54 UTC (rollback tag pre-core59dbdaa2).
- ddp-sync rebuilt from main 60791f5 with DDP_CORE_SHA=59dbdaa2f3628821baa4cd45f1fabeb2cf936c7a and DDP_SCRAPERS_SHA=cc028db6d4ccf5d85ea51b94473b3ce015ce2108. Build ran check-scraper-imports.sh: 56 checked, 55 import OK, 0 missing a module, 1 missing a credential, 0 other. Image 7eca372d6de9; rollback tag ddp-sync:pre-core59dbdaa2 = c55415e76541.
- Checks: healthy, Redis connected, 0 Traceback, 0 legbot_replica_list_unreadable, /schedule 35 jobs, DDP_SCRAPERS_COMMIT = cc028db6..., check-core-pin-drift (image core 59dbdaa2) exit 0 on api-v3 + ddp-sync.
- Resumed after restart: ma (ma-87867254a85b), fl 2026 (fl-6264ea52502d), pa nightly (pa-44ff521cb05c). None was loading. Locks probe shows no pipeline locks until they resume. I will start archives by hand when they load.
- Next: Maryland people load, then v86 re-imports one at a time on Ramon's go. OK/IA S3 locks expire ~23:03 / ~23:50 UTC today.
