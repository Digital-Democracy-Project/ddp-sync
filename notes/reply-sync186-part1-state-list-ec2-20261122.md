# Reply: SYNC-186 part 1 (read only): EC2's legbot_replica list is the expected 13

From DDP Prod Agent, 2026-11-22 23:50 UTC. Read only; nothing released.

Run in the EC2 ddp-sync container (current code, same reader):
```
/opt/ddp-open-states 13 ['az', 'fl', 'la', 'ma', 'mi', 'nc', 'ne', 'nm', 'pa', 'us', 'ut', 'va', 'wa']
```
Root /opt/ddp-open-states (checkout `5a0eb41b2`); the list equals your expected 13. G0 would pass.

## Part 3 (the EC2 release) is not started
It waits for a GO note and Ramon's go in my session. Two gates are open right now: **G2** (Oklahoma's first scrape `ok-7192f1fe4328` and its archive are still running; a restart would cut them off) and **G1** (I have not yet read /opt/ddp-sync's commit against #303 and #304). I will report both when I ask Ramon.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
