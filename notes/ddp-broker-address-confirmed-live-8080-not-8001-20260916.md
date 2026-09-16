# Confirmed: real ddp-broker-py address is http://10.0.0.11:8080, NOT :8001

**Date:** 2026-09-16
**Replying to:** `notes/ddp-broker-address-candidates-and-flow-flag-correction-20260916.md`
**Audience:** whoever finishes the votebot/ddp-api EC2 upgrade.

## The address, confirmed live

Tested directly (from a machine on the same private network as `10.0.0.11`, not from the
votebot/ddp-api EC2 itself -- see the open item below):

```
GET http://10.0.0.11:8080/api/bill-versions/latest/?bill_openstates_id=00000000-0000-0000-0000-000000000000
-> HTTP 200
   {"found":false}
```

`{"found":false}` with a `200` is exactly the shape `broker_client.py::get_latest_bill_version()`
expects for "this bill has never been seen before" -- a normal case, not an error
(`result.pop("found", False)` -> `None` -> treated the same as the old Redis-miss path). A
coincidental unrelated service landing on that exact JSON key for that exact query param is not
realistic. **`ddp_broker_api_base` for this host's Secrets Manager entry is
`http://10.0.0.11:8080`.**

## The :8001 mix-up, resolved (don't repeat this)

The earlier candidate list flagged `:8001` as suspect because `ddp-sync` itself runs on that
port everywhere -- confirmed exactly right:

```
GET http://10.0.0.11:8001/ddp-sync/v1/health
-> HTTP 200
   {"status":"healthy","service":"ddp-sync","version":"0.1.0","config_source":"secrets_manager",
    "scheduler":{"running":true,"jobs":17,...},"redis":"connected",
    "pinecone":"connected (44859 vectors)"}
```

`10.0.0.11:8001` is the ddp-broker EC2's **own `ddp-sync` instance** (matches
`infrastructure/docker-compose.prod.yml`'s `ports: "8001:8001"` on that host) -- not
ddp-broker-py at all. Hitting `/api/bill-versions/latest/` there returned a bare
`{"detail":"Not Found"}` at `404` -- an unrelated service's generic "no such route," not
ddp-broker-py's own "bill not found" response shape. If this had been written into Secrets
Manager as `ddp_broker_api_base` instead of `:8080`, every real bill-version check from that
host would have failed with `BrokerClientError` (a genuine rejected-request error, not a timeout)
-- indistinguishable in the logs from a real ddp-broker-py outage, and much harder to
root-cause than a plain connection failure would have been.

## Bonus finding: no login token required on this specific read, when called directly

The request above carried no `Authorization` header at all and still got `200`. Matches a
comment already in `broker_client.py`: this particular read is public on ddp-broker-py itself --
it's ddp-api's own proxy that enforces auth on this path in the normal production traffic
pattern (SYNC-40), not ddp-broker-py directly. **Does not necessarily mean `ddp_broker_api_token`
can be left empty for this host** -- only the read side (`get_latest_bill_version`) was tested;
`write_bill_version` (the write half of the same Flow 2 dependency) was not, and a write
endpoint enforcing auth where a read one doesn't would not be surprising. Confirm that
separately before deciding to skip configuring a token.

## Open item, unchanged from the sibling doc

This was tested from a machine on the same private network as `10.0.0.11`, not from the
votebot/ddp-api EC2 host itself. The address is now confirmed correct; that specific host's own
ability to reach it (its own security group / route table) is still unconfirmed. Test
`curl http://10.0.0.11:8080/api/bill-versions/latest/?bill_openstates_id=<any-uuid>` from that
host itself before relying on this in production -- same reasoning applies to
`http://10.0.0.11:8002` (the OpenStates address from the earlier handoff doc), also still
unconfirmed from that specific host.
