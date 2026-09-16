# Reachability confirmed FROM the votebot/ddp-api EC2 host itself: both :8002 and :8080 open

**Date:** 2026-09-16
**Replying to:** the open item at the end of
`notes/ddp-broker-address-confirmed-live-8080-not-8001-20260916.md` (and the equivalent open
item in the original `point-votebot-ec2-at-production-api-v3-20260915.md`) -- both flagged that
prior tests were run from elsewhere on the private network, not from this specific host.
**Audience:** Ramon / whoever finishes the votebot/ddp-api EC2 upgrade.

## Tested directly from the votebot/ddp-api EC2 host

```
$ curl -sS -m 8 "http://10.0.0.11:8002/bills?jurisdiction=fl"
{"detail":"Must provide API Key as ?apikey or X-API-KEY. Login and visit
https://openstates.org/account/profile/ for your API key."}
HTTP_STATUS:403

$ curl -sS -m 8 "http://10.0.0.11:8080/api/bill-versions/latest/?bill_openstates_id=00000000-0000-0000-0000-000000000000"
{"found":false}
HTTP_STATUS:200
```

## What this confirms

- **`10.0.0.11:8002`** (local OpenStates api-v3): reachable. The `403` is a genuine
  application-level auth response (not a timeout, not a connection refusal, not nginx's default
  page) -- the VPC/security-group path from this host is open, and the service just needs the
  real `local_openstates_api_key` to actually authenticate. That key is still the one open
  blocker on this whole upgrade (get it from Ramon).
- **`10.0.0.11:8080`** (ddp-broker-py): reachable, and returns the exact same
  `{"found":false}`/`200` shape the sibling note observed testing from a different machine on
  the network -- same result, now confirmed from the specific host that matters.

Both open reachability items from the last two notes are resolved. Nothing here changes any of
the values already decided (`local_openstates_api_base=http://10.0.0.11:8002`,
`ddp_broker_api_base=http://10.0.0.11:8080`) -- this just confirms the network path exists from
the host that will actually use them.

## Remaining open items (unchanged)

1. Real `local_openstates_api_key` value -- the only hard blocker left.
2. Whether `ddp_broker_api_token` is required for `write_bill_version` specifically (the read
   side, tested above and in the sibling note, doesn't need one; the write side wasn't tested).
