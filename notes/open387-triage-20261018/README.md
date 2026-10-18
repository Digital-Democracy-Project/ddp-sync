# OPEN-387 triage lists (2026-10-18)

Source: the dev ddp-broker (a snapshot of production from the evening of 2026-10-17, per the operator). Ids are the broker's artifact ids from that snapshot and must be verified against production before use. No summary text is stored here.

* `summary.json`: per state, rows and the size of each list.
* `<STATE>_REWRITE_NOW.txt`, `_OPTIONAL.txt`, `_HOLD_LARGE.txt`, `_HIDE_DECISION.txt`: artifact ids, one per line. A row is in at most one of these lists.
* `<STATE>_<A..G>_*.csv`: every row in a category (a row can be in several): A wrong pair, B never completed, C contradiction, D footer-only listed as a change, E no diff today, F version missing in api-v3, G large diff with a skipped phrase. Columns: artifact_id, state, session, gov_id, new_version_note, summary_compares_from, review_status, status, diff_chars_today, flags.
* Read the request note (`request-open387-...-20261018.md`) before using any list. Nothing is to be changed on production until the operator says go.
