"""SYNC-121: the Voatz-to-Brevo import must never send a contact's block flags.

The monthly full sync re-imports every existing contact with updateExistingContacts, so
`emailBlacklisted: false` / `smsBlacklisted: false` could clear a block the person chose.
"""

from unittest.mock import MagicMock, patch

from ddp_sync.pipelines import voatz_brevo


def test_import_payload_carries_no_block_flags():
    users = [{"emailAddress": "someone@example.org", "customerId": 1, "Voter_Id": "V1",
              "firstName": "a", "lastName": "b", "precinct": "VIRGINIA-1"}]
    response = MagicMock(status_code=202)
    with patch.object(voatz_brevo.requests, "post", return_value=response) as post, \
            patch.object(voatz_brevo.time, "sleep"):
        assert voatz_brevo.add_contacts_to_brevo("key", 7, users) == (1, 0, 0)

    payload = post.call_args.kwargs["json"]
    assert payload["updateExistingContacts"] is True
    assert payload["jsonBody"][0]["email"] == "someone@example.org"
    for key in ("emailBlacklist", "smsBlacklist"):
        assert key not in payload
    for key in ("emailBlacklisted", "smsBlacklisted", "whatsappBlacklisted"):
        assert key not in payload["jsonBody"][0]
