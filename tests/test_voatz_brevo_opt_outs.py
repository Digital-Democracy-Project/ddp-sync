"""SYNC-122: the Voatz-to-Brevo jobs never add or re-import someone who opted out.

Opted out means: on DDP's suppression list (ddp-broker-py BROKER-305), blocklisted for email in
Brevo, or unsubscribed from this Brevo list (Brevo then drops them from the list, which the
30-minute diff would otherwise read as a new sign-up). If either source cannot answer, nobody
is added for that run.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ddp_sync.pipelines import voatz_brevo

LIST_ID = 7
ORG = {"name": "Test", "voatz_org_id": 1, "voatz_email": "x", "voatz_password": "y",
       "brevo_api_key": "key", "brevo_list_id": LIST_ID, "blacklist": []}


def _voatz_user(email, customer_id):
    return {"email": email, "customerId": customer_id, "phone": None,
            "orgVerificationStatus": {"keyValues": [{"key": "Voter_Id", "value": f"V{customer_id}"}]}}


def _response(status, body=None):
    return MagicMock(status_code=status, json=MagicMock(return_value=body or {}))


@pytest.fixture
def voatz():
    users = [_voatz_user("new@example.org", 1), _voatz_user("blocked@example.org", 2),
             _voatz_user("left@example.org", 3), _voatz_user("suppressed@example.org", 4),
             _voatz_user("member@example.org", 5)]
    with patch.object(voatz_brevo, "get_voatz_tokens", return_value=("ws", "csrf")), \
            patch.object(voatz_brevo, "fetch_voatz_users", return_value=users), \
            patch.object(voatz_brevo.time, "sleep"):
        yield


BREVO_LIST = [{"email": "member@example.org", "attributes": {}, "emailBlacklisted": False},
              {"email": "blocked@example.org", "attributes": {}, "emailBlacklisted": True}]

# What GET /v3/contacts/{email} answers for addresses that are not in the list
BREVO_CONTACTS = {
    "new@example.org": _response(404),
    # not in this list fetch, so the diff reads it as new; the lookup must stop it
    "blocked@example.org": _response(200, {"emailBlacklisted": True, "listUnsubscribed": []}),
    "left@example.org": _response(200, {"emailBlacklisted": False, "listUnsubscribed": [LIST_ID]}),
    "suppressed@example.org": _response(404),
}


def _lookup(url, **kwargs):
    return BREVO_CONTACTS[url.rsplit("/", 1)[1]]


# ── the 30-minute job ───────────────────────────────────────────────────────────────


def test_sync_adds_only_the_new_sign_up(voatz):
    with patch.object(voatz_brevo, "fetch_brevo_contacts", return_value=[BREVO_LIST[0]]), \
            patch.object(voatz_brevo.requests, "get", side_effect=_lookup), \
            patch.object(voatz_brevo, "add_contacts_to_brevo", return_value=(1, 0, 0)) as add, \
            patch.object(voatz_brevo, "remove_contacts_from_brevo", return_value=(0, 0)):
        voatz_brevo.sync_org(ORG, {}, {"suppressed@example.org"})

    added = [u["emailAddress"] for u in add.call_args.args[2]]
    assert added == ["new@example.org"]


def test_sync_adds_nobody_when_the_suppression_list_cannot_be_read(voatz):
    with patch.object(voatz_brevo, "fetch_brevo_contacts", return_value=[BREVO_LIST[0]]), \
            patch.object(voatz_brevo.requests, "get") as get, \
            patch.object(voatz_brevo, "add_contacts_to_brevo") as add, \
            patch.object(voatz_brevo, "remove_contacts_from_brevo", return_value=(0, 0)):
        voatz_brevo.sync_org(ORG, {}, None)
    add.assert_not_called()
    get.assert_not_called()


def test_sync_leaves_an_address_brevo_cannot_answer_for_until_the_next_run(voatz):
    with patch.object(voatz_brevo, "fetch_brevo_contacts", return_value=[BREVO_LIST[0]]), \
            patch.object(voatz_brevo.requests, "get", return_value=_response(500)), \
            patch.object(voatz_brevo, "add_contacts_to_brevo") as add, \
            patch.object(voatz_brevo, "remove_contacts_from_brevo", return_value=(0, 0)):
        voatz_brevo.sync_org(ORG, {}, set())
    add.assert_not_called()


# ── the monthly job ─────────────────────────────────────────────────────────────────


def test_full_sync_re_imports_only_list_members_who_have_not_opted_out(voatz):
    with patch.object(voatz_brevo, "fetch_brevo_contacts", return_value=BREVO_LIST), \
            patch.object(voatz_brevo, "add_contacts_to_brevo", return_value=(1, 0, 0)) as add:
        voatz_brevo.full_sync_org(ORG, {}, {"suppressed@example.org"})
    assert [u["emailAddress"] for u in add.call_args.args[2]] == ["member@example.org"]


def test_full_sync_never_imports_someone_who_left_the_list():
    # left@ is not a list member at all: the monthly job must not put them back
    with patch.object(voatz_brevo, "get_voatz_tokens", return_value=("ws", "csrf")), \
            patch.object(voatz_brevo, "fetch_voatz_users", return_value=[_voatz_user("left@example.org", 3)]), \
            patch.object(voatz_brevo, "fetch_brevo_contacts", return_value=BREVO_LIST), \
            patch.object(voatz_brevo, "add_contacts_to_brevo") as add:
        assert voatz_brevo.full_sync_org(ORG, {}, set()) is None
    add.assert_not_called()


@pytest.mark.parametrize("brevo_list, suppressed", [(None, set()), (BREVO_LIST, None)])
def test_full_sync_skips_the_org_when_either_list_cannot_be_read(voatz, brevo_list, suppressed):
    with patch.object(voatz_brevo, "fetch_brevo_contacts", return_value=brevo_list), \
            patch.object(voatz_brevo, "add_contacts_to_brevo") as add:
        assert voatz_brevo.full_sync_org(ORG, {}, suppressed) is None
    add.assert_not_called()


# ── the two lookups ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("response, expected", [
    (_response(404), False),
    (_response(200, {"emailBlacklisted": False, "listUnsubscribed": [99]}), False),
    (_response(200, {"emailBlacklisted": True}), True),
    (_response(200, {"emailBlacklisted": False, "listUnsubscribed": [LIST_ID]}), True),
    (_response(429), None),
])
def test_brevo_contact_opted_out(response, expected):
    with patch.object(voatz_brevo.requests, "get", return_value=response), patch.object(voatz_brevo.time, "sleep"):
        assert voatz_brevo.brevo_contact_opted_out("key", "a@example.org", LIST_ID) is expected


SETTINGS = SimpleNamespace(ddp_broker_api_base="https://broker.test", ddp_broker_api_token="tok")


def test_suppression_list_is_read_with_the_service_token_and_lower_cased():
    with patch.object(voatz_brevo, "get_settings", return_value=SETTINGS), \
            patch.object(voatz_brevo.requests, "get",
                         return_value=_response(200, {"emails": ["A@Example.org"]})) as get:
        assert voatz_brevo.fetch_ddp_suppressed_emails() == {"a@example.org"}
    assert get.call_args.args[0] == "https://broker.test/api/email/suppressions/"
    assert get.call_args.kwargs["headers"] == {"Authorization": "Bearer tok"}


@pytest.mark.parametrize("settings, response", [
    (SimpleNamespace(ddp_broker_api_base="", ddp_broker_api_token="tok"), None),
    (SETTINGS, _response(404)),
    (SETTINGS, _response(200, {"unexpected": True})),
])
def test_suppression_list_unreadable_is_none(settings, response):
    with patch.object(voatz_brevo, "get_settings", return_value=settings), \
            patch.object(voatz_brevo.requests, "get", return_value=response):
        assert voatz_brevo.fetch_ddp_suppressed_emails() is None
