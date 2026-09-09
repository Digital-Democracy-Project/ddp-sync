"""Tests for rds_credentials.py (OPEN-260).

A small fake Secrets Manager client rather than botocore.stub.Stubber -- get_secret_value is
the one method this module calls, so a fake keeps these tests focused on this module's own
JSON-to-DSN assembly and error handling rather than re-verifying botocore's own request/response
validation (same reasoning test_cloud_scrape_trigger.py already gives for its FakeEcsClient).
"""

from __future__ import annotations

import json
from unittest.mock import patch

from ddp_sync.services import rds_credentials as rc


class FakeSecretsManagerClient:
    def __init__(self, secret_string=None, error=None):
        self._secret_string = secret_string
        self._error = error
        self.get_secret_value_calls = []

    def get_secret_value(self, **kwargs):
        self.get_secret_value_calls.append(kwargs)
        if self._error:
            raise self._error
        return {"SecretString": self._secret_string}


def _real_shaped_secret(**overrides):
    # 2026-09-09 (found live): the real RDS-managed secret only ever carries these two keys --
    # confirmed directly against the real secret's own keys on the ddp-sync host. host/port/
    # dbname are NOT in it; those come from their own env vars (RDS_HOST/RDS_PORT/RDS_DBNAME),
    # patched separately below, matching render-env.sh's existing precedent for the same values.
    secret = {
        "username": "openstates_admin",
        "password": "correct horse battery staple",
    }
    secret.update(overrides)
    return json.dumps(secret)


def _patch_connection_details(**overrides):
    """Context manager patching RDS_HOST/RDS_PORT/RDS_DBNAME to real-shaped test values,
    with overrides for the one test that needs to vary one of them."""
    values = {
        "RDS_HOST": "ddp-openstates.cvxdhm1ogxug.us-east-1.rds.amazonaws.com",
        "RDS_PORT": "5432",
        "RDS_DBNAME": "openstates",
    }
    values.update(overrides)
    return patch.multiple(rc, **values)


def test_missing_secret_arn_refuses_without_calling_secrets_manager():
    with patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", None), _patch_connection_details():
        url, error = rc.resolve_rds_database_url(secretsmanager_client=FakeSecretsManagerClient())

    assert url is None
    assert "RDS_CREDENTIALS_SECRET_ARN not set" in error


def test_missing_connection_details_refuses_without_calling_secrets_manager():
    """The secret alone was never enough (it only has username/password) -- RDS_HOST/RDS_PORT/
    RDS_DBNAME must all be set too, checked up front rather than discovered as a KeyError deep
    inside DSN assembly."""
    client = FakeSecretsManagerClient(secret_string=_real_shaped_secret())
    with (
        patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"),
        _patch_connection_details(RDS_HOST=None),
    ):
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert url is None
    assert "RDS_HOST/RDS_PORT/RDS_DBNAME" in error
    assert client.get_secret_value_calls == []  # fails before ever touching Secrets Manager


def test_successful_fetch_assembles_a_valid_postgres_url():
    client = FakeSecretsManagerClient(secret_string=_real_shaped_secret())
    with (
        patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:1:secret:rds!x"),
        _patch_connection_details(),
    ):
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert error == ""
    assert url == (
        "postgresql://openstates_admin:correct%20horse%20battery%20staple"
        "@ddp-openstates.cvxdhm1ogxug.us-east-1.rds.amazonaws.com:5432/openstates"
    )
    assert client.get_secret_value_calls == [{"SecretId": "arn:aws:secretsmanager:us-east-1:1:secret:rds!x"}]


def test_password_with_url_special_characters_is_percent_encoded():
    """The exact class of bug a raw-string DATABASE_URL would risk: a password containing
    characters that are valid JSON but would corrupt or misparse an unescaped URL."""
    client = FakeSecretsManagerClient(secret_string=_real_shaped_secret(password="p@ss:w/rd%25"))
    with patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"), _patch_connection_details():
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert error == ""
    # urllib.parse.quote with safe="" percent-encodes every reserved character, so the DSN
    # parser can never misread part of the password as a delimiter. urlparse() itself doesn't
    # decode percent-escapes back out (that's unquote's job) -- round-tripping through both
    # confirms the encoding is correct, not just present.
    from urllib.parse import unquote, urlparse

    parsed = urlparse(url)
    assert unquote(parsed.password) == "p@ss:w/rd%25"
    assert unquote(parsed.username) == "openstates_admin"


def test_secrets_manager_api_error_fails_loudly_not_silently():
    client = FakeSecretsManagerClient(error=RuntimeError("AccessDeniedException"))
    with patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"), _patch_connection_details():
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert url is None
    assert "AccessDeniedException" in error


def test_malformed_secret_shape_fails_cleanly_instead_of_raising():
    """A secret missing username/password must come back as the normal (None, error) result,
    not an uncaught KeyError escaping into the caller."""
    client = FakeSecretsManagerClient(secret_string=json.dumps({"username": "x"}))
    with patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"), _patch_connection_details():
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert url is None
    assert "unexpected shape" in error


def test_non_json_secret_string_fails_cleanly_instead_of_raising():
    client = FakeSecretsManagerClient(secret_string="not-json-at-all")
    with patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"), _patch_connection_details():
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert url is None
    assert "unexpected shape" in error


def test_no_injected_client_constructs_a_real_boto3_client():
    """Production callers pass no client at all -- confirms the real boto3 construction path
    is reached with the right region, not just the injectable test path."""
    with (
        patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"),
        patch.object(rc, "AWS_REGION", "us-east-1"),
        _patch_connection_details(),
        patch("boto3.client") as mock_boto_client,
    ):
        mock_boto_client.return_value = FakeSecretsManagerClient(secret_string=_real_shaped_secret())
        url, error = rc.resolve_rds_database_url()

    mock_boto_client.assert_called_once_with("secretsmanager", region_name="us-east-1")
    assert error == ""
    assert url is not None


def test_boto3_client_construction_failure_fails_cleanly_instead_of_raising():
    """pm-review: boto3.client() itself can raise (region/credential-provider/botocore config
    problems), not just get_secret_value() -- both must land in the same (None, error) tuple
    contract, not let a client-construction failure escape uncaught."""
    with (
        patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"),
        _patch_connection_details(),
        patch("boto3.client", side_effect=RuntimeError("no region configured")),
    ):
        url, error = rc.resolve_rds_database_url()

    assert url is None
    assert "no region configured" in error


def test_dbname_with_url_special_characters_is_percent_encoded():
    """The same class of bug the password test covers, for the DSN's path component instead
    of its userinfo component -- an unescaped '/' or '?' in RDS_DBNAME would otherwise corrupt
    or misparse the URL just as badly as an unescaped password character would."""
    client = FakeSecretsManagerClient(secret_string=_real_shaped_secret())
    with (
        patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"),
        _patch_connection_details(RDS_DBNAME="weird/db?name"),
    ):
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert error == ""
    from urllib.parse import unquote, urlparse

    parsed = urlparse(url)
    assert unquote(parsed.path.lstrip("/")) == "weird/db?name"


def test_null_field_in_secret_fails_cleanly_instead_of_producing_a_garbage_dsn():
    """pm-review: without this check, a JSON `null` password would stringify to the literal
    text "None" via str(None) and silently produce a well-formed-looking but wrong DSN,
    instead of a clear error."""
    client = FakeSecretsManagerClient(secret_string=_real_shaped_secret(password=None))
    with patch.object(rc, "RDS_CREDENTIALS_SECRET_ARN", "arn:secret"), _patch_connection_details():
        url, error = rc.resolve_rds_database_url(secretsmanager_client=client)

    assert url is None
    assert "unexpected shape" in error
