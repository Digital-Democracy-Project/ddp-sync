"""
Voatz -> Brevo user sync pipeline.

Moved from DDP-API scheduler.py. Syncs user data from Voatz to Brevo
contact lists, with phone conflict resolution and overseas detection.
"""

import logging
import re
import requests
import time
from datetime import datetime

from email_validator import validate_email, EmailNotValidError

from ddp_sync.config import get_settings

logger = logging.getLogger(__name__)

# Voatz API endpoints
LOGIN_URL = "https://vapi-vrb.nimsim.com/voatz/organizations/users/login"
USERS_URL = "https://vapi-vrb.nimsim.com/voatz/customers/delegate/signups/byorg"

LOGIN_HEADERS = {
    'Accept-Encoding': 'identity',
    'Content-Type': 'application/json',
    'Origin': 'http://vapi-vrb.nimsim.com'
}

# State name to 2-letter code mapping
STATE_CODES = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
    "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
    "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC"
}

# Overseas users list ID
OVERSEAS_LIST_ID = 58


def _get_org_configs() -> list[dict]:
    """Get org configs, merging root-level brevo_api_key and blacklist."""
    settings = get_settings()
    orgs = settings.organizations or []
    root_brevo_api_key = settings.brevo_api_key
    root_blacklist = settings.blacklist or []

    result = []
    for org in orgs:
        merged = dict(org)
        merged["brevo_api_key"] = org.get("brevo_api_key") or root_brevo_api_key
        merged["blacklist"] = org.get("blacklist") if org.get("blacklist") else root_blacklist
        result.append(merged)
    return result


def clean_email(email: str) -> str | None:
    """Pre-clean an email address before validation, then normalize via email-validator."""
    if not email:
        return None
    # Strip whitespace
    email = email.strip()
    # Strip leading/trailing dots from the whole address
    email = email.strip(".")
    # Clean up dots around the @ sign
    if "@" in email:
        local, domain = email.rsplit("@", 1)
        local = local.rstrip(".")
        domain = domain.strip(".")
        email = f"{local}@{domain}"
    # Validate and normalize
    try:
        result = validate_email(email, check_deliverability=False)
        return result.normalized
    except EmailNotValidError:
        return None


def get_state_code_from_precinct(precinct: str) -> str | None:
    """Extract state code from precinct string (e.g., 'FLORIDA-SEM-7-38-10' -> 'FL')."""
    if not precinct:
        return None

    # Precinct format is typically "STATE-..."
    parts = precinct.upper().split("-")
    if parts:
        state_name = parts[0]
        return STATE_CODES.get(state_name)

    return None


def is_us_phone_number(phone: str) -> bool:
    """
    Check if a phone number is a US number.

    US numbers are:
    - +1XXXXXXXXXX (12 chars with +)
    - 1XXXXXXXXXX (11 digits starting with 1)
    - XXXXXXXXXX (10 digits, assumed US)
    """
    if not phone:
        return True  # No phone = treat as domestic (don't add to overseas list)

    # Remove all non-digit characters except leading +
    cleaned = re.sub(r'[^\d+]', '', phone)

    # +1XXXXXXXXXX
    if cleaned.startswith('+1') and len(cleaned) == 12:
        return True

    # 1XXXXXXXXXX
    if cleaned.startswith('1') and len(cleaned) == 11:
        return True

    # XXXXXXXXXX (10-digit US local)
    if len(cleaned) == 10:
        return True

    # Also handle case where + was stripped but it's still 11 digits starting with 1
    digits_only = re.sub(r'\D', '', phone)
    if digits_only.startswith('1') and len(digits_only) == 11:
        return True
    if len(digits_only) == 10:
        return True

    return False


def get_voatz_tokens(email: str, password: str, org_id: int) -> tuple[str, str] | None:
    """Authenticate with Voatz and get WS/CSRF tokens."""
    payload = {
        "emailAddress": email,
        "password": password,
        "authData": [{"key": "organizationid", "value": str(org_id)}]
    }

    try:
        response = requests.post(LOGIN_URL, headers=LOGIN_HEADERS, json=payload, timeout=30)
        if response.status_code == 200 and response.text.strip() == "OK":
            ws_token = response.cookies.get('WS') or response.headers.get('WS')
            csrf_token = response.cookies.get('Csrf-Token') or response.headers.get('Csrf-Token')
            if ws_token and csrf_token:
                return ws_token, csrf_token
        logger.error(f"Voatz login failed for org {org_id}: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Voatz login error for org {org_id}: {e}")

    return None


def fetch_voatz_users(ws_token: str, csrf_token: str, org_id: int) -> list[dict] | None:
    """Fetch all users from Voatz for an organization.

    Returns None if an error occurs, to prevent callers from treating
    an empty result as 'no users exist' (which causes mass removals)."""
    headers = {
        'Accept-Encoding': 'identity',
        'Content-Type': 'application/json',
        'Origin': 'http://vapi-vrb.nimsim.com',
        'WS': ws_token,
        'Csrf-Token': csrf_token,
        'Cookie': f"WS={ws_token}; Csrf-Token={csrf_token}"
    }

    users = []
    min_id = None

    while True:
        payload = {"organizationId": org_id, "limit": 1000}
        if min_id:
            payload["minId"] = min_id

        try:
            response = requests.post(USERS_URL, headers=headers, json=payload, timeout=60)
            if response.status_code != 200:
                logger.error(f"Voatz users fetch failed: {response.status_code} - {response.text}")
                return None

            data = response.json()
            result = data.get("result", [])
            if not result:
                break

            users.extend(result)
            min_id = data.get("minId")

        except Exception as e:
            logger.error(f"Voatz users fetch error: {e}")
            return None

    return users


def fetch_brevo_contacts(api_key: str, list_id: int) -> list[dict] | None:
    """Fetch all contacts from a Brevo list.

    Returns None if an error occurs mid-pagination, to prevent callers
    from treating a partial fetch as a complete list (which causes false
    diffs and mass re-imports).
    """
    headers = {
        "Accept": "application/json",
        "api-key": api_key
    }

    contacts = []
    offset = 0
    limit = 500
    base_url = f"https://api.brevo.com/v3/contacts/lists/{list_id}/contacts"

    while True:
        try:
            response = requests.get(
                base_url,
                headers=headers,
                params={"limit": limit, "offset": offset},
                timeout=180
            )
            if response.status_code != 200:
                logger.error(f"Brevo fetch failed: {response.status_code} - {response.text}")
                return None

            data = response.json()
            page = data.get("contacts", [])
            if not page:
                break

            contacts.extend(page)
            if len(page) < limit:
                break
            offset += limit

        except Exception as e:
            logger.error(f"Brevo fetch error: {e}")
            return None

    return contacts


def flatten_voatz_user(user: dict) -> dict:
    """Flatten Voatz user structure for comparison."""
    flattened = {
        "Voter_Id": None,
        "customerId": user.get("customerId"),
        "firstName": None,
        "lastName": None,
        "emailAddress": user.get("email"),
        "phone": user.get("phone"),
        "precinct": None,
        "birthDate": None,
        "zip5": None,
        "timestamp": user.get("timestamp")
    }

    kv_pairs = user.get("orgVerificationStatus", {}).get("keyValues", [])
    for pair in kv_pairs:
        key = pair.get("key")
        value = pair.get("value")
        if not value:
            continue
        if key == "Voter_Id":
            flattened["Voter_Id"] = str(value).strip()
        elif key == "First_Name":
            flattened["firstName"] = str(value).strip()
        elif key == "Last_Name":
            flattened["lastName"] = str(value).strip()
        elif key == "Precinct":
            flattened["precinct"] = str(value).strip()
        elif key == "Birth_Date":
            flattened["birthDate"] = str(value).strip()
        elif key == "Zip5":
            flattened["zip5"] = str(value).strip()

    return flattened


def add_contacts_to_brevo(api_key: str, list_id: int, users: list[dict],
                          claimed_phones: dict | None = None,
                          brevo_phones: dict | None = None) -> tuple[int, int, int]:
    """
    Add contacts to Brevo list.

    Non-US phone numbers are also added to the overseas list (ID 58).

    claimed_phones is a dict mapping formatted phone -> email. It tracks which
    contact "owns" each phone across orgs. Brevo treats sms and WHATSAPP as
    unique keys, so only the first contact to claim a phone gets those fields.

    brevo_phones is a dict mapping formatted phone -> email (or None for
    email-less contacts). It tracks which Brevo contact currently owns each
    phone, enabling conflict resolution before import.

    Returns tuple of (successful_count, failed_count, overseas_count).
    """
    if not users:
        return 0, 0, 0

    if claimed_phones is None:
        claimed_phones = {}
    if brevo_phones is None:
        brevo_phones = {}

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "api-key": api_key
    }

    # Build contact list for import
    contacts = []
    overseas_count = 0

    for user in users:
        # Get raw phone for overseas check
        raw_phone = user.get("phone", "")
        is_overseas = not is_us_phone_number(raw_phone)

        # Format phone number (remove non-digits, ensure starts with 1 for US)
        phone = raw_phone
        if phone:
            phone = "".join(c for c in phone if c.isdigit())
            if phone and not phone.startswith("1") and is_us_phone_number(raw_phone):
                phone = "1" + phone

        # Get state code from precinct
        state_code = get_state_code_from_precinct(user.get("precinct"))

        # Title case names
        first_name = user.get("firstName")
        last_name = user.get("lastName")
        if first_name:
            first_name = first_name.title()
        if last_name:
            last_name = last_name.title()

        # Determine list IDs - add to overseas list if non-US phone
        if is_overseas and raw_phone:
            list_ids = [list_id, OVERSEAS_LIST_ID]
            overseas_count += 1
        else:
            list_ids = [list_id]

        # Clean, validate, and normalize email
        raw_email = user.get("emailAddress")
        email = clean_email(raw_email)
        if not email:
            logger.warning(f"Skipping contact with invalid email: {raw_email}")
            continue

        contact = {
            "email": email,
            "attributes": {
                "FIRSTNAME": first_name,
                "LASTNAME": last_name,
                "VOTER_ID": user.get("Voter_Id"),
                "VOATZ_ID": user.get("customerId"),
                "BALLOT_ID": user.get("precinct"),
                "RESIDENCE_STATE": state_code,
                "RESIDENCE_ZIP": user.get("zip5"),
                "BIRTH_DATE": user.get("birthDate"),
                "SIGNUP_TIMESTAMP": user.get("timestamp"),
            },
            "listIds": list_ids,
            "updateEnabled": True,
            "emailBlacklisted": False,
            "smsBlacklisted": False
        }

        # Add phone/SMS if available. Brevo treats both `sms` and
        # `WHATSAPP` as unique keys -- resolve conflicts before assigning.
        if phone:
            if resolve_phone_ownership(api_key, phone, email, claimed_phones, brevo_phones):
                contact["sms"] = phone
                contact["attributes"]["WHATSAPP"] = phone

        contacts.append(contact)

    # Use Brevo import endpoint for bulk add
    import_url = "https://api.brevo.com/v3/contacts/import"

    successful = 0
    failed = 0
    chunk_size = 500  # Brevo recommends max 500 per request

    for i in range(0, len(contacts), chunk_size):
        chunk = contacts[i:i + chunk_size]
        payload = {
            "jsonBody": chunk,
            "listIds": [list_id],
            "updateExistingContacts": True
        }

        try:
            response = requests.post(import_url, headers=headers, json=payload, timeout=60)
            if 200 <= response.status_code < 300:
                successful += len(chunk)
                logger.info(f"Added {len(chunk)} contacts to Brevo list {list_id}")
            else:
                failed += len(chunk)
                logger.error(f"Brevo import failed: {response.status_code} - {response.text}")
        except Exception as e:
            failed += len(chunk)
            logger.error(f"Brevo import error: {e}")

        # Rate limiting delay
        time.sleep(0.1)

    if overseas_count > 0:
        logger.info(f"Added {overseas_count} overseas users to list {OVERSEAS_LIST_ID}")

    return successful, failed, overseas_count


def remove_contacts_from_brevo(api_key: str, list_id: int, emails: list[str]) -> tuple[int, int]:
    """
    Remove contacts from Brevo list.

    Returns tuple of (successful_count, failed_count).
    """
    if not emails:
        return 0, 0

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "api-key": api_key
    }

    successful = 0
    failed = 0

    # Remove contacts from list (not deleting them entirely, just from this list)
    remove_url = f"https://api.brevo.com/v3/contacts/lists/{list_id}/contacts/remove"

    # Process in chunks
    chunk_size = 150  # Brevo limit for this endpoint

    for i in range(0, len(emails), chunk_size):
        chunk = emails[i:i + chunk_size]
        payload = {"emails": chunk}

        try:
            response = requests.post(remove_url, headers=headers, json=payload, timeout=60)
            if 200 <= response.status_code < 300:
                successful += len(chunk)
                logger.info(f"Removed {len(chunk)} contacts from Brevo list {list_id}")
            else:
                failed += len(chunk)
                logger.error(f"Brevo remove failed: {response.status_code} - {response.text}")
        except Exception as e:
            failed += len(chunk)
            logger.error(f"Brevo remove error: {e}")

        # Rate limiting delay
        time.sleep(0.1)

    return successful, failed


def clear_phone_from_brevo_contact(api_key: str, phone: str) -> bool:
    """
    Clear sms/WHATSAPP from an existing Brevo contact that owns the given phone.

    If the owning contact has a different email, clear the phone fields via PUT.
    If the owning contact has no email (orphan), delete it entirely.
    Returns True if the phone is now available, False on failure.
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "api-key": api_key
    }

    # Look up who currently owns this phone in Brevo
    lookup_url = f"https://api.brevo.com/v3/contacts/{phone}"
    try:
        resp = requests.get(
            lookup_url, headers=headers,
            params={"identifierType": "phone_id"},
            timeout=30
        )
        time.sleep(0.1)
    except Exception as e:
        logger.error(f"Phone lookup failed for {phone}: {e}")
        return False

    if resp.status_code == 404:
        return True  # No conflict

    if resp.status_code != 200:
        logger.error(f"Phone lookup unexpected status for {phone}: {resp.status_code} - {resp.text}")
        return False

    owner = resp.json()
    owner_email = owner.get("email")

    if owner_email:
        # Contact has an email -- clear sms and WHATSAPP via PUT
        put_url = f"https://api.brevo.com/v3/contacts/{owner_email}"
        try:
            put_resp = requests.put(
                put_url, headers=headers,
                json={"attributes": {"WHATSAPP": ""}, "sms": ""},
                timeout=30
            )
            time.sleep(0.1)
        except Exception as e:
            logger.error(f"Failed to clear phone from {owner_email}: {e}")
            return False

        if 200 <= put_resp.status_code < 300:
            logger.info(f"Resolved phone conflict: {phone} cleared from {owner_email}")
            return True
        else:
            logger.error(f"Failed to clear phone from {owner_email}: {put_resp.status_code} - {put_resp.text}")
            return False
    else:
        # Orphan contact with no email -- delete it
        del_url = f"https://api.brevo.com/v3/contacts/{phone}"
        try:
            del_resp = requests.delete(
                del_url, headers=headers,
                params={"identifierType": "phone_id"},
                timeout=30
            )
            time.sleep(0.1)
        except Exception as e:
            logger.error(f"Failed to delete orphan contact with phone {phone}: {e}")
            return False

        if 200 <= del_resp.status_code < 300:
            logger.info(f"Resolved phone conflict: {phone} transferred from (email-less contact) — orphan deleted")
            return True
        else:
            logger.error(f"Failed to delete orphan contact with phone {phone}: {del_resp.status_code} - {del_resp.text}")
            return False


def resolve_phone_ownership(api_key: str, phone: str, new_email: str,
                            claimed_phones: dict, brevo_phones: dict) -> bool:
    """
    Determine whether new_email can claim the given phone number.

    Checks cross-org claims (claimed_phones), the Brevo phone index
    (brevo_phones), and falls back to a live Brevo API lookup.
    Returns True if the phone can be assigned to new_email.
    """
    new_email_lower = new_email.lower()

    # 1. Cross-org claim by a different email -- respect it
    cross_owner = claimed_phones.get(phone)
    if cross_owner is not None and cross_owner != new_email_lower:
        return False

    # 2. Check brevo_phones index
    if phone in brevo_phones:
        brevo_owner = brevo_phones[phone]

        if brevo_owner == new_email_lower:
            # Same email already owns it -- no conflict
            claimed_phones[phone] = new_email_lower
            return True

        if brevo_owner is not None:
            # Different email owns it in Brevo -- clear it
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "api-key": api_key
            }
            put_url = f"https://api.brevo.com/v3/contacts/{brevo_owner}"
            try:
                put_resp = requests.put(
                    put_url, headers=headers,
                    json={"attributes": {"WHATSAPP": ""}, "sms": ""},
                    timeout=30
                )
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Failed to clear phone {phone} from {brevo_owner}: {e}")
                return False

            if 200 <= put_resp.status_code < 300:
                logger.info(f"Resolved phone conflict: {phone} cleared from {brevo_owner}")
                claimed_phones[phone] = new_email_lower
                brevo_phones[phone] = new_email_lower
                return True
            else:
                logger.error(f"Failed to clear phone {phone} from {brevo_owner}: {put_resp.status_code}")
                return False
        else:
            # Email-less orphan owns it -- delete the orphan
            if clear_phone_from_brevo_contact(api_key, phone):
                claimed_phones[phone] = new_email_lower
                brevo_phones[phone] = new_email_lower
                return True
            return False

    # 3. Phone not in brevo_phones -- live lookup
    if clear_phone_from_brevo_contact(api_key, phone):
        claimed_phones[phone] = new_email_lower
        brevo_phones[phone] = new_email_lower
        return True
    return False


def sync_org(org_config: dict, claimed_phones: dict | None = None) -> dict | None:
    """
    Sync users for a single organization.

    - Adds new users to Brevo
    - Removes departed users from Brevo
    - Returns summary of changes
    """
    org_name = org_config.get("name", "Unknown")
    org_id = org_config["voatz_org_id"]
    blacklist = set(str(b) for b in org_config.get("blacklist", []))
    brevo_api_key = org_config["brevo_api_key"]
    brevo_list_id = org_config["brevo_list_id"]

    logger.info(f"Syncing org: {org_name} (ID: {org_id})")

    # Authenticate with Voatz
    tokens = get_voatz_tokens(
        org_config["voatz_email"],
        org_config["voatz_password"],
        org_id
    )
    if not tokens:
        logger.error(f"Failed to authenticate for org: {org_name}")
        return None

    ws_token, csrf_token = tokens

    # Fetch Voatz users
    voatz_users = fetch_voatz_users(ws_token, csrf_token, org_id)
    if voatz_users is None:
        logger.error(f"Skipping org {org_name}: Voatz fetch failed (empty result unreliable for diff)")
        return None
    logger.info(f"Fetched {len(voatz_users)} users from Voatz for {org_name}")

    # Extract emails and details from Voatz (keyed by email for diff)
    voatz_emails = set()
    voatz_details_by_email = {}
    voatz_blacklisted_count = 0
    voatz_no_customer_id_count = 0
    voatz_invalid_email_count = 0
    for user in voatz_users:
        flattened = flatten_voatz_user(user)
        customer_id = flattened.get("customerId")
        voter_id = flattened.get("Voter_Id")
        if not customer_id:
            voatz_no_customer_id_count += 1
        elif voter_id and voter_id in blacklist:
            voatz_blacklisted_count += 1
        else:
            email = clean_email(flattened.get("emailAddress"))
            if not email:
                voatz_invalid_email_count += 1
            else:
                email = email.lower()
                voatz_emails.add(email)
                voatz_details_by_email[email] = flattened

    # Fetch Brevo contacts
    brevo_contacts = fetch_brevo_contacts(brevo_api_key, brevo_list_id)
    if brevo_contacts is None:
        logger.error(f"Skipping org {org_name}: Brevo fetch failed (partial data unreliable for diff)")
        return None
    logger.info(f"Fetched {len(brevo_contacts)} contacts from Brevo for {org_name}")

    # Extract emails from Brevo (for diff matching)
    if claimed_phones is None:
        claimed_phones = {}
    brevo_phones = {}
    brevo_emails = set()
    brevo_blacklisted_count = 0
    brevo_no_email_count = 0
    for contact in brevo_contacts:
        voter_id = contact.get("attributes", {}).get("VOTER_ID")
        email = contact.get("email")
        whatsapp = contact.get("attributes", {}).get("WHATSAPP")
        if not email:
            brevo_no_email_count += 1
            # Track email-less contacts' phones so we can resolve conflicts
            if whatsapp:
                brevo_phones[str(whatsapp)] = None
        elif voter_id and str(voter_id).strip() in blacklist:
            brevo_blacklisted_count += 1
        else:
            brevo_emails.add(email.lower())
            # Seed claimed_phones and brevo_phones from existing Brevo contacts
            if whatsapp:
                claimed_phones[str(whatsapp)] = email.lower()
                brevo_phones[str(whatsapp)] = email.lower()

    # Diagnostic logging
    logger.info(f"  Voatz breakdown: {len(voatz_emails)} valid, {voatz_blacklisted_count} blacklisted, {voatz_no_customer_id_count} no customer ID, {voatz_invalid_email_count} invalid email")
    logger.info(f"  Brevo breakdown: {len(brevo_emails)} valid, {brevo_blacklisted_count} blacklisted, {brevo_no_email_count} no email")

    # Calculate differences (by email)
    if len(voatz_emails) == 0 and len(brevo_emails) > 0:
        logger.warning(f"Skipping org {org_name}: Voatz returned 0 valid emails but Brevo has {len(brevo_emails)} — refusing to remove all contacts")
        return None

    added_emails = voatz_emails - brevo_emails
    removed_emails = brevo_emails - voatz_emails

    # Log the differences
    logger.info(f"  Diff: {len(added_emails)} emails in Voatz but not Brevo, {len(removed_emails)} emails in Brevo but not Voatz")

    users_to_add = [voatz_details_by_email[e] for e in added_emails]
    emails_to_remove = list(removed_emails)

    logger.info(f"Org {org_name}: {len(users_to_add)} to add, {len(emails_to_remove)} to remove")

    # Perform sync operations
    added_success, added_failed, overseas_count = 0, 0, 0
    removed_success, removed_failed = 0, 0

    if users_to_add:
        added_success, added_failed, overseas_count = add_contacts_to_brevo(
            brevo_api_key, brevo_list_id, users_to_add, claimed_phones, brevo_phones
        )
        logger.info(f"Org {org_name}: Added {added_success} contacts ({added_failed} failed, {overseas_count} overseas)")

    if emails_to_remove:
        removed_success, removed_failed = remove_contacts_from_brevo(brevo_api_key, brevo_list_id, emails_to_remove)
        logger.info(f"Org {org_name}: Removed {removed_success} contacts ({removed_failed} failed)")

    # Return summary if there were any changes
    if users_to_add or emails_to_remove:
        return {
            "organization_name": org_name,
            "organization_id": org_id,
            "voatz_total": len(voatz_emails),
            "brevo_total": len(brevo_emails),
            "added_count": added_success,
            "added_failed": added_failed,
            "overseas_count": overseas_count,
            "removed_count": removed_success,
            "removed_failed": removed_failed,
            "synced_at": datetime.utcnow().isoformat() + "Z"
        }

    return None


def full_sync_org(org_config: dict, claimed_phones: dict | None = None) -> dict | None:
    """
    Full-attribute sync for a single organization.

    Re-imports all Voatz users to Brevo, updating any changed attributes.
    Unlike sync_org(), this does not diff -- it pushes all users through
    add_contacts_to_brevo() which uses updateExistingContacts: True.
    """
    org_name = org_config.get("name", "Unknown")
    org_id = org_config["voatz_org_id"]
    blacklist = set(str(b) for b in org_config.get("blacklist", []))
    brevo_api_key = org_config["brevo_api_key"]
    brevo_list_id = org_config["brevo_list_id"]

    logger.info(f"Full-attribute sync for org: {org_name} (ID: {org_id})")

    # Authenticate with Voatz
    tokens = get_voatz_tokens(
        org_config["voatz_email"],
        org_config["voatz_password"],
        org_id
    )
    if not tokens:
        logger.error(f"Failed to authenticate for org: {org_name}")
        return None

    ws_token, csrf_token = tokens

    # Fetch all Voatz users
    voatz_users = fetch_voatz_users(ws_token, csrf_token, org_id)
    if voatz_users is None:
        logger.error(f"Skipping full sync for org {org_name}: Voatz fetch failed")
        return None
    logger.info(f"Fetched {len(voatz_users)} users from Voatz for {org_name}")

    # Flatten and filter
    users_to_sync = []
    blacklisted_count = 0
    no_customer_id_count = 0

    for user in voatz_users:
        flattened = flatten_voatz_user(user)
        customer_id = flattened.get("customerId")
        voter_id = flattened.get("Voter_Id")

        if not customer_id:
            no_customer_id_count += 1
        elif voter_id and voter_id in blacklist:
            blacklisted_count += 1
        else:
            users_to_sync.append(flattened)

    logger.info(f"  Breakdown: {len(users_to_sync)} valid, {blacklisted_count} blacklisted, {no_customer_id_count} no customer ID")

    if not users_to_sync:
        logger.info(f"No users to sync for {org_name}")
        return None

    # Fetch Brevo contacts to build phone index (avoids per-user API lookups)
    brevo_phones = {}
    brevo_contacts = fetch_brevo_contacts(brevo_api_key, brevo_list_id)
    if brevo_contacts is not None:
        for contact in brevo_contacts:
            email = contact.get("email")
            whatsapp = contact.get("attributes", {}).get("WHATSAPP")
            if whatsapp:
                brevo_phones[str(whatsapp)] = email.lower() if email else None
        logger.info(f"  Built phone index from {len(brevo_contacts)} Brevo contacts ({len(brevo_phones)} phones)")
    else:
        logger.warning(f"  Brevo fetch failed for {org_name} — phone conflicts will use live lookups")

    # Push all users to Brevo (updates existing contacts matched by email)
    added_success, added_failed, overseas_count = add_contacts_to_brevo(
        brevo_api_key, brevo_list_id, users_to_sync, claimed_phones, brevo_phones
    )
    logger.info(f"Org {org_name}: Synced {added_success} contacts ({added_failed} failed, {overseas_count} overseas)")

    return {
        "organization_name": org_name,
        "organization_id": org_id,
        "voatz_total": len(voatz_users),
        "synced_count": added_success,
        "synced_failed": added_failed,
        "overseas_count": overseas_count,
        "synced_at": datetime.utcnow().isoformat() + "Z"
    }


def push_alert_to_zapier(webhook_url: str, summaries: list[dict]) -> bool:
    """Push sync summary alert to Zapier webhook."""
    if not webhook_url:
        logger.error("No Zapier webhook URL configured")
        return False

    # Build summary message
    total_added = sum(s.get("added_count", 0) for s in summaries)
    total_removed = sum(s.get("removed_count", 0) for s in summaries)
    total_overseas = sum(s.get("overseas_count", 0) for s in summaries)

    payload = {
        "alert_type": "user_sync_complete",
        "summary": f"Synced {len(summaries)} organizations: {total_added} added ({total_overseas} overseas), {total_removed} removed",
        "total_added": total_added,
        "total_removed": total_removed,
        "total_overseas": total_overseas,
        "organizations_synced": len(summaries),
        "details": summaries,
        "synced_at": datetime.utcnow().isoformat() + "Z"
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        if response.status_code == 200:
            logger.info(f"Successfully sent sync alert to Zapier: {total_added} added, {total_removed} removed")
            return True
        else:
            logger.error(f"Zapier webhook failed: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Zapier webhook error: {e}")

    return False


def run_sync_job():
    """Main sync job - syncs all orgs and sends alert to Zapier."""
    logger.info("Starting scheduled user sync job")

    try:
        settings = get_settings()
        orgs = _get_org_configs()

        # Sort so Federal syncs last -- state orgs claim shared phone numbers
        # first, since SMS campaigns are state-focused.
        orgs.sort(key=lambda o: o.get("name", "").lower() == "federal")

        all_summaries = []
        # Track phone ownership across orgs to avoid Brevo unique-key conflicts
        claimed_phones = {}

        for org_config in orgs:
            try:
                summary = sync_org(org_config, claimed_phones)
                if summary:
                    all_summaries.append(summary)
            except Exception as e:
                org_name = org_config.get("name", "Unknown")
                logger.error(f"Error syncing org {org_name}: {e}")

        # Send alert to Zapier if there were any changes
        if all_summaries:
            push_alert_to_zapier(settings.zapier_webhook_url, all_summaries)
        else:
            logger.info("No changes found across all organizations")

    except Exception as e:
        logger.error(f"Sync job failed: {e}")

    logger.info("Scheduled user sync job completed")


def run_full_sync_job():
    """Full-attribute sync job - re-imports all users to update attributes."""
    logger.info("Starting full-attribute sync job")

    try:
        settings = get_settings()
        orgs = _get_org_configs()

        # Sort so Federal syncs last (same as regular sync)
        orgs.sort(key=lambda o: o.get("name", "").lower() == "federal")

        all_summaries = []
        claimed_phones = {}

        for org_config in orgs:
            try:
                summary = full_sync_org(org_config, claimed_phones)
                if summary:
                    all_summaries.append(summary)
            except Exception as e:
                org_name = org_config.get("name", "Unknown")
                logger.error(f"Error in full sync for org {org_name}: {e}")

        # Send alert to Zapier
        if all_summaries:
            total_synced = sum(s.get("synced_count", 0) for s in all_summaries)
            total_overseas = sum(s.get("overseas_count", 0) for s in all_summaries)

            payload = {
                "alert_type": "full_attribute_sync_complete",
                "summary": f"Full-attribute sync for {len(all_summaries)} organizations: {total_synced} contacts updated ({total_overseas} overseas)",
                "total_synced": total_synced,
                "total_overseas": total_overseas,
                "organizations_synced": len(all_summaries),
                "details": all_summaries,
                "synced_at": datetime.utcnow().isoformat() + "Z"
            }

            if settings.zapier_webhook_url:
                try:
                    response = requests.post(settings.zapier_webhook_url, json=payload, timeout=30)
                    if response.status_code == 200:
                        logger.info(f"Sent full sync alert to Zapier: {total_synced} contacts updated")
                    else:
                        logger.error(f"Zapier webhook failed: {response.status_code} - {response.text}")
                except Exception as e:
                    logger.error(f"Zapier webhook error: {e}")
        else:
            logger.info("No organizations synced in full-attribute sync")

    except Exception as e:
        logger.error(f"Full-attribute sync job failed: {e}")

    logger.info("Full-attribute sync job completed")
