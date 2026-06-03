"""Constants used across all Webflow CMS operations."""

# Webflow API
API_BASE = "https://api.webflow.com/v2"
API_VERSION = "1.0.0"

# DDP Map Application
MAP_BASE_URL = "https://mapapp.digitaldemocracyproject.org/ddp-mapapp-new/"

# Zapier integration
ZAPIER_WEBHOOK_URL = "https://hooks.zapier.com/hooks/catch/18610493/u6k1oxx/"

# HTTP status codes that trigger automatic retry with backoff
TRANSIENT_STATUS_CODES = (429, 502)

# Maximum backoff duration in seconds for retry logic
MAX_BACKOFF_SECONDS = 60

# Pagination
PAGE_LIMIT = 100

# Throttle delay between API calls to avoid rate limits
THROTTLE_SECONDS = 0.5

# Key fields that indicate a "complete" bill record
KEY_FIELDS = [
    "name",
    "slug",
    "open-states-url-2",
    "gov-url",
    "map-url",
    "voatzid",
    "session-code",
    "bill-prefix",
    "bill-number",
    "post-body",
    "category",
    "kialo-url",
    "support",
    "oppose",
    "description",
    "member-organizations",
    "organizations-oppose",
]

# CMS field slug mapping for parsed about-organization sections
ABOUT_FIELD_MAP = {
    "description": "description-4",
    "type": "type-2",
    "policies": "policies-2",
    "funding": "funding-2",
    "affiliates": "affiliates-2",
}
