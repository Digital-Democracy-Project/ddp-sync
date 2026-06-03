"""Exception hierarchy for Webflow CMS operations.

Replaces sys.exit(1) calls from the original scripts so that errors
can be caught and handled by API routes or CLI wrappers.
"""


class WebflowCMSError(Exception):
    """Base exception for all webflow_cms errors."""


class ConfigurationError(WebflowCMSError):
    """Missing or invalid configuration (token, collection ID, etc.)."""


class WebflowAPIError(WebflowCMSError):
    """Non-retryable error from the Webflow API."""

    def __init__(self, message: str, status_code: int | None = None, response_text: str = ""):
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(message)


class WebflowConflictError(WebflowAPIError):
    """409 Conflict — item has incoming references that must be removed first."""

    def __init__(self, message: str, references: list | None = None):
        self.references = references or []
        super().__init__(message, status_code=409)


class ParseError(WebflowCMSError):
    """Failed to parse a URL or text field."""
