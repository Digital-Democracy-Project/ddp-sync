"""API key authentication for DDP-Sync."""

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

from ddp_sync.config import get_settings

api_key_header = APIKeyHeader(name="Authorization", auto_error=False)


async def api_key_auth(api_key: str = Security(api_key_header)):
    """Validate Bearer token against configured API key."""
    settings = get_settings()
    expected = f"Bearer {settings.api_key}"
    if not api_key or api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key
