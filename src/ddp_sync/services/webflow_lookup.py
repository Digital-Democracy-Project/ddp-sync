"""Webflow CMS write operations for sync pipelines.

Provides:
- update_bill_fields(): PATCH bill CMS fields (status, gov-url, status-date, status-chamber)
- update_bill_gov_url(): Thin wrapper for backward compat
"""

import httpx
import structlog

from ddp_sync.config import Settings, get_settings

logger = structlog.get_logger()


class WebflowLookupService:
    """Webflow CMS write service for sync pipelines."""

    BASE_URL = "https://api.webflow.com/v2"

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.api_key = self.settings.webflow_scheduler_api_key or self.settings.webflow_votebot_api_key
        self.bills_collection_id = self.settings.webflow_bills_collection_id

    async def update_bill_fields(
        self,
        webflow_id: str,
        field_data: dict[str, str],
        api_key: str | None = None,
    ) -> bool:
        """Update arbitrary fields for a bill in Webflow CMS.

        Uses PATCH /v2/collections/{collection_id}/items/{item_id}/live
        to publish changes immediately. Requires CMS:write scope on the API token.

        Args:
            webflow_id: Webflow item ID for the bill
            field_data: Dict of field names to values (e.g., {"gov-url": url, "status": status})
            api_key: Optional API key override (e.g., scheduler key with write scope).
                     Falls back to self.api_key if not provided.

        Returns:
            True on success, False on failure
        """
        if not webflow_id or not field_data:
            logger.warning("Missing webflow_id or field_data for bill update")
            return False

        key = api_key or self.api_key
        url = f"{self.BASE_URL}/collections/{self.bills_collection_id}/items/{webflow_id}/live"
        headers = {
            "Authorization": f"Bearer {key}",
            "accept": "application/json",
            "content-type": "application/json",
        }
        payload = {"fieldData": field_data}

        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                response = await client.patch(url, headers=headers, json=payload)
                if response.status_code == 200:
                    logger.info(
                        "Updated bill fields in Webflow CMS",
                        webflow_id=webflow_id,
                        fields=list(field_data.keys()),
                    )
                    return True
                else:
                    logger.error(
                        "Failed to update bill fields in Webflow CMS",
                        webflow_id=webflow_id,
                        fields=list(field_data.keys()),
                        status_code=response.status_code,
                        response_text=response.text[:200],
                    )
                    return False
            except Exception as e:
                logger.error(
                    "Error updating bill fields in Webflow CMS",
                    webflow_id=webflow_id,
                    error=str(e),
                )
                return False

    async def update_bill_gov_url(
        self,
        webflow_id: str,
        new_url: str,
        api_key: str | None = None,
    ) -> bool:
        """Update the gov-url field for a bill in Webflow CMS.

        Thin wrapper around update_bill_fields() for backward compatibility.

        Args:
            webflow_id: Webflow item ID for the bill
            new_url: New government URL to set
            api_key: Optional API key override (e.g., scheduler key with write scope).

        Returns:
            True on success, False on failure
        """
        if not new_url:
            logger.warning("Missing new_url for gov-url update")
            return False
        return await self.update_bill_fields(webflow_id, {"gov-url": new_url}, api_key)
