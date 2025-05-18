import requests
from typing import List, Dict, Any
import logging
import time

logger = logging.getLogger(__name__)

class HubSpotClient:
    """Client to interact with HubSpot API."""
    
    def __init__(self, access_token: str):
        """Initialize HubSpot client with access token."""
        self.base_url = "https://api.hubapi.com/crm/v3"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        self.rate_limit_remaining = 100  # Default value, updated by API responses
        self.rate_limit_reset = 0  # Timestamp for rate limit reset
        logger.info("HubSpot client initialized successfully.")

    def _handle_rate_limit(self, response: requests.Response):
        """Handle HubSpot API rate limiting (10 requests/second, 100,000/day)."""
        self.rate_limit_remaining = int(response.headers.get("X-HubSpot-RateLimit-Remaining", 100))
        self.rate_limit_reset = int(response.headers.get("X-HubSpot-RateLimit-Reset", 0))
        if self.rate_limit_remaining <= 5:
            sleep_time = max(self.rate_limit_reset - int(time.time()), 1)
            logger.warning(f"Rate limit low ({self.rate_limit_remaining}). Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make an API request with error handling and rate limit management."""
        try:
            response = requests.get(f"{self.base_url}/{endpoint}", headers=self.headers, params=params)
            self._handle_rate_limit(response)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {endpoint}: {str(e)}")
            raise

    def fetch_contacts(self) -> List[Dict]:
        """Fetch all contacts with pagination."""
        contacts = []
        after = None
        while True:
            params = {"limit": 100, "properties": ["firstname", "lastname", "email", "createdate", "lifecyclestage"]}
            if after:
                params["after"] = after
            data = self._make_request("objects/contacts", params=params)
            contacts.extend(data.get("results", []))
            logger.info(f"Fetched {len(contacts)} contacts so far.")
            paging = data.get("paging")
            if not paging or not paging.get("next"):
                break
            after = paging["next"]["after"]
        return contacts

    def fetch_companies(self) -> List[Dict]:
        """Fetch all companies with pagination."""
        companies = []
        after = None
        while True:
            params = {"limit": 100, "properties": ["name", "domain", "createdate"]}
            if after:
                params["after"] = after
            data = self._make_request("objects/companies", params=params)
            companies.extend(data.get("results", []))
            logger.info(f"Fetched {len(companies)} companies so far.")
            paging = data.get("paging")
            if not paging or not paging.get("next"):
                break
            after = paging["next"]["after"]
        return companies

    def fetch_leads(self) -> List[Dict]:
        """Fetch contacts marked as leads (lifecyclestage includes 'lead')."""
        contacts = self.fetch_contacts()
        leads = [
            {
                "id": contact["id"],
                "contact_id": contact["id"],
                "lifecyclestage": contact["properties"].get("lifecyclestage", ""),
                "created_at": contact["properties"].get("createdate", "")
            }
            for contact in contacts
            if contact["properties"].get("lifecyclestage", "").lower() == "lead"
        ]
        logger.info(f"Fetched {len(leads)} leads.")
        return leads

    def fetch_deals(self) -> List[Dict]:
        """Fetch all deals with pagination."""
        deals = []
        after = None
        while True:
            params = {"limit": 100, "properties": ["dealname", "amount", "dealstage", "createdate"]}
            if after:
                params["after"] = after
            data = self._make_request("objects/deals", params=params)
            deals.extend(data.get("results", []))
            logger.info(f"Fetched {len(deals)} deals so far.")
            paging = data.get("paging")
            if not paging or not paging.get("next"):
                break
            after = paging["next"]["after"]
        return deals

    def fetch_metadata(self, entity_type: str, entity: Dict) -> List[Dict[str, Any]]:
        """Extract metadata (properties) for a HubSpot entity."""
        metadata = []
        properties = entity.get("properties", {})
        for key, value in properties.items():
            metadata.append({
                "entity_type": entity_type,
                "entity_id": entity["id"],
                "key": key,
                "value": str(value) if value is not None else ""
            })
        return metadata