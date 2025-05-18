import requests
from typing import List, Dict, Any
import logging
import time
import json

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
            params = {"limit": 100, "properties": ["firstname", "lastname", "email", "createdate", "lastmodifieddate", "lifecyclestage"]}
            if after:
                params["after"] = after
            data = self._make_request("objects/contacts", params=params)
            for contact in data.get("results", []):
                props = contact.get("properties", {})
                payload = {k: v for k, v in contact.items() if k not in ["id", "properties", "createdAt", "updatedAt"]}
                payload["properties"] = {k: v for k, v in props.items() if k not in ["firstname", "lastname", "email", "createdate", "lastmodifieddate", "lifecyclestage"]}
                contacts.append({
                    "id": contact["id"],
                    "type": "contact",
                    "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
                    "email": props.get("email", ""),
                    "created_at": props.get("createdate", ""),
                    "updated_at": props.get("lastmodifieddate", ""),
                    "source": "hubspot",
                    "json_payload": json.dumps(payload)
                })
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
            params = {"limit": 100, "properties": ["name", "domain", "createdate", "lastmodifieddate"]}
            if after:
                params["after"] = after
            data = self._make_request("objects/companies", params=params)
            for company in data.get("results", []):
                props = company.get("properties", {})
                payload = {k: v for k, v in company.items() if k not in ["id", "properties", "createdAt", "updatedAt"]}
                payload["properties"] = {k: v for k, v in props.items() if k not in ["name", "domain", "createdate", "lastmodifieddate"]}
                companies.append({
                    "id": company["id"],
                    "type": "company",
                    "name": props.get("name", ""),
                    "domain": props.get("domain", ""),
                    "created_at": props.get("createdate", ""),
                    "updated_at": props.get("lastmodifieddate", ""),
                    "source": "hubspot",
                    "json_payload": json.dumps(payload)
                })
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
                "parent_type": "contact",
                "parent_id": contact["id"],
                "child_type": "lead",
                "child_id": contact["id"],
                "relationship_type": "lead_status",
                "json_payload": json.dumps({"lifecyclestage": contact["json_payload"].get("properties", {}).get("lifecyclestage", "")})
            }
            for contact in contacts
            if json.loads(contact["json_payload"]).get("properties", {}).get("lifecyclestage", "").lower() == "lead"
        ]
        logger.info(f"Fetched {len(leads)} leads.")
        return leads

    def fetch_deals(self) -> List[Dict]:
        """Fetch all deals with pagination."""
        deals = []
        after = None
        while True:
            params = {"limit": 100, "properties": ["dealname", "amount", "dealstage", "createdate", "lastmodifieddate"]}
            if after:
                params["after"] = after
            data = self._make_request("objects/deals", params=params)
            for deal in data.get("results", []):
                props = deal.get("properties", {})
                payload = {k: v for k, v in deal.items() if k not in ["id", "properties", "createdAt", "updatedAt"]}
                payload["properties"] = {k: v for k, v in props.items() if k not in ["dealname", "amount", "dealstage", "createdate", "lastmodifieddate"]}
                deals.append({
                    "id": deal["id"],
                    "type": "deal",
                    "name": props.get("dealname", ""),
                    "status": props.get("dealstage", ""),
                    "amount": props.get("amount", ""),
                    "created_at": props.get("createdate", ""),
                    "updated_at": props.get("lastmodifieddate", ""),
                    "source": "hubspot",
                    "json_payload": json.dumps(payload)
                })
            logger.info(f"Fetched {len(deals)} deals so far.")
            paging = data.get("paging")
            if not paging or not paging.get("next"):
                break
            after = paging["next"]["after"]
        return deals