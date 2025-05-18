import requests
from requests.auth import HTTPBasicAuth
from typing import List, Dict, Any
import logging
import time
import json

logger = logging.getLogger(__name__)

class ZendeskClient:
    """Client to interact with Zendesk API using direct HTTP requests."""
    
    def __init__(self, domain: str, email: str, api_token: str):
        """Initialize Zendesk client with credentials."""
        self.base_url = f"https://{domain}/api/v2"
        self.auth = HTTPBasicAuth(f"{email}/token", api_token)
        self.headers = {
            "Content-Type": "application/json"
        }
        self.rate_limit_remaining = 100  # Default value, updated by API responses
        self.rate_limit_reset = 0  # Timestamp for rate limit reset
        logger.info("Zendesk client initialized successfully.")

    def _handle_rate_limit(self, response: requests.Response):
        """Handle Zendesk API rate limiting."""
        self.rate_limit_remaining = int(response.headers.get("X-Rate-Limit-Remaining", 0))
        self.rate_limit_reset = int(response.headers.get("X-Rate-Limit-Reset", 0))
        if self.rate_limit_remaining <= 5:
            sleep_time = max(self.rate_limit_reset - int(time.time()), 1)
            logger.warning(f"Rate limit low ({self.rate_limit_remaining}). Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make an API request with error handling and rate limit management."""
        try:
            response = requests.get(f"{self.base_url}/{endpoint}", auth=self.auth, headers=self.headers, params=params)
            self._handle_rate_limit(response)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {endpoint}: {str(e)}")
            raise

    def fetch_tickets(self, query: str = "type:ticket") -> List[Dict]:
        """Fetch tickets matching the query with pagination."""
        tickets = []
        params = {"query": query}
        while True:
            data = self._make_request("search.json", params=params)
            for ticket in data.get("results", []):
                payload = {k: v for k, v in ticket.items() if k not in ["id", "subject", "status", "created_at", "updated_at"]}
                tickets.append({
                    "id": ticket["id"],
                    "type": "ticket",
                    "name": ticket.get("subject", ""),
                    "status": ticket.get("status", ""),
                    "created_at": ticket.get("created_at", ""),
                    "updated_at": ticket.get("updated_at", ""),
                    "source": "zendesk",
                    "json_payload": json.dumps(payload)
                })
            logger.info(f"Fetched {len(tickets)} tickets so far.")
            next_page = data.get("next_page")
            if not next_page:
                break
            params["page"] = data.get("next_page").split("page=")[1].split("&")[0]
        return tickets

    def fetch_comments(self, ticket_id: int) -> List[Dict]:
        """Fetch comments for a specific ticket."""
        data = self._make_request(f"tickets/{ticket_id}/comments.json")
        comments = []
        for comment in data.get("comments", []):
            payload = {k: v for k, v in comment.items() if k not in ["id", "ticket_id", "author_id", "body", "created_at", "public"]}
            comments.append({
                "id": comment["id"],
                "entity_type": "ticket",
                "entity_id": ticket_id,
                "author_id": comment["author_id"],
                "body": comment.get("body", ""),
                "created_at": comment.get("created_at", ""),
                "is_public": comment.get("public", True),
                "source": "zendesk",
                "json_payload": json.dumps(payload)
            })
        logger.info(f"Fetched {len(comments)} comments for ticket {ticket_id}.")
        return comments

    def fetch_user(self, user_id: int) -> Dict:
        """Fetch user details by ID."""
        data = self._make_request(f"users/{user_id}.json")
        user = data.get("user", {})
        payload = {k: v for k, v in user.items() if k not in ["id", "name", "email", "role", "created_at", "updated_at"]}
        result = {
            "id": user.get("id"),
            "type": "user",
            "name": user.get("name", ""),
            "email": user.get("email", ""),
            "role": user.get("role", ""),
            "created_at": user.get("created_at", ""),
            "updated_at": user.get("updated_at", ""),
            "source": "zendesk",
            "json_payload": json.dumps(payload)
        }
        logger.info(f"Fetched user {user_id}.")
        return result