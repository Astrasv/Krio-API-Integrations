import requests
import json
import logging
import time
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class GooglePlayClient:
    """Client to interact with Google Play Android Developer API using direct HTTP requests."""
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, package_name: str):
        """Initialize Google Play client with OAuth 2.0 credentials and package name."""
        self.base_url = f"https://www.googleapis.com/androidpublisher/v3/applications/{package_name}"
        self.token_url = "https://accounts.google.com/o/oauth2/token"
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.package_name = package_name
        self.access_token = None
        self._refresh_access_token()
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        logger.info(f"Google Play client initialized for package {package_name}.")

    def _refresh_access_token(self):
        """Refresh OAuth 2.0 access token using the refresh token."""
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token
        }
        try:
            response = requests.post(self.token_url, data=payload)
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            if not self.access_token:
                raise ValueError("No access token received in token response.")
            logger.info("Access token refreshed successfully.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to refresh access token: {str(e)}")
            raise

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make an API request with error handling and token refresh."""
        try:
            response = requests.get(f"{self.base_url}/{endpoint}", headers=self.headers, params=params)
            if response.status_code == 401:
                logger.warning("Unauthorized request. Refreshing token and retrying...")
                self._refresh_access_token()
                self.headers["Authorization"] = f"Bearer {self.access_token}"
                response = requests.get(f"{self.base_url}/{endpoint}", headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {endpoint}: {str(e)}")
            raise

    def fetch_reviews(self) -> List[Dict]:
        """Fetch user reviews for the app."""
        reviews = []
        token = None
        while True:
            params = {"token": token} if token else {}
            data = self._make_request("reviews", params=params)
            for review in data.get("reviews", []):
                comment = review.get("comments", [{}])[0].get("userComment", {})
                payload = {
                    k: v for k, v in review.items()
                    if k not in ["reviewId", "comments"]
                }
                payload["userComment"] = {
                    k: v for k, v in comment.items()
                    if k not in ["text", "starRating", "lastModified"]
                }
                reviews.append({
                    "id": review.get("reviewId"),
                    "type": "review",
                    "package_name": self.package_name,
                    "rating": str(comment.get("starRating", "")),
                    "comment": comment.get("text", ""),
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "source": "google_play",
                    "json_payload": json.dumps(payload)
                })
            logger.info(f"Fetched {len(reviews)} reviews so far.")
            token = data.get("tokenPagination", {}).get("nextPageToken")
            if not token:
                break
        return reviews