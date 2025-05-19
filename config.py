import os
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

def load_config() -> Dict[str, str]:
    """Load configuration from environment variables or .env file."""
    load_dotenv()  # Load .env file
    project_dir = Path(__file__).parent
    default_db_path = str(project_dir / 'crm_data.db')
    config = {
        # HubSpot configuration
        'hubspot_access_token': os.getenv('HUBSPOT_ACCESS_TOKEN', 'your_hubspot_access_token'),
        # Zendesk configuration
        'zendesk_domain': os.getenv('ZENDESK_DOMAIN', 'yourcompany.zendesk.com'),
        'zendesk_email': os.getenv('ZENDESK_EMAIL', 'your_email@example.com'),
        'zendesk_api_token': os.getenv('ZENDESK_API_TOKEN', 'your_zendesk_api_token'),
        # Google Play configuration (secrets only)
        'google_play_client_id': os.getenv('GOOGLE_PLAY_CLIENT_ID', 'your_google_play_client_id'),
        'google_play_client_secret': os.getenv('GOOGLE_PLAY_CLIENT_SECRET', 'your_google_play_client_secret'),
        'google_play_refresh_token': os.getenv('GOOGLE_PLAY_REFRESH_TOKEN', 'your_google_play_refresh_token'),
        # Database configuration
        'database_path': os.getenv('DATABASE_PATH', default_db_path)
    }

    return config