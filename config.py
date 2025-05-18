import os
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

def load_config() -> Dict[str, str]:
    """Load configuration from environment variables or .env file."""
    load_dotenv()  # Load .env file
    config = {
        # HubSpot configuration
        'hubspot_access_token': os.getenv('HUBSPOT_ACCESS_TOKEN'),
        
        # Zendesk configuration
        'zendesk_domain': os.getenv('ZENDESK_DOMAIN'),
        'zendesk_email': os.getenv('ZENDESK_EMAIL'),
        'zendesk_api_token': os.getenv('ZENDESK_API_TOKEN'),
        
        # Database configuration
        'database_path': os.getenv('DATABASE_PATH', str(Path(__file__).parent / 'crm_data.db'))
    }

    return config