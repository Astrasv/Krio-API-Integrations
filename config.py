import os
from pathlib import Path
from typing import Dict
from dotenv import load_dotenv

def load_config() -> Dict[str, str]:
    """Load configuration from environment variables or .env file."""
    load_dotenv()  # Load .env file
    config = {
        'zendesk_domain': os.getenv('ZENDESK_DOMAIN', 'yourcompany.zendesk.com'),
        'zendesk_email': os.getenv('ZENDESK_EMAIL', 'your_email@example.com'),
        'zendesk_api_token': os.getenv('ZENDESK_API_TOKEN', 'your_api_token'),
        'database_path': os.getenv('DATABASE_PATH', str(Path(__file__).parent / 'zendesk_data.db'))
    }

    return config