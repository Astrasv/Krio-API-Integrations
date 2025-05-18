import logging
from config import load_config
from hubspot.hubspot_api import HubSpotClient
from zendesk.zendesk_api import ZendeskClient
from storage.database import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crm_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function to fetch and store HubSpot and Zendesk data."""
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded.")

        # Initialize database
        db = Database(config['database_path'])
        logger.info(f"Using database at: {config['database_path']}")

        # Initialize HubSpot client
        hubspot = HubSpotClient(access_token=config['hubspot_access_token'])

        # Fetch and store HubSpot data
        logger.info("Fetching HubSpot contacts...")
        contacts = hubspot.fetch_contacts()
        logger.info(f"Fetched {len(contacts)} contacts.")
        db.store_entities(contacts)

        logger.info("Fetching HubSpot companies...")
        companies = hubspot.fetch_companies()
        logger.info(f"Fetched {len(companies)} companies.")
        db.store_entities(companies)

        logger.info("Fetching HubSpot leads...")
        leads = hubspot.fetch_leads()
        logger.info(f"Fetched {len(leads)} leads.")
        db.store_relationships(leads)

        logger.info("Fetching HubSpot deals...")
        deals = hubspot.fetch_deals()
        logger.info(f"Fetched {len(deals)} deals.")
        db.store_entities(deals)

        # Initialize Zendesk client
        zendesk = ZendeskClient(
            domain=config['zendesk_domain'],
            email=config['zendesk_email'],
            api_token=config['zendesk_api_token']
        )

        # Fetch and store Zendesk data
        logger.info("Fetching Zendesk tickets...")
        tickets = zendesk.fetch_tickets(query="type:ticket status:open")
        logger.info(f"Fetched {len(tickets)} tickets.")
        db.store_entities(tickets)

        logger.info("Processing Zendesk tickets for comments and users...")
        unique_users = set()
        for ticket in tickets:
            comments = zendesk.fetch_comments(ticket['id'])
            logger.info(f"Fetched {len(comments)} comments for ticket {ticket['id']}.")
            db.store_interactions(comments)
            user_ids = {comment['author_id'] for comment in comments}
            unique_users.update(user_ids)

        logger.info("Fetching Zendesk users...")
        users = [zendesk.fetch_user(user_id) for user_id in unique_users]
        logger.info(f"Fetched {len(users)} users.")
        db.store_entities(users)

        logger.info("Data fetching and storage completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()