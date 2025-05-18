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

        # Initialize HubSpot client
        hubspot = HubSpotClient(access_token=config['hubspot_access_token'])

        # Fetch and store HubSpot data
        logger.info("Fetching HubSpot contacts...")
        contacts = hubspot.fetch_contacts()
        for contact in contacts:
            contact["metadata"] = hubspot.fetch_metadata("contact", contact)
        db.store_contacts(contacts)

        logger.info("Fetching HubSpot companies...")
        companies = hubspot.fetch_companies()
        for company in companies:
            company["metadata"] = hubspot.fetch_metadata("company", company)
        db.store_companies(companies)

        logger.info("Fetching HubSpot leads...")
        leads = hubspot.fetch_leads()
        db.store_leads(leads)

        logger.info("Fetching HubSpot deals...")
        deals = hubspot.fetch_deals()
        for deal in deals:
            deal["metadata"] = hubspot.fetch_metadata("deal", deal)
        db.store_deals(deals)

        # Initialize Zendesk client
        zendesk = ZendeskClient(
            domain=config['zendesk_domain'],
            email=config['zendesk_email'],
            api_token=config['zendesk_api_token']
        )

        # Fetch and store Zendesk data
        logger.info("Fetching Zendesk tickets...")
        tickets = zendesk.fetch_tickets(query="type:ticket status:open")
        for ticket in tickets:
            ticket["metadata"] = ticket.get("custom_fields", {})
        db.store_tickets(tickets)

        logger.info("Processing Zendesk tickets for comments and users...")
        unique_users = set()
        for ticket in tickets:
            comments = zendesk.fetch_comments_metadata(ticket['id'])
            db.store_comments(comments)
            user_ids = {comment['author_id'] for comment in comments}
            unique_users.update(user_ids)

        logger.info("Fetching Zendesk users...")
        users = [zendesk.fetch_user(user_id) for user_id in unique_users]
        db.store_users(users)

        logger.info("Data fetching and storage completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()