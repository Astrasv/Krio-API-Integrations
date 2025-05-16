import logging
from config import load_config
from zendesk_api import ZendeskClient
from database import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('zendesk_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function to fetch and store Zendesk ticket comments and user data."""
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded.")

        # Initialize Zendesk client
        zendesk = ZendeskClient(
            domain=config['zendesk_domain'],
            email=config['zendesk_email'],
            api_token=config['zendesk_api_token']
        )

        # Initialize database
        db = Database(config['database_path'])

        # Fetch tickets
        tickets = zendesk.fetch_tickets(query="type:ticket status:open")
        
        # Process each ticket
        for ticket in tickets:
            logger.info(f"Processing ticket {ticket['id']}.")
            
            # Fetch and store comments with metadata
            comments = zendesk.fetch_comments_metadata(ticket['id'])
            db.store_comments(comments)
            
            # Fetch and store unique users
            user_ids = set(comment['author_id'] for comment in comments)
            for user_id in user_ids:
                user = zendesk.fetch_user(user_id)
                db.store_user(user)

        logger.info("Data fetching and storage completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()