import sqlite3
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class Database:
    """Manage SQLite database operations for HubSpot and Zendesk data."""
    
    def __init__(self, db_path: str):
        """Initialize database connection and create tables."""
        self.db_path = db_path
        self.create_tables()
        logger.info(f"Database initialized at {db_path}.")

    def create_tables(self):
        """Create tables for HubSpot and Zendesk entities and metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # HubSpot tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contacts (
                    id TEXT PRIMARY KEY,
                    firstname TEXT,
                    lastname TEXT,
                    email TEXT,
                    created_at TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS companies (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    domain TEXT,
                    created_at TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS leads (
                    id TEXT PRIMARY KEY,
                    contact_id TEXT,
                    lifecyclestage TEXT,
                    created_at TEXT,
                    FOREIGN KEY (contact_id) REFERENCES contacts(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS deals (
                    id TEXT PRIMARY KEY,
                    dealname TEXT,
                    amount TEXT,
                    dealstage TEXT,
                    created_at TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS hubspot_metadata (
                    entity_type TEXT,
                    entity_id TEXT,
                    key TEXT,
                    value TEXT,
                    PRIMARY KEY (entity_type, entity_id, key)
                )
            ''')
            # Zendesk tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tickets (
                    id INTEGER PRIMARY KEY,
                    subject TEXT,
                    status TEXT,
                    created_at TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY,
                    ticket_id INTEGER,
                    author_id INTEGER,
                    body TEXT,
                    created_at TEXT,
                    public BOOLEAN,
                    FOREIGN KEY (ticket_id) REFERENCES tickets(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    role TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS zendesk_metadata (
                    entity_type TEXT,
                    entity_id INTEGER,
                    key TEXT,
                    value TEXT,
                    PRIMARY KEY (entity_type, entity_id, key)
                )
            ''')
            conn.commit()
            logger.info("Database tables created or verified.")

    def store_contacts(self, contacts: List[Dict]):
        """Store HubSpot contacts and their metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for contact in contacts:
                props = contact.get("properties", {})
                cursor.execute('''
                    INSERT OR REPLACE INTO contacts (id, firstname, lastname, email, created_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    contact["id"],
                    props.get("firstname", ""),
                    props.get("lastname", ""),
                    props.get("email", ""),
                    props.get("createdate", "")
                ))
                # Store metadata
                for meta in contact.get("metadata", []):
                    cursor.execute('''
                        INSERT OR REPLACE INTO hubspot_metadata (entity_type, entity_id, key, value)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        meta["entity_type"],
                        meta["entity_id"],
                        meta["key"],
                        meta["value"]
                    ))
            conn.commit()
            logger.info(f"Stored {len(contacts)} contacts and their metadata.")

    def store_companies(self, companies: List[Dict]):
        """Store HubSpot companies and their metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for company in companies:
                props = company.get("properties", {})
                cursor.execute('''
                    INSERT OR REPLACE INTO companies (id, name, domain, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (
                    company["id"],
                    props.get("name", ""),
                    props.get("domain", ""),
                    props.get("createdate", "")
                ))
                # Store metadata
                for meta in company.get("metadata", []):
                    cursor.execute('''
                        INSERT OR REPLACE INTO hubspot_metadata (entity_type, entity_id, key, value)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        meta["entity_type"],
                        meta["entity_id"],
                        meta["key"],
                        meta["value"]
                    ))
            conn.commit()
            logger.info(f"Stored {len(companies)} companies and their metadata.")

    def store_leads(self, leads: List[Dict]):
        """Store HubSpot leads."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for lead in leads:
                cursor.execute('''
                    INSERT OR REPLACE INTO leads (id, contact_id, lifecyclestage, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (
                    lead["id"],
                    lead["contact_id"],
                    lead["lifecyclestage"],
                    lead["created_at"]
                ))
            conn.commit()
            logger.info(f"Stored {len(leads)} leads.")

    def store_deals(self, deals: List[Dict]):
        """Store HubSpot deals and their metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for deal in deals:
                props = deal.get("properties", {})
                cursor.execute('''
                    INSERT OR REPLACE INTO deals (id, dealname, amount, dealstage, created_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    deal["id"],
                    props.get("dealname", ""),
                    props.get("amount", ""),
                    props.get("dealstage", ""),
                    props.get("createdate", "")
                ))
                # Store metadata
                for meta in deal.get("metadata", []):
                    cursor.execute('''
                        INSERT OR REPLACE INTO hubspot_metadata (entity_type, entity_id, key, value)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        meta["entity_type"],
                        meta["entity_id"],
                        meta["key"],
                        meta["value"]
                    ))
            conn.commit()
            logger.info(f"Stored {len(deals)} deals and their metadata.")

    def store_tickets(self, tickets: List[Dict]):
        """Store Zendesk tickets and their metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for ticket in tickets:
                cursor.execute('''
                    INSERT OR REPLACE INTO tickets (id, subject, status, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (
                    ticket["id"],
                    ticket.get("subject", ""),
                    ticket.get("status", ""),
                    ticket.get("created_at", "")
                ))
                # Store metadata (custom_fields is a list of dictionaries)
                for field in ticket.get("metadata", []):
                    field_id = field.get("id")
                    value = field.get("value")
                    if field_id is not None and value is not None:
                        cursor.execute('''
                            INSERT OR REPLACE INTO zendesk_metadata (entity_type, entity_id, key, value)
                            VALUES (?, ?, ?, ?)
                        ''', ("ticket", ticket["id"], str(field_id), str(value)))
            conn.commit()
            logger.info(f"Stored {len(tickets)} tickets and their metadata.")

    def store_comments(self, comments: List[Dict]):
        """Store Zendesk comments and their metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for comment in comments:
                cursor.execute('''
                    INSERT OR REPLACE INTO comments (id, ticket_id, author_id, body, created_at, public)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    comment["id"],
                    comment["ticket_id"],
                    comment["author_id"],
                    comment["body"],
                    comment["created_at"],
                    comment["public"]
                ))
                # Store metadata
                for key, value in comment.get("metadata", {}).items():
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            cursor.execute('''
                                INSERT OR REPLACE INTO zendesk_metadata (entity_type, entity_id, key, value)
                                VALUES (?, ?, ?, ?)
                            ''', ("comment", comment["id"], f"{key}.{sub_key}", str(sub_value)))
                    else:
                        cursor.execute('''
                            INSERT OR REPLACE INTO zendesk_metadata (entity_type, entity_id, key, value)
                            VALUES (?, ?, ?, ?)
                        ''', ("comment", comment["id"], key, str(value)))
            conn.commit()
            logger.info(f"Stored {len(comments)} comments and their metadata.")

    def store_users(self, users: List[Dict]):
        """Store Zendesk users."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for user in users:
                cursor.execute('''
                    INSERT OR REPLACE INTO users (id, name, email, role)
                    VALUES (?, ?, ?, ?)
                ''', (
                    user.get("id"),
                    user.get("name", ""),
                    user.get("email", ""),
                    user.get("role", "")
                ))
            conn.commit()
            logger.info(f"Stored {len(users)} users.")