import sqlite3
from typing import List, Dict, Any
import logging
import os
import json

logger = logging.getLogger(__name__)

class Database:
    """Manage SQLite database operations for unified HubSpot, Zendesk, and Google Play data."""
    
    def __init__(self, db_path: str):
        """Initialize database connection and create tables."""
        self.db_path = db_path
        # Ensure database file is created
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.commit()
        self.create_tables()
        logger.info(f"Database initialized at {db_path}.")

    def create_tables(self):
        """Create unified tables for entities, relationships, interactions, and Google Play data."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Entities table (contacts, companies, deals, tickets, users)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT,
                    type TEXT,
                    name TEXT,
                    email TEXT,
                    status TEXT,
                    amount TEXT,
                    domain TEXT,
                    role TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    source TEXT,
                    json_payload TEXT,
                    PRIMARY KEY (id, source)
                )
            ''')
            # Relationships table (leads, deal-company, comment-ticket)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS relationships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_type TEXT,
                    parent_id TEXT,
                    child_type TEXT,
                    child_id TEXT,
                    relationship_type TEXT,
                    json_payload TEXT
                )
            ''')
            # Interactions table (comments, notes)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS interactions (
                    id TEXT PRIMARY KEY,
                    entity_type TEXT,
                    entity_id TEXT,
                    author_id TEXT,
                    body TEXT,
                    created_at TEXT,
                    is_public BOOLEAN,
                    source TEXT,
                    json_payload TEXT
                )
            ''')
            # Google Play data table (reviews)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS google_play_data (
                    id TEXT,
                    type TEXT,
                    package_name TEXT,
                    rating TEXT,
                    comment TEXT,
                    created_at TEXT,
                    source TEXT,
                    json_payload TEXT,
                    PRIMARY KEY (id, source)
                )
            ''')
            conn.commit()
            logger.info(f"Database tables created or verified at {self.db_path}.")

    def store_entities(self, entities: List[Dict]):
        """Store entities (contacts, companies, deals, tickets, users)."""
        if not entities:
            logger.info("No entities to store.")
            return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for entity in entities:
                cursor.execute('''
                    INSERT OR REPLACE INTO entities (
                        id, type, name, email, status, amount, domain, role, created_at, updated_at, source, json_payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    entity.get("id"),
                    entity.get("type"),
                    entity.get("name", ""),
                    entity.get("email", ""),
                    entity.get("status", ""),
                    entity.get("amount", ""),
                    entity.get("domain", ""),
                    entity.get("role", ""),
                    entity.get("created_at", ""),
                    entity.get("updated_at", ""),
                    entity.get("source"),
                    entity.get("json_payload")
                ))
            conn.commit()
            logger.info(f"Stored {len(entities)} entities.")

    def store_relationships(self, relationships: List[Dict]):
        """Store relationships (leads, deal-company, etc.)."""
        if not relationships:
            logger.info("No relationships to store.")
            return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for rel in relationships:
                cursor.execute('''
                    INSERT OR REPLACE INTO relationships (
                        parent_type, parent_id, child_type, child_id, relationship_type, json_payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    rel.get("parent_type"),
                    rel.get("parent_id"),
                    rel.get("child_type"),
                    rel.get("child_id"),
                    rel.get("relationship_type"),
                    rel.get("json_payload")
                ))
            conn.commit()
            logger.info(f"Stored {len(relationships)} relationships.")

    def store_interactions(self, interactions: List[Dict]):
        """Store interactions (comments, notes)."""
        if not interactions:
            logger.info("No interactions to store.")
            return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for interaction in interactions:
                cursor.execute('''
                    INSERT OR REPLACE INTO interactions (
                        id, entity_type, entity_id, author_id, body, created_at, is_public, source, json_payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    interaction.get("id"),
                    interaction.get("entity_type"),
                    interaction.get("entity_id"),
                    interaction.get("author_id"),
                    interaction.get("body", ""),
                    interaction.get("created_at", ""),
                    interaction.get("is_public", True),
                    interaction.get("source"),
                    interaction.get("json_payload")
                ))
            conn.commit()
            logger.info(f"Stored {len(interactions)} interactions.")

    def store_google_play_data(self, data: List[Dict]):
        """Store Google Play data (reviews)."""
        if not data:
            logger.info("No Google Play data to store.")
            return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for item in data:
                cursor.execute('''
                    INSERT OR REPLACE INTO google_play_data (
                        id, type, package_name, rating, comment, created_at, source, json_payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    item.get("id"),
                    item.get("type"),
                    item.get("package_name"),
                    item.get("rating"),
                    item.get("comment", ""),
                    item.get("created_at"),
                    item.get("source"),
                    item.get("json_payload")
                ))
            conn.commit()
            logger.info(f"Stored {len(data)} Google Play data records.")