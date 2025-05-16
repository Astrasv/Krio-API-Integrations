import sqlite3
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class Database:
    """Manage SQLite database operations."""
    
    def __init__(self, db_path: str):
        """Initialize database connection and create tables."""
        self.db_path = db_path
        self.create_tables()
        logger.info(f"Database initialized at {db_path}.")

    def create_tables(self):
        """Create tables for comments, users, and metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Comments table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY,
                    ticket_id INTEGER,
                    author_id INTEGER,
                    body TEXT,
                    created_at TEXT,
                    public BOOLEAN
                )
            ''')
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    role TEXT
                )
            ''')
            # Metadata table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    comment_id INTEGER,
                    key TEXT,
                    value TEXT,
                    FOREIGN KEY (comment_id) REFERENCES comments(id)
                )
            ''')
            conn.commit()
            logger.info("Database tables created or verified.")

    def store_comments(self, comments: List[Dict[str, Any]]):
        """Store comments and their metadata in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for comment in comments:
                # Insert comment
                cursor.execute('''
                    INSERT OR REPLACE INTO comments (id, ticket_id, author_id, body, created_at, public)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    comment['id'],
                    comment['ticket_id'],
                    comment['author_id'],
                    comment['body'],
                    comment['created_at'],
                    comment['public']
                ))
                # Insert metadata
                for key, value in comment['metadata'].items():
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            cursor.execute('''
                                INSERT INTO metadata (comment_id, key, value)
                                VALUES (?, ?, ?)
                            ''', (comment['id'], f"{key}.{sub_key}", str(sub_value)))
                    else:
                        cursor.execute('''
                            INSERT INTO metadata (comment_id, key, value)
                            VALUES (?, ?, ?)
                        ''', (comment['id'], key, str(value)))
            conn.commit()
            logger.info(f"Stored {len(comments)} comments and their metadata.")

    def store_user(self, user: Dict):
        """Store user details in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO users (id, name, email, role)
                VALUES (?, ?, ?, ?)
            ''', (
                user.get('id'),
                user.get('name', ''),
                user.get('email', ''),
                user.get('role', '')
            ))
            conn.commit()
            logger.info(f"Stored user {user.get('id')}.")