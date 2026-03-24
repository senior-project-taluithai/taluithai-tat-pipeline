"""
PostgreSQL Object Tracker
=========================
เก็บ progress tracking ใน PostgreSQL แทน SQLite
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger('TTCS.PostgresTracker')


class ObjectStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    ERROR = "error"
    RETRY = "retry"


class PostgresObjectTracker:
    """Track scraping progress using PostgreSQL"""
    
    def __init__(self,
                 host: str = "localhost",
                 port: int = 5432,
                 database: str = "tiktok_scraper",
                 user: str = "postgres",
                 password: str = "postgres",
                 connection_string: str = None):
        """
        Initialize PostgreSQL connection
        
        Parameters:
        -----------
        host : str
            PostgreSQL host
        port : int
            PostgreSQL port
        database : str
            Database name
        user : str
            Username
        password : str
            Password
        connection_string : str
            Full connection string (overrides other params)
            Format: "postgresql://user:password@host:port/database"
        """
        self.connection_string = connection_string
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        self.conn = None
        self._connect()
        self._create_tables()
        self._create_indexes()
        
        # Stats tracking
        self.n_scraped_total = 0
        self.n_errors_total = 0
        self.n_pending = 0
        self.n_retry = 0
        self.n_total = 0
        self.mean_iter_time = 0
        self.queue_eta = "0:00:00"
    
    def _connect(self):
        """Establish connection to PostgreSQL"""
        try:
            if self.connection_string:
                # Remove channel_binding parameter if present (not supported by psycopg2)
                conn_str = self.connection_string.replace("&channel_binding=require", "")
                self.conn = psycopg2.connect(conn_str)
            else:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
            self.conn.autocommit = False
            logger.info(f"Connected to PostgreSQL: {self.database}")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def _create_tables(self):
        """Create necessary tables"""
        create_objects_table = """
        CREATE TABLE IF NOT EXISTS objects (
            id VARCHAR(255) PRIMARY KEY,
            status VARCHAR(50) NOT NULL,
            title TEXT,
            type VARCHAR(50),
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            attempts INTEGER DEFAULT 0,
            last_error TEXT,
            last_attempt TIMESTAMP,
            file_path TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        create_metadata_table = """
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create function and trigger for updated_at
        create_update_function = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        """
        
        create_trigger = """
        DROP TRIGGER IF EXISTS update_objects_updated_at ON objects;
        CREATE TRIGGER update_objects_updated_at
            BEFORE UPDATE ON objects
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_objects_table)
                cur.execute(create_metadata_table)
                try:
                    cur.execute(create_update_function)
                    cur.execute(create_trigger)
                except psycopg2.Error as trigger_error:
                    # Ignore concurrent update errors for triggers/functions
                    # (they're already created by another process)
                    if "concurrently updated" not in str(trigger_error):
                        raise
                    logger.debug(f"Trigger already exists, skipping: {trigger_error}")
            self.conn.commit()
            logger.info("PostgreSQL tables created successfully")
        except psycopg2.Error as e:
            self.conn.rollback()
            # If it's a concurrent update error, tables are likely already set up
            if "concurrently updated" in str(e):
                logger.warning("Tables being created by another process, continuing...")
                self._connect()  # Reconnect to get fresh state
            else:
                logger.error(f"Error creating tables: {e}")
                raise
    
    def _create_indexes(self):
        """Create indexes for better performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_objects_status ON objects(status)",
            "CREATE INDEX IF NOT EXISTS idx_objects_type ON objects(type)",
            "CREATE INDEX IF NOT EXISTS idx_objects_added_at ON objects(added_at)",
            "CREATE INDEX IF NOT EXISTS idx_objects_completed_at ON objects(completed_at)",
        ]
        
        try:
            with self.conn.cursor() as cur:
                for index_sql in indexes:
                    try:
                        cur.execute(index_sql)
                    except psycopg2.Error as idx_error:
                        if "concurrently updated" not in str(idx_error):
                            raise
                        logger.debug(f"Index already exists, skipping: {idx_error}")
            self.conn.commit()
            logger.info("PostgreSQL indexes created successfully")
        except psycopg2.Error as e:
            self.conn.rollback()
            if "concurrently updated" in str(e):
                logger.warning("Indexes being created by another process, continuing...")
            else:
                logger.error(f"Error creating indexes: {e}")
                raise
    
    def add_object(self, id: str, title: Optional[str] = None, type: Optional[str] = None):
        """Add a new object to track"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO objects (id, status, title, type, added_at, attempts)
                    VALUES (%s, %s, %s, %s, %s, 0)
                    ON CONFLICT (id) DO NOTHING
                """, (id, ObjectStatus.PENDING.value, title, type, datetime.now()))
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error adding object {id}: {e}")
            raise
    
    def add_objects(self, ids: List[str], title: Optional[str] = None, type: Optional[str] = None):
        """Add multiple objects to track"""
        try:
            current_time = datetime.now()
            with self.conn.cursor() as cur:
                for id in ids:
                    cur.execute("""
                        INSERT INTO objects (id, status, title, type, added_at, attempts)
                        VALUES (%s, %s, %s, %s, %s, 0)
                        ON CONFLICT (id) DO NOTHING
                    """, (id, ObjectStatus.PENDING.value, title, type, current_time))
            self.conn.commit()
            logger.info(f"Added {len(ids)} objects to tracker")
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error adding objects: {e}")
            raise
    
    def mark_completed(self, id: str, file_path: Optional[str] = None):
        """Mark object as completed"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE objects 
                    SET status = %s, completed_at = %s, file_path = %s
                    WHERE id = %s
                """, (ObjectStatus.COMPLETED.value, datetime.now(), file_path, id))
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error marking object {id} as completed: {e}")
            raise
    
    def mark_error(self, id: str, error_message: Optional[str] = None):
        """Mark object as error"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE objects 
                    SET status = %s, last_error = %s, last_attempt = %s, attempts = attempts + 1
                    WHERE id = %s
                """, (ObjectStatus.ERROR.value, error_message, datetime.now(), id))
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error marking object {id} as error: {e}")
            raise
    
    def get_pending_objects(self, type: str = "all", limit: int = 100) -> Dict[str, Dict]:
        """Get pending objects"""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                if type == "all":
                    cur.execute("""
                        SELECT id, status, title, type, added_at
                        FROM objects
                        WHERE status = %s
                        ORDER BY added_at
                        LIMIT %s
                    """, (ObjectStatus.PENDING.value, limit))
                else:
                    cur.execute("""
                        SELECT id, status, title, type, added_at
                        FROM objects
                        WHERE status = %s AND type = %s
                        ORDER BY added_at
                        LIMIT %s
                    """, (ObjectStatus.PENDING.value, type, limit))
                
                rows = cur.fetchall()
                return {row['id']: dict(row) for row in rows}
        except psycopg2.Error as e:
            logger.error(f"Error getting pending objects: {e}")
            raise
    
    def get_stats(self, type: str = "all") -> Dict[str, int]:
        """Get statistics"""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                if type == "all":
                    cur.execute("""
                        SELECT status, COUNT(*) as count
                        FROM objects
                        GROUP BY status
                    """)
                else:
                    cur.execute("""
                        SELECT status, COUNT(*) as count
                        FROM objects
                        WHERE type = %s
                        GROUP BY status
                    """, (type,))
                
                rows = cur.fetchall()
                stats = {
                    "pending": 0,
                    "completed": 0,
                    "errors": 0,
                    "retry": 0
                }
                for row in rows:
                    if row['status'] == 'pending':
                        stats['pending'] = row['count']
                    elif row['status'] == 'completed':
                        stats['completed'] = row['count']
                    elif row['status'] == 'error':
                        stats['errors'] = row['count']
                    elif row['status'] == 'retry':
                        stats['retry'] = row['count']
                
                return stats
        except psycopg2.Error as e:
            logger.error(f"Error getting stats: {e}")
            raise
    
    def get_all_objects(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get all objects with pagination"""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM objects
                    ORDER BY added_at DESC
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                return [dict(row) for row in cur.fetchall()]
        except psycopg2.Error as e:
            logger.error(f"Error getting all objects: {e}")
            raise
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")


# Example usage
if __name__ == "__main__":
    tracker = PostgresObjectTracker(
        host="localhost",
        port=5432,
        database="tiktok_scraper",
        user="postgres",
        password="postgres"
    )
    
    print(f"Stats: {tracker.get_stats()}")
    tracker.close()
