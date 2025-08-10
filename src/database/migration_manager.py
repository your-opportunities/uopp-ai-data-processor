"""Database migration manager for handling schema changes."""

import asyncio
from typing import List, Optional, Callable, Awaitable
from datetime import datetime
import asyncpg

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


class Migration:
    """Represents a database migration."""
    
    def __init__(self, version: int, name: str, up_func: Callable[[asyncpg.Connection], Awaitable[None]]):
        self.version = version
        self.name = name
        self.up_func = up_func
    
    async def up(self, conn: asyncpg.Connection) -> None:
        """Execute the migration."""
        await self.up_func(conn)


class MigrationManager:
    """Manages database migrations."""
    
    def __init__(self):
        self.connection_string = settings.postgres.connection_string
        self.migrations: List[Migration] = []
        self._register_migrations()
    
    def _register_migrations(self) -> None:
        """Register all migrations."""
        self.migrations = [
            Migration(1, "create_processed_events_table", self._migration_001_create_processed_events_table),
            Migration(2, "add_search_indexes", self._migration_002_add_search_indexes),
            Migration(3, "add_new_fields_indexes", self._migration_003_add_new_fields_indexes),
        ]
        # Sort by version
        self.migrations.sort(key=lambda m: m.version)
    
    async def create_migrations_table(self, conn: asyncpg.Connection) -> None:
        """Create the migrations tracking table."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                id SERIAL PRIMARY KEY,
                version INTEGER UNIQUE NOT NULL,
                migration_name VARCHAR(255) NOT NULL,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
    
    async def get_applied_migrations(self, conn: asyncpg.Connection) -> List[int]:
        """Get list of already applied migration versions."""
        rows = await conn.fetch("""
            SELECT version FROM migrations 
            ORDER BY version
        """)
        return [row['version'] for row in rows]
    
    async def mark_migration_applied(
        self, 
        conn: asyncpg.Connection, 
        version: int,
        migration_name: str
    ) -> None:
        """Mark a migration as applied."""
        await conn.execute("""
            INSERT INTO migrations (version, migration_name)
            VALUES ($1, $2)
        """, version, migration_name)
    
    async def run_migration(self, conn: asyncpg.Connection, migration: Migration) -> None:
        """Run a single migration."""
        logger.info(f"Running migration: {migration.name} (version {migration.version})")
        
        try:
            # Execute the migration
            await migration.up(conn)
            
            # Mark as applied
            await self.mark_migration_applied(conn, migration.version, migration.name)
            
            logger.info(f"Migration applied successfully: {migration.name}")
            
        except Exception as e:
            logger.error(f"Migration failed: {migration.name}", error=str(e))
            raise
    
    async def run_migrations(self) -> None:
        """Run all pending migrations."""
        logger.info("Starting database migrations")
        
        try:
            # Connect to database
            conn = await asyncpg.connect(self.connection_string)
            
            try:
                # Create migrations table if it doesn't exist
                await self.create_migrations_table(conn)
                
                # Get applied migrations
                applied_versions = await self.get_applied_migrations(conn)
                
                # Find pending migrations
                pending_migrations = [
                    migration for migration in self.migrations
                    if migration.version not in applied_versions
                ]
                
                if not pending_migrations:
                    logger.info("No pending migrations")
                    return
                
                logger.info(f"Found {len(pending_migrations)} pending migrations")
                
                # Run pending migrations
                for migration in pending_migrations:
                    await self.run_migration(conn, migration)
                
                logger.info("All migrations completed successfully")
                
            finally:
                await conn.close()
                
        except Exception as e:
            logger.error("Migration process failed", error=str(e))
            raise
    
    # Migration implementations
    async def _migration_001_create_processed_events_table(self, conn: asyncpg.Connection) -> None:
        """Create the initial processed_events table."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                id SERIAL PRIMARY KEY,
                post_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                post_scraped_at TIMESTAMP WITH TIME ZONE NOT NULL,
                raw_text TEXT NOT NULL,
                extracted_data JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        # Create basic indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_post_created_at 
            ON processed_events(post_created_at)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_post_scraped_at 
            ON processed_events(post_scraped_at)
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_created_at 
            ON processed_events(created_at)
        """)
        
        # Create GIN index for JSONB queries
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_extracted_data 
            ON processed_events USING GIN (extracted_data)
        """)
        
        # Create updated_at trigger function
        await conn.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """)
        
        # Create trigger for updated_at
        await conn.execute("""
            DROP TRIGGER IF EXISTS update_processed_events_updated_at ON processed_events;
            CREATE TRIGGER update_processed_events_updated_at
                BEFORE UPDATE ON processed_events
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
        """)
    
    async def _migration_002_add_search_indexes(self, conn: asyncpg.Connection) -> None:
        """Add additional search and performance indexes."""
        # Partial indexes for specific queries
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_categories 
            ON processed_events USING GIN ((extracted_data->'categories'))
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_asap 
            ON processed_events ((extracted_data->>'is_asap')) 
            WHERE (extracted_data->>'is_asap') = 'true'
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_online 
            ON processed_events ((extracted_data->>'format')) 
            WHERE (extracted_data->>'format') = 'online'
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_offline 
            ON processed_events ((extracted_data->>'format')) 
            WHERE (extracted_data->>'format') = 'offline'
        """)
        
        # Search indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_title 
            ON processed_events ((extracted_data->>'title'))
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_city 
            ON processed_events ((extracted_data->>'city'))
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_free 
            ON processed_events ((extracted_data->>'price')) 
            WHERE (extracted_data->>'price') = 'free'
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_paid 
            ON processed_events ((extracted_data->>'price')) 
            WHERE (extracted_data->>'price') != 'free' AND (extracted_data->>'price') IS NOT NULL
        """)
    
    async def _migration_003_add_new_fields_indexes(self, conn: asyncpg.Connection) -> None:
        """Add indexes for new fields (is_regular_event, date, deadline)."""
        # Index for regular events
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_regular 
            ON processed_events ((extracted_data->>'is_regular_event')) 
            WHERE (extracted_data->>'is_regular_event') = 'true'
        """)
        
        # Index for events with dates
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_with_date 
            ON processed_events ((extracted_data->>'date')) 
            WHERE (extracted_data->>'date') IS NOT NULL
        """)
        
        # Index for events with deadlines
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_events_with_deadline 
            ON processed_events ((extracted_data->>'deadline')) 
            WHERE (extracted_data->>'deadline') IS NOT NULL
        """)
        
        logger.info("New fields indexes added successfully")
