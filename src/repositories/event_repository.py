"""PostgreSQL repository for storing processed Ukrainian events."""

import json
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import asyncpg
from asyncpg import Pool, Connection

from ..config.settings import settings
from ..models.ukrainian_event import UkrainianEvent
from ..utils.logger import get_logger

logger = get_logger(__name__)


class EventRepository:
    """PostgreSQL repository for storing processed Ukrainian events."""
    
    def __init__(self):
        self.pool: Optional[Pool] = None
        self._connection_string = settings.postgres.connection_string
    
    async def connect(self) -> None:
        """Establish connection pool to PostgreSQL."""
        try:
            self.pool = await asyncpg.create_pool(
                self._connection_string,
                min_size=5,
                max_size=20,
                command_timeout=60,
                server_settings={
                    'application_name': f"{settings.app.name}-events"
                }
            )
            logger.info("Event repository connection pool established")
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL for events", error=str(e))
            raise
    
    async def disconnect(self) -> None:
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Event repository connection pool closed")
    
    async def create_tables(self) -> None:
        """Create necessary database tables (deprecated - use migrations instead)."""
        logger.warning("create_tables() is deprecated - use migrations instead")
        # This method is kept for backward compatibility but should not be used
        # Migrations handle table creation
    
    async def save_event(
        self, 
        post_created_at: datetime,
        post_scraped_at: datetime,
        raw_text: str,
        ukrainian_event: UkrainianEvent
    ) -> int:
        """Save a processed Ukrainian event to the database."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO processed_events 
                (post_created_at, post_scraped_at, raw_text, extracted_data)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """, 
            post_created_at,
            post_scraped_at,
            raw_text,
            json.dumps(ukrainian_event.model_dump())
            )
            
            event_id = row['id']
            logger.info("Event saved successfully", event_id=event_id, title=ukrainian_event.title)
            return event_id
    
    async def get_event_by_id(self, event_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve an event by ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM processed_events 
                WHERE id = $1
            """, event_id)
            
            if row:
                return {
                    'id': row['id'],
                    'post_created_at': row['post_created_at'],
                    'post_scraped_at': row['post_scraped_at'],
                    'raw_text': row['raw_text'],
                    'extracted_data': json.loads(row['extracted_data']) if isinstance(row['extracted_data'], str) else row['extracted_data'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
            return None
    
    async def get_events_by_category(self, category: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get events by category."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM processed_events 
                WHERE extracted_data->>'categories' LIKE $1
                ORDER BY created_at DESC 
                LIMIT $2
            """, f'%{category}%', limit)
            
            return [
                {
                    'id': row['id'],
                    'post_created_at': row['post_created_at'],
                    'post_scraped_at': row['post_scraped_at'],
                    'raw_text': row['raw_text'],
                    'extracted_data': json.loads(row['extracted_data']) if isinstance(row['extracted_data'], str) else row['extracted_data'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                for row in rows
            ]
    
    async def get_asap_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get urgent/ASAP events."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM processed_events 
                WHERE extracted_data->>'is_asap' = 'true'
                ORDER BY created_at DESC 
                LIMIT $1
            """, limit)
            
            return [
                {
                    'id': row['id'],
                    'post_created_at': row['post_created_at'],
                    'post_scraped_at': row['post_scraped_at'],
                    'raw_text': row['raw_text'],
                    'extracted_data': json.loads(row['extracted_data']) if isinstance(row['extracted_data'], str) else row['extracted_data'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                for row in rows
            ]
    
    async def get_events_by_format(self, format_type: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get events by format (online/offline)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM processed_events 
                WHERE extracted_data->>'format' = $1
                ORDER BY created_at DESC 
                LIMIT $2
            """, format_type, limit)
            
            return [
                {
                    'id': row['id'],
                    'post_created_at': row['post_created_at'],
                    'post_scraped_at': row['post_scraped_at'],
                    'raw_text': row['raw_text'],
                    'extracted_data': json.loads(row['extracted_data']) if isinstance(row['extracted_data'], str) else row['extracted_data'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                for row in rows
            ]
    
    async def get_events_by_city(self, city: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get events by city."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM processed_events 
                WHERE extracted_data->>'city' ILIKE $1
                ORDER BY created_at DESC 
                LIMIT $2
            """, f'%{city}%', limit)
            
            return [
                {
                    'id': row['id'],
                    'post_created_at': row['post_created_at'],
                    'post_scraped_at': row['post_scraped_at'],
                    'raw_text': row['raw_text'],
                    'extracted_data': json.loads(row['extracted_data']) if isinstance(row['extracted_data'], str) else row['extracted_data'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                for row in rows
            ]
    
    async def get_recent_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent events."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM processed_events 
                ORDER BY created_at DESC 
                LIMIT $1
            """, limit)
            
            return [
                {
                    'id': row['id'],
                    'post_created_at': row['post_created_at'],
                    'post_scraped_at': row['post_scraped_at'],
                    'raw_text': row['raw_text'],
                    'extracted_data': json.loads(row['extracted_data']) if isinstance(row['extracted_data'], str) else row['extracted_data'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                for row in rows
            ]
    
    async def search_events(self, search_term: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Search events by text content."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM processed_events 
                WHERE raw_text ILIKE $1 OR extracted_data->>'title' ILIKE $1
                ORDER BY created_at DESC 
                LIMIT $2
            """, f'%{search_term}%', limit)
            
            return [
                {
                    'id': row['id'],
                    'post_created_at': row['post_created_at'],
                    'post_scraped_at': row['post_scraped_at'],
                    'raw_text': row['raw_text'],
                    'extracted_data': json.loads(row['extracted_data']) if isinstance(row['extracted_data'], str) else row['extracted_data'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                for row in rows
            ]
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get event processing statistics."""
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN extracted_data->>'is_asap' = 'true' THEN 1 END) as asap_events,
                    COUNT(CASE WHEN extracted_data->>'format' = 'online' THEN 1 END) as online_events,
                    COUNT(CASE WHEN extracted_data->>'format' = 'offline' THEN 1 END) as offline_events,
                    COUNT(CASE WHEN extracted_data->>'price' = 'free' THEN 1 END) as free_events,
                    COUNT(CASE WHEN extracted_data->>'price' != 'free' AND extracted_data->>'price' IS NOT NULL THEN 1 END) as paid_events,
                    MIN(created_at) as first_event_date,
                    MAX(created_at) as last_event_date
                FROM processed_events
            """)
            
            # Get category statistics
            category_stats = await conn.fetch("""
                SELECT 
                    jsonb_array_elements_text(extracted_data->'categories') as category,
                    COUNT(*) as count
                FROM processed_events
                GROUP BY category
                ORDER BY count DESC
                LIMIT 10
            """)
            
            return {
                'total_events': stats['total_events'],
                'asap_events': stats['asap_events'],
                'online_events': stats['online_events'],
                'offline_events': stats['offline_events'],
                'free_events': stats['free_events'],
                'paid_events': stats['paid_events'],
                'first_event_date': stats['first_event_date'].isoformat() if stats['first_event_date'] else None,
                'last_event_date': stats['last_event_date'].isoformat() if stats['last_event_date'] else None,
                'top_categories': [
                    {'category': row['category'], 'count': row['count']}
                    for row in category_stats
                ]
            }
    
    async def delete_event(self, event_id: int) -> bool:
        """Delete an event by ID."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM processed_events 
                WHERE id = $1
            """, event_id)
            
            if result == "DELETE 1":
                logger.info("Event deleted successfully", event_id=event_id)
                return True
            else:
                logger.warning("Event not found for deletion", event_id=event_id)
                return False
    
    async def health_check(self) -> bool:
        """Perform health check on the database connection."""
        try:
            if not self.pool:
                return False
            
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
                return True
                
        except Exception as e:
            logger.error("Event repository health check failed", error=str(e))
            return False
