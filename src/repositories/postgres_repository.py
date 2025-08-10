"""PostgreSQL repository for data storage."""

import json
from typing import Any, Dict, List, Optional
import asyncpg
from datetime import datetime

from ..config.settings import settings
from ..models.message import ProcessingResult, ProcessingStatus
from ..utils.logger import get_logger

logger = get_logger(__name__)


class PostgresRepository:
    """PostgreSQL repository for storing processing results."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
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
                    'application_name': settings.app.name
                }
            )
            logger.info("PostgreSQL connection pool established")
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL", error=str(e))
            raise
    
    async def disconnect(self) -> None:
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")
    
    async def create_tables(self) -> None:
        """Create necessary database tables."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_results (
                    id SERIAL PRIMARY KEY,
                    message_id VARCHAR(255) NOT NULL,
                    processed_content JSONB NOT NULL,
                    processing_time FLOAT NOT NULL,
                    success BOOLEAN NOT NULL,
                    error_message TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    model_used VARCHAR(100) NOT NULL,
                    confidence_score FLOAT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_status (
                    id SERIAL PRIMARY KEY,
                    message_id VARCHAR(255) UNIQUE NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    progress FLOAT DEFAULT 0.0,
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    error_details TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Create indexes for better performance
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_processing_results_message_id 
                ON processing_results(message_id)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_processing_results_timestamp 
                ON processing_results(timestamp)
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_processing_status_message_id 
                ON processing_status(message_id)
            """)
            
            logger.info("Database tables created successfully")
    
    async def save_processing_result(self, result: ProcessingResult) -> None:
        """Save processing result to database."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO processing_results 
                (message_id, processed_content, processing_time, success, error_message, model_used, confidence_score)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, 
            result.message_id,
            json.dumps(result.processed_content),
            result.processing_time,
            result.success,
            result.error_message,
            result.model_used,
            result.confidence_score
            )
            
            logger.info("Processing result saved", message_id=result.message_id)
    
    async def get_processing_result(self, message_id: str) -> Optional[ProcessingResult]:
        """Retrieve processing result by message ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM processing_results 
                WHERE message_id = $1
                ORDER BY created_at DESC
                LIMIT 1
            """, message_id)
            
            if row:
                return ProcessingResult(
                    message_id=row['message_id'],
                    processed_content=json.loads(row['processed_content']),
                    processing_time=row['processing_time'],
                    success=row['success'],
                    error_message=row['error_message'],
                    timestamp=row['timestamp'],
                    model_used=row['model_used'],
                    confidence_score=row['confidence_score']
                )
            return None
    
    async def update_processing_status(self, status: ProcessingStatus) -> None:
        """Update processing status."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO processing_status 
                (message_id, status, progress, started_at, completed_at, error_details)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (message_id) 
                DO UPDATE SET 
                    status = EXCLUDED.status,
                    progress = EXCLUDED.progress,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    error_details = EXCLUDED.error_details,
                    updated_at = NOW()
            """,
            status.message_id,
            status.status,
            status.progress,
            status.started_at,
            status.completed_at,
            status.error_details
            )
    
    async def get_processing_status(self, message_id: str) -> Optional[ProcessingStatus]:
        """Get processing status by message ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM processing_status 
                WHERE message_id = $1
            """, message_id)
            
            if row:
                return ProcessingStatus(
                    message_id=row['message_id'],
                    status=row['status'],
                    progress=row['progress'],
                    started_at=row['started_at'],
                    completed_at=row['completed_at'],
                    error_details=row['error_details']
                )
            return None
    
    async def get_recent_results(self, limit: int = 100) -> List[ProcessingResult]:
        """Get recent processing results."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM processing_results 
                ORDER BY created_at DESC 
                LIMIT $1
            """, limit)
            
            return [
                ProcessingResult(
                    message_id=row['message_id'],
                    processed_content=json.loads(row['processed_content']),
                    processing_time=row['processing_time'],
                    success=row['success'],
                    error_message=row['error_message'],
                    timestamp=row['timestamp'],
                    model_used=row['model_used'],
                    confidence_score=row['confidence_score']
                )
                for row in rows
            ]
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_processed,
                    COUNT(CASE WHEN success = true THEN 1 END) as successful,
                    COUNT(CASE WHEN success = false THEN 1 END) as failed,
                    AVG(processing_time) as avg_processing_time,
                    AVG(confidence_score) as avg_confidence
                FROM processing_results
            """)
            
            return {
                'total_processed': stats['total_processed'],
                'successful': stats['successful'],
                'failed': stats['failed'],
                'success_rate': (stats['successful'] / stats['total_processed'] * 100) if stats['total_processed'] > 0 else 0,
                'avg_processing_time': float(stats['avg_processing_time']) if stats['avg_processing_time'] else 0,
                'avg_confidence': float(stats['avg_confidence']) if stats['avg_confidence'] else 0
            }
