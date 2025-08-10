"""Main data processing service that orchestrates the processing pipeline."""

import asyncio
import time
from typing import Optional, Dict, Any
from datetime import datetime

from ..config.settings import settings
from ..models.message import ProcessingMessage, ProcessingResult, ProcessingStatus
from ..models.ukrainian_event import UkrainianEvent, EventCategory
from ..repositories.rabbitmq_repository import RabbitMQRepository
from ..repositories.event_repository import EventRepository
from ..services.openrouter_service import OpenRouterService
from ..services.rabbitmq_consumer_service import RabbitMQConsumerService
from ..database.migration_manager import MigrationManager
from ..utils.logger import get_logger

logger = get_logger(__name__)


class DataProcessingService:
    """Main service for processing data through the entire pipeline."""
    
    def __init__(self):
        self.event_repo = EventRepository()
        self.rabbitmq_repo = RabbitMQRepository()
        self.openrouter_service = OpenRouterService()
        self.consumer_service = RabbitMQConsumerService(self._process_message, self.rabbitmq_repo)
        self.migration_manager = MigrationManager()
        self._processing_semaphore: Optional[asyncio.Semaphore] = None
        self._running = False
        
        # Processing statistics
        self.messages_processed = 0
        self.messages_failed = 0
        self.api_calls_made = 0
        self.api_calls_failed = 0
        self.db_operations_made = 0
        self.db_operations_failed = 0
    
    async def start(self) -> None:
        """Start the data processing service."""
        try:
            logger.info("Starting data processing service")
            
            # Initialize semaphore for concurrent processing
            self._processing_semaphore = asyncio.Semaphore(settings.app.max_concurrent_processing)
            
            # Run database migrations
            await self.migration_manager.run_migrations()
            
            # Connect to databases and services
            await self.event_repo.connect()
            await self.rabbitmq_repo.connect()
            await self.openrouter_service.connect()
            
            # Start consuming messages using the consumer service
            await self.consumer_service.start()
            
            self._running = True
            logger.info("Data processing service started successfully")
            
        except Exception as e:
            logger.error("Failed to start data processing service", error=str(e))
            await self.stop()
            raise
    
    async def stop(self) -> None:
        """Stop the data processing service."""
        logger.info("Stopping data processing service")
        
        self._running = False
        
        try:
            # Stop consuming messages
            await self.consumer_service.stop()
            
            # Disconnect from services
            await self.openrouter_service.disconnect()
            await self.rabbitmq_repo.disconnect()
            await self.event_repo.disconnect()
            
            logger.info("Data processing service stopped successfully")
            
        except Exception as e:
            logger.error("Error stopping data processing service", error=str(e))
    
    async def _process_message(self, message: ProcessingMessage) -> None:
        """Process a single message through the pipeline."""
        async with self._processing_semaphore:
            await self._process_message_with_retry(message)
    
    async def _process_message_with_retry(self, message: ProcessingMessage) -> None:
        """Process message with retry logic."""
        for attempt in range(settings.app.retry_attempts):
            try:
                await self._process_message_internal(message)
                return  # Success, exit retry loop
                
            except Exception as e:
                logger.error(
                    "Processing attempt failed",
                    message_id=message.id,
                    attempt=attempt + 1,
                    max_attempts=settings.app.retry_attempts,
                    error=str(e)
                )
                
                if attempt < settings.app.retry_attempts - 1:
                    await asyncio.sleep(settings.app.retry_delay)
                else:
                    # Final attempt failed, create error result
                    await self._create_error_result(message, str(e))
    
    async def _process_message_internal(self, message: ProcessingMessage) -> None:
        """Internal message processing logic."""
        start_time = time.time()
        
        logger.info("Starting message processing", message_id=message.id)
        
        try:
            # Step 1: Extract structured data using OpenRouter
            logger.info("Extracting structured data", message_id=message.id)
            
            # Check if OpenRouter API key is configured
            if not settings.openrouter.api_key:
                logger.warning(
                    "OpenRouter API key not configured, skipping event extraction",
                    message_id=message.id
                )
                self.messages_failed += 1
                return
            
            try:
                ukrainian_event = await self.openrouter_service.extract_ukrainian_event(message.content)
                self.api_calls_made += 1
                logger.info("API extraction successful", message_id=message.id)
            except Exception as api_error:
                self.api_calls_failed += 1
                logger.error(
                    "API extraction failed",
                    message_id=message.id,
                    error=str(api_error)
                )
                # Create a minimal event with basic info even if API extraction fails
                # Extract a meaningful title from the content
                content_preview = message.content[:100] if len(message.content) > 100 else message.content
                cleaned_content = content_preview.replace('"', '').replace('\n', ' ')
                title = f"Failed to extract: {cleaned_content}"
                
                ukrainian_event = UkrainianEvent(
                    title=title,
                    is_asap=False,
                    is_regular_event=True,
                    format="offline",  # Default value
                    categories=[EventCategory.VOLUNTEERING],  # Default category
                    detailed_location=None,
                    city=None,
                    price=None,
                    date=None,
                    deadline=None
                )
                logger.info("Created fallback event for failed extraction", message_id=message.id)
            
            # Step 2: Save event to database
            logger.info("Saving event to database", message_id=message.id)
            try:
                event_id = await self.event_repo.save_event(
                    post_created_at=message.timestamp,
                    post_scraped_at=datetime.utcnow(),
                    raw_text=message.content,
                    ukrainian_event=ukrainian_event
                )
                self.db_operations_made += 1
                logger.info("Database save successful", event_id=event_id, message_id=message.id)
            except Exception as db_error:
                self.db_operations_failed += 1
                logger.error(
                    "Database save failed",
                    message_id=message.id,
                    error=str(db_error)
                )
                # Continue processing - acknowledge message even if DB save failed
                self.messages_failed += 1
                return
            
            # Success - update statistics
            processing_time = time.time() - start_time
            self.messages_processed += 1
            
            logger.info(
                "Message processed successfully",
                message_id=message.id,
                event_id=event_id,
                processing_time=processing_time
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            self.messages_failed += 1
            
            logger.error(
                "Unexpected error in message processing",
                message_id=message.id,
                processing_time=processing_time,
                error=str(e)
            )
            # Don't re-raise - let the consumer acknowledge the message
    
    async def _create_error_result(self, message: ProcessingMessage, error_message: str) -> None:
        """Create an error result for failed processing."""
        self.messages_failed += 1
        logger.error(
            "Message processing failed after all retries",
            message_id=message.id,
            error=error_message
        )
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all services."""
        return {
            "event_repository": await self.event_repo.health_check(),
            "rabbitmq": await self.rabbitmq_repo.health_check(),
            "openrouter": await self.openrouter_service.health_check(),
            "consumer": await self.consumer_service.health_check()
        }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        db_stats = await self.event_repo.get_statistics()
        
        # Add pipeline statistics
        pipeline_stats = {
            "pipeline": {
                "messages_processed": self.messages_processed,
                "messages_failed": self.messages_failed,
                "success_rate": (self.messages_processed / max(self.messages_processed + self.messages_failed, 1)) * 100,
                "api_calls_made": self.api_calls_made,
                "api_calls_failed": self.api_calls_failed,
                "api_success_rate": (self.api_calls_made / max(self.api_calls_made + self.api_calls_failed, 1)) * 100,
                "db_operations_made": self.db_operations_made,
                "db_operations_failed": self.db_operations_failed,
                "db_success_rate": (self.db_operations_made / max(self.db_operations_made + self.db_operations_failed, 1)) * 100,
            }
        }
        
        return {**db_stats, **pipeline_stats}
    
    async def get_queue_info(self) -> Dict[str, Any]:
        """Get RabbitMQ queue information."""
        return await self.consumer_service.get_queue_info()
    
    def is_running(self) -> bool:
        """Check if the service is running."""
        return self._running
