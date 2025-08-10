"""Main data processing service that orchestrates the processing pipeline."""

import asyncio
import time
from typing import Optional, Dict
from datetime import datetime

from ..config.settings import settings
from ..models.message import ProcessingMessage, ProcessingResult, ProcessingStatus
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
        self.consumer_service = RabbitMQConsumerService(self._process_message)
        self.migration_manager = MigrationManager()
        self._processing_semaphore: Optional[asyncio.Semaphore] = None
        self._running = False
    
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
        
        # Update status to processing
        status = ProcessingStatus(
            message_id=message.id,
            status="processing",
            progress=0.0,
            started_at=datetime.utcnow()
        )
        await self.postgres_repo.update_processing_status(status)
        
        try:
            # Extract structured data using OpenRouter
            logger.info("Extracting structured data", message_id=message.id)
            ukrainian_event = await self.openrouter_service.extract_ukrainian_event(message.content)
            
            # Update progress (status tracking removed for simplicity)
            status.progress = 0.5
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Save event to database
            event_id = await self.event_repo.save_event(
                post_created_at=message.timestamp,
                post_scraped_at=datetime.utcnow(),
                raw_text=message.content,
                ukrainian_event=ukrainian_event
            )
            
            logger.info(
                "Event processed and saved successfully",
                event_id=event_id,
                message_id=message.id,
                processing_time=processing_time
            )
            
            # Update final status (status tracking simplified)
            status.status = "completed"
            status.progress = 1.0
            status.completed_at = datetime.utcnow()
            
            logger.info(
                "Message processed successfully",
                message_id=message.id,
                processing_time=processing_time
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            
            # Update status to failed (status tracking simplified)
            status.status = "failed"
            status.error_details = str(e)
            status.completed_at = datetime.utcnow()
            
            logger.error(
                "Message processing failed",
                message_id=message.id,
                processing_time=processing_time,
                error=str(e)
            )
            raise
    
    async def _create_error_result(self, message: ProcessingMessage, error_message: str) -> None:
        """Create an error result for failed processing."""
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
    
    async def get_statistics(self) -> Dict[str, any]:
        """Get processing statistics."""
        return await self.event_repo.get_statistics()
    
    async def get_queue_info(self) -> Dict[str, Any]:
        """Get RabbitMQ queue information."""
        return await self.consumer_service.get_queue_info()
    
    def is_running(self) -> bool:
        """Check if the service is running."""
        return self._running
