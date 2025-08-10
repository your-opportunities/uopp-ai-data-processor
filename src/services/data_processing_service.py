"""Main data processing service that orchestrates the processing pipeline."""

import asyncio
import time
from typing import Optional, Dict
from datetime import datetime

from ..config.settings import settings
from ..models.message import ProcessingMessage, ProcessingResult, ProcessingStatus
from ..repositories.postgres_repository import PostgresRepository
from ..repositories.rabbitmq_repository import RabbitMQRepository
from ..services.deepseek_service import DeepSeekService
from ..services.rabbitmq_consumer_service import RabbitMQConsumerService
from ..utils.logger import get_logger

logger = get_logger(__name__)


class DataProcessingService:
    """Main service for processing data through the entire pipeline."""
    
    def __init__(self):
        self.postgres_repo = PostgresRepository()
        self.rabbitmq_repo = RabbitMQRepository()
        self.deepseek_service = DeepSeekService()
        self.consumer_service = RabbitMQConsumerService(self._process_message)
        self._processing_semaphore: Optional[asyncio.Semaphore] = None
        self._running = False
    
    async def start(self) -> None:
        """Start the data processing service."""
        try:
            logger.info("Starting data processing service")
            
            # Initialize semaphore for concurrent processing
            self._processing_semaphore = asyncio.Semaphore(settings.app.max_concurrent_processing)
            
            # Connect to databases and services
            await self.postgres_repo.connect()
            await self.rabbitmq_repo.connect()
            await self.deepseek_service.connect()
            
            # Create database tables
            await self.postgres_repo.create_tables()
            
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
            await self.deepseek_service.disconnect()
            await self.rabbitmq_repo.disconnect()
            await self.postgres_repo.disconnect()
            
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
            # Extract structured data using DeepSeek
            logger.info("Extracting structured data", message_id=message.id)
            structured_data = await self.deepseek_service.extract_structured_data(message.content)
            
            # Update progress
            status.progress = 0.5
            await self.postgres_repo.update_processing_status(status)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Create processing result
            result = ProcessingResult(
                message_id=message.id,
                processed_content=structured_data,
                processing_time=processing_time,
                success=True,
                model_used=settings.deepseek.model,
                confidence_score=0.8  # Placeholder, could be extracted from API response
            )
            
            # Save result to database
            await self.postgres_repo.save_processing_result(result)
            
            # Update final status
            status.status = "completed"
            status.progress = 1.0
            status.completed_at = datetime.utcnow()
            await self.postgres_repo.update_processing_status(status)
            
            logger.info(
                "Message processed successfully",
                message_id=message.id,
                processing_time=processing_time
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            
            # Update status to failed
            status.status = "failed"
            status.error_details = str(e)
            status.completed_at = datetime.utcnow()
            await self.postgres_repo.update_processing_status(status)
            
            logger.error(
                "Message processing failed",
                message_id=message.id,
                processing_time=processing_time,
                error=str(e)
            )
            raise
    
    async def _create_error_result(self, message: ProcessingMessage, error_message: str) -> None:
        """Create an error result for failed processing."""
        result = ProcessingResult(
            message_id=message.id,
            processed_content={},
            processing_time=0.0,
            success=False,
            error_message=error_message,
            model_used=settings.deepseek.model
        )
        
        await self.postgres_repo.save_processing_result(result)
        
        # Update status
        status = ProcessingStatus(
            message_id=message.id,
            status="failed",
            progress=0.0,
            error_details=error_message,
            completed_at=datetime.utcnow()
        )
        await self.postgres_repo.update_processing_status(status)
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all services."""
        return {
            "postgres": await self.postgres_repo.pool is not None,
            "rabbitmq": await self.rabbitmq_repo.health_check(),
            "deepseek": await self.deepseek_service.health_check(),
            "consumer": await self.consumer_service.health_check()
        }
    
    async def get_statistics(self) -> Dict[str, any]:
        """Get processing statistics."""
        return await self.postgres_repo.get_statistics()
    
    async def get_queue_info(self) -> Dict[str, Any]:
        """Get RabbitMQ queue information."""
        return await self.consumer_service.get_queue_info()
    
    def is_running(self) -> bool:
        """Check if the service is running."""
        return self._running
