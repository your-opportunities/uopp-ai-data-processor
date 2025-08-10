"""RabbitMQ Consumer Service with robust error handling and reconnection logic."""

import asyncio
import json
import time
from typing import Any, Callable, Dict, Optional, Awaitable
from aio_pika import IncomingMessage

from ..config.settings import settings
from ..models.message import ProcessingMessage
from ..utils.logger import get_logger

logger = get_logger(__name__)


class RabbitMQConsumerService:
    """Simple RabbitMQ consumer service that uses existing repository connection."""
    
    def __init__(
        self,
        message_handler: Callable[[ProcessingMessage], Awaitable[None]],
        rabbitmq_repo,
        queue_name: Optional[str] = None,
        prefetch_count: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 5.0
    ):
        self.message_handler = message_handler
        self.rabbitmq_repo = rabbitmq_repo
        self.queue_name = queue_name or settings.rabbitmq.queue_name
        self.prefetch_count = prefetch_count or settings.app.max_concurrent_processing
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Consumer state
        self._consumer_tag: Optional[str] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._processing_semaphore: Optional[asyncio.Semaphore] = None
        
        # Statistics
        self.messages_processed = 0
        self.messages_failed = 0
    
    async def start(self) -> None:
        """Start the consumer service using repository connection."""
        logger.info("Starting RabbitMQ consumer service", queue_name=self.queue_name)
        
        self._running = True
        self._processing_semaphore = asyncio.Semaphore(self.prefetch_count)
        
        # Use repository's queue and start consuming
        await self._start_consuming()
        
        # Wait for shutdown signal
        await self._shutdown_event.wait()
        
        logger.info("RabbitMQ consumer service stopped")
    
    async def stop(self) -> None:
        """Stop the consumer service gracefully."""
        logger.info("Stopping RabbitMQ consumer service")
        
        self._running = False
        self._shutdown_event.set()
        
        # Stop consuming
        await self._stop_consuming()
        
        logger.info("RabbitMQ consumer service stopped successfully")
    
    async def _start_consuming(self) -> None:
        """Start consuming messages using repository's queue."""
        if not self.rabbitmq_repo.queue:
            raise RuntimeError("RabbitMQ repository not connected")
        
        try:
            # Use repository's queue and channel
            queue = self.rabbitmq_repo.queue
            channel = self.rabbitmq_repo.channel
            
            # Set QoS for this consumer
            await channel.set_qos(prefetch_count=self.prefetch_count)
            
            # Start consuming
            self._consumer_tag = await queue.consume(self._process_message)
            logger.info("Started consuming messages", consumer_tag=self._consumer_tag)
            
        except Exception as e:
            logger.error("Failed to start consuming", error=str(e))
            raise
    
    async def _stop_consuming(self) -> None:
        """Stop consuming messages."""
        if self._consumer_tag and self.rabbitmq_repo.channel:
            await self.rabbitmq_repo.channel.cancel(self._consumer_tag)
            self._consumer_tag = None
            logger.info("Stopped consuming messages")
    
    async def _process_message(self, message: IncomingMessage) -> None:
        """Process a single message with error handling."""
        async with self._processing_semaphore:
            await self._process_message_with_retry(message)
    
    async def _process_message_with_retry(self, message: IncomingMessage) -> None:
        """Process message with retry logic."""
        processing_message = None
        
        for attempt in range(self.max_retries):
            try:
                # Parse message body
                body = message.body.decode('utf-8')
                
                # Try to parse as JSON first, fallback to raw text
                try:
                    data = json.loads(body)
                    processing_message = ProcessingMessage.model_validate(data)
                except (json.JSONDecodeError, ValueError):
                    # Handle raw text messages
                    processing_message = ProcessingMessage(
                        id=f"msg_{int(time.time() * 1000)}",
                        content=body,
                        metadata={
                            "source": "raw_text",
                            "original_body": body[:100] + "..." if len(body) > 100 else body
                        }
                    )
                
                logger.info(
                    "Processing message",
                    message_id=processing_message.id,
                    attempt=attempt + 1,
                    content_length=len(processing_message.content)
                )
                
                # Call message handler
                await self.message_handler(processing_message)
                
                # Update statistics
                self.messages_processed += 1
                
                logger.info(
                    "Message processed successfully",
                    message_id=processing_message.id,
                    messages_processed=self.messages_processed
                )
                
                # Acknowledge message on success
                await message.ack()
                return  # Success, exit retry loop
                
            except Exception as e:
                logger.error(
                    "Message processing failed",
                    attempt=attempt + 1,
                    max_attempts=self.max_retries,
                    error=str(e)
                )
                
                if attempt < self.max_retries - 1:
                    # Wait before retry
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    # Final attempt failed
                    self.messages_failed += 1
                    logger.error(
                        "Message processing failed after all retries",
                        message_id=getattr(processing_message, 'id', 'unknown'),
                        error=str(e)
                    )
                    
                    # Always acknowledge the message to prevent reprocessing
                    # This follows the error handling strategy: log error, skip DB insert, ack message
                    await message.ack()
    

    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on the consumer."""
        return {
            "running": self._running,
            "connected": self.rabbitmq_repo.connection is not None and not self.rabbitmq_repo.connection.is_closed,
            "consuming": self._consumer_tag is not None,
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "queue_name": self.queue_name,
            "prefetch_count": self.prefetch_count
        }
    
    async def get_queue_info(self) -> Dict[str, Any]:
        """Get queue information."""
        if not self.rabbitmq_repo.queue:
            return {}
        
        try:
            queue_info = await self.rabbitmq_repo.queue.declare(passive=True)
            return {
                'name': queue_info.name,
                'message_count': queue_info.message_count,
                'consumer_count': queue_info.consumer_count,
                'durable': queue_info.durable
            }
        except Exception as e:
            logger.error("Failed to get queue info", error=str(e))
            return {}
    
    def is_running(self) -> bool:
        """Check if the consumer is running."""
        return self._running
