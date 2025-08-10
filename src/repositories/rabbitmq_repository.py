"""RabbitMQ repository for message handling."""

import json
import asyncio
from typing import Any, Callable, Dict, Optional
import aio_pika
from aio_pika import Message, DeliveryMode

from ..config.settings import settings
from ..models.message import ProcessingMessage
from ..utils.logger import get_logger

logger = get_logger(__name__)


class RabbitMQRepository:
    """RabbitMQ repository for message consumption and publishing."""
    
    def __init__(self):
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self._consumer_tag: Optional[str] = None
    
    async def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        try:
            # Get connection parameters
            conn_params = settings.rabbitmq.connection_params
            
            # Create connection
            if 'ssl_options' in conn_params:
                self.connection = await aio_pika.connect_robust(
                    conn_params['url'],
                    ssl_options=conn_params['ssl_options']
                )
            else:
                self.connection = await aio_pika.connect_robust(conn_params['url'])
            
            # Create channel
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=settings.app.max_concurrent_processing)
            
            # Declare queue with minimal configuration for compatibility
            self.queue = await self.channel.declare_queue(
                settings.rabbitmq.queue_name,
                durable=True
            )
            
            logger.info("RabbitMQ connection established")
            
        except Exception as e:
            logger.error("Failed to connect to RabbitMQ", error=str(e))
            raise
    
    async def disconnect(self) -> None:
        """Close RabbitMQ connection."""
        if self._consumer_tag:
            await self.channel.cancel(self._consumer_tag)
            logger.info("RabbitMQ consumer cancelled")
        
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed")
    
    async def publish_message(self, message: ProcessingMessage) -> None:
        """Publish a message to RabbitMQ queue."""
        if not self.queue:
            raise RuntimeError("RabbitMQ not connected")
        
        try:
            message_body = message.model_dump_json().encode()
            
            rabbitmq_message = Message(
                body=message_body,
                delivery_mode=DeliveryMode.PERSISTENT,
                headers={
                    'message_id': message.id,
                    'timestamp': message.timestamp.isoformat(),
                    'source': message.source or 'unknown'
                }
            )
            
            await self.queue.publish(rabbitmq_message)
            
            logger.info("Message published", message_id=message.id)
            
        except Exception as e:
            logger.error("Failed to publish message", message_id=message.id, error=str(e))
            raise
    
    async def start_consuming(self, message_handler: Callable[[ProcessingMessage], None]) -> None:
        """Start consuming messages from RabbitMQ."""
        if not self.queue:
            raise RuntimeError("RabbitMQ not connected")
        
        async def process_message(message: aio_pika.IncomingMessage):
            """Process incoming message."""
            async with message.process():
                try:
                    # Parse message body
                    body = message.body.decode()
                    processing_message = ProcessingMessage.model_validate_json(body)
                    
                    logger.info("Processing message", message_id=processing_message.id)
                    
                    # Call message handler
                    await message_handler(processing_message)
                    
                    logger.info("Message processed successfully", message_id=processing_message.id)
                    
                except Exception as e:
                    logger.error("Failed to process message", error=str(e))
                    # Reject message and requeue
                    await message.reject(requeue=True)
        
        # Start consuming
        self._consumer_tag = await self.queue.consume(process_message)
        logger.info("Started consuming messages from RabbitMQ")
    
    async def stop_consuming(self) -> None:
        """Stop consuming messages."""
        if self._consumer_tag and self.channel:
            await self.channel.cancel(self._consumer_tag)
            self._consumer_tag = None
            logger.info("Stopped consuming messages")
    
    async def get_queue_info(self) -> Dict[str, Any]:
        """Get queue information."""
        if not self.queue:
            return {}
        
        try:
            queue_info = await self.queue.declare(passive=True)
            return {
                'name': queue_info.name,
                'message_count': queue_info.message_count,
                'consumer_count': queue_info.consumer_count,
                'durable': queue_info.durable
            }
        except Exception as e:
            logger.error("Failed to get queue info", error=str(e))
            return {}
    
    async def purge_queue(self) -> None:
        """Purge all messages from the queue."""
        if not self.queue:
            raise RuntimeError("RabbitMQ not connected")
        
        await self.queue.purge()
        logger.info("Queue purged")
    
    async def health_check(self) -> bool:
        """Perform health check on RabbitMQ connection."""
        try:
            if not self.connection or self.connection.is_closed:
                return False
            
            # Try to get queue info as a health check
            await self.get_queue_info()
            return True
            
        except Exception as e:
            logger.error("RabbitMQ health check failed", error=str(e))
            return False
