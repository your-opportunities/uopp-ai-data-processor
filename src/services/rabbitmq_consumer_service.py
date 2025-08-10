"""RabbitMQ Consumer Service with robust error handling and reconnection logic."""

import asyncio
import json
import time
from typing import Any, Callable, Dict, Optional, Awaitable
from datetime import datetime
import aio_pika
from aio_pika import Message, DeliveryMode, IncomingMessage
from aio_pika.exceptions import AMQPError, ConnectionClosed, ChannelClosed

from ..config.settings import settings
from ..models.message import ProcessingMessage
from ..utils.logger import get_logger

logger = get_logger(__name__)


class RabbitMQConsumerService:
    """Robust RabbitMQ consumer service with reconnection and error handling."""
    
    def __init__(
        self,
        message_handler: Callable[[ProcessingMessage], Awaitable[None]],
        queue_name: Optional[str] = None,
        prefetch_count: Optional[int] = None,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        connection_timeout: float = 30.0
    ):
        self.message_handler = message_handler
        self.queue_name = queue_name or settings.rabbitmq.queue_name
        self.prefetch_count = prefetch_count or settings.app.max_concurrent_processing
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_timeout = connection_timeout
        
        # Connection state
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self._consumer_tag: Optional[str] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._reconnect_task: Optional[asyncio.Task] = None
        self._processing_semaphore: Optional[asyncio.Semaphore] = None
        
        # Statistics
        self.messages_processed = 0
        self.messages_failed = 0
        self.connection_attempts = 0
        self.last_connection_time: Optional[datetime] = None
    
    async def start(self) -> None:
        """Start the consumer service."""
        logger.info("Starting RabbitMQ consumer service", queue_name=self.queue_name)
        
        self._running = True
        self._processing_semaphore = asyncio.Semaphore(self.prefetch_count)
        
        # Start connection and consumption
        await self._connect_and_consume()
        
        # Wait for shutdown signal
        await self._shutdown_event.wait()
        
        logger.info("RabbitMQ consumer service stopped")
    
    async def stop(self) -> None:
        """Stop the consumer service gracefully."""
        logger.info("Stopping RabbitMQ consumer service")
        
        self._running = False
        self._shutdown_event.set()
        
        # Cancel reconnect task if running
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        
        # Stop consuming and close connections
        await self._disconnect()
        
        logger.info("RabbitMQ consumer service stopped successfully")
    
    async def _connect_and_consume(self) -> None:
        """Establish connection and start consuming messages."""
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._connect()
                await self._setup_queue()
                await self._start_consuming()
                
                # Wait for connection to close or shutdown
                await self._wait_for_connection_close()
                
            except Exception as e:
                logger.error("Connection error", error=str(e))
                
                if not self._running:
                    break
                
                # Schedule reconnection
                await self._schedule_reconnection()
    
    async def _connect(self) -> None:
        """Establish connection to RabbitMQ with timeout."""
        self.connection_attempts += 1
        start_time = time.time()
        
        logger.info(
            "Connecting to RabbitMQ",
            attempt=self.connection_attempts,
            host=settings.rabbitmq.host,
            port=settings.rabbitmq.port
        )
        
        try:
            # Create connection with timeout
            self.connection = await asyncio.wait_for(
                aio_pika.connect_robust(
                    host=settings.rabbitmq.host,
                    port=settings.rabbitmq.port,
                    login=settings.rabbitmq.username,
                    password=settings.rabbitmq.password,
                    virtualhost=settings.rabbitmq.virtual_host,
                    timeout=self.connection_timeout
                ),
                timeout=self.connection_timeout
            )
            
            # Create channel
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=self.prefetch_count)
            
            self.last_connection_time = datetime.utcnow()
            self.connection_attempts = 0
            
            logger.info(
                "Successfully connected to RabbitMQ",
                connection_time=time.time() - start_time
            )
            
        except asyncio.TimeoutError:
            logger.error("Connection timeout", timeout=self.connection_timeout)
            raise
        except Exception as e:
            logger.error("Failed to connect to RabbitMQ", error=str(e))
            raise
    
    async def _setup_queue(self) -> None:
        """Setup queue and exchange."""
        if not self.channel:
            raise RuntimeError("Channel not available")
        
        try:
            # Declare exchange
            exchange = await self.channel.declare_exchange(
                settings.rabbitmq.exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            # Declare queue
            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                arguments={
                    'x-message-ttl': 24 * 60 * 60 * 1000,  # 24 hours TTL
                    'x-max-length': 10000,  # Max 10k messages
                    'x-overflow': 'drop-head',  # Drop oldest when full
                    'x-dead-letter-exchange': f"{self.queue_name}.dlx",  # Dead letter exchange
                    'x-dead-letter-routing-key': 'failed'
                }
            )
            
            # Bind queue to exchange
            await self.queue.bind(
                exchange,
                routing_key=settings.rabbitmq.routing_key
            )
            
            # Declare dead letter exchange and queue
            dlx_exchange = await self.channel.declare_exchange(
                f"{self.queue_name}.dlx",
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            dlx_queue = await self.channel.declare_queue(
                f"{self.queue_name}.dlx",
                durable=True
            )
            
            await dlx_queue.bind(dlx_exchange, routing_key='failed')
            
            logger.info("Queue setup completed", queue_name=self.queue_name)
            
        except Exception as e:
            logger.error("Failed to setup queue", error=str(e))
            raise
    
    async def _start_consuming(self) -> None:
        """Start consuming messages from the queue."""
        if not self.queue:
            raise RuntimeError("Queue not available")
        
        try:
            self._consumer_tag = await self.queue.consume(self._process_message)
            logger.info("Started consuming messages", consumer_tag=self._consumer_tag)
            
        except Exception as e:
            logger.error("Failed to start consuming", error=str(e))
            raise
    
    async def _process_message(self, message: IncomingMessage) -> None:
        """Process a single message with error handling."""
        async with self._processing_semaphore:
            await self._process_message_with_retry(message)
    
    async def _process_message_with_retry(self, message: IncomingMessage) -> None:
        """Process message with retry logic."""
        for attempt in range(self.max_retries):
            try:
                async with message.process():
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
                    
                    # Reject message and don't requeue (will go to dead letter queue)
                    await message.reject(requeue=False)
    
    async def _wait_for_connection_close(self) -> None:
        """Wait for connection to close or shutdown signal."""
        if not self.connection:
            return
        
        try:
            # Wait for connection to close or shutdown event
            await asyncio.wait(
                [
                    self.connection.closed,
                    self._shutdown_event.wait()
                ],
                return_when=asyncio.FIRST_COMPLETED
            )
        except Exception as e:
            logger.error("Error waiting for connection close", error=str(e))
    
    async def _schedule_reconnection(self) -> None:
        """Schedule reconnection attempt."""
        if not self._running or self._shutdown_event.is_set():
            return
        
        logger.info(
            "Scheduling reconnection",
            delay=self.retry_delay,
            attempt=self.connection_attempts
        )
        
        # Cancel existing reconnect task
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        
        # Schedule new reconnect task
        self._reconnect_task = asyncio.create_task(
            self._delayed_reconnect()
        )
        
        try:
            await self._reconnect_task
        except asyncio.CancelledError:
            pass
    
    async def _delayed_reconnect(self) -> None:
        """Delayed reconnection attempt."""
        await asyncio.sleep(self.retry_delay)
        
        if self._running and not self._shutdown_event.is_set():
            logger.info("Attempting reconnection")
            await self._connect_and_consume()
    
    async def _disconnect(self) -> None:
        """Disconnect from RabbitMQ."""
        try:
            # Stop consuming
            if self._consumer_tag and self.channel:
                await self.channel.cancel(self._consumer_tag)
                logger.info("Consumer cancelled", consumer_tag=self._consumer_tag)
            
            # Close channel
            if self.channel:
                await self.channel.close()
                logger.info("Channel closed")
            
            # Close connection
            if self.connection:
                await self.connection.close()
                logger.info("Connection closed")
                
        except Exception as e:
            logger.error("Error during disconnect", error=str(e))
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on the consumer."""
        return {
            "running": self._running,
            "connected": self.connection is not None and not self.connection.is_closed,
            "consuming": self._consumer_tag is not None,
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "connection_attempts": self.connection_attempts,
            "last_connection_time": self.last_connection_time.isoformat() if self.last_connection_time else None,
            "queue_name": self.queue_name,
            "prefetch_count": self.prefetch_count
        }
    
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
    
    def is_running(self) -> bool:
        """Check if the consumer is running."""
        return self._running
