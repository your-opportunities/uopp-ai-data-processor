#!/usr/bin/env python3
"""
Example script demonstrating the RabbitMQ Consumer Service.

This script shows how to:
- Create a custom message handler
- Initialize and start the consumer service
- Handle graceful shutdown
- Monitor consumer health
"""

import asyncio
import signal
import sys
from datetime import datetime

# Add the src directory to the path
sys.path.insert(0, '..')

from src.services.rabbitmq_consumer_service import RabbitMQConsumerService
from src.models.message import ProcessingMessage
from src.utils.logger import setup_logging, get_logger

logger = get_logger(__name__)


class ExampleMessageHandler:
    """Example message handler that processes incoming messages."""
    
    def __init__(self):
        self.processed_count = 0
    
    async def handle_message(self, message: ProcessingMessage) -> None:
        """Handle a single message."""
        self.processed_count += 1
        
        logger.info(
            "Processing message",
            message_id=message.id,
            content_length=len(message.content),
            processed_count=self.processed_count,
            source=message.source
        )
        
        # Simulate some processing work
        await asyncio.sleep(0.1)
        
        # Log the content (truncated for readability)
        content_preview = message.content[:100] + "..." if len(message.content) > 100 else message.content
        logger.info(
            "Message content",
            message_id=message.id,
            content_preview=content_preview,
            metadata=message.metadata
        )
        
        logger.info(
            "Message processed successfully",
            message_id=message.id,
            total_processed=self.processed_count
        )


class ConsumerExample:
    """Example consumer application."""
    
    def __init__(self):
        self.handler = ExampleMessageHandler()
        self.consumer = RabbitMQConsumerService(
            message_handler=self.handler.handle_message,
            queue_name="example_queue",
            prefetch_count=5,
            max_retries=3,
            retry_delay=2.0,
            connection_timeout=10.0
        )
        self._shutdown_event = asyncio.Event()
    
    async def start(self) -> None:
        """Start the consumer example."""
        logger.info("Starting consumer example")
        
        # Setup logging
        setup_logging()
        
        # Start the consumer service
        consumer_task = asyncio.create_task(self.consumer.start())
        
        # Start health monitoring
        monitor_task = asyncio.create_task(self._monitor_health())
        
        # Wait for shutdown signal
        await self._shutdown_event.wait()
        
        # Stop the consumer
        await self.consumer.stop()
        
        # Cancel tasks
        consumer_task.cancel()
        monitor_task.cancel()
        
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        
        logger.info("Consumer example stopped")
    
    async def _monitor_health(self) -> None:
        """Monitor consumer health and statistics."""
        while not self._shutdown_event.is_set():
            try:
                # Get health status
                health = await self.consumer.health_check()
                queue_info = await self.consumer.get_queue_info()
                
                logger.info(
                    "Consumer health check",
                    running=health["running"],
                    connected=health["connected"],
                    consuming=health["consuming"],
                    messages_processed=health["messages_processed"],
                    messages_failed=health["messages_failed"],
                    queue_message_count=queue_info.get("message_count", 0),
                    queue_consumer_count=queue_info.get("consumer_count", 0)
                )
                
                # Wait before next check
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error("Health monitoring error", error=str(e))
                await asyncio.sleep(5)
    
    def signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating shutdown")
        self._shutdown_event.set()


async def main() -> None:
    """Main entry point."""
    example = ConsumerExample()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, example.signal_handler)
    signal.signal(signal.SIGTERM, example.signal_handler)
    
    try:
        await example.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Consumer example error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
