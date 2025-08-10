#!/usr/bin/env python3
"""
UOPP AI Data Processor - Main Application Entry Point

This is the main entry point for the data processing service that:
- Consumes messages from RabbitMQ
- Processes them using DeepSeek API for structured data extraction
- Stores results in PostgreSQL
"""

import asyncio
import signal
import sys
from typing import Optional

from src.config.settings import settings
from src.services.data_processing_service import DataProcessingService
from src.utils.logger import setup_logging, log_startup_info, get_logger

logger = get_logger(__name__)


class DataProcessorApp:
    """Main application class for the data processor."""
    
    def __init__(self):
        self.processing_service = DataProcessingService()
        self._shutdown_event = asyncio.Event()
        self._running = False
    
    async def start(self) -> None:
        """Start the application."""
        try:
            # Setup logging
            setup_logging()
            log_startup_info()
            
            # Start the processing service
            await self.processing_service.start()
            
            self._running = True
            logger.info("Application started successfully")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
        except Exception as e:
            logger.error("Failed to start application", error=str(e))
            raise
        finally:
            await self.stop()
    
    async def stop(self) -> None:
        """Stop the application."""
        if not self._running:
            return
        
        logger.info("Stopping application")
        self._running = False
        
        try:
            await self.processing_service.stop()
            logger.info("Application stopped successfully")
        except Exception as e:
            logger.error("Error stopping application", error=str(e))
    
    def signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating shutdown")
        self._shutdown_event.set()


async def main() -> None:
    """Main application entry point."""
    app = DataProcessorApp()
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, app.signal_handler)
    signal.signal(signal.SIGTERM, app.signal_handler)
    
    try:
        await app.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Application error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    # Run the application
    asyncio.run(main())
