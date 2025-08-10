"""Logging configuration for the application."""

import logging
import sys
from typing import Any, Dict
import structlog
from colorama import Fore, Style, init

from ..config.settings import settings

# Initialize colorama for colored output
init(autoreset=True)


def setup_logging() -> None:
    """Setup structured logging configuration."""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if settings.app.env == "production" else structlog.dev.ConsoleRenderer(colors=True),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.app.log_level.upper()),
    )
    
    # Set specific logger levels
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output for development."""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA,
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{color}{record.levelname}{Style.RESET_ALL}"
        return super().format(record)


def log_startup_info() -> None:
    """Log application startup information."""
    logger = get_logger("startup")
    
    logger.info(
        "Starting UOPP AI Data Processor",
        version="1.0.0",
        environment=settings.app.env,
        log_level=settings.app.log_level,
        max_concurrent_processing=settings.app.max_concurrent_processing,
    )
    
    logger.info(
        "Configuration loaded",
        rabbitmq_url=settings.rabbitmq.url,
        postgres_host=settings.postgres.host,
        openrouter_model=settings.openrouter.model,
    )
