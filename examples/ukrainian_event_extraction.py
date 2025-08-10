#!/usr/bin/env python3
"""
Example script demonstrating Ukrainian event extraction using OpenRouter API.

This script shows how to:
- Extract structured event data from Ukrainian text
- Handle API responses and validation
- Monitor extraction statistics
- Handle errors gracefully
"""

import asyncio
import sys
from datetime import datetime

# Add the src directory to the path
sys.path.insert(0, '..')

from src.services.openrouter_service import OpenRouterService
from src.models.ukrainian_event import UkrainianEvent, EventCategory
from src.utils.logger import setup_logging, get_logger

logger = get_logger(__name__)


class UkrainianEventExtractor:
    """Example Ukrainian event extractor."""
    
    def __init__(self):
        self.openrouter_service = OpenRouterService()
        self.extraction_count = 0
        self.success_count = 0
        self.failure_count = 0
    
    async def extract_events(self, texts: list[str]) -> list[UkrainianEvent]:
        """Extract events from multiple Ukrainian texts."""
        results = []
        
        for i, text in enumerate(texts, 1):
            logger.info(f"Processing text {i}/{len(texts)}")
            
            try:
                event = await self.openrouter_service.extract_ukrainian_event(text)
                results.append(event)
                self.success_count += 1
                
                logger.info(
                    "Event extracted successfully",
                    title=event.title,
                    categories=event.categories,
                    format=event.format,
                    is_asap=event.is_asap
                )
                
            except Exception as e:
                self.failure_count += 1
                logger.error(f"Failed to extract event from text {i}", error=str(e))
                continue
        
        self.extraction_count += len(texts)
        return results
    
    def print_statistics(self):
        """Print extraction statistics."""
        logger.info(
            "Extraction statistics",
            total_texts=self.extraction_count,
            successful=self.success_count,
            failed=self.failure_count,
            success_rate=(self.success_count / max(self.extraction_count, 1)) * 100
        )
        
        service_stats = self.openrouter_service.get_statistics()
        logger.info(
            "Service statistics",
            calls_made=service_stats["calls_made"],
            calls_failed=service_stats["calls_failed"],
            success_rate=service_stats["success_rate"],
            rate_limit_per_minute=service_stats["rate_limit_per_minute"]
        )


async def main() -> None:
    """Main entry point."""
    # Setup logging
    setup_logging()
    
    logger.info("Starting Ukrainian event extraction example")
    
    # Sample Ukrainian texts for testing
    sample_texts = [
        """Терміново! Вебінар "Основи програмування на Python" 
        Завтра о 15:00 онлайн. Безкоштовно. 
        Реєстрація: https://example.com""",
        
        """Майстер-клас з живопису у Львові
        Адреса: вул. Шевченка, 15, Львів
        Дата: 25 грудня 2024
        Вартість: 500 грн
        Формат: офлайн""",
        
        """Конкурс стартапів "Інновації 2024"
        Місце: Київ, НСК "Олімпійський"
        Призовий фонд: 100,000 грн
        Дедлайн: 30 грудня
        Категорії: технології, екологія, соціальні проєкти""",
        
        """Благодійна подія "Допомога дітям"
        Онлайн збір коштів для дитячого будинку
        Дата: 20 грудня 2024
        Безкоштовно для учасників
        Мета: зібрати 50,000 грн""",
        
        """Хакатон "Цифрові рішення для міста"
        Місце: Харків, IT-кластер
        Тривалість: 48 годин
        Команди: 3-5 осіб
        Призи: ноутбуки та гранти на розвиток проєктів"""
    ]
    
    extractor = UkrainianEventExtractor()
    
    try:
        # Connect to OpenRouter service
        await extractor.openrouter_service.connect()
        
        # Check service health
        health = await extractor.openrouter_service.health_check()
        logger.info("Service health check", health=health)
        
        # Extract events
        events = await extractor.extract_events(sample_texts)
        
        # Print results
        logger.info(f"Successfully extracted {len(events)} events")
        
        for i, event in enumerate(events, 1):
            print(f"\n--- Event {i} ---")
            print(f"Title: {event.title}")
            print(f"Categories: {', '.join(event.categories)}")
            print(f"Format: {event.format}")
            print(f"ASAP: {event.is_asap}")
            print(f"Location: {event.detailed_location or 'N/A'}")
            print(f"City: {event.city or 'N/A'}")
            print(f"Price: {event.price or 'N/A'}")
        
        # Print statistics
        extractor.print_statistics()
        
    except Exception as e:
        logger.error("Extraction example failed", error=str(e))
        sys.exit(1)
    
    finally:
        # Disconnect from service
        await extractor.openrouter_service.disconnect()
        logger.info("Ukrainian event extraction example completed")


if __name__ == "__main__":
    asyncio.run(main())
