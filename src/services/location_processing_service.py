"""Location processing service for adding coordinates to events."""

import asyncio
import time
from typing import Optional, Dict, Any
import re

from ..models.ukrainian_event import UkrainianEvent
from ..services.google_maps_service import GoogleMapsService
from ..utils.logger import get_logger

logger = get_logger(__name__)


class LocationProcessingService:
    """Service for processing and enriching location data in events."""
    
    def __init__(self):
        self.google_maps_service = GoogleMapsService()
        
        # Statistics
        self.locations_processed = 0
        self.locations_enriched = 0
        self.locations_failed = 0
        
        # Common Ukrainian city patterns for better address extraction
        self.city_patterns = [
            r'Київ',
            r'Львів',
            r'Харків',
            r'Одеса',
            r'Дніпро',
            r'Запоріжжя',
            r'Івано-Франківськ',
            r'Тернопіль',
            r'Черкаси',
            r'Вінниця',
            r'Полтава',
            r'Суми',
            r'Чернігів',
            r'Чернівці',
            r'Рівне',
            r'Луцьк',
            r'Ужгород',
            r'Хмельницький',
            r'Кропивницький',
            r'Миколаїв',
            r'Херсон',
            r'Кривий Ріг',
            r'Маріуполь',
            r'Севастополь',
            r'Сімферополь'
        ]
    
    async def connect(self) -> None:
        """Connect to the Google Maps service."""
        await self.google_maps_service.connect()
        logger.info("Location processing service connected")
    
    async def disconnect(self) -> None:
        """Disconnect from the Google Maps service."""
        await self.google_maps_service.disconnect()
        logger.info("Location processing service disconnected")
    
    async def enrich_event_location(self, event: UkrainianEvent) -> UkrainianEvent:
        """
        Enrich an event with location data from Google Maps.
        
        Args:
            event: The Ukrainian event to enrich
            
        Returns:
            The enriched event with location data
        """
        self.locations_processed += 1
        
        try:
            # Skip if already has coordinates
            if event.coordinates:
                logger.debug("Event already has coordinates, skipping enrichment")
                return event
            
            # Extract location information
            location_info = self._extract_location_info(event)
            
            if not location_info['address']:
                logger.debug("No address found in event, skipping geocoding")
                return event
            
            # Geocode the address
            geocoding_result = await self.google_maps_service.geocode_address(
                address=location_info['address'],
                city=location_info['city']
            )
            
            if geocoding_result:
                # Update event with location data
                event.coordinates = geocoding_result['coordinates']
                event.formatted_address = geocoding_result['formatted_address']
                event.place_id = geocoding_result['place_id']
                event.location_confidence = geocoding_result['confidence']
                
                self.locations_enriched += 1
                
                logger.info(
                    "Event location enriched successfully",
                    event_title=event.title[:50],
                    coordinates=geocoding_result['coordinates'],
                    confidence=geocoding_result['confidence']
                )
            else:
                logger.warning(
                    "Failed to geocode address",
                    event_title=event.title[:50],
                    address=location_info['address']
                )
                self.locations_failed += 1
            
            return event
            
        except Exception as e:
            logger.error(
                "Error enriching event location",
                event_title=getattr(event, 'title', 'unknown')[:50],
                error=str(e)
            )
            self.locations_failed += 1
            return event
    
    def _extract_location_info(self, event: UkrainianEvent) -> Dict[str, Optional[str]]:
        """
        Extract location information from an event.
        
        Args:
            event: The Ukrainian event
            
        Returns:
            Dictionary with address and city information
        """
        address = None
        city = None
        
        # Use detailed_location if available
        if event.detailed_location:
            address = event.detailed_location.strip()
        
        # Use city if available
        if event.city:
            city = event.city.strip()
        
        # If no detailed_location but we have city, try to extract from title
        if not address and city:
            # Look for address patterns in the title
            address_patterns = [
                r'вул\.\s*([^,\n]+)',
                r'просп\.\s*([^,\n]+)',
                r'площа\s*([^,\n]+)',
                r'бульвар\s*([^,\n]+)',
                r'набережна\s*([^,\n]+)',
                r'([А-ЯІЇЄ][а-яіїє\s]+,\s*\d+)',  # Street name with number
            ]
            
            for pattern in address_patterns:
                match = re.search(pattern, event.title, re.IGNORECASE)
                if match:
                    address = match.group(1).strip()
                    break
        
        # If still no address, try to extract from title content
        if not address:
            # Look for any address-like patterns in the title
            title_address_patterns = [
                r'([А-ЯІЇЄ][а-яіїє\s]+,\s*\d+)',
                r'(вул\.\s*[^,\n]+)',
                r'(просп\.\s*[^,\n]+)',
                r'(площа\s*[^,\n]+)',
            ]
            
            for pattern in title_address_patterns:
                match = re.search(pattern, event.title, re.IGNORECASE)
                if match:
                    address = match.group(1).strip()
                    break
        
        # If no city but we have address, try to extract city from address
        if not city and address:
            for city_pattern in self.city_patterns:
                if re.search(city_pattern, address, re.IGNORECASE):
                    city = city_pattern
                    break
        
        # Clean up the address
        if address:
            # Remove extra whitespace and common prefixes
            address = re.sub(r'\s+', ' ', address.strip())
            address = re.sub(r'^адреса:\s*', '', address, flags=re.IGNORECASE)
            address = re.sub(r'^місце:\s*', '', address, flags=re.IGNORECASE)
            address = re.sub(r'^де:\s*', '', address, flags=re.IGNORECASE)
        
        return {
            'address': address,
            'city': city
        }
    
    async def batch_enrich_locations(self, events: list[UkrainianEvent]) -> list[UkrainianEvent]:
        """
        Enrich multiple events with location data.
        
        Args:
            events: List of Ukrainian events to enrich
            
        Returns:
            List of enriched events
        """
        logger.info(f"Starting batch location enrichment for {len(events)} events")
        
        # Process events concurrently with rate limiting
        semaphore = asyncio.Semaphore(10)  # Limit concurrent geocoding requests
        
        async def enrich_single_event(event: UkrainianEvent) -> UkrainianEvent:
            async with semaphore:
                return await self.enrich_event_location(event)
        
        # Process all events concurrently
        enriched_events = await asyncio.gather(
            *[enrich_single_event(event) for event in events],
            return_exceptions=True
        )
        
        # Handle any exceptions
        for i, result in enumerate(enriched_events):
            if isinstance(result, Exception):
                logger.error(
                    "Error enriching event in batch",
                    event_index=i,
                    error=str(result)
                )
                # Keep the original event if enrichment failed
                enriched_events[i] = events[i]
        
        logger.info(
            "Batch location enrichment completed",
            total_events=len(events),
            enriched_count=self.locations_enriched,
            failed_count=self.locations_failed
        )
        
        return enriched_events
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on the location processing service."""
        google_maps_health = await self.google_maps_service.health_check()
        
        return {
            "google_maps": google_maps_health,
            "locations_processed": self.locations_processed,
            "locations_enriched": self.locations_enriched,
            "locations_failed": self.locations_failed,
            "success_rate": (
                self.locations_enriched / max(self.locations_processed, 1) * 100
            )
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get location processing statistics."""
        return {
            "locations_processed": self.locations_processed,
            "locations_enriched": self.locations_enriched,
            "locations_failed": self.locations_failed,
            "success_rate": (
                self.locations_enriched / max(self.locations_processed, 1) * 100
            )
        }
