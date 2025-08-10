"""Google Maps service for geocoding and location processing."""

import asyncio
import time
from typing import Optional, Dict, Any, Tuple
import httpx
from urllib.parse import quote

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


class GoogleMapsService:
    """Service for Google Maps API operations including geocoding."""
    
    def __init__(self):
        self.api_key = settings.google_maps.api_key
        self.base_url = "https://maps.googleapis.com/maps/api"
        self.client: Optional[httpx.AsyncClient] = None
        self._rate_limit_semaphore: Optional[asyncio.Semaphore] = None
        
        # Statistics
        self.geocoding_requests_made = 0
        self.geocoding_requests_failed = 0
        self.cache_hits = 0
        
        # Simple in-memory cache for geocoding results
        self._geocoding_cache: Dict[str, Dict[str, Any]] = {}
    
    async def connect(self) -> None:
        """Initialize the HTTP client and rate limiting."""
        if not self.api_key:
            logger.warning("Google Maps API key not configured")
            return
        
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )
        
        # Rate limiting: Google Maps allows 50 requests per second
        self._rate_limit_semaphore = asyncio.Semaphore(settings.google_maps.rate_limit_per_second)
        
        logger.info("Google Maps service connected")
    
    async def disconnect(self) -> None:
        """Close the HTTP client."""
        if self.client:
            await self.client.aclose()
            self.client = None
            logger.info("Google Maps service disconnected")
    
    async def geocode_address(self, address: str, city: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Geocode an address to get coordinates and formatted address.
        
        Args:
            address: The address to geocode
            city: Optional city to add context
            
        Returns:
            Dictionary with coordinates and formatted address, or None if failed
        """
        if not self.api_key or not self.client:
            logger.warning("Google Maps service not properly configured")
            return None
        
        # Create search query
        search_query = address
        if city:
            search_query = f"{address}, {city}"
        
        # Check cache first
        cache_key = search_query.lower().strip()
        if cache_key in self._geocoding_cache:
            self.cache_hits += 1
            logger.debug("Geocoding cache hit", query=search_query)
            return self._geocoding_cache[cache_key]
        
        async with self._rate_limit_semaphore:
            try:
                # Prepare the request
                params = {
                    'address': search_query,
                    'key': self.api_key,
                    'language': 'uk',  # Ukrainian language for better results
                    'region': 'ua'     # Ukraine region bias
                }
                
                url = f"{self.base_url}/geocode/json"
                
                start_time = time.time()
                response = await self.client.get(url, params=params)
                request_time = time.time() - start_time
                
                self.geocoding_requests_made += 1
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == 'OK' and data.get('results'):
                        result = data['results'][0]
                        
                        # Extract coordinates
                        location = result['geometry']['location']
                        coordinates = {
                            'latitude': location['lat'],
                            'longitude': location['lng']
                        }
                        
                        # Extract formatted address
                        formatted_address = result.get('formatted_address', address)
                        
                        # Extract address components for better parsing
                        address_components = result.get('address_components', [])
                        parsed_address = self._parse_address_components(address_components)
                        
                        geocoding_result = {
                            'coordinates': coordinates,
                            'formatted_address': formatted_address,
                            'parsed_address': parsed_address,
                            'place_id': result.get('place_id'),
                            'confidence': self._calculate_confidence(result)
                        }
                        
                        # Cache the result
                        self._geocoding_cache[cache_key] = geocoding_result
                        
                        logger.info(
                            "Geocoding successful",
                            query=search_query,
                            coordinates=coordinates,
                            request_time=request_time
                        )
                        
                        return geocoding_result
                    
                    else:
                        logger.warning(
                            "Geocoding failed - no results",
                            query=search_query,
                            status=data.get('status'),
                            error_message=data.get('error_message')
                        )
                        self.geocoding_requests_failed += 1
                        return None
                
                else:
                    logger.error(
                        "Geocoding HTTP error",
                        query=search_query,
                        status_code=response.status_code,
                        response_text=response.text[:200]
                    )
                    self.geocoding_requests_failed += 1
                    return None
                    
            except Exception as e:
                logger.error(
                    "Geocoding request failed",
                    query=search_query,
                    error=str(e)
                )
                self.geocoding_requests_failed += 1
                return None
    
    def _parse_address_components(self, components: list) -> Dict[str, str]:
        """Parse Google Maps address components into structured data."""
        parsed = {}
        
        for component in components:
            types = component.get('types', [])
            long_name = component.get('long_name', '')
            short_name = component.get('short_name', '')
            
            if 'street_number' in types:
                parsed['street_number'] = long_name
            elif 'route' in types:
                parsed['street'] = long_name
            elif 'locality' in types:
                parsed['city'] = long_name
            elif 'administrative_area_level_1' in types:
                parsed['region'] = long_name
            elif 'country' in types:
                parsed['country'] = long_name
            elif 'postal_code' in types:
                parsed['postal_code'] = long_name
        
        return parsed
    
    def _calculate_confidence(self, result: Dict[str, Any]) -> float:
        """Calculate confidence score based on geocoding result."""
        geometry = result.get('geometry', {})
        location_type = geometry.get('location_type', '')
        
        # Confidence based on location type
        confidence_map = {
            'ROOFTOP': 1.0,
            'RANGE_INTERPOLATED': 0.8,
            'GEOMETRIC_CENTER': 0.6,
            'APPROXIMATE': 0.4
        }
        
        return confidence_map.get(location_type, 0.5)
    
    async def reverse_geocode(self, latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
        """
        Reverse geocode coordinates to get address information.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            Dictionary with address information, or None if failed
        """
        if not self.api_key or not self.client:
            logger.warning("Google Maps service not properly configured")
            return None
        
        async with self._rate_limit_semaphore:
            try:
                params = {
                    'latlng': f"{latitude},{longitude}",
                    'key': self.api_key,
                    'language': 'uk',
                    'region': 'ua'
                }
                
                url = f"{self.base_url}/geocode/json"
                
                response = await self.client.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == 'OK' and data.get('results'):
                        result = data['results'][0]
                        
                        return {
                            'formatted_address': result.get('formatted_address'),
                            'place_id': result.get('place_id'),
                            'address_components': result.get('address_components', [])
                        }
                
                return None
                
            except Exception as e:
                logger.error(
                    "Reverse geocoding failed",
                    latitude=latitude,
                    longitude=longitude,
                    error=str(e)
                )
                return None
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on the Google Maps service."""
        return {
            "connected": self.client is not None and not self.client.is_closed,
            "api_key_configured": bool(self.api_key),
            "geocoding_requests_made": self.geocoding_requests_made,
            "geocoding_requests_failed": self.geocoding_requests_failed,
            "cache_hits": self.cache_hits,
            "success_rate": (
                (self.geocoding_requests_made - self.geocoding_requests_failed) / 
                max(self.geocoding_requests_made, 1) * 100
            ),
            "cache_size": len(self._geocoding_cache)
        }
    
    def clear_cache(self) -> None:
        """Clear the geocoding cache."""
        self._geocoding_cache.clear()
        logger.info("Geocoding cache cleared")
