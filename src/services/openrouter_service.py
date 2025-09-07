"""OpenRouter API service for Ukrainian event extraction."""

import json
import time
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import aiohttp
from aiohttp import ClientTimeout, ClientSession
from aiohttp.client_exceptions import ClientError, ClientResponseError

from ..config.settings import settings
from ..models.ukrainian_event import (
    UkrainianEvent, OpenRouterRequest, OpenRouterResponse, OpenRouterMessage,
    create_ukrainian_event_prompt
)
from ..utils.logger import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, max_calls_per_minute: int):
        self.max_calls_per_minute = max_calls_per_minute
        self.calls = []
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Acquire permission to make an API call."""
        async with self._lock:
            now = datetime.utcnow()
            # Remove calls older than 1 minute
            self.calls = [call_time for call_time in self.calls 
                         if now - call_time < timedelta(minutes=1)]
            
            # Check if we can make another call
            if len(self.calls) >= self.max_calls_per_minute:
                # Wait until we can make another call
                oldest_call = min(self.calls)
                wait_time = 60 - (now - oldest_call).total_seconds()
                if wait_time > 0:
                    logger.warning(
                        "🚫 API rate limit reached - waiting for API availability",
                        wait_seconds=wait_time,
                        calls_made=len(self.calls),
                        max_calls_per_minute=self.max_calls_per_minute
                    )
                    await asyncio.sleep(wait_time)
                    logger.info("✅ API rate limit cooldown completed - proceeding with request")
            
            # Add current call
            self.calls.append(now)


class OpenRouterService:
    """OpenRouter API service for Ukrainian event extraction."""
    
    def __init__(self):
        self.api_url = settings.openrouter.api_url
        self.api_key = settings.openrouter.api_key
        self.model = settings.openrouter.model
        self.max_tokens = settings.openrouter.max_tokens
        self.temperature = settings.openrouter.temperature
        self.max_retries = settings.openrouter.max_retries
        self.retry_delay = settings.openrouter.retry_delay
        
        # Rate limiting
        self.rate_limiter = RateLimiter(settings.openrouter.rate_limit_per_minute)
        
        # HTTP client
        self._session: Optional[ClientSession] = None
        self._timeout = ClientTimeout(total=60, connect=10)
        
        # Statistics
        self.calls_made = 0
        self.calls_failed = 0
        self.last_call_time: Optional[datetime] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
    
    async def connect(self) -> None:
        """Initialize HTTP client."""
        if self._session is None or self._session.closed:
            headers = {
                "Content-Type": "application/json",
                "HTTP-Referer": "https://uopp-ai-data-processor",
                "X-Title": "UOPP AI Data Processor"
            }
            
            # Add Authorization header only if API key is provided
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            
            self._session = ClientSession(
                timeout=self._timeout,
                headers=headers
            )
            logger.info("OpenRouter service initialized")
    
    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.info("OpenRouter service disconnected")
    
    async def extract_ukrainian_event(self, text: str) -> UkrainianEvent:
        """Extract structured Ukrainian event data from text."""
        if not self._session:
            raise RuntimeError("OpenRouter service not connected")
        
        start_time = time.time()
        
        try:
            logger.info("⏳ Waiting for rate limiter approval")
            # Wait for rate limiter
            await self.rate_limiter.acquire()
            logger.info("✅ Rate limiter approved - proceeding with API call")
            
            # Create prompt for Ukrainian event extraction
            messages = create_ukrainian_event_prompt(text)
            logger.info("📝 Created prompt for Ukrainian event extraction", prompt_length=len(str(messages)))
            
            # Prepare request
            request_data = OpenRouterRequest(
                model=self.model,
                messages=messages,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                stream=False
            )
            logger.info("🚀 Making API call to OpenRouter", model=self.model, max_tokens=self.max_tokens)
            
            # Make API call with retry logic
            response_data = await self._make_api_call_with_retry(request_data)
            logger.info("📥 Received API response", response_keys=list(response_data.keys()) if isinstance(response_data, dict) else "non-dict")
            
            # Parse response
            openrouter_response = OpenRouterResponse.model_validate(response_data)
            
            # Extract content from response
            if not openrouter_response.choices:
                raise ValueError("No choices in OpenRouter response")
            
            content = openrouter_response.choices[0].message.content
            logger.info("📄 Extracted content from API response", content_length=len(content), content_preview=content[:200] + "..." if len(content) > 200 else content)
            
            # Parse JSON response (handle markdown code blocks)
            try:
                # Try to parse as pure JSON first
                logger.info("🔍 Attempting to parse response as pure JSON")
                event_data = json.loads(content)
                logger.info("✅ Successfully parsed as pure JSON")
            except json.JSONDecodeError:
                logger.info("⚠️ Pure JSON parsing failed, trying markdown extraction")
                # If that fails, try to extract JSON from markdown code blocks
                import re
                
                # Look for JSON in markdown code blocks
                logger.info("🔍 Looking for JSON in markdown code blocks")
                json_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', content, re.DOTALL)
                if json_match:
                    json_content = json_match.group(1).strip()
                    logger.info("📋 Found JSON in markdown", json_content_preview=json_content[:200] + "..." if len(json_content) > 200 else json_content)
                    try:
                        event_data = json.loads(json_content)
                        logger.info("✅ Successfully parsed JSON from markdown")
                    except json.JSONDecodeError as e:
                        logger.error("❌ Failed to parse JSON from markdown", error=str(e), content=content)
                        raise ValueError(f"Invalid JSON in markdown response: {str(e)}")
                else:
                    logger.info("🔍 No markdown code blocks found, looking for raw JSON")
                    # Try to find JSON object without markdown
                    json_match = re.search(r'\{.*\}', content, re.DOTALL)
                    if json_match:
                        json_content = json_match.group(0)
                        logger.info("📋 Found raw JSON", json_content_preview=json_content[:200] + "..." if len(json_content) > 200 else json_content)
                        try:
                            event_data = json.loads(json_content)
                            logger.info("✅ Successfully parsed raw JSON")
                        except json.JSONDecodeError as e:
                            logger.error("❌ Failed to parse extracted JSON", error=str(e), content=content)
                            raise ValueError(f"Invalid JSON response from API: {str(e)}")
                    else:
                        logger.error("❌ No JSON found in response", content=content)
                        raise ValueError("No valid JSON found in API response")
            
            # Log the parsed data for debugging
            logger.info("📊 Parsed event data from API", event_data=event_data)
            
            # Validate and create UkrainianEvent
            try:
                logger.info("🔍 Validating parsed data against UkrainianEvent model")
                ukrainian_event = UkrainianEvent.model_validate(event_data)
                logger.info("✅ Successfully validated and created UkrainianEvent", title=ukrainian_event.title, categories=ukrainian_event.categories)
            except Exception as validation_error:
                logger.warning("⚠️ Validation failed, attempting to fix common issues", error=str(validation_error))
                logger.error("Validation failed for API response", error=str(validation_error), event_data=event_data)
                
                # Try to fix common issues
                fixed_data = event_data.copy()
                
                # Fix title if null or empty
                if not fixed_data.get('title') or fixed_data.get('title') is None:
                    fixed_data['title'] = "Подія без назви"
                
                # Fix format if null
                if not fixed_data.get('format') or fixed_data.get('format') is None:
                    fixed_data['format'] = "offline"
                
                # Fix categories if null or empty
                if not fixed_data.get('categories') or fixed_data.get('categories') is None or len(fixed_data.get('categories', [])) == 0:
                    fixed_data['categories'] = ["волонтерство"]
                
                # Fix is_asap if null (default to False for non-urgent events)
                if fixed_data.get('is_asap') is None:
                    fixed_data['is_asap'] = False
                
                # Fix is_regular_event if null (default to True for regular events)
                if fixed_data.get('is_regular_event') is None:
                    fixed_data['is_regular_event'] = True
                
                logger.info("Attempting to create event with fixed data", fixed_data=fixed_data)
                ukrainian_event = UkrainianEvent.model_validate(fixed_data)
            
            processing_time = time.time() - start_time
            self.calls_made += 1
            self.last_call_time = datetime.utcnow()
            
            logger.info(
                "🎉 Ukrainian event extraction completed successfully",
                processing_time=processing_time,
                tokens_used=openrouter_response.usage.total_tokens if openrouter_response.usage else None,
                title=ukrainian_event.title,
                categories=ukrainian_event.categories,
                format=ukrainian_event.format,
                is_asap=ukrainian_event.is_asap
            )
            
            return ukrainian_event
            
        except Exception as e:
            processing_time = time.time() - start_time
            self.calls_failed += 1
            
            logger.error(
                "Ukrainian event extraction failed",
                error=str(e),
                processing_time=processing_time,
                text_preview=text[:100] + "..." if len(text) > 100 else text
            )
            raise
    
    async def _make_api_call_with_retry(self, request_data: OpenRouterRequest) -> Dict[str, Any]:
        """Make API call with retry logic."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return await self._make_api_call(request_data)
                
            except ClientResponseError as e:
                last_exception = e
                if e.status in [429, 500, 502, 503, 504]:  # Retryable errors
                    # For rate limits (429), use much longer delays
                    if e.status == 429:
                        delay = min(60 * (2 ** attempt), 300)  # Max 5 minutes
                        logger.warning(
                            "🚫 API rate limit hit - waiting for API availability before retry",
                            attempt=attempt + 1,
                            max_attempts=self.max_retries,
                            delay_seconds=delay,
                            error=str(e)
                        )
                        await asyncio.sleep(delay)
                        logger.info("✅ API rate limit wait completed - retrying request")
                    else:
                        delay = self.retry_delay * (2 ** attempt)
                        logger.warning(
                            "⚠️ API call failed - waiting before retry",
                            attempt=attempt + 1,
                            max_attempts=self.max_retries,
                            status_code=e.status,
                            delay_seconds=delay,
                            error=str(e)
                        )
                    
                    if attempt < self.max_retries - 1:
                        continue
                else:
                    raise  # Non-retryable error
                    
            except (ClientError, asyncio.TimeoutError) as e:
                last_exception = e
                logger.warning(
                    "API call failed, retrying",
                    attempt=attempt + 1,
                    max_attempts=self.max_retries,
                    error=str(e)
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                continue
        
        # All retries failed
        raise last_exception or Exception("API call failed after all retries")
    
    async def _make_api_call(self, request_data: OpenRouterRequest) -> Dict[str, Any]:
        """Make a single API call."""
        url = f"{self.api_url}/chat/completions"
        
        async with self._session.post(
            url,
            json=request_data.model_dump(exclude_none=True)
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on OpenRouter API."""
        if not self._session:
            return {"status": "disconnected"}
        
        try:
            # Try a simple request to check API connectivity
            url = f"{self.api_url}/models"
            async with self._session.get(url) as response:
                if response.status == 200:
                    return {
                        "status": "healthy",
                        "calls_made": self.calls_made,
                        "calls_failed": self.calls_failed,
                        "last_call_time": self.last_call_time.isoformat() if self.last_call_time else None
                    }
                else:
                    return {"status": "unhealthy", "status_code": response.status}
        except Exception as e:
            logger.error("OpenRouter health check failed", error=str(e))
            return {"status": "error", "error": str(e)}
    
    async def get_model_info(self) -> Optional[Dict[str, Any]]:
        """Get information about available models."""
        if not self._session:
            return None
        
        try:
            url = f"{self.api_url}/models"
            async with self._session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error("Failed to get model info", error=str(e))
            return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get service statistics."""
        return {
            "calls_made": self.calls_made,
            "calls_failed": self.calls_failed,
            "success_rate": (self.calls_made - self.calls_failed) / max(self.calls_made, 1) * 100,
            "last_call_time": self.last_call_time.isoformat() if self.last_call_time else None,
            "rate_limit_per_minute": self.rate_limiter.max_calls_per_minute
        }
