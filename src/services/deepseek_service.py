"""DeepSeek API service for data processing."""

import json
import time
from typing import Any, Dict, Optional
import httpx
from httpx import HTTPStatusError

from ..config.settings import settings
from ..models.deepseek import DeepSeekRequest, DeepSeekResponse, DeepSeekMessage, create_processing_prompt
from ..utils.logger import get_logger

logger = get_logger(__name__)


class DeepSeekService:
    """Service for interacting with DeepSeek API."""
    
    def __init__(self):
        self.api_url = settings.deepseek.api_url
        self.api_key = settings.deepseek.api_key
        self.model = settings.deepseek.model
        self.max_tokens = settings.deepseek.max_tokens
        self.temperature = settings.deepseek.temperature
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
    
    async def connect(self) -> None:
        """Initialize HTTP client."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
        )
        logger.info("DeepSeek service initialized")
    
    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("DeepSeek service disconnected")
    
    async def extract_structured_data(self, content: str, schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract structured data from content using DeepSeek API."""
        if not self._client:
            raise RuntimeError("DeepSeek service not connected")
        
        start_time = time.time()
        
        try:
            # Create prompt for structured data extraction
            messages = create_processing_prompt(content, schema)
            
            # Prepare request
            request_data = DeepSeekRequest(
                model=self.model,
                messages=messages,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                stream=False
            )
            
            # Make API call
            response = await self._client.post(
                f"{self.api_url}/chat/completions",
                json=request_data.model_dump(exclude_none=True)
            )
            
            # Handle HTTP errors
            response.raise_for_status()
            
            # Parse response
            response_data = response.json()
            deepseek_response = DeepSeekResponse.model_validate(response_data)
            
            # Extract content from response
            if not deepseek_response.choices:
                raise ValueError("No choices in DeepSeek response")
            
            content = deepseek_response.choices[0].message.content
            
            # Parse JSON response
            try:
                structured_data = json.loads(content)
            except json.JSONDecodeError as e:
                logger.warning("Failed to parse JSON response, treating as text", error=str(e))
                structured_data = {"extracted_text": content}
            
            processing_time = time.time() - start_time
            
            logger.info(
                "Data extraction completed",
                processing_time=processing_time,
                tokens_used=deepseek_response.usage.total_tokens if deepseek_response.usage else None
            )
            
            return structured_data
            
        except HTTPStatusError as e:
            processing_time = time.time() - start_time
            logger.error(
                "DeepSeek API HTTP error",
                status_code=e.response.status_code,
                error=str(e),
                processing_time=processing_time
            )
            raise
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(
                "DeepSeek API error",
                error=str(e),
                processing_time=processing_time
            )
            raise
    
    async def health_check(self) -> bool:
        """Perform health check on DeepSeek API."""
        if not self._client:
            return False
        
        try:
            # Try a simple request to check API connectivity
            response = await self._client.get(f"{self.api_url}/models")
            return response.status_code == 200
        except Exception as e:
            logger.error("DeepSeek health check failed", error=str(e))
            return False
    
    async def get_model_info(self) -> Optional[Dict[str, Any]]:
        """Get information about available models."""
        if not self._client:
            return None
        
        try:
            response = await self._client.get(f"{self.api_url}/models")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error("Failed to get model info", error=str(e))
            return None
