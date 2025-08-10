"""DeepSeek API models."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class DeepSeekMessage(BaseModel):
    """Message model for DeepSeek API."""
    
    role: str = Field(..., description="Message role: system, user, or assistant")
    content: str = Field(..., description="Message content")


class DeepSeekRequest(BaseModel):
    """Request model for DeepSeek API."""
    
    model: str = Field(..., description="Model to use for processing")
    messages: List[DeepSeekMessage] = Field(..., description="List of messages")
    max_tokens: Optional[int] = Field(default=None, description="Maximum tokens to generate")
    temperature: Optional[float] = Field(default=None, description="Sampling temperature")
    top_p: Optional[float] = Field(default=None, description="Top-p sampling parameter")
    frequency_penalty: Optional[float] = Field(default=None, description="Frequency penalty")
    presence_penalty: Optional[float] = Field(default=None, description="Presence penalty")
    stream: bool = Field(default=False, description="Whether to stream the response")


class DeepSeekChoice(BaseModel):
    """Choice model from DeepSeek API response."""
    
    index: int = Field(..., description="Choice index")
    message: DeepSeekMessage = Field(..., description="Generated message")
    finish_reason: Optional[str] = Field(default=None, description="Reason for finishing")


class DeepSeekUsage(BaseModel):
    """Usage model from DeepSeek API response."""
    
    prompt_tokens: int = Field(..., description="Number of prompt tokens")
    completion_tokens: int = Field(..., description="Number of completion tokens")
    total_tokens: int = Field(..., description="Total number of tokens")


class DeepSeekResponse(BaseModel):
    """Response model from DeepSeek API."""
    
    id: str = Field(..., description="Response ID")
    object: str = Field(..., description="Object type")
    created: int = Field(..., description="Creation timestamp")
    model: str = Field(..., description="Model used")
    choices: List[DeepSeekChoice] = Field(..., description="Generated choices")
    usage: Optional[DeepSeekUsage] = Field(default=None, description="Token usage information")


class DeepSeekError(BaseModel):
    """Error model from DeepSeek API."""
    
    error: Dict[str, Any] = Field(..., description="Error details")
    status_code: int = Field(..., description="HTTP status code")


def create_processing_prompt(content: str, schema: Optional[Dict[str, Any]] = None) -> List[DeepSeekMessage]:
    """Create a processing prompt for structured data extraction."""
    
    system_prompt = """You are a data extraction assistant. Your task is to extract structured information from the provided content and return it in JSON format.

Guidelines:
1. Extract all relevant information from the content
2. Structure the data logically and consistently
3. Use appropriate data types (strings, numbers, booleans, arrays, objects)
4. Handle missing information gracefully
5. Maintain the original meaning and context

Return only valid JSON without any additional text or formatting."""

    if schema:
        system_prompt += f"\n\nExpected schema:\n{str(schema)}"
    
    user_prompt = f"Please extract structured information from the following content:\n\n{content}"
    
    return [
        DeepSeekMessage(role="system", content=system_prompt),
        DeepSeekMessage(role="user", content=user_prompt)
    ]
