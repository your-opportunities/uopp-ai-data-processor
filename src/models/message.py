"""Message models for data processing."""

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class ProcessingMessage(BaseModel):
    """Message model for incoming data processing requests."""
    
    id: str = Field(..., description="Unique message identifier")
    content: str = Field(..., description="Content to be processed")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Message timestamp")
    source: Optional[str] = Field(default=None, description="Source of the message")
    priority: int = Field(default=1, description="Processing priority (1-10)")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ProcessingResult(BaseModel):
    """Result model for processed data."""
    
    message_id: str = Field(..., description="Original message ID")
    processed_content: Dict[str, Any] = Field(..., description="Processed structured data")
    processing_time: float = Field(..., description="Processing time in seconds")
    success: bool = Field(..., description="Whether processing was successful")
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Processing timestamp")
    ai_model: str = Field(..., description="AI model used for processing")
    confidence_score: Optional[float] = Field(default=None, description="Confidence score of the result")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ProcessingStatus(BaseModel):
    """Status model for processing operations."""
    
    message_id: str
    status: str = Field(..., description="Processing status: pending, processing, completed, failed")
    progress: float = Field(default=0.0, description="Processing progress (0.0 to 1.0)")
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_details: Optional[str] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
