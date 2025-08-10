"""Data models for Ukrainian event extraction."""

from typing import List, Optional, Union, Dict
from pydantic import BaseModel, Field, validator
from enum import Enum


class EventFormat(str, Enum):
    """Event format enumeration."""
    OFFLINE = "offline"
    ONLINE = "online"


class EventCategory(str, Enum):
    """Event category enumeration."""
    WEBINAR = "вебінар"
    VOLUNTEERING = "волонтерство"
    GRANT = "грант"
    COMPETITION = "конкурс"
    CONFERENCE = "конференція"
    COURSE = "курс"
    LECTURE = "лекція"
    MASTERCLASS = "майстер-клас"
    HACKATHON = "хакатон"
    EXCHANGE = "обмін"
    VACANCY = "вакансія"
    PROJECT = "проєкт"
    INTERNSHIP = "стажування"
    SCHOLARSHIP = "стипендія"
    CAMP = "табір"
    TOURNAMENT = "турнір"
    TRAINING = "тренінг"
    RECREATION = "відпочинок"
    CONCERT = "концерт"
    PERFORMANCE = "виступ"
    PRESENTATION = "презентація"
    SEMINAR = "семінар"
    FORUM = "форум"
    PANEL_DISCUSSION = "панельна дискусія"
    MEETING = "зустріч"
    QUEST = "квест"
    INTERACTIVE_EXHIBITION = "інтерактивна виставка"
    FESTIVAL = "фестиваль"
    FAIR = "ярмарок"
    MOVIE_SCREENING = "кінопоказ"
    THEATER_PRODUCTION = "театральна постановка"
    CHARITY_EVENT = "благодійна подія"
    DEMONSTRATION = "демонстрація"
    WORKSHOP = "воркшоп"
    ONLINE_COURSE = "онлайн-курс"
    DEBATE = "дебати"
    INTENSIVE = "інтенсив"
    SPORTS_COMPETITION = "спортивні змагання"
    EXCURSION = "екскурсія"
    READING = "читання"
    ART_PERFORMANCE = "арт-перформанс"
    GASTRONOMIC_EVENT = "гастрономічний захід"
    NETWORKING_SESSION = "нетворкінг сесія"
    OPENING = "відкриття"
    TESTING = "тестування"
    DEMO_DAY = "демо-день"
    PITCHING = "пітчинг"
    TRAINING_CAMP = "тренувальний табір"


class UkrainianEvent(BaseModel):
    """Structured Ukrainian event data model."""
    
    title: str = Field(..., description="Short title of the event/content")
    is_asap: bool = Field(..., description="Whether the event is urgent/терміново")
    is_regular_event: bool = Field(default=True, description="Whether this is a regular event (not urgent/one-time)")
    format: EventFormat = Field(..., description="Event format: offline or online")
    categories: List[EventCategory] = Field(..., description="Event categories from predefined set")
    detailed_location: Optional[str] = Field(None, description="Address/venue or null")
    city: Optional[str] = Field(None, description="City name or null")
    price: Optional[Union[float, str]] = Field(None, description="Numeric value, 'free', or null")
    date: Optional[str] = Field(None, description="Event date in ISO format (YYYY-MM-DD) or null")
    deadline: Optional[str] = Field(None, description="Application deadline in ISO format (YYYY-MM-DD) or null")
    
    # Location data from Google Maps geocoding
    coordinates: Optional[Dict[str, float]] = Field(None, description="Latitude and longitude coordinates")
    formatted_address: Optional[str] = Field(None, description="Formatted address from Google Maps")
    place_id: Optional[str] = Field(None, description="Google Maps place ID")
    location_confidence: Optional[float] = Field(None, description="Confidence score for location accuracy (0.0-1.0)")
    

    
    @validator('price')
    def validate_price(cls, v):
        """Validate price format."""
        if v is None:
            return v
        if isinstance(v, str):
            if v.lower() == 'free':
                return 'free'
            try:
                return float(v)
            except ValueError:
                raise ValueError("Price must be numeric, 'free', or null")
        return v
    
    @validator('title', pre=True)
    def validate_title(cls, v):
        """Validate and provide default for title."""
        if v is None or not v or not v.strip():
            raise ValueError("Title cannot be empty or null")
        return v.strip()
    
    @validator('format', pre=True)
    def validate_format(cls, v):
        """Validate and provide default for format."""
        if v is None:
            raise ValueError("Format cannot be null")
        if v not in ['offline', 'online']:
            raise ValueError("Format must be 'offline' or 'online'")
        return v
    
    @validator('categories', pre=True)
    def validate_categories(cls, v):
        """Validate and provide default for categories."""
        if v is None or not v or len(v) == 0:
            raise ValueError("At least one category must be specified")
        return v


class OpenRouterMessage(BaseModel):
    """OpenRouter API message model."""
    
    role: str = Field(..., description="Message role: system, user, or assistant")
    content: str = Field(..., description="Message content")


class OpenRouterRequest(BaseModel):
    """OpenRouter API request model."""
    
    model: str = Field(..., description="Model to use for processing")
    messages: List[OpenRouterMessage] = Field(..., description="List of messages")
    max_tokens: Optional[int] = Field(default=2048, description="Maximum tokens to generate")
    temperature: Optional[float] = Field(default=0.1, description="Sampling temperature")
    top_p: Optional[float] = Field(default=1.0, description="Top-p sampling parameter")
    stream: bool = Field(default=False, description="Whether to stream the response")


class OpenRouterChoice(BaseModel):
    """OpenRouter API choice model."""
    
    index: int = Field(..., description="Choice index")
    message: OpenRouterMessage = Field(..., description="Generated message")
    finish_reason: Optional[str] = Field(default=None, description="Reason for finishing")


class OpenRouterUsage(BaseModel):
    """OpenRouter API usage model."""
    
    prompt_tokens: int = Field(..., description="Number of prompt tokens")
    completion_tokens: int = Field(..., description="Number of completion tokens")
    total_tokens: int = Field(..., description="Total number of tokens")


class OpenRouterResponse(BaseModel):
    """OpenRouter API response model."""
    
    id: str = Field(..., description="Response ID")
    object: str = Field(..., description="Object type")
    created: int = Field(..., description="Creation timestamp")
    model: str = Field(..., description="Model used")
    choices: List[OpenRouterChoice] = Field(..., description="Generated choices")
    usage: Optional[OpenRouterUsage] = Field(default=None, description="Token usage information")


class OpenRouterError(BaseModel):
    """OpenRouter API error model."""
    
    error: dict = Field(..., description="Error details")
    status_code: int = Field(..., description="HTTP status code")


def create_ukrainian_event_prompt(text: str) -> List[OpenRouterMessage]:
    """Create a prompt for Ukrainian event extraction."""
    
    system_prompt = """Ти - експерт з аналізу українських текстів та витягування структурованих даних про події.

ВАЖЛИВО: ВСІ ПОЛЯ ПОВИННІ БУТИ ЗАПОВНЕНІ. НЕ ПОВЕРТАЙ null.

Твоє завдання - проаналізувати текст та витягнути інформацію про подію у форматі JSON.

ОБОВ'ЯЗКОВІ ПОЛЯ:
- title: коротка назва події (ЗАВЖДИ ЗАПОВНЮЙ)
- format: "offline" або "online" (ЗАВЖДИ ЗАПОВНЮЙ)
- categories: список категорій (ЗАВЖДИ МІНІМУМ 1)
- is_asap: true/false (чи є терміновим)
- is_regular_event: true/false (чи є звичайною подією)

ДОПОВНЮВАЛЬНІ ПОЛЯ (можуть бути null):
- detailed_location: адреса/місце проведення
- city: назва міста
- price: число, "free" або null
- date: дата події (YYYY-MM-DD)
- deadline: дедлайн подачі (YYYY-MM-DD)

Категорії: вебінар, волонтерство, грант, конкурс, конференція, курс, лекція, майстер-клас, хакатон, обмін, вакансія, проєкт, стажування, стипендія, табір, турнір, тренінг, відпочинок, концерт, виступ, презентація, семінар, форум, панельна дискусія, зустріч, квест, інтерактивна виставка, фестиваль, ярмарок, кінопоказ, театральна постановка, благодійна подія, демонстрація, воркшоп, онлайн-курс, дебати, інтенсив, спортивні змагання, екскурсія, читання, арт-перформанс, гастрономічний захід, нетворкінг сесія, відкриття, тестування, демо-день, пітчинг, тренувальний табір

ПРАВИЛА:
1. ВСІ ПОЛЯ ПОВИННІ БУТИ ЗАПОВНЕНІ (не null)
2. is_asap та is_regular_event - завжди true/false
3. Якщо не впевнений - використову найближчу відповідність

Поверни тільки JSON без додаткового тексту."""

    user_prompt = f"Проаналізуй наступний текст та витягни інформацію про подію:\n\n{text}"
    
    return [
        OpenRouterMessage(role="system", content=system_prompt),
        OpenRouterMessage(role="user", content=user_prompt)
    ]
