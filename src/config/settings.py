"""Application settings configuration."""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class RabbitMQSettings(BaseSettings):
    """RabbitMQ connection settings."""
    
    url: str = Field(default="amqp://guest:guest@localhost:5672/", alias="RABBITMQ_URL")
    queue_name: str = Field(default="data_processing_queue", alias="RABBITMQ_QUEUE_NAME")
    
    @property
    def connection_params(self) -> dict:
        """Parse RabbitMQ URL and return connection parameters."""
        from urllib.parse import urlparse
        
        parsed = urlparse(self.url)
            
        # Handle SSL connections
        if parsed.scheme == 'amqps':
            return {
                'url': self.url,
                'ssl_options': {
                    'ssl_version': 'PROTOCOL_TLSv1_2',
                    'cert_reqs': 'CERT_NONE'
                }
            }
        else:
            return {'url': self.url}
    
    class Config:
        env_file = ".env"
        extra = "ignore"


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings."""
    
    host: str = Field(default="localhost", alias="POSTGRES_HOST")
    port: int = Field(default=5432, alias="POSTGRES_PORT")
    database: str = Field(default="uopp_ai_data", alias="POSTGRES_DATABASE")
    username: str = Field(default="postgres", alias="POSTGRES_USERNAME")
    password: str = Field(default="password", alias="POSTGRES_PASSWORD")
    ssl_mode: str = Field(default="prefer", alias="POSTGRES_SSL_MODE")
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        # Only include SSL for non-localhost connections
        if self.host == "localhost":
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl_mode}"
    
    class Config:
        env_file = ".env"
        extra = "ignore"


class OpenRouterSettings(BaseSettings):
    """OpenRouter API settings."""
    
    api_url: str = Field(default="https://openrouter.ai/api/v1", alias="OPENROUTER_API_URL")
    api_key: Optional[str] = Field(default=None, alias="OPENROUTER_API_KEY")
    model: str = Field(default="deepseek/deepseek-r1:free", alias="OPENROUTER_MODEL")
    max_tokens: int = Field(default=2048, alias="OPENROUTER_MAX_TOKENS")
    temperature: float = Field(default=0.1, alias="OPENROUTER_TEMPERATURE")
    max_retries: int = Field(default=3, alias="OPENROUTER_MAX_RETRIES")
    retry_delay: float = Field(default=1.0, alias="OPENROUTER_RETRY_DELAY")
    rate_limit_per_minute: int = Field(default=60, alias="OPENROUTER_RATE_LIMIT_PER_MINUTE")
    
    class Config:
        env_file = ".env"
        extra = "ignore"


class AppSettings(BaseSettings):
    """Application general settings."""
    
    name: str = Field(default="uopp-ai-data-processor", alias="APP_NAME")
    env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    max_concurrent_processing: int = Field(default=10, alias="MAX_CONCURRENT_PROCESSING")
    retry_attempts: int = Field(default=3, alias="RETRY_ATTEMPTS")
    retry_delay: int = Field(default=5, alias="RETRY_DELAY")
    health_check_interval: int = Field(default=30, alias="HEALTH_CHECK_INTERVAL")
    metrics_enabled: bool = Field(default=True, alias="METRICS_ENABLED")
    
    class Config:
        env_file = ".env"
        extra = "ignore"


class Settings(BaseSettings):
    """Main application settings."""
    
    rabbitmq: RabbitMQSettings = RabbitMQSettings()
    postgres: PostgresSettings = PostgresSettings()
    openrouter: OpenRouterSettings = OpenRouterSettings()
    app: AppSettings = AppSettings()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields from environment variables


# Global settings instance
settings = Settings()
