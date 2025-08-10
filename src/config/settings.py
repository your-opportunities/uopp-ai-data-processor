"""Application settings configuration."""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class RabbitMQSettings(BaseSettings):
    """RabbitMQ connection settings."""
    
    host: str = Field(default="localhost", env="RABBITMQ_HOST")
    port: int = Field(default=5672, env="RABBITMQ_PORT")
    username: str = Field(default="guest", env="RABBITMQ_USERNAME")
    password: str = Field(default="guest", env="RABBITMQ_PASSWORD")
    virtual_host: str = Field(default="/", env="RABBITMQ_VIRTUAL_HOST")
    queue_name: str = Field(default="data_processing_queue", env="RABBITMQ_QUEUE_NAME")
    exchange_name: str = Field(default="data_processing_exchange", env="RABBITMQ_EXCHANGE_NAME")
    routing_key: str = Field(default="data_processing", env="RABBITMQ_ROUTING_KEY")
    
    class Config:
        env_prefix = "RABBITMQ_"


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings."""
    
    host: str = Field(default="localhost", env="POSTGRES_HOST")
    port: int = Field(default=5432, env="POSTGRES_PORT")
    database: str = Field(default="uopp_ai_data", env="POSTGRES_DATABASE")
    username: str = Field(default="postgres", env="POSTGRES_USERNAME")
    password: str = Field(default="password", env="POSTGRES_PASSWORD")
    ssl_mode: str = Field(default="prefer", env="POSTGRES_SSL_MODE")
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    class Config:
        env_prefix = "POSTGRES_"


class DeepSeekSettings(BaseSettings):
    """DeepSeek API settings."""
    
    api_url: str = Field(default="https://api.deepseek.com/v1", env="DEEPSEEK_API_URL")
    api_key: str = Field(env="DEEPSEEK_API_KEY")
    model: str = Field(default="deepseek-chat", env="DEEPSEEK_MODEL")
    max_tokens: int = Field(default=4096, env="DEEPSEEK_MAX_TOKENS")
    temperature: float = Field(default=0.1, env="DEEPSEEK_TEMPERATURE")
    
    class Config:
        env_prefix = "DEEPSEEK_"


class AppSettings(BaseSettings):
    """Application general settings."""
    
    name: str = Field(default="uopp-ai-data-processor", env="APP_NAME")
    env: str = Field(default="development", env="APP_ENV")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    max_concurrent_processing: int = Field(default=10, env="MAX_CONCURRENT_PROCESSING")
    retry_attempts: int = Field(default=3, env="RETRY_ATTEMPTS")
    retry_delay: int = Field(default=5, env="RETRY_DELAY")
    health_check_interval: int = Field(default=30, env="HEALTH_CHECK_INTERVAL")
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")
    
    class Config:
        env_prefix = "APP_"


class Settings(BaseSettings):
    """Main application settings."""
    
    rabbitmq: RabbitMQSettings = RabbitMQSettings()
    postgres: PostgresSettings = PostgresSettings()
    deepseek: DeepSeekSettings = DeepSeekSettings()
    app: AppSettings = AppSettings()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
