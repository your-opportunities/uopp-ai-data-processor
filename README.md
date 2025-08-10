# UOPP AI Data Processor

An asynchronous data processing service that consumes messages from RabbitMQ, processes them using the DeepSeek API for structured data extraction, and stores results in PostgreSQL.

## Features

- **Asynchronous Processing**: Built with asyncio for high-performance concurrent processing
- **Message Queue Integration**: Consumes messages from RabbitMQ with automatic retry logic
- **AI-Powered Extraction**: Uses OpenRouter API for intelligent Ukrainian event extraction
- **Persistent Storage**: Stores results in PostgreSQL with comprehensive tracking
- **Structured Logging**: Comprehensive logging with structured output
- **Health Monitoring**: Built-in health checks for all services
- **Configuration Management**: Environment-based configuration with Pydantic validation
- **Graceful Shutdown**: Proper signal handling and resource cleanup

## Project Structure

```
uopp-ai-data-processor/
├── src/
│   ├── config/           # Configuration management
│   │   ├── __init__.py
│   │   └── settings.py   # Pydantic settings classes
│   ├── models/           # Data models
│   │   ├── __init__.py
│   │   ├── message.py    # Message and result models
│   │   └── deepseek.py   # DeepSeek API models
│   ├── repositories/     # Data access layer
│   │   ├── __init__.py
│   │   ├── postgres_repository.py
│   │   └── rabbitmq_repository.py
│   ├── services/         # Business logic
│   │   ├── __init__.py
│   │   ├── deepseek_service.py
│   │   ├── data_processing_service.py
│   │   └── rabbitmq_consumer_service.py  # Robust RabbitMQ consumer
│   └── utils/            # Utilities
│       ├── __init__.py
│       └── logger.py     # Logging configuration
├── examples/             # Example scripts
│   ├── consumer_example.py  # RabbitMQ consumer usage example
│   └── ukrainian_event_extraction.py  # Ukrainian event extraction example
├── main.py               # Application entry point
├── requirements.txt      # Python dependencies
├── env.template          # Environment variables template
└── README.md            # This file
```

## Prerequisites

- Python 3.8+
- RabbitMQ server
- PostgreSQL database
- DeepSeek API key

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd uopp-ai-data-processor
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**:
   ```bash
   cp env.template .env
   # Edit .env with your configuration
   ```

## Configuration

Copy `env.template` to `.env` and configure the following variables:

### RabbitMQ Configuration
```env
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_QUEUE_NAME=data_processing_queue
```

### PostgreSQL Configuration
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=uopp_ai_data
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=password
```

### OpenRouter API Configuration
```env
OPENROUTER_API_URL=https://openrouter.ai/api/v1
OPENROUTER_API_KEY=your_openrouter_api_key_here
OPENROUTER_MODEL=deepseek/deepseek-r1:free
OPENROUTER_MAX_TOKENS=2048
OPENROUTER_TEMPERATURE=0.1
OPENROUTER_MAX_RETRIES=3
OPENROUTER_RETRY_DELAY=1.0
OPENROUTER_RATE_LIMIT_PER_MINUTE=60
```

### Application Configuration
```env
APP_NAME=uopp-ai-data-processor
APP_ENV=development
LOG_LEVEL=INFO
MAX_CONCURRENT_PROCESSING=10
RETRY_ATTEMPTS=3
RETRY_DELAY=5
```

## Usage

### Running the Service

Start the data processing service:

```bash
python main.py
```

The service will:
1. Connect to RabbitMQ and start consuming messages
2. Process each message using DeepSeek API
3. Store results in PostgreSQL
4. Handle errors with retry logic

### Sending Test Messages

You can send test messages to RabbitMQ using the provided message format:

```python
import json
from datetime import datetime

message = {
    "id": "test-message-001",
    "content": "John Doe is a software engineer at Tech Corp with 5 years of experience.",
    "metadata": {"source": "test"},
    "timestamp": datetime.utcnow().isoformat(),
    "source": "test",
    "priority": 1
}

# Send to RabbitMQ queue
```

### Using the Consumer Service

The RabbitMQ consumer service can be used independently:

```python
from src.services.rabbitmq_consumer_service import RabbitMQConsumerService

async def handle_message(message):
    # Process the message
    print(f"Processing: {message.content}")

consumer = RabbitMQConsumerService(
    message_handler=handle_message,
    queue_name="my_queue",
    prefetch_count=10
)

await consumer.start()
```

See `examples/consumer_example.py` for a complete usage example.

### Ukrainian Event Extraction

The OpenRouter service can extract structured event data from Ukrainian text:

```python
from src.services.openrouter_service import OpenRouterService

async def extract_event():
    service = OpenRouterService()
    await service.connect()
    
    text = "Вебінар 'Основи Python' завтра о 15:00 онлайн. Безкоштовно."
    event = await service.extract_ukrainian_event(text)
    
    print(f"Title: {event.title}")
    print(f"Categories: {event.categories}")
    print(f"Format: {event.format}")
    print(f"ASAP: {event.is_asap}")
    
    await service.disconnect()

# Run extraction
asyncio.run(extract_event())
```

See `examples/ukrainian_event_extraction.py` for a complete usage example.

### Monitoring

The service provides several monitoring endpoints and features:

- **Health Checks**: All services (PostgreSQL, RabbitMQ, DeepSeek) are monitored
- **Statistics**: Processing statistics are available via the repository
- **Logging**: Structured logging with different levels (DEBUG, INFO, WARNING, ERROR)

## Development

### Code Style

The project uses:
- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting
- **mypy** for type checking

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-mock

# Run tests
pytest
```

### Development Setup

```bash
# Install development dependencies
pip install black isort flake8 mypy

# Format code
black src/ main.py
isort src/ main.py

# Lint code
flake8 src/ main.py
mypy src/ main.py
```

## Architecture

### Components

1. **DataProcessingService**: Main orchestrator that coordinates the entire pipeline
2. **OpenRouterService**: Specialized service for Ukrainian event extraction
3. **PostgresRepository**: Manages database operations and result storage
4. **RabbitMQRepository**: Handles message publishing
5. **RabbitMQConsumerService**: Robust consumer with reconnection and error handling
6. **Configuration**: Centralized configuration management with Pydantic

### Data Flow

1. **Message Reception**: RabbitMQ consumer service receives messages with automatic reconnection
2. **API Processing**: OpenRouter API extracts structured Ukrainian event data
3. **Data Storage**: Events are stored in PostgreSQL with comprehensive indexing
4. **Error Handling**: Robust error handling with message acknowledgment and retry logic
5. **Monitoring**: Comprehensive logging and statistics throughout the pipeline

### Concurrency & Performance

- Uses asyncio for non-blocking I/O operations
- Semaphore limits concurrent processing to prevent overload
- Connection pooling for database and message queue connections
- Rate limiting for API calls
- Comprehensive performance monitoring and statistics

## Troubleshooting

### Common Issues

1. **Connection Errors**: Check that RabbitMQ and PostgreSQL are running
2. **API Errors**: Verify DeepSeek API key and endpoint configuration
3. **Memory Issues**: Reduce `MAX_CONCURRENT_PROCESSING` if needed
4. **Database Errors**: Ensure PostgreSQL is accessible and tables exist

### Logs

The service uses structured logging. Check logs for:
- Connection status
- Processing errors
- Performance metrics
- Health check results

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]
