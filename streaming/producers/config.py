"""
Configuration settings for Kafka producers
"""
import os
from typing import Dict, Any

# Kafka Configuration (minimal for maximum compatibility)
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client_id': 'animeverse-producer',
    'acks': 1,  # Wait for leader acknowledgment
    'retries': 3,
    'request_timeout_ms': 30000,
    # Removed compression and advanced settings for compatibility
}

# Topic Configuration
TOPICS = {
    'pos_transactions': {
        'name': 'pos-transactions',
        'partitions': 3,
        'replication_factor': 1,
        'config': {
            'retention.ms': 604800000,  # 7 days
            'segment.ms': 86400000,     # 1 day
        }
    },
    'cinema_tickets': {
        'name': 'cinema-tickets',
        'partitions': 2,
        'replication_factor': 1,
        'config': {
            'retention.ms': 604800000,  # 7 days
            'segment.ms': 86400000,     # 1 day
        }
    },
    'concession_sales': {
        'name': 'concession-sales',
        'partitions': 2,
        'replication_factor': 1,
        'config': {
            'retention.ms': 604800000,  # 7 days
            'segment.ms': 86400000,     # 1 day
        }
    }
}

# Producer Settings
PRODUCER_SETTINGS = {
    'pos_transactions': {
        'messages_per_second': 5,
        'burst_every_minutes': 15,
        'burst_multiplier': 3,
        'late_data_probability': 0.05,  # 5% chance of late data
        'max_late_hours': 48,
    },
    'cinema_tickets': {
        'messages_per_second': 2,
        'burst_every_minutes': 30,
        'burst_multiplier': 4,
        'late_data_probability': 0.03,  # 3% chance of late data
        'max_late_hours': 24,
    },
    'concession_sales': {
        'messages_per_second': 3,
        'burst_every_minutes': 20,
        'burst_multiplier': 2,
        'late_data_probability': 0.02,  # 2% chance of late data
        'max_late_hours': 12,
    }
}

# Data Generation Settings
DATA_SETTINGS = {
    'customers': {
        'total_customers': 10000,
        'active_percentage': 0.3,  # 30% of customers are active
    },
    'products': {
        'total_products': 500,
        'popular_percentage': 0.2,  # 20% of products are popular
    },
    'anime_titles': {
        'total_titles': 100,
        'currently_airing': 20,
        'popular_percentage': 0.3,
    },
    'stores': {
        'total_stores': 5,
        'store_names': ['Tokyo Central', 'Shibuya Branch', 'Akihabara Store', 'Harajuku Shop', 'Osaka Main'],
    },
    'cinemas': {
        'total_cinemas': 3,
        'cinema_names': ['AnimeVerse Cinema Tokyo', 'AnimeVerse Cinema Osaka', 'AnimeVerse Cinema Kyoto'],
        'screens_per_cinema': 4,
    }
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'colored': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'log_colors': {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            },
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'colored',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': '/tmp/producer.log',
            'mode': 'a',
        }
    },
    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['console', 'file']
        },
        'kafka': {
            'level': 'WARNING',
            'handlers': ['console']
        }
    }
}