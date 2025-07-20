"""
AnimeVerse Test Consumers Package

This package contains Kafka consumers for testing and monitoring
the streaming data flow in the AnimeVerse pipeline.
"""

__version__ = "1.0.0"

from .test_consumer import AnimeVerseTestConsumer

__all__ = ['AnimeVerseTestConsumer']