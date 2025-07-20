"""
Concession Sales Producer
Generates realistic concession sales data for AnimeVerse cinemas
"""
import json
import time
import random
import logging
import logging.config
from datetime import datetime, timedelta
from typing import Dict, Any, List
from dataclasses import dataclass, asdict
from kafka import KafkaProducer
from faker import Faker
import uuid

from producers.config import KAFKA_CONFIG, TOPICS, PRODUCER_SETTINGS, DATA_SETTINGS, LOGGING_CONFIG

# Setup logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

fake = Faker()

@dataclass
class ConcessionSale:
    """Concession sale data model"""
    sale_id: str
    screening_id: str
    customer_id: str
    item_name: str
    item_quantity: int
    item_unit_price: float
    sale_timestamp: str
    pos_terminal_id: str
    cinema_location: str
    item_category: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class ConcessionSalesProducer:
    """Producer for concession sales events"""
    
    def __init__(self):
        self.producer = KafkaProducer(
            **KAFKA_CONFIG,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.topic = TOPICS['concession_sales']['name']
        self.settings = PRODUCER_SETTINGS['concession_sales']
        
        # Generate reference data
        self.customers = self._generate_customers()
        self.concession_items = self._generate_concession_items()
        self.cinemas = self._generate_cinemas()
        self.screenings = self._generate_screenings()
        self.terminals = self._generate_pos_terminals()
        
        logger.info(f"Concession Producer initialized with {len(self.concession_items)} items, "
                   f"{len(self.screenings)} screenings, {len(self.cinemas)} cinemas")
    
    def _generate_customers(self) -> List[str]:
        """Generate customer IDs"""
        total = DATA_SETTINGS['customers']['total_customers']
        return [f"CUST_{i:06d}" for i in range(1, total + 1)]
    
    def _generate_concession_items(self) -> List[Dict[str, Any]]:
        """Generate concession item catalog"""
        items = []
        
        # Define categories and items
        categories = {
            'Snacks': [
                {'name': 'Popcorn Small', 'price': 6.50, 'popularity': 0.95},
                {'name': 'Popcorn Medium', 'price': 8.50, 'popularity': 0.90},
                {'name': 'Popcorn Large', 'price': 11.00, 'popularity': 0.85},
                {'name': 'Nachos & Cheese', 'price': 7.50, 'popularity': 0.70},
                {'name': 'Pretzel', 'price': 5.50, 'popularity': 0.60},
                {'name': 'Candy Assorted', 'price': 4.50, 'popularity': 0.75},
                {'name': 'Chocolate Bar', 'price': 3.50, 'popularity': 0.65},
                {'name': 'Gummy Bears', 'price': 4.00, 'popularity': 0.55},
            ],
            'Beverages': [
                {'name': 'Soft Drink Small', 'price': 4.50, 'popularity': 0.90},
                {'name': 'Soft Drink Medium', 'price': 6.00, 'popularity': 0.85},
                {'name': 'Soft Drink Large', 'price': 7.50, 'popularity': 0.80},
                {'name': 'Water Bottle', 'price': 3.00, 'popularity': 0.70},
                {'name': 'Coffee', 'price': 5.00, 'popularity': 0.60},
                {'name': 'Hot Chocolate', 'price': 5.50, 'popularity': 0.45},
                {'name': 'Energy Drink', 'price': 6.50, 'popularity': 0.40},
                {'name': 'Juice Box', 'price': 3.50, 'popularity': 0.50},
            ],
            'Combo Deals': [
                {'name': 'Movie Combo - Small', 'price': 12.50, 'popularity': 0.85},
                {'name': 'Movie Combo - Medium', 'price': 16.00, 'popularity': 0.80},
                {'name': 'Movie Combo - Large', 'price': 19.50, 'popularity': 0.70},
                {'name': 'Family Pack', 'price': 35.00, 'popularity': 0.60},
                {'name': 'Date Night Special', 'price': 25.00, 'popularity': 0.55},
            ],
            'Anime Specials': [
                {'name': 'Demon Slayer Themed Drink', 'price': 8.50, 'popularity': 0.75},
                {'name': 'Attack on Titan Popcorn Bucket', 'price': 15.00, 'popularity': 0.80},
                {'name': 'Studio Ghibli Bento Box', 'price': 12.00, 'popularity': 0.70},
                {'name': 'One Piece Treasure Combo', 'price': 18.00, 'popularity': 0.65},
                {'name': 'Naruto Ramen Cup', 'price': 9.50, 'popularity': 0.60},
            ]
        }
        
        item_id = 1
        for category, category_items in categories.items():
            for item in category_items:
                items.append({
                    'item_id': f"ITEM_{item_id:04d}",
                    'name': item['name'],
                    'category': category,
                    'price': item['price'],
                    'popularity': item['popularity']
                })
                item_id += 1
        
        return items
    
    def _generate_cinemas(self) -> List[Dict[str, Any]]:
        """Generate cinema information"""
        cinemas = []
        cinema_names = DATA_SETTINGS['cinemas']['cinema_names']
        
        for i, name in enumerate(cinema_names, 1):
            cinemas.append({
                'cinema_id': f"CINEMA_{i:02d}",
                'name': name,
                'location': fake.city()
            })
        
        return cinemas
    
    def _generate_screenings(self) -> List[str]:
        """Generate screening IDs"""
        # Generate screening IDs that would exist from cinema bookings
        return [f"SCREEN_{i:06d}" for i in range(1, 1001)]  # 1000 screenings
    
    def _generate_pos_terminals(self) -> List[str]:
        """Generate POS terminal IDs"""
        terminals = []
        for cinema in self.cinemas:
            for terminal_num in range(1, 4):  # 3 terminals per cinema
                terminals.append(f"{cinema['cinema_id']}_POS_{terminal_num:02d}")
        return terminals
    
  