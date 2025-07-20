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
    
    def _select_weighted_item(self) -> Dict[str, Any]:
        """Select item with popularity weighting"""
        # Popular items are more likely to be sold
        popular_threshold = 0.7
        popular_items = [item for item in self.concession_items if item['popularity'] > popular_threshold]
        
        # 70% chance to select popular item, 30% random
        if popular_items and random.random() < 0.7:
            return random.choice(popular_items)
        else:
            return random.choice(self.concession_items)
    
    def _generate_sale_timestamp(self) -> datetime:
        """Generate sale timestamp with occasional late data"""
        base_time = datetime.now()
        
        # Check if this should be late data
        if random.random() < self.settings['late_data_probability']:
            # Generate late data (up to max_late_hours old)
            hours_late = random.uniform(1, self.settings['max_late_hours'])
            sale_time = base_time - timedelta(hours=hours_late)
            logger.debug(f"Generated late concession data: {hours_late:.1f} hours late")
        else:
            # Normal data (within last few hours - concessions sold around showtime)
            hours_ago = random.uniform(0, 3)
            sale_time = base_time - timedelta(hours=hours_ago)
        
        return sale_time
    
    def _get_quantity_for_item(self, item: Dict[str, Any]) -> int:
        """Determine quantity based on item type"""
        category = item['category']
        
        if category == 'Combo Deals':
            # Combos are usually 1 per customer/group
            return random.choices([1, 2], weights=[80, 20])[0]
        elif category == 'Beverages':
            # People often buy multiple drinks
            return random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
        elif category == 'Snacks':
            # Snacks can be shared
            return random.choices([1, 2, 3], weights=[70, 25, 5])[0]
        elif category == 'Anime Specials':
            # Special items are usually 1 per person
            return random.choices([1, 2], weights=[85, 15])[0]
        else:
            return 1
    
    def generate_sale(self) -> ConcessionSale:
        """Generate a single concession sale"""
        item = self._select_weighted_item()
        customer = random.choice(self.customers)
        screening = random.choice(self.screenings)
        terminal = random.choice(self.terminals)
        
        # Extract cinema from terminal ID
        cinema_id = terminal.split('_')[0] + '_' + terminal.split('_')[1]
        cinema = next(c for c in self.cinemas if c['cinema_id'] == cinema_id)
        
        sale_time = self._generate_sale_timestamp()
        quantity = self._get_quantity_for_item(item)
        
        return ConcessionSale(
            sale_id=str(uuid.uuid4()),
            screening_id=screening,
            customer_id=customer,
            item_name=item['name'],
            item_quantity=quantity,
            item_unit_price=item['price'],
            sale_timestamp=sale_time.isoformat(),
            pos_terminal_id=terminal,
            cinema_location=cinema['name'],
            item_category=item['category']
        )
    
    def send_sale(self, sale: ConcessionSale):
        """Send sale to Kafka topic"""
        try:
            # Use screening_id as key for partitioning (same as cinema tickets)
            self.producer.send(
                topic=self.topic,
                key=sale.screening_id,
                value=sale.to_dict(),
                timestamp_ms=int(datetime.fromisoformat(sale.sale_timestamp).timestamp() * 1000)
            )
            
            logger.debug(f"Sent concession sale: {sale.sale_id} "
                        f"for screening {sale.screening_id}")
            
        except Exception as e:
            logger.error(f"Failed to send sale {sale.sale_id}: {e}")
    
    def run_producer(self, duration_minutes: int = None):
        """Run the producer for specified duration"""
        logger.info(f"Starting concession sales producer...")
        logger.info(f"Target rate: {self.settings['messages_per_second']} messages/second")
        
        message_count = 0
        start_time = time.time()
        last_burst = time.time()
        
        try:
            while True:
                # Check if we should exit (for testing)
                if duration_minutes and (time.time() - start_time) > (duration_minutes * 60):
                    break
                
                # Check for burst period (pre-show rush)
                current_time = time.time()
                is_burst = (current_time - last_burst) % (self.settings['burst_every_minutes'] * 60) < 90
                
                if is_burst:
                    rate = self.settings['messages_per_second'] * self.settings['burst_multiplier']
                    if current_time - last_burst > (self.settings['burst_every_minutes'] * 60):
                        last_burst = current_time
                        logger.info(f"Starting concession rush - rate: {rate} msg/sec")
                else:
                    rate = self.settings['messages_per_second']
                
                # Generate and send sale
                sale = self.generate_sale()
                self.send_sale(sale)
                
                message_count += 1
                
                # Log progress
                if message_count % 50 == 0:
                    elapsed = time.time() - start_time
                    actual_rate = message_count / elapsed
                    logger.info(f"Sent {message_count} concession sales. "
                               f"Rate: {actual_rate:.2f} msg/sec")
                
                # Sleep to maintain rate
                sleep_time = 1.0 / rate
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Concession producer stopped by user")
        except Exception as e:
            logger.error(f"Concession producer error: {e}")
        finally:
            # Flush and close producer
            self.producer.flush()
            self.producer.close()
            
            elapsed = time.time() - start_time
            logger.info(f"Concession producer finished. Sent {message_count} sales in {elapsed:.1f}s")

def main():
    """Main function to run the producer"""
    producer = ConcessionSalesProducer()
    
    # Run for 2 minutes for testing, or indefinitely for production
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        producer.run_producer(duration_minutes=2)
    else:
        producer.run_producer()

if __name__ == "__main__":
    main()