"""
POS Transactions Producer
Generates realistic Point of Sale transaction data for the AnimeVerse store
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
class POSTransaction:
    """POS Transaction data model"""
    transaction_id: str
    product_id: str
    customer_id: str
    quantity_purchased: int
    unit_price: float
    transaction_timestamp: str
    event_type: str
    channel: str
    store_id: str
    cashier_id: str
    payment_method: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class POSTransactionProducer:
    """Producer for POS transaction events"""
    
    def __init__(self):
        self.producer = KafkaProducer(
            **KAFKA_CONFIG,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.topic = TOPICS['pos_transactions']['name']
        self.settings = PRODUCER_SETTINGS['pos_transactions']
        
        # Generate reference data
        self.customers = self._generate_customers()
        self.products = self._generate_products()
        self.stores = self._generate_stores()
        self.cashiers = self._generate_cashiers()
        
        logger.info(f"POS Producer initialized with {len(self.customers)} customers, "
                   f"{len(self.products)} products, {len(self.stores)} stores")
    
    def _generate_customers(self) -> List[str]:
        """Generate customer IDs"""
        total = DATA_SETTINGS['customers']['total_customers']
        return [f"CUST_{i:06d}" for i in range(1, total + 1)]
    
    def _generate_products(self) -> List[Dict[str, Any]]:
        """Generate product catalog"""
        products = []
        categories = [
            'T-Shirts', 'Hoodies', 'Figures', 'Mugs', 'Posters', 
            'Keychains', 'Plushies', 'Manga', 'DVDs', 'Accessories'
        ]
        
        anime_titles = [
            'Attack on Titan', 'One Piece', 'Naruto', 'Dragon Ball Z', 'My Hero Academia',
            'Demon Slayer', 'Jujutsu Kaisen', 'Tokyo Ghoul', 'Death Note', 'One Punch Man',
            'Mob Psycho 100', 'Hunter x Hunter', 'Fullmetal Alchemist', 'Bleach', 'Spy x Family'
        ]
        
        for i in range(1, DATA_SETTINGS['products']['total_products'] + 1):
            category = random.choice(categories)
            anime = random.choice(anime_titles)
            
            # Price ranges by category
            price_ranges = {
                'T-Shirts': (25, 45), 'Hoodies': (50, 80), 'Figures': (30, 150),
                'Mugs': (15, 25), 'Posters': (10, 30), 'Keychains': (8, 15),
                'Plushies': (20, 60), 'Manga': (10, 15), 'DVDs': (25, 40),
                'Accessories': (5, 35)
            }
            
            min_price, max_price = price_ranges[category]
            
            products.append({
                'product_id': f"PROD_{i:05d}",
                'name': f"{anime} {category}",
                'category': category,
                'anime_title': anime,
                'price': round(random.uniform(min_price, max_price), 2),
                'popularity': random.random()  # For weighted selection
            })
        
        return products
    
    def _generate_stores(self) -> List[Dict[str, Any]]:
        """Generate store information"""
        stores = []
        store_names = DATA_SETTINGS['stores']['store_names']
        
        for i, name in enumerate(store_names, 1):
            stores.append({
                'store_id': f"STORE_{i:02d}",
                'name': name,
                'location': fake.city(),
                'timezone': 'Asia/Tokyo'
            })
        
        return stores
    
    def _generate_cashiers(self) -> List[str]:
        """Generate cashier IDs"""
        return [f"CASH_{i:03d}" for i in range(1, 51)]  # 50 cashiers
    
    def _select_weighted_product(self) -> Dict[str, Any]:
        """Select product with popularity weighting"""
        popular_threshold = 0.7
        popular_products = [p for p in self.products if p['popularity'] > popular_threshold]
        
        # 80% chance to select popular product, 20% random
        if popular_products and random.random() < 0.8:
            return random.choice(popular_products)
        else:
            return random.choice(self.products)
    
    def _generate_transaction_timestamp(self) -> datetime:
        """Generate transaction timestamp with occasional late data"""
        base_time = datetime.now()
        
        # Check if this should be late data
        if random.random() < self.settings['late_data_probability']:
            # Generate late data (up to max_late_hours old)
            hours_late = random.uniform(1, self.settings['max_late_hours'])
            transaction_time = base_time - timedelta(hours=hours_late)
            logger.debug(f"Generated late data: {hours_late:.1f} hours late")
        else:
            # Normal data (within last few minutes)
            minutes_ago = random.uniform(0, 10)
            transaction_time = base_time - timedelta(minutes=minutes_ago)
        
        return transaction_time
    
    def generate_transaction(self) -> POSTransaction:
        """Generate a single POS transaction"""
        product = self._select_weighted_product()
        store = random.choice(self.stores)
        customer = random.choice(self.customers)
        cashier = random.choice(self.cashiers)
        
        # Business hours logic (9 AM - 10 PM JST)
        transaction_time = self._generate_transaction_timestamp()
        
        # Quantity logic - higher for popular items
        if product['popularity'] > 0.8:
            quantity = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
        else:
            quantity = random.choices([1, 2, 3], weights=[70, 25, 5])[0]
        
        # Payment methods with realistic distribution
        payment_methods = ['Credit Card', 'Cash', 'Digital Wallet', 'Gift Card']
        payment_weights = [45, 25, 25, 5]
        payment_method = random.choices(payment_methods, weights=payment_weights)[0]
        
        # Event types
        event_types = ['sale', 'return', 'exchange']
        event_weights = [90, 7, 3]
        event_type = random.choices(event_types, weights=event_weights)[0]
        
        # Channels
        channels = ['in-store', 'online', 'mobile-app']
        channel_weights = [70, 20, 10]
        channel = random.choices(channels, weights=channel_weights)[0]
        
        return POSTransaction(
            transaction_id=str(uuid.uuid4()),
            product_id=product['product_id'],
            customer_id=customer,
            quantity_purchased=quantity,
            unit_price=product['price'],
            transaction_timestamp=transaction_time.isoformat(),
            event_type=event_type,
            channel=channel,
            store_id=store['store_id'],
            cashier_id=cashier,
            payment_method=payment_method
        )
    
    def send_transaction(self, transaction: POSTransaction):
        """Send transaction to Kafka topic"""
        try:
            # Use customer_id as key for partitioning
            self.producer.send(
                topic=self.topic,
                key=transaction.customer_id,
                value=transaction.to_dict(),
                timestamp_ms=int(datetime.fromisoformat(transaction.transaction_timestamp).timestamp() * 1000)
            )
            
            logger.debug(f"Sent transaction: {transaction.transaction_id} "
                        f"for customer {transaction.customer_id}")
            
        except Exception as e:
            logger.error(f"Failed to send transaction {transaction.transaction_id}: {e}")
    
    def run_producer(self, duration_minutes: int = None):
        """Run the producer for specified duration"""
        logger.info(f"Starting POS transaction producer...")
        logger.info(f"Target rate: {self.settings['messages_per_second']} messages/second")
        
        message_count = 0
        start_time = time.time()
        last_burst = time.time()
        
        try:
            while True:
                # Check if we should exit (for testing)
                if duration_minutes and (time.time() - start_time) > (duration_minutes * 60):
                    break
                
                # Check for burst period
                current_time = time.time()
                is_burst = (current_time - last_burst) % (self.settings['burst_every_minutes'] * 60) < 120
                
                if is_burst:
                    rate = self.settings['messages_per_second'] * self.settings['burst_multiplier']
                    if current_time - last_burst > (self.settings['burst_every_minutes'] * 60):
                        last_burst = current_time
                        logger.info(f"Starting burst period - rate: {rate} msg/sec")
                else:
                    rate = self.settings['messages_per_second']
                
                # Generate and send transaction
                transaction = self.generate_transaction()
                self.send_transaction(transaction)
                
                message_count += 1
                
                # Log progress
                if message_count % 100 == 0:
                    elapsed = time.time() - start_time
                    actual_rate = message_count / elapsed
                    logger.info(f"Sent {message_count} messages. "
                               f"Rate: {actual_rate:.2f} msg/sec")
                
                # Sleep to maintain rate
                sleep_time = 1.0 / rate
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            # Flush and close producer
            self.producer.flush()
            self.producer.close()
            
            elapsed = time.time() - start_time
            logger.info(f"Producer finished. Sent {message_count} messages in {elapsed:.1f}s")

def main():
    """Main function to run the producer"""
    producer = POSTransactionProducer()
    
    # Run for 5 minutes for testing, or indefinitely for production
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        producer.run_producer(duration_minutes=2)
    else:
        producer.run_producer()

if __name__ == "__main__":
    main()