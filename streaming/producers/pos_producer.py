import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_pos_transaction():
    anime_products = [
        'Attack on Titan Manga Vol 1', 'Naruto Figure', 'One Piece Hoodie',
        'Dragon Ball Poster', 'Demon Slayer Keychain', 'My Hero Academia T-shirt'
    ]
    
    return {
        'transaction_id': f'POS_{random.randint(10000, 99999)}',
        'customer_id': f'CUST_{random.randint(1000, 9999)}',
        'product_name': random.choice(anime_products),
        'quantity': random.randint(1, 3),
        'unit_price': round(random.uniform(9.99, 59.99), 2),
        'timestamp': datetime.now().isoformat(),
        'store_location': random.choice(['Tokyo', 'Osaka', 'Kyoto'])
    }

if __name__ == '__main__':
    print('Starting POS Producer...')
    try:
        for i in range(50):
            transaction = generate_pos_transaction()
            producer.send('pos-transactions', transaction)
            print(f'Sent {i+1}: {transaction}')
            time.sleep(1)
    except Exception as e:
        print(f'Error: {e}')
    finally:
        producer.close()