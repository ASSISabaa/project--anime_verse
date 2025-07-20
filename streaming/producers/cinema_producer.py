import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_cinema_sale():
    anime_movies = [
        'Attack on Titan: Final Season',
        'Demon Slayer: Infinity Train', 
        'Your Name',
        'Spirited Away',
        'One Piece: Red',
        'Jujutsu Kaisen 0',
        'Weathering with You'
    ]
    
    ticket_types = ['standard', 'premium', 'vip', '4dx']
    
    return {
        'booking_id': f'CINEMA_{random.randint(10000, 99999)}',
        'customer_id': f'CUST_{random.randint(1000, 9999)}',
        'anime_title': random.choice(anime_movies),
        'screening_time': datetime.now().isoformat(),
        'ticket_type': random.choice(ticket_types),
        'seats_booked': random.randint(1, 6),
        'ticket_price': round(random.uniform(12.99, 29.99), 2),
        'concession_amount': round(random.uniform(0, 25.50), 2),
        'theater_id': f'THEATER_{random.randint(1, 8)}',
        'payment_method': random.choice(['credit_card', 'cash', 'mobile_pay'])
    }

if __name__ == '__main__':
    print('Starting Cinema Producer...')
    try:
        for i in range(30):
            sale = generate_cinema_sale()
            producer.send('cinema-sales', sale)
            print(f'Sent {i+1}: {sale["anime_title"]} - ${sale["ticket_price"]}')
            time.sleep(2)
    except Exception as e:
        print(f'Error: {e}')
    finally:
        producer.close()