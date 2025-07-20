import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_inventory_update():
    anime_products = [
        'Luffy Action Figure', 'Pikachu Plushie', 'Goku Statue',
        'Sailor Moon Wand', 'Totoro Backpack', 'Evangelion Model Kit',
        'Death Note Replica', 'Akatsuki Cloak', 'Chopper Hat'
    ]
    
    suppliers = ['TokyoToys Ltd', 'Anime Direct Co', 'Manga Wholesale Inc', 'Otaku Supply Chain']
    
    return {
        'update_id': f'INV_{random.randint(10000, 99999)}',
        'product_id': f'PROD_{random.randint(100, 999)}',
        'product_name': random.choice(anime_products),
        'supplier_id': random.choice(suppliers),
        'quantity_received': random.randint(5, 100),
        'unit_cost': round(random.uniform(3.99, 89.99), 2),
        'shipment_date': datetime.now().isoformat(),
        'warehouse_location': random.choice(['Tokyo_Main', 'Osaka_Branch', 'Kyoto_Store']),
        'quality_status': random.choice(['excellent', 'good', 'acceptable']),
        'expiry_date': None if random.random() > 0.3 else '2026-12-31'
    }

if __name__ == '__main__':
    print('Starting Inventory Producer...')
    try:
        for i in range(25):
            update = generate_inventory_update()
            producer.send('inventory-updates', update)
            print(f'Sent {i+1}: {update["product_name"]} - Qty: {update["quantity_received"]}')
            time.sleep(3)
    except Exception as e:
        print(f'Error: {e}')
    finally:
        producer.close()