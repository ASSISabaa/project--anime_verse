import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime
from data_generators.anime_data import *

class InventoryProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic = 'inventory-updates'
    
    def generate_inventory_update(self):
        update_id = f"INV{random.randint(100000, 999999)}"
        
        # Simulate late arrival data (supplier reports arriving late)
        if random.random() < 0.3:
            timestamp = generate_timestamp()  # Late arrival
        else:
            timestamp = generate_recent_timestamp()
            
        inventory_update = {
            "update_id": update_id,
            "product_id": generate_product_id(),
            "supplier_id": f"SUP{random.randint(1, 5):03d}",
            "quantity_received": random.randint(10, 100),
            "unit_cost": round(random.uniform(3.00, 50.00), 2),
            "shipment_date": timestamp,
            "expected_delivery": generate_recent_timestamp(),
            "warehouse_location": random.choice(["A1", "B2", "C3", "D4"]),
            "quality_status": random.choice(["approved", "pending", "rejected"])
        }
        
        return inventory_update
    
    def start_producing(self, interval=3, max_messages=20):
        print(f"Starting Inventory Producer - Topic: {self.topic}")
        print(f"Will send {max_messages} messages then stop")
        
        count = 0
        try:
            while count < max_messages:
                update = self.generate_inventory_update()
                
                self.producer.send(
                    self.topic,
                    key=update['update_id'],
                    value=update
                )
                
                count += 1
                print(f"[{count}/{max_messages}] Sent inventory update: {update['update_id']}")
                time.sleep(interval)
            
            print("✅ Finished! All inventory updates sent")
                
        except KeyboardInterrupt:
            print("Stopping Inventory Producer...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    producer = InventoryProducer()
    producer.start_producing()