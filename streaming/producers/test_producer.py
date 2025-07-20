import json
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for i in range(5):
    data = {
        'id': f'TEST_{i}',
        'message': f'Hello from producer {i}',
        'timestamp': datetime.now().isoformat()
    }
    producer.send('pos-transactions', data)
    print(f'Sent: {data}')
    time.sleep(2)

producer.close()