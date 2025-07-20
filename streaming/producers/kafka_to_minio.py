import json
import time
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
import io

# MinIO client with error handling
try:
    minio_client = Minio(
        'localhost:9000',
        access_key='admin',
        secret_key='password123',
        secure=False
    )
    
    # Test MinIO connection
    if minio_client.bucket_exists('bronze-layer'):
        print("✅ MinIO connection successful")
    else:
        print("❌ bronze-layer bucket not found")
        
except Exception as e:
    print(f"❌ MinIO connection failed: {e}")
    exit(1)

# Kafka consumer with better configuration
consumer = KafkaConsumer(
    'pos-transactions',
    'cinema-sales', 
    'inventory-updates',
    'customer-reviews',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Changed from 'latest' to get existing messages
    group_id='bronze-ingestion-group',  # Added consumer group
    consumer_timeout_ms=5000  # Timeout after 5 seconds of no messages
)

def save_to_minio(topic, data):
    """Save data to MinIO bronze layer"""
    try:
        # Create unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')  # Added microseconds for uniqueness
        
        # Get ID from data for filename
        record_id = data.get('transaction_id') or data.get('booking_id') or data.get('update_id') or data.get('review_id') or data.get('id', 'unknown')
        
        filename = f"{topic}/{timestamp}_{record_id}.json"
        
        # Prepare data
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        data_bytes = json_data.encode('utf-8')
        
        # Upload to MinIO
        result = minio_client.put_object(
            'bronze-layer',
            filename,
            io.BytesIO(data_bytes),
            len(data_bytes),
            content_type='application/json'
        )
        
        print(f"✅ Saved to MinIO: {filename} (Size: {len(data_bytes)} bytes)")
        return True
        
    except Exception as e:
        print(f"❌ Error saving to MinIO: {e}")
        print(f"   Data: {data}")
        return False

def test_minio_write():
    """Test MinIO write functionality"""
    try:
        test_data = {
            'test': True,
            'timestamp': datetime.now().isoformat(),
            'message': 'Test message'
        }
        
        test_filename = f"test/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json_data = json.dumps(test_data, indent=2)
        data_bytes = json_data.encode('utf-8')
        
        minio_client.put_object(
            'bronze-layer',
            test_filename,
            io.BytesIO(data_bytes),
            len(data_bytes),
            content_type='application/json'
        )
        
        print(f"✅ Test file created: {test_filename}")
        return True
        
    except Exception as e:
        print(f"❌ Test write failed: {e}")
        return False

if __name__ == '__main__':
    print("🚀 Starting Kafka to MinIO Pipeline...")
    
    # Test MinIO write first
    print("🧪 Testing MinIO write capability...")
    if test_minio_write():
        print("✅ MinIO write test successful")
    else:
        print("❌ MinIO write test failed - exiting")
        exit(1)
    
    print("📡 Listening for Kafka messages...")
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            topic = message.topic
            data = message.value
            
            print(f"📨 Message #{message_count} from {topic}")
            print(f"   Data: {data}")
            
            # Save to MinIO
            success = save_to_minio(topic, data)
            
            if success:
                print(f"💾 Data saved successfully!")
            else:
                print(f"❌ Failed to save data")
            
            print("-" * 60)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Pipeline stopped. Processed {message_count} messages.")
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
    finally:
        consumer.close()
        print("🔒 Kafka consumer closed")