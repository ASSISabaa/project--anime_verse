from kafka import KafkaConsumer
import json
import boto3
from datetime import datetime
import logging

class StreamToBronzeConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password123'
        )
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def consume_topic_to_s3(self, topic_name, bucket_name):
        """Consume messages from Kafka topic and store in S3"""
        
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.logger.info(f"Starting consumer for topic: {topic_name}")
        
        batch_size = 100
        batch_messages = []
        
        try:
            for message in consumer:
                # Add ingestion timestamp
                message_data = message.value
                message_data['ingestion_timestamp'] = datetime.now().isoformat()
                message_data['kafka_offset'] = message.offset
                message_data['kafka_partition'] = message.partition
                
                batch_messages.append(message_data)
                
                # Write batch to S3 when full
                if len(batch_messages) >= batch_size:
                    self._write_batch_to_s3(batch_messages, topic_name, bucket_name)
                    batch_messages = []
                    
        except KeyboardInterrupt:
            self.logger.info("Consumer stopped by user")
        finally:
            # Write remaining messages
            if batch_messages:
                self._write_batch_to_s3(batch_messages, topic_name, bucket_name)
            consumer.close()
    
    def _write_batch_to_s3(self, messages, topic_name, bucket_name):
        """Write batch of messages to S3"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        key = f"{topic_name}/year={datetime.now().year}/month={datetime.now().month:02d}/day={datetime.now().day:02d}/{timestamp}.json"
        
        try:
            # Convert to JSONL format
            jsonl_content = '\n'.join([json.dumps(msg) for msg in messages])
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=jsonl_content.encode('utf-8'),
                ContentType='application/json'
            )
            
            self.logger.info(f"Written {len(messages)} messages to s3://{bucket_name}/{key}")
            
        except Exception as e:
            self.logger.error(f"Error writing to S3: {str(e)}")

def main():
    consumer = StreamToBronzeConsumer()
    
    # List of topics to consume
    topics = [
        'pos-transactions',
        'cinema-sales', 
        'inventory-updates',
        'customer-reviews'
    ]
    
    # In production, you would run each topic consumer in a separate process
    # For demo, we'll just consume one topic
    topic = topics[0]  # pos-transactions
    consumer.consume_topic_to_s3(topic, 'bronze-layer')

if __name__ == "__main__":
    main()