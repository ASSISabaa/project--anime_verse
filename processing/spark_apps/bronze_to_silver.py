import json
import os
from minio import Minio
from datetime import datetime
import io

# MinIO client
minio_client = Minio(
    'localhost:9000',
    access_key='admin',
    secret_key='password123',
    secure=False
)

def process_bronze_to_silver():
    """Process data from Bronze to Silver layer"""
    print("🔄 Starting Bronze to Silver processing...")
    
    try:
        # List all objects in bronze-layer
        objects = minio_client.list_objects('bronze-layer', recursive=True)
        
        processed_count = 0
        
        for obj in objects:
            if obj.object_name.endswith('.json'):
                try:
                    # Read from Bronze
                    response = minio_client.get_object('bronze-layer', obj.object_name)
                    data = json.loads(response.read().decode('utf-8'))
                    
                    # Clean and transform data (Silver processing)
                    cleaned_data = clean_data(data, obj.object_name)
                    
                    # Save to Silver layer
                    silver_filename = obj.object_name.replace('/', '/silver_')
                    save_to_silver(silver_filename, cleaned_data)
                    
                    processed_count += 1
                    print(f"✅ Processed: {obj.object_name}")
                    
                except Exception as e:
                    print(f"❌ Error processing {obj.object_name}: {e}")
        
        print(f"🎯 Silver processing completed: {processed_count} files")
        
    except Exception as e:
        print(f"❌ Silver processing failed: {e}")

def clean_data(data, filename):
    """Clean and transform data for Silver layer"""
    
    # Add processing metadata
    cleaned_data = data.copy()
    cleaned_data['processed_at'] = datetime.now().isoformat()
    cleaned_data['source_file'] = filename
    cleaned_data['processing_stage'] = 'silver'
    
    # Data cleaning based on topic
    if 'pos-transactions' in filename:
        # Clean POS data
        if 'unit_price' in cleaned_data:
            cleaned_data['unit_price'] = round(float(cleaned_data['unit_price']), 2)
        if 'quantity' in cleaned_data:
            cleaned_data['quantity'] = int(cleaned_data['quantity'])
        cleaned_data['total_amount'] = cleaned_data.get('unit_price', 0) * cleaned_data.get('quantity', 0)
        
    elif 'cinema-sales' in filename:
        # Clean Cinema data
        if 'ticket_price' in cleaned_data:
            cleaned_data['ticket_price'] = round(float(cleaned_data['ticket_price']), 2)
        if 'seats_booked' in cleaned_data:
            cleaned_data['seats_booked'] = int(cleaned_data['seats_booked'])
        cleaned_data['total_revenue'] = cleaned_data.get('ticket_price', 0) * cleaned_data.get('seats_booked', 0)
        
    elif 'inventory-updates' in filename:
        # Clean Inventory data
        if 'quantity_received' in cleaned_data:
            cleaned_data['quantity_received'] = int(cleaned_data['quantity_received'])
        if 'unit_cost' in cleaned_data:
            cleaned_data['unit_cost'] = round(float(cleaned_data['unit_cost']), 2)
        cleaned_data['total_cost'] = cleaned_data.get('unit_cost', 0) * cleaned_data.get('quantity_received', 0)
    
    return cleaned_data

def save_to_silver(filename, data):
    """Save processed data to Silver layer"""
    try:
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        data_bytes = json_data.encode('utf-8')
        
        minio_client.put_object(
            'silver-layer',
            filename,
            io.BytesIO(data_bytes),
            len(data_bytes),
            content_type='application/json'
        )
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving to Silver: {e}")
        return False

if __name__ == '__main__':
    process_bronze_to_silver()