#!/usr/bin/env python3
"""
Test Data Generator for Animeverse Data Platform
Creates sample data files and uploads them to MinIO
"""

import json
import csv
import random
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import io

# MinIO Configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin" 
MINIO_SECRET_KEY = "password123"

def create_minio_client():
    """Create MinIO client"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def generate_pos_transactions(num_records=100):
    """Generate sample POS transaction data"""
    transactions = []
    products = [
        "MANGA-AOT-VOL45", "MANGA-DS-VOL23", "FIGURE-NARUTO-001", 
        "FIGURE-GOKU-002", "TSHIRT-ONEPIECE-L", "HOODIE-JUJUTSU-M",
        "POSTER-SPIRITED-A3", "KEYCHAIN-TOTORO-01"
    ]
    
    for i in range(num_records):
        transaction = {
            "transaction_id": f"TXN-{datetime.now().strftime('%Y%m%d')}-{i+1:03d}",
            "customer_id": f"CUST-{random.randint(10000, 99999)}",
            "product_id": random.choice(products),
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(9.99, 59.99), 2),
            "total_amount": 0,  # Will calculate
            "payment_method": random.choice(["card", "cash", "digital_wallet"]),
            "store_id": f"STORE-{random.randint(1, 10):02d}",
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 1440))).isoformat()
        }
        transaction["total_amount"] = round(transaction["quantity"] * transaction["unit_price"], 2)
        transactions.append(transaction)
    
    return transactions

def generate_cinema_sales(num_records=50):
    """Generate sample cinema sales data"""
    movies = [
        "Attack on Titan: Final Season", "Demon Slayer: Infinity Train",
        "Your Name", "Spirited Away", "Princess Mononoke", "Jujutsu Kaisen Movie"
    ]
    
    sales = []
    for i in range(num_records):
        sale = {
            "booking_id": f"BOOK-{datetime.now().strftime('%Y%m%d')}-{i+1:03d}",
            "customer_id": f"CUST-{random.randint(10000, 99999)}",
            "movie_title": random.choice(movies),
            "theater_id": f"THR-{random.randint(1, 5):03d}",
            "show_time": random.choice(["14:00", "17:00", "19:30", "22:00"]),
            "seats_booked": random.randint(1, 4),
            "ticket_price": round(random.uniform(12.00, 18.50), 2),
            "total_amount": 0,  # Will calculate
            "booking_date": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat()
        }
        sale["total_amount"] = round(sale["seats_booked"] * sale["ticket_price"], 2)
        sales.append(sale)
    
    return sales

def generate_inventory_updates(num_records=75):
    """Generate sample inventory update data"""
    products = [
        "MANGA-AOT-VOL45", "MANGA-DS-VOL23", "FIGURE-NARUTO-001", 
        "FIGURE-GOKU-002", "TSHIRT-ONEPIECE-L", "HOODIE-JUJUTSU-M"
    ]
    
    updates = []
    for i in range(num_records):
        update = {
            "update_id": f"INV-{datetime.now().strftime('%Y%m%d')}-{i+1:03d}",
            "product_id": random.choice(products),
            "warehouse_id": f"WH-{random.choice(['TOKYO', 'OSAKA', 'KYOTO'])}-{random.randint(1, 3):02d}",
            "stock_change": random.randint(-10, 50),
            "current_stock": random.randint(5, 200),
            "reorder_level": random.randint(10, 30),
            "supplier_id": random.choice(["SUP-BANDAI", "SUP-KOTOBUKIYA", "SUP-GOODSMILE"]),
            "update_reason": random.choice(["restock", "sale", "damaged", "returned"]),
            "timestamp": (datetime.now() - timedelta(hours=random.randint(0, 72))).isoformat()
        }
        updates.append(update)
    
    return updates

def generate_customer_reviews(num_records=30):
    """Generate sample customer review data"""
    products = [
        "MANGA-AOT-VOL45", "MANGA-DS-VOL23", "FIGURE-NARUTO-001", 
        "FIGURE-GOKU-002", "TSHIRT-ONEPIECE-L"
    ]
    
    reviews = []
    review_texts = [
        "Amazing quality and fast shipping!", "Love the artwork and attention to detail",
        "Great value for money", "Perfect addition to my collection",
        "Exceeded my expectations", "Could be better packaging",
        "Fantastic product, highly recommend", "Good quality but took long to arrive"
    ]
    
    for i in range(num_records):
        review = {
            "review_id": f"REV-{datetime.now().strftime('%Y%m%d')}-{i+1:03d}",
            "customer_id": f"CUST-{random.randint(10000, 99999)}",
            "product_id": random.choice(products),
            "rating": random.randint(3, 5),
            "review_text": random.choice(review_texts),
            "helpful_votes": random.randint(0, 25),
            "verified_purchase": random.choice([True, False]),
            "review_date": (datetime.now() - timedelta(days=random.randint(0, 60))).isoformat()
        }
        reviews.append(review)
    
    return reviews

def upload_json_to_minio(client, bucket_name, object_name, data):
    """Upload JSON data to MinIO"""
    try:
        # Convert data to JSON string
        json_data = json.dumps(data, indent=2)
        json_bytes = json_data.encode('utf-8')
        
        # Upload to MinIO
        client.put_object(
            bucket_name,
            object_name,
            io.BytesIO(json_bytes),
            len(json_bytes),
            content_type="application/json"
        )
        print(f"✅ Uploaded {object_name} to {bucket_name}")
        return True
        
    except S3Error as e:
        print(f"❌ Error uploading {object_name}: {e}")
        return False

def upload_csv_to_minio(client, bucket_name, object_name, data):
    """Upload CSV data to MinIO"""
    try:
        if not data:
            return False
            
        # Create CSV string
        output = io.StringIO()
        if isinstance(data[0], dict):
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        csv_data = output.getvalue()
        csv_bytes = csv_data.encode('utf-8')
        
        # Upload to MinIO
        client.put_object(
            bucket_name,
            object_name,
            io.BytesIO(csv_bytes),
            len(csv_bytes),
            content_type="text/csv"
        )
        print(f"✅ Uploaded {object_name} to {bucket_name}")
        return True
        
    except S3Error as e:
        print(f"❌ Error uploading {object_name}: {e}")
        return False

def main():
    """Main function to generate and upload test data"""
    print("🚀 Animeverse Test Data Generator")
    print("=" * 50)
    
    # Create MinIO client
    client = create_minio_client()
    
    # Check if buckets exist
    buckets = ["bronze-layer", "silver-layer", "gold-layer", "warehouse"]
    for bucket in buckets:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"📁 Created bucket: {bucket}")
        except S3Error as e:
            print(f"❌ Error with bucket {bucket}: {e}")
    
    # Generate test data
    print("\n📊 Generating test data...")
    
    # Bronze layer data
    pos_data = generate_pos_transactions(100)
    cinema_data = generate_cinema_sales(50)
    inventory_data = generate_inventory_updates(75)
    reviews_data = generate_customer_reviews(30)
    
    # Upload bronze layer data
    print("\n🥉 Uploading Bronze Layer data...")
    today = datetime.now().strftime('%Y-%m-%d')
    
    upload_json_to_minio(client, "bronze-layer", f"pos_transactions/date={today}/pos_transactions.json", pos_data)
    upload_json_to_minio(client, "bronze-layer", f"cinema_sales/date={today}/cinema_sales.json", cinema_data)
    upload_json_to_minio(client, "bronze-layer", f"inventory_updates/date={today}/inventory_updates.json", inventory_data)
    upload_json_to_minio(client, "bronze-layer", f"customer_reviews/date={today}/customer_reviews.json", reviews_data)
    
    # Upload CSV versions too
    upload_csv_to_minio(client, "bronze-layer", f"pos_transactions/date={today}/pos_transactions.csv", pos_data)
    upload_csv_to_minio(client, "bronze-layer", f"cinema_sales/date={today}/cinema_sales.csv", cinema_data)
    upload_csv_to_minio(client, "bronze-layer", f"inventory_updates/date={today}/inventory_updates.csv", inventory_data)
    upload_csv_to_minio(client, "bronze-layer", f"customer_reviews/date={today}/customer_reviews.csv", reviews_data)
    
    # Generate some silver layer sample data
    print("\n🥈 Creating Silver Layer samples...")
    
    # Clean POS data (simplified)
    pos_clean = []
    for transaction in pos_data[:20]:  # Sample
        clean_transaction = transaction.copy()
        clean_transaction["product_category"] = "manga" if "MANGA" in transaction["product_id"] else "merchandise"
        clean_transaction["customer_segment"] = random.choice(["premium", "regular", "occasional"])
        pos_clean.append(clean_transaction)
    
    upload_json_to_minio(client, "silver-layer", f"pos_transactions_clean/date={today}/pos_clean.json", pos_clean)
    
    # Generate gold layer sample data
    print("\n🥇 Creating Gold Layer samples...")
    
    # Daily sales summary
    daily_summary = {
        "date": today,
        "total_revenue": sum(t["total_amount"] for t in pos_data),
        "transaction_count": len(pos_data),
        "avg_transaction_value": round(sum(t["total_amount"] for t in pos_data) / len(pos_data), 2),
        "top_products": ["MANGA-AOT-VOL45", "FIGURE-NARUTO-001", "TSHIRT-ONEPIECE-L"],
        "cinema_revenue": sum(s["total_amount"] for s in cinema_data),
        "tickets_sold": sum(s["seats_booked"] for s in cinema_data)
    }
    
    upload_json_to_minio(client, "gold-layer", f"daily_summary/date={today}/summary.json", daily_summary)
    
    print("\n✨ Test data generation completed!")
    print(f"📈 Generated:")
    print(f"   • {len(pos_data)} POS transactions")
    print(f"   • {len(cinema_data)} cinema bookings") 
    print(f"   • {len(inventory_data)} inventory updates")
    print(f"   • {len(reviews_data)} customer reviews")
    print(f"\n🎯 Check MinIO at http://localhost:9000")
    print(f"   Username: admin")
    print(f"   Password: password123")

if __name__ == "__main__":
    main()