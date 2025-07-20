import json
from minio import Minio
from datetime import datetime
import io
from collections import defaultdict

# MinIO client
minio_client = Minio(
    'localhost:9000',
    access_key='admin',
    secret_key='password123',
    secure=False
)

def process_silver_to_gold():
    """Create Gold layer analytics from Silver data"""
    print("📊 Starting Silver to Gold analytics...")
    
    # Collect all Silver data
    pos_data = []
    cinema_data = []
    inventory_data = []
    
    try:
        # Read all Silver layer data
        objects = minio_client.list_objects('silver-layer', recursive=True)
        
        for obj in objects:
            if obj.object_name.endswith('.json'):
                try:
                    response = minio_client.get_object('silver-layer', obj.object_name)
                    data = json.loads(response.read().decode('utf-8'))
                    
                    # Categorize data
                    if 'pos-transactions' in obj.object_name:
                        pos_data.append(data)
                    elif 'cinema-sales' in obj.object_name:
                        cinema_data.append(data)
                    elif 'inventory-updates' in obj.object_name:
                        inventory_data.append(data)
                        
                except Exception as e:
                    print(f"❌ Error reading {obj.object_name}: {e}")
        
        # Generate analytics
        analytics = generate_analytics(pos_data, cinema_data, inventory_data)
        
        # Save analytics to Gold layer
        save_analytics(analytics)
        
        print("🏆 Gold analytics completed!")
        
    except Exception as e:
        print(f"❌ Gold processing failed: {e}")

def generate_analytics(pos_data, cinema_data, inventory_data):
    """Generate business analytics"""
    
    analytics = {
        'generated_at': datetime.now().isoformat(),
        'data_summary': {
            'pos_transactions_count': len(pos_data),
            'cinema_sales_count': len(cinema_data),
            'inventory_updates_count': len(inventory_data)
        }
    }
    
    # POS Analytics
    if pos_data:
        total_revenue = sum(item.get('total_amount', 0) for item in pos_data)
        avg_transaction = total_revenue / len(pos_data) if pos_data else 0
        
        # Product popularity
        product_sales = defaultdict(int)
        for item in pos_data:
            product_sales[item.get('product_name', 'Unknown')] += item.get('quantity', 0)
        
        analytics['pos_analytics'] = {
            'total_revenue': round(total_revenue, 2),
            'average_transaction_value': round(avg_transaction, 2),
            'top_products': dict(sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:5])
        }
    
    # Cinema Analytics
    if cinema_data:
        total_tickets = sum(item.get('seats_booked', 0) for item in cinema_data)
        total_cinema_revenue = sum(item.get('total_revenue', 0) for item in cinema_data)
        
        # Movie popularity
        movie_popularity = defaultdict(int)
        for item in cinema_data:
            movie_popularity[item.get('anime_title', 'Unknown')] += item.get('seats_booked', 0)
        
        analytics['cinema_analytics'] = {
            'total_tickets_sold': total_tickets,
            'total_cinema_revenue': round(total_cinema_revenue, 2),
            'average_ticket_price': round(total_cinema_revenue / total_tickets if total_tickets else 0, 2),
            'popular_movies': dict(sorted(movie_popularity.items(), key=lambda x: x[1], reverse=True)[:5])
        }
    
    # Inventory Analytics
    if inventory_data:
        total_inventory_value = sum(item.get('total_cost', 0) for item in inventory_data)
        total_items_received = sum(item.get('quantity_received', 0) for item in inventory_data)
        
        analytics['inventory_analytics'] = {
            'total_inventory_value': round(total_inventory_value, 2),
            'total_items_received': total_items_received,
            'average_unit_cost': round(total_inventory_value / total_items_received if total_items_received else 0, 2)
        }
    
    return analytics

def save_analytics(analytics):
    """Save analytics to Gold layer"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"analytics/business_summary_{timestamp}.json"
        
        json_data = json.dumps(analytics, indent=2, ensure_ascii=False)
        data_bytes = json_data.encode('utf-8')
        
        minio_client.put_object(
            'gold-layer',
            filename,
            io.BytesIO(data_bytes),
            len(data_bytes),
            content_type='application/json'
        )
        
        print(f"✅ Analytics saved: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Error saving analytics: {e}")
        return False

if __name__ == '__main__':
    process_silver_to_gold()