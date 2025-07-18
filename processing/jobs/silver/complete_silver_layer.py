#!/usr/bin/env python3


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("AnimeVerse-Complete-Silver") \
        .master("local[*]") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def create_and_load_silver(spark):
    """Create all Silver tables with cleaned and transformed data"""
    
    print("CREATING COMPLETE SILVER LAYER...")
    print("="*60)
    
    # Create Silver schema
    spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.silver")
    print(" Silver schema created")
    
    # 1. Clean Transactions (from POS data)
    print(" Creating Silver Transactions...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_transactions")
    
    transactions_silver = spark.sql("""
        SELECT 
            transaction_id,
            product_id,
            quantity,
            price,
            timestamp,
            event_type,
            channel,
            customer_id,
            -- Add business logic
            (quantity * price) as total_amount,
            date(timestamp) as transaction_date,
            hour(timestamp) as transaction_hour,
            CASE 
                WHEN hour(timestamp) BETWEEN 6 AND 12 THEN 'morning'
                WHEN hour(timestamp) BETWEEN 13 AND 18 THEN 'afternoon'
                WHEN hour(timestamp) BETWEEN 19 AND 23 THEN 'evening'
                ELSE 'night'
            END as time_period,
            CASE 
                WHEN channel = 'online' THEN 'digital'
                WHEN channel = 'mobile' THEN 'digital'
                ELSE 'physical'
            END as channel_type
        FROM my_catalog.bronze.bronze_pos_transactions
        WHERE transaction_id IS NOT NULL
          AND quantity > 0
          AND price > 0
    """)
    
    transactions_silver.write.mode("overwrite").saveAsTable("my_catalog.silver.silver_transactions")
    count = transactions_silver.count()
    print(f"    Silver Transactions: {count} records processed successfully")
    
    # 2. Clean Cinema Screenings
    print(" Creating Silver Cinema Screenings...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_cinema_screenings")
    
    cinema_silver = spark.sql("""
        SELECT 
            screening_id,
            customer_id,
            title_name as anime_title_id,
            ticket_type,
            showtime,
            seats_sold,
            concession_amt as concessions_sales,
            -- Add business logic
            date(showtime) as date_key,
            CASE 
                WHEN ticket_type = 'premium' THEN seats_sold * 25.0
                WHEN ticket_type = 'imax' THEN seats_sold * 35.0
                ELSE seats_sold * 15.0
            END as ticket_revenue,
            CASE 
                WHEN hour(showtime) BETWEEN 18 AND 22 THEN 'prime_time'
                WHEN hour(showtime) BETWEEN 14 AND 17 THEN 'matinee'
                ELSE 'off_peak'
            END as screening_period
        FROM my_catalog.bronze.bronze_cinema_sales
        WHERE screening_id IS NOT NULL
          AND seats_sold > 0
    """)
    
    cinema_silver.write.mode("overwrite").saveAsTable("my_catalog.silver.silver_cinema_screenings")
    count = cinema_silver.count()
    print(f"    Silver Cinema Screenings: {count} records processed successfully")
    
    # 3. Clean Customers
    print(" Creating Silver Customers...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_customers")
    
    customers_silver = spark.sql("""
        SELECT 
            customer_id,
            name,
            email,
            preferred_genres,
            preferred_channels,
            signup_date,
            -- Add business logic
            CASE 
                WHEN signup_date >= '2024-04-01' THEN 'new_customer'
                WHEN signup_date >= '2024-01-01' THEN 'regular_customer'
                ELSE 'long_term_customer'
            END as segment,
            split(preferred_genres, ',') as genre_list,
            split(preferred_channels, ',') as channel_list,
            length(name) as name_length
        FROM my_catalog.bronze.bronze_customer_profiles
        WHERE customer_id IS NOT NULL
          AND email IS NOT NULL
          AND email LIKE '%@%'
    """)
    
    customers_silver.write.mode("overwrite").saveAsTable("my_catalog.silver.silver_customers")
    count = customers_silver.count()
    print(f"   Silver Customers: {count} records processed successfully")
    
    # 4. Clean Products (derived from transactions and inventory)
    print(" Creating Silver Products...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_products")
    
    # Create products from transaction data and pricing
    products_silver = spark.sql("""
        SELECT DISTINCT
            t.product_id,
            CASE 
                WHEN t.product_id LIKE '%dragon%' THEN 'Dragon Warrior Figure'
                WHEN t.product_id LIKE '%mystic%' THEN 'Mystic Academy Poster'
                WHEN t.product_id LIKE '%cyber%' THEN 'Cyber Knights Keychain'
                WHEN t.product_id LIKE '%bluray%' THEN 'Dragon Warrior BluRay'
                WHEN t.product_id LIKE '%manga%' THEN 'Fantasy Manga Collection'
                ELSE 'Unknown Product'
            END as product_name,
            CASE 
                WHEN t.product_id LIKE '%figure%' THEN 'collectible'
                WHEN t.product_id LIKE '%poster%' THEN 'artwork'
                WHEN t.product_id LIKE '%keychain%' THEN 'accessory'
                WHEN t.product_id LIKE '%bluray%' THEN 'media'
                WHEN t.product_id LIKE '%manga%' THEN 'media'
                ELSE 'merchandise'
            END as category,
            p.price as current_price,
            CASE 
                WHEN p.price >= 50 THEN 'premium'
                WHEN p.price >= 20 THEN 'standard'
                ELSE 'budget'
            END as price_tier
        FROM my_catalog.bronze.bronze_pos_transactions t
        JOIN my_catalog.bronze.bronze_product_pricing p ON t.product_id = p.product_id
        WHERE t.product_id IS NOT NULL
    """)
    
    products_silver.write.mode("overwrite").saveAsTable("my_catalog.silver.silver_products")
    count = products_silver.count()
    print(f"   Silver Products: {count} records processed successfully")
    
    # 5. Clean Inventory
    print(" Creating Silver Inventory...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_inventory")
    
    inventory_silver = spark.sql("""
        SELECT 
            supplier_code,
            product_sku as product_id,
            report_date,
            quantity_on_hand,
            unit_cost,
            shipment_date,
            -- Add business logic
            CASE 
                WHEN quantity_on_hand <= 50 THEN 'low_stock'
                WHEN quantity_on_hand <= 150 THEN 'medium_stock'
                ELSE 'high_stock'
            END as stock_level,
            datediff(report_date, shipment_date) as days_since_shipment,
            quantity_on_hand * unit_cost as inventory_value
        FROM my_catalog.bronze.bronze_supplier_inventory
        WHERE supplier_code IS NOT NULL
          AND quantity_on_hand >= 0
          AND unit_cost > 0
    """)
    
    inventory_silver.write.mode("overwrite").saveAsTable("my_catalog.silver.silver_inventory")
    count = inventory_silver.count()
    print(f"    Silver Inventory: {count} records processed successfully")
    
    # 6. Clean Theaters (from anime releases)
    print(" Creating Silver Theaters...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_theaters")
    
    theaters_silver = spark.sql("""
        SELECT 
            title_name as anime_title_id,
            title_name as title_name,
            release_date,
            genres as genre,
            platform,
            synopsis,
            -- Add business logic
            CASE 
                WHEN release_date >= '2024-06-01' THEN 'Summer2024'
                WHEN release_date >= '2024-04-01' THEN 'Spring2024'
                ELSE 'Winter2024'
            END as season,
            CASE 
                WHEN genres LIKE '%Action%' THEN 85.0
                WHEN genres LIKE '%Fantasy%' THEN 80.0
                WHEN genres LIKE '%Sci-Fi%' THEN 75.0
                WHEN genres LIKE '%Romance%' THEN 70.0
                ELSE 65.0
            END + 
            CASE 
                WHEN platform = 'Netflix' THEN 10.0
                WHEN platform = 'Crunchyroll' THEN 8.0
                ELSE 5.0
            END as popularity_score
        FROM my_catalog.bronze.bronze_anime_releases
        WHERE title_name IS NOT NULL
    """)
    
    theaters_silver.write.mode("overwrite").saveAsTable("my_catalog.silver.silver_theaters")
    count = theaters_silver.count()
    print(f"   Silver Theaters: {count} records processed successfully")
    
    # 7. Clean Customer Reviews
    print(" Creating Silver Customer Reviews...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_customer_reviews")
    
    reviews_silver = spark.sql("""
        SELECT 
            review_id,
            customer_id,
            product_id,
            cinema_id,
            rating,
            review_text,
            review_date,
            -- Add business logic
            CASE 
                WHEN rating >= 5 THEN 0.9
                WHEN rating >= 4 THEN 0.7
                WHEN rating >= 3 THEN 0.5
                WHEN rating >= 2 THEN 0.3
                ELSE 0.1
            END as sentiment_score,
            CASE 
                WHEN rating >= 4 THEN 'positive'
                WHEN rating >= 3 THEN 'neutral'
                ELSE 'negative'
            END as sentiment_category,
            length(review_text) as review_length
        FROM my_catalog.bronze.bronze_customer_reviews
        WHERE review_id IS NOT NULL
          AND rating BETWEEN 1 AND 5
    """)
    
    reviews_silver.write.mode("overwrite").saveAsTable("my_catalog.silver.silver_customer_reviews")
    count = reviews_silver.count()
    print(f"   Silver Customer Reviews: {count} records processed successfully")

def verify_silver_layer(spark):
    """Final verification of Silver layer"""
    print("\n" + "="*60)
    print("🔍 FINAL SILVER LAYER VERIFICATION")
    print("="*60)
    
    tables = [
        ("silver_transactions", "Cleaned POS data"),
        ("silver_cinema_screenings", "Cleaned cinema data"),
        ("silver_customers", "Cleaned customer data"),
        ("silver_products", "Derived product catalog"),
        ("silver_inventory", "Cleaned inventory data"),
        ("silver_theaters", "Cleaned anime data"),
        ("silver_customer_reviews", "Cleaned review data")
    ]
    
    total_records = 0
    for table_name, description in tables:
        count = spark.sql(f"SELECT COUNT(*) as count FROM my_catalog.silver.{table_name}").collect()[0]["count"]
        print(f"   {table_name}: {count} records ({description})")
        total_records += count
    
    print("="*60)
    print("SILVER LAYER COMPLETE!")
    print(f" Total Records: {total_records}")
    print(" Silver Layer Features:")
    print("   🧹 Data cleaning and validation")
    print("   🔄 Business logic applied")
    print("   📊 Derived metrics calculated")
    print("   ✅ Quality rules enforced")
    print("   📊 TOTAL: 7 Silver Tables")
    print("="*60)

def main():
    spark = create_spark_session()
    
    try:
        create_and_load_silver(spark)
        verify_silver_layer(spark)
        
       
    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
