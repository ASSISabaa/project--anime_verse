#!/usr/bin/env python3


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("AnimeVerse-Complete-Bronze") \
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

def create_and_load_bronze(spark):
    """Create all Bronze tables and load sample data"""
    
    print("CREATING COMPLETE BRONZE LAYER...")
    print("="*60)
    
    # Create Bronze schema
    spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.bronze")
    print("Bronze schema created successfully")
    
    # 1. POS Transactions (Streaming source)
    print("Creating POS Transactions table...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_pos_transactions")
    spark.sql("""
        CREATE TABLE my_catalog.bronze.bronze_pos_transactions (
            transaction_id STRING, product_id STRING, quantity INT, price FLOAT,
            timestamp TIMESTAMP, event_type STRING, channel STRING, customer_id STRING
        ) USING ICEBERG PARTITIONED BY (days(timestamp))
    """)
    
    # Load POS data
    pos_data = [
        ("txn_001", "prod_dragon_figure", 2, 29.99, "2024-07-15 10:30:00", "purchase", "online", "cust_001"),
        ("txn_002", "prod_mystic_poster", 1, 15.50, "2024-07-15 14:20:00", "purchase", "store", "cust_002"),
        ("txn_003", "prod_cyber_keychain", 5, 8.99, "2024-07-16 09:15:00", "purchase", "online", "cust_003"),
        ("txn_004", "prod_dragon_bluray", 1, 89.99, "2024-07-16 16:45:00", "purchase", "store", "cust_001"),
        ("txn_005", "prod_fantasy_manga", 3, 12.50, "2024-07-17 12:00:00", "purchase", "mobile", "cust_004")
    ]
    
    pos_schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", FloatType()),
        StructField("timestamp", StringType()),
        StructField("event_type", StringType()),
        StructField("channel", StringType()),
        StructField("customer_id", StringType())
    ])
    
    pos_df = spark.createDataFrame(pos_data, pos_schema) \
        .withColumn("timestamp", to_timestamp("timestamp"))
    pos_df.write.mode("overwrite").saveAsTable("my_catalog.bronze.bronze_pos_transactions")
    print("   POS Transactions: 5 records loaded successfully")
    
    # 2. Cinema Sales (Streaming source)
    print("Creating Cinema Sales table...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_cinema_sales")
    spark.sql("""
        CREATE TABLE my_catalog.bronze.bronze_cinema_sales (
            screening_id STRING, customer_id STRING, title_name STRING, ticket_type STRING,
            showtime TIMESTAMP, seats_sold INT, concession_amt FLOAT
        ) USING ICEBERG PARTITIONED BY (days(showtime))
    """)
    
    # Load Cinema data
    cinema_data = [
        ("screen_001", "cust_001", "Dragon Warrior Chronicles", "premium", "2024-07-15 19:00:00", 2, 15.50),
        ("screen_002", "cust_002", "Mystic Academy", "standard", "2024-07-15 21:30:00", 1, 8.00),
        ("screen_003", "cust_003", "Cyber Knights", "premium", "2024-07-16 18:00:00", 3, 22.75),
        ("screen_004", "cust_004", "Dragon Warrior Chronicles", "imax", "2024-07-16 20:15:00", 2, 35.00)
    ]
    
    cinema_schema = StructType([
        StructField("screening_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("title_name", StringType()),
        StructField("ticket_type", StringType()),
        StructField("showtime", StringType()),
        StructField("seats_sold", IntegerType()),
        StructField("concession_amt", FloatType())
    ])
    
    cinema_df = spark.createDataFrame(cinema_data, cinema_schema) \
        .withColumn("showtime", to_timestamp("showtime"))
    cinema_df.write.mode("overwrite").saveAsTable("my_catalog.bronze.bronze_cinema_sales")
    print("   Cinema Sales: 4 records loaded successfully")
    
    # 3. Customer Profiles (Batch/API source & SCD source)
    print("Creating Customer Profiles table...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_customer_profiles")
    spark.sql("""
        CREATE TABLE my_catalog.bronze.bronze_customer_profiles (
            customer_id STRING, name STRING, email STRING, preferred_genres STRING,
            preferred_channels STRING, signup_date DATE
        ) USING ICEBERG PARTITIONED BY (signup_date)
    """)
    
    # Load Customer data
    customer_data = [
        ("cust_001", "Akira Tanaka", "akira@anime.com", "Action,Fantasy", "online,store", "2024-01-15"),
        ("cust_002", "Sakura Yamamoto", "sakura@manga.jp", "Fantasy,Romance", "store,cinema", "2024-02-20"),
        ("cust_003", "Hiroshi Sato", "hiroshi@otaku.net", "Sci-Fi,Action", "online,mobile", "2024-03-10"),
        ("cust_004", "Emi Watanabe", "emi@cosplay.org", "Fantasy,Drama", "online,cinema", "2024-04-05")
    ]
    
    customer_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("preferred_genres", StringType()),
        StructField("preferred_channels", StringType()),
        StructField("signup_date", StringType())
    ])
    
    customer_df = spark.createDataFrame(customer_data, customer_schema) \
        .withColumn("signup_date", to_date("signup_date"))
    customer_df.write.mode("overwrite").saveAsTable("my_catalog.bronze.bronze_customer_profiles")
    print("   Customer Profiles: 4 records loaded successfully")
    
    # 4. Anime Releases (Batch/API source)
    print(" Creating Anime Releases table...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_anime_releases")
    spark.sql("""
        CREATE TABLE my_catalog.bronze.bronze_anime_releases (
            title_name STRING, release_date DATE, genres STRING, platform STRING, synopsis STRING
        ) USING ICEBERG PARTITIONED BY (release_date)
    """)
    
    # Load Anime data with proper DATE casting
    spark.sql("INSERT INTO my_catalog.bronze.bronze_anime_releases VALUES ('Dragon Warrior Chronicles', DATE('2024-04-01'), 'Action,Adventure', 'Netflix', 'Epic dragon adventure')")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_anime_releases VALUES ('Mystic Academy', DATE('2024-05-15'), 'Fantasy,Drama', 'Crunchyroll', 'Magic school adventures')")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_anime_releases VALUES ('Cyber Knights', DATE('2024-06-20'), 'Sci-Fi,Action', 'Funimation', 'Future warriors')")
    print("    Anime Releases: 3 records loaded successfully")
    
    # 5. Supplier Inventory (Late arrival source)
    print(" Creating Supplier Inventory table...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_supplier_inventory")
    spark.sql("""
        CREATE TABLE my_catalog.bronze.bronze_supplier_inventory (
            supplier_code STRING, product_sku STRING, report_date DATE,
            quantity_on_hand INT, unit_cost FLOAT, shipment_date DATE
        ) USING ICEBERG PARTITIONED BY (report_date)
    """)
    
    # Load Supplier data
    spark.sql("INSERT INTO my_catalog.bronze.bronze_supplier_inventory VALUES ('SUP_001', 'prod_dragon_figure', DATE('2024-07-15'), 150, 12.50, DATE('2024-07-10'))")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_supplier_inventory VALUES ('SUP_002', 'prod_mystic_poster', DATE('2024-07-15'), 200, 5.25, DATE('2024-07-12'))")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_supplier_inventory VALUES ('SUP_003', 'prod_cyber_keychain', DATE('2024-07-16'), 500, 3.50, DATE('2024-07-14'))")
    print("   Supplier Inventory: 3 records loaded successfully")
    
    # 6. Customer Reviews (Late arrival source)
    print("Creating Customer Reviews table...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_customer_reviews")
    spark.sql("""
        CREATE TABLE my_catalog.bronze.bronze_customer_reviews (
            review_id STRING, customer_id STRING, product_id STRING, cinema_id STRING,
            rating INT, review_text STRING, review_date DATE
        ) USING ICEBERG PARTITIONED BY (review_date)
    """)
    
    # Load Reviews data
    spark.sql("INSERT INTO my_catalog.bronze.bronze_customer_reviews VALUES ('rev_001', 'cust_001', 'prod_dragon_figure', 'cinema_001', 5, 'Amazing quality!', DATE('2024-07-16'))")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_customer_reviews VALUES ('rev_002', 'cust_002', 'prod_mystic_poster', 'cinema_002', 4, 'Great artwork', DATE('2024-07-16'))")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_customer_reviews VALUES ('rev_003', 'cust_003', 'prod_cyber_keychain', 'cinema_001', 5, 'Perfect size', DATE('2024-07-17'))")
    print("   Customer Reviews: 3 records loaded    successfully")
    
    # 7. Product Pricing (SCD source)
    print(" Creating Product Pricing table...")
    spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_product_pricing")
    spark.sql("""
        CREATE TABLE my_catalog.bronze.bronze_product_pricing (
            product_id STRING, price FLOAT, start_date DATE, end_date DATE
        ) USING ICEBERG PARTITIONED BY (start_date)
    """)
    
    # Load Pricing data
    spark.sql("INSERT INTO my_catalog.bronze.bronze_product_pricing VALUES ('prod_dragon_figure', 29.99, DATE('2024-01-01'), DATE('9999-12-31'))")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_product_pricing VALUES ('prod_mystic_poster', 15.50, DATE('2024-01-01'), DATE('9999-12-31'))")
    spark.sql("INSERT INTO my_catalog.bronze.bronze_product_pricing VALUES ('prod_cyber_keychain', 8.99, DATE('2024-01-01'), DATE('9999-12-31'))")
    print("  Product Pricing: 3 records loaded successfully")

def verify_bronze_layer(spark):
    """Final verification of Bronze layer"""
    print("\n" + "="*60)
    print(" FINAL BRONZE LAYER VERIFICATION")
    print("="*60)
    
    tables = [
        ("bronze_pos_transactions", "Streaming"),
        ("bronze_cinema_sales", "Streaming"),
        ("bronze_customer_profiles", "Batch/API + SCD"),
        ("bronze_anime_releases", "Batch/API"),
        ("bronze_supplier_inventory", "Late Arrival"),
        ("bronze_customer_reviews", "Late Arrival"),
        ("bronze_product_pricing", "SCD Source")
    ]
    
    total_records = 0
    for table_name, source_type in tables:
        count = spark.sql(f"SELECT COUNT(*) as count FROM my_catalog.bronze.{table_name}").collect()[0]["count"]
        print(f"   ✅ {table_name}: {count} records ({source_type})")
        total_records += count
    
    print("="*60)
    print(" BRONZE LAYER COMPLETE!")
    print(f" Total Records: {total_records}")
    print("   🌊 Streaming Sources: 2 tables")
    print("   ⏰ Late Arrival Sources: 2 tables")
    print("   📦 Batch/API Sources: 2 tables")
    print("   🔄 SCD Sources: 1 table")
    print("   📊 TOTAL: 7 Bronze Tables")
    print("="*60)

def main():
    spark = create_spark_session()
    
    try:
        create_and_load_bronze(spark)
        verify_bronze_layer(spark)
        
       
        
    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
