#!/usr/bin/env python3
"""
Create Silver Schema - All cleaned table structures
"""

from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Schema-Creation") \
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

def create_silver_schemas(spark):
    """Create all Silver table schemas"""
    print("🥈 Creating Silver schemas...")
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.silver")
    
    # Create all 7 Silver tables
    schemas = [
        """CREATE TABLE IF NOT EXISTS my_catalog.silver.silver_transactions (
            transaction_id STRING, product_id STRING, quantity INT, price FLOAT,
            timestamp TIMESTAMP, event_type STRING, channel STRING, customer_id STRING,
            total_amount FLOAT, transaction_date DATE, transaction_hour INT,
            time_period STRING, channel_type STRING
        ) USING ICEBERG PARTITIONED BY (transaction_date)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.silver.silver_cinema_screenings (
            screening_id STRING, customer_id STRING, anime_title_id STRING, ticket_type STRING,
            showtime TIMESTAMP, seats_sold INT, concessions_sales FLOAT,
            date_key STRING, ticket_revenue FLOAT, screening_period STRING
        ) USING ICEBERG PARTITIONED BY (date_key)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.silver.silver_customers (
            customer_id STRING, name STRING, email STRING, preferred_genres STRING,
            preferred_channels STRING, signup_date DATE, segment STRING,
            genre_list ARRAY<STRING>, channel_list ARRAY<STRING>, name_length INT
        ) USING ICEBERG PARTITIONED BY (segment)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.silver.silver_products (
            product_id STRING, product_name STRING, category STRING,
            current_price FLOAT, price_tier STRING
        ) USING ICEBERG PARTITIONED BY (category)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.silver.silver_inventory (
            supplier_code STRING, product_id STRING, report_date DATE,
            quantity_on_hand INT, unit_cost FLOAT, shipment_date DATE,
            stock_level STRING, days_since_shipment INT, inventory_value FLOAT
        ) USING ICEBERG PARTITIONED BY (report_date)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.silver.silver_theaters (
            anime_title_id STRING, title_name STRING, release_date DATE,
            genre STRING, platform STRING, synopsis STRING,
            season STRING, popularity_score FLOAT
        ) USING ICEBERG PARTITIONED BY (season)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.silver.silver_customer_reviews (
            review_id STRING, customer_id STRING, product_id STRING, cinema_id STRING,
            rating INT, review_text STRING, review_date DATE,
            sentiment_score FLOAT, sentiment_category STRING, review_length INT
        ) USING ICEBERG PARTITIONED BY (sentiment_category)"""
    ]
    
    for schema in schemas:
        spark.sql(schema)
    
    print("✅ All Silver schemas created!")

def main():
    spark = create_spark_session()
    try:
        create_silver_schemas(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()