#!/usr/bin/env python3
"""
Create Bronze Schema - All table structures
"""

from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-Schema-Creation") \
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

def create_bronze_schemas(spark):
    """Create all Bronze table schemas"""
    print("🥉 Creating Bronze schemas...")
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.bronze")
    
    # Create all 7 Bronze tables
    schemas = [
        """CREATE TABLE IF NOT EXISTS my_catalog.bronze.bronze_pos_transactions (
            transaction_id STRING, product_id STRING, quantity INT, price FLOAT,
            timestamp TIMESTAMP, event_type STRING, channel STRING, customer_id STRING
        ) USING ICEBERG PARTITIONED BY (days(timestamp))""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.bronze.bronze_cinema_sales (
            screening_id STRING, customer_id STRING, title_name STRING, ticket_type STRING,
            showtime TIMESTAMP, seats_sold INT, concession_amt FLOAT
        ) USING ICEBERG PARTITIONED BY (days(showtime))""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.bronze.bronze_customer_profiles (
            customer_id STRING, name STRING, email STRING, preferred_genres STRING,
            preferred_channels STRING, signup_date DATE
        ) USING ICEBERG PARTITIONED BY (signup_date)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.bronze.bronze_anime_releases (
            title_name STRING, release_date DATE, genres STRING, platform STRING, synopsis STRING
        ) USING ICEBERG PARTITIONED BY (release_date)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.bronze.bronze_supplier_inventory (
            supplier_code STRING, product_sku STRING, report_date DATE,
            quantity_on_hand INT, unit_cost FLOAT, shipment_date DATE
        ) USING ICEBERG PARTITIONED BY (report_date)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.bronze.bronze_customer_reviews (
            review_id STRING, customer_id STRING, product_id STRING, cinema_id STRING,
            rating INT, review_text STRING, review_date DATE
        ) USING ICEBERG PARTITIONED BY (review_date)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.bronze.bronze_product_pricing (
            product_id STRING, price FLOAT, start_date DATE, end_date DATE
        ) USING ICEBERG PARTITIONED BY (start_date)"""
    ]
    
    for schema in schemas:
        spark.sql(schema)
    
    print("✅ All Bronze schemas created!")

def main():
    spark = create_spark_session()
    try:
        create_bronze_schemas(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()