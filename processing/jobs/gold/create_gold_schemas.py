#!/usr/bin/env python3
"""
Create Gold Schema - All analytics-ready table structures
"""

from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Gold-Schema-Creation") \
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

def create_gold_schemas(spark):
    """Create all Gold table schemas"""
    print("🥇 Creating Gold schemas...")
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.gold")
    
    # Create all 9 Gold tables
    schemas = [
        # Fact Tables
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_fact_sales (
            sales_id STRING, customer_id STRING, product_id STRING, date_key STRING,
            channel STRING, quantity INT, unit_price FLOAT, total_amount FLOAT,
            time_period STRING, channel_type STRING
        ) USING ICEBERG PARTITIONED BY (date_key)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_fact_cinema_attendance (
            attendance_id STRING, customer_id STRING, anime_title_id STRING, cinema_id STRING,
            date_key STRING, ticket_type STRING, seats_sold INT, concession_sales FLOAT
        ) USING ICEBERG PARTITIONED BY (date_key)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_fact_customer_engagement (
            engagement_id STRING, customer_id STRING, anime_title_id STRING,
            first_touch_channel STRING, conversion_channel STRING,
            engagement_score FLOAT, date_key STRING
        ) USING ICEBERG PARTITIONED BY (date_key)""",
        
        # Dimension Tables
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_dim_customers (
            customer_id STRING, name STRING, email STRING, signup_date DATE,
            segment STRING, preferred_genres STRING, preferred_channels STRING
        ) USING ICEBERG PARTITIONED BY (segment)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_dim_products (
            product_id STRING, product_name STRING, category STRING,
            current_price FLOAT, price_tier STRING
        ) USING ICEBERG PARTITIONED BY (category)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_dim_anime_titles (
            anime_title_id STRING, title_id STRING, genre STRING,
            release_season STRING, popularity_score FLOAT, status STRING
        ) USING ICEBERG PARTITIONED BY (release_season)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_dim_calendar (
            date_key STRING, year INT, month INT, day INT,
            quarter INT, day_name STRING, is_weekend BOOLEAN
        ) USING ICEBERG PARTITIONED BY (year, quarter)""",
        
        # SCD2 Tables
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_dim_customer_preferences_scd (
            customer_id STRING, version_id INT, preferred_genres STRING,
            preferred_channels STRING, start_date DATE, end_date DATE, is_current BOOLEAN
        ) USING ICEBERG PARTITIONED BY (is_current)""",
        
        """CREATE TABLE IF NOT EXISTS my_catalog.gold.gold_dim_product_pricing_scd (
            product_id STRING, version_id INT, price FLOAT,
            start_date DATE, end_date DATE, is_current BOOLEAN
        ) USING ICEBERG PARTITIONED BY (is_current)"""
    ]
    
    for schema in schemas:
        spark.sql(schema)
    
    print("✅ All Gold schemas created!")

def main():
    spark = create_spark_session()
    try:
        create_gold_schemas(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()