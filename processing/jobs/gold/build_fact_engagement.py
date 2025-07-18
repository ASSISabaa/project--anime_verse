#!/usr/bin/env python3
"""
Build Fact Customer Engagement - Customer interaction analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Gold-Fact-Engagement") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def build_fact_engagement(spark):
    """Build customer engagement fact table"""
    transactions = spark.table("my_catalog.silver.silver_transactions")
    screenings = spark.table("my_catalog.silver.silver_cinema_screenings")
    
    # Sales engagement
    sales_engagement = transactions.select(
        col("customer_id"),
        col("anime_title_id"),
        col("channel").alias("first_touch_channel"),
        col("channel").alias("conversion_channel"),
        (col("quantity") * col("price") / 10).alias("engagement_score"),
        col("date_key")
    )
    
    # Cinema engagement
    cinema_engagement = screenings.select(
        col("customer_id"),
        col("anime_title_id"),
        lit("cinema").alias("first_touch_channel"),
        lit("cinema").alias("conversion_channel"),
        (col("seats_sold") * 15).alias("engagement_score"),
        col("date_key")
    )
    
    # Combine all engagement
    all_engagement = sales_engagement.unionByName(cinema_engagement)
    
    fact_engagement = all_engagement.withColumn(
        "engagement_id",
        concat(lit("eng_"), monotonically_increasing_id())
    ).select(
        "engagement_id", "customer_id", "anime_title_id",
        "first_touch_channel", "conversion_channel", 
        "engagement_score", "date_key"
    )
    
    fact_engagement.writeTo("my_catalog.gold.gold_fact_customer_engagement").overwritePartitions()
    print(f"✅ Built engagement fact: {fact_engagement.count()} records")

def main():
    spark = create_spark_session()
    try:
        build_fact_engagement(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
