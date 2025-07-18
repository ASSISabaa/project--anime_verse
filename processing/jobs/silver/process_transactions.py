#!/usr/bin/env python3
"""
Process Transactions - Transform POS data to Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Process-Transactions") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_transactions(spark):
    """Clean and transform POS transaction data"""
    transactions_bronze = spark.table("my_catalog.bronze.bronze_pos_transactions")
    
    transactions_silver = transactions_bronze.select(
        col("transaction_id"),
        col("product_id"),
        col("quantity"),
        col("price"),
        col("timestamp"),
        col("event_type"),
        col("channel"),
        col("customer_id"),
        # Add business logic
        (col("quantity") * col("price")).alias("total_amount"),
        date(col("timestamp")).alias("transaction_date"),
        hour(col("timestamp")).alias("transaction_hour"),
        # Time period classification
        when(hour(col("timestamp")).between(6, 12), "morning")
        .when(hour(col("timestamp")).between(13, 18), "afternoon")
        .when(hour(col("timestamp")).between(19, 23), "evening")
        .otherwise("night").alias("time_period"),
        # Channel type classification
        when(col("channel").isin("online", "mobile"), "digital")
        .otherwise("physical").alias("channel_type"),
        # Create date key for dimensional modeling
        date_format(col("timestamp"), "yyyyMMdd").alias("date_key")
    ).filter(
        col("transaction_id").isNotNull() &
        (col("quantity") > 0) &
        (col("price") > 0)
    )
    
    transactions_silver.writeTo("my_catalog.silver.silver_transactions").overwritePartitions()
    print(f"✅ Processed {transactions_silver.count()} transaction records")

def main():
    spark = create_spark_session()
    try:
        process_transactions(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()