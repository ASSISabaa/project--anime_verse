#!/usr/bin/env python3
"""
Build Fact Sales - Sales analytics fact table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Gold-Fact-Sales") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def build_fact_sales(spark):
    """Build sales fact table from Silver transactions"""
    transactions = spark.table("my_catalog.silver.silver_transactions")
    
    fact_sales = transactions.select(
        concat(lit("sale_"), col("transaction_id")).alias("sales_id"),
        col("customer_id"),
        col("product_id"),
        col("date_key"),
        col("channel"),
        col("quantity"),
        col("price").alias("unit_price"),
        col("total_amount"),
        col("time_period"),
        col("channel_type")
    )
    
    fact_sales.writeTo("my_catalog.gold.gold_fact_sales").overwritePartitions()
    print(f"✅ Built sales fact: {fact_sales.count()} records")

def main():
    spark = create_spark_session()
    try:
        build_fact_sales(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()