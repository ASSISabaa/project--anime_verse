#!/usr/bin/env python3
"""
Process Reviews - Transform customer reviews to Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Process-Reviews") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_reviews(spark):
    """Transform customer reviews with sentiment analysis"""
    reviews_bronze = spark.table("my_catalog.bronze.bronze_customer_reviews")
    
    reviews_silver = reviews_bronze.select(
        col("review_id"),
        col("customer_id"),
        col("product_id"),
        col("cinema_id"),
        col("rating"),
        col("review_text"),
        col("review_date"),
        # Calculate sentiment score based on rating and text analysis
        when(col("rating") >= 5, 0.9)
        .when(col("rating") >= 4, 0.7)
        .when(col("rating") >= 3, 0.5)
        .when(col("rating") >= 2, 0.3)
        .otherwise(0.1).alias("sentiment_score")
    ).filter(col("review_id").isNotNull())
    
    reviews_silver.writeTo("my_catalog.silver.silver_customer_reviews").overwritePartitions()
    print(f"✅ Processed {reviews_silver.count()} review records")

def main():
    spark = create_spark_session()
    try:
        process_reviews(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
