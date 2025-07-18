#!/usr/bin/env python3
"""
Process Customers - Transform customer profiles to Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Process-Customers") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_customers(spark):
    """Clean and transform customer profile data"""
    customers_bronze = spark.table("my_catalog.bronze.bronze_customer_profiles")
    
    customers_silver = customers_bronze.select(
        col("customer_id"),
        col("name"),
        col("email"),
        col("preferred_genres"),
        col("preferred_channels"),
        col("signup_date"),
        # Customer segmentation based on signup date
        when(col("signup_date") >= "2024-04-01", "new_customer")
        .when(col("signup_date") >= "2024-01-01", "regular_customer")
        .otherwise("long_term_customer").alias("segment"),
        # Split comma-separated values into arrays
        split(col("preferred_genres"), ",").alias("genre_list"),
        split(col("preferred_channels"), ",").alias("channel_list"),
        # Additional metrics
        length(col("name")).alias("name_length"),
        # Email domain extraction
        regexp_extract(col("email"), "@(.+)", 1).alias("email_domain")
    ).filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull() &
        col("email").rlike("^[^@]+@[^@]+\\.[^@]+$")  # Valid email format
    )
    
    customers_silver.writeTo("my_catalog.silver.silver_customers").overwritePartitions()
    print(f"✅ Processed {customers_silver.count()} customer records")

def main():
    spark = create_spark_session()
    try:
        process_customers(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()