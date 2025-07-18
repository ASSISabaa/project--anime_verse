#!/usr/bin/env python3
"""
Build SCD2 Tables - Slowly Changing Dimensions Type 2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("Gold-Build-SCD2") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def build_scd2_tables(spark):
    """Build both SCD2 tables"""
    
    # 1. Customer Preferences SCD2
    customer_profiles = spark.table("my_catalog.bronze.bronze_customer_profiles")
    
    customer_prefs_scd = customer_profiles.select(
        col("customer_id"),
        lit(1).alias("version_id"),
        col("preferred_genres"),
        col("preferred_channels"),
        col("signup_date").alias("start_date"),
        lit("9999-12-31").cast("date").alias("end_date"),
        lit(True).alias("is_current")
    )
    
    customer_prefs_scd.writeTo("my_catalog.gold.gold_dim_customer_preferences_scd").overwritePartitions()
    print(f"✅ Built customer preferences SCD2: {customer_prefs_scd.count()} records")
    
    # 2. Product Pricing SCD2
    pricing = spark.table("my_catalog.bronze.bronze_product_pricing")
    
    pricing_scd = pricing.withColumn(
        "version_id",
        row_number().over(Window.partitionBy("product_id").orderBy("start_date"))
    ).select(
        col("product_id"),
        col("version_id"),
        col("price"),
        col("start_date"),
        col("end_date"),
        when(col("end_date") == "9999-12-31", True).otherwise(False).alias("is_current")
    )
    
    pricing_scd.writeTo("my_catalog.gold.gold_dim_product_pricing_scd").overwritePartitions()
    print(f"✅ Built product pricing SCD2: {pricing_scd.count()} records")

def main():
    spark = create_spark_session()
    try:
        build_scd2_tables(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
