#!/usr/bin/env python3
"""
Build Dimensions - All dimension tables for Gold layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Gold-Build-Dimensions") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def build_dimensions(spark):
    """Build all dimension tables"""
    
    # 1. Dim Customers
    customers = spark.table("my_catalog.silver.silver_customers")
    customer_profiles = spark.table("my_catalog.bronze.bronze_customer_profiles")
    
    dim_customers = customers.join(
        customer_profiles, "customer_id", "left"
    ).select(
        customers["customer_id"],
        customers["name"],
        customer_profiles["email"],
        customers["signup_date"],
        customers["segment"]
    )
    
    dim_customers.writeTo("my_catalog.gold.gold_dim_customers").overwritePartitions()
    print(f"✅ Built customer dimension: {dim_customers.count()} records")
    
    # 2. Dim Products
    products = spark.table("my_catalog.silver.silver_products")
    dim_products = products.select("*")
    
    dim_products.writeTo("my_catalog.gold.gold_dim_products").overwritePartitions()
    print(f"✅ Built product dimension: {dim_products.count()} records")
    
    # 3. Dim Anime Titles
    theaters = spark.table("my_catalog.silver.silver_theaters")
    
    dim_anime = theaters.select(
        col("anime_title_id"),
        col("anime_title_id").alias("title_id"),
        col("genre"),
        col("season").alias("release_season"),
        col("popularity_score"),
        lit("active").alias("status")
    )
    
    dim_anime.writeTo("my_catalog.gold.gold_dim_anime_titles").overwritePartitions()
    print(f"✅ Built anime dimension: {dim_anime.count()} records")

def main():
    spark = create_spark_session()
    try:
        build_dimensions(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
