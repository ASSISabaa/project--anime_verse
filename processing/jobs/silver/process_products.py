#!/usr/bin/env python3
"""
Process Products - Create product catalog from transactions and pricing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Process-Products") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_products(spark):
    """Create product catalog from transaction and pricing data"""
    transactions = spark.table("my_catalog.bronze.bronze_pos_transactions")
    pricing = spark.table("my_catalog.bronze.bronze_product_pricing")
    
    products_silver = transactions.select("product_id").distinct() \
        .join(pricing.filter(col("is_current") == True), "product_id", "left") \
        .select(
            col("product_id"),
            # Product name mapping
            when(col("product_id").contains("dragon_figure"), "Dragon Warrior Figure")
            .when(col("product_id").contains("mystic_poster"), "Mystic Academy Poster")
            .when(col("product_id").contains("cyber_keychain"), "Cyber Knights Keychain")
            .when(col("product_id").contains("dragon_bluray"), "Dragon Warrior BluRay")
            .when(col("product_id").contains("fantasy_manga"), "Fantasy Manga Collection")
            .when(col("product_id").contains("action_tshirt"), "Action Hero T-Shirt")
            .when(col("product_id").contains("mystic_nendoroid"), "Mystic Academy Nendoroid")
            .when(col("product_id").contains("space_model"), "Space Odyssey Model")
            .when(col("product_id").contains("samurai_sword"), "Samurai Chronicles Replica Sword")
            .when(col("product_id").contains("cafe_mug"), "Slice of Life Cafe Mug")
            .otherwise("Unknown Product").alias("product_name"),
            # Category classification
            when(col("product_id").contains("figure"), "collectible")
            .when(col("product_id").contains("nendoroid"), "collectible")
            .when(col("product_id").contains("poster"), "artwork")
            .when(col("product_id").contains("keychain"), "accessory")
            .when(col("product_id").contains("mug"), "accessory")
            .when(col("product_id").contains("tshirt"), "apparel")
            .when(col("product_id").contains("bluray"), "media")
            .when(col("product_id").contains("manga"), "media")
            .when(col("product_id").contains("model"), "model")
            .when(col("product_id").contains("sword"), "replica")
            .otherwise("merchandise").alias("category"),
            # Current price
            coalesce(col("price"), lit(0.0)).alias("current_price"),
            # Price tier classification
            when(coalesce(col("price"), lit(0.0)) >= 50, "premium")
            .when(coalesce(col("price"), lit(0.0)) >= 20, "standard")
            .otherwise("budget").alias("price_tier")
        )
    
    products_silver.writeTo("my_catalog.silver.silver_products").overwritePartitions()
    print(f"✅ Processed {products_silver.count()} product records")

def main():
    spark = create_spark_session()
    try:
        process_products(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()