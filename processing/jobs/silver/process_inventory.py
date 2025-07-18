#!/usr/bin/env python3
"""
Process Inventory - Transform supplier inventory to Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Process-Inventory") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_inventory(spark):
    """Clean and transform inventory data"""
    inventory_bronze = spark.table("my_catalog.bronze.bronze_supplier_inventory")
    
    inventory_silver = inventory_bronze.select(
        concat(col("supplier_code"), lit("_"), col("product_sku"), lit("_"), 
               date_format(col("report_date"), "yyyyMMdd")).alias("inventory_id"),
        col("product_sku").alias("product_id"),
        col("supplier_code").alias("supplier_id"),
        date_format(col("report_date"), "yyyyMMdd").alias("date_key"),
        col("quantity_on_hand").alias("stock_level"),
        col("shipment_date"),
        # Calculate expected delivery (shipment + 3 days)
        date_add(col("shipment_date"), 3).alias("expected_delivery_date"),
        col("unit_cost")
    ).filter(col("product_sku").isNotNull())
    
    inventory_silver.writeTo("my_catalog.silver.silver_inventory").overwritePartitions()
    print(f"✅ Processed {inventory_silver.count()} inventory records")

def main():
    spark = create_spark_session()
    try:
        process_inventory(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
