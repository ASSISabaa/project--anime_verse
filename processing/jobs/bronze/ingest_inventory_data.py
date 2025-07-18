#!/usr/bin/env python3
"""
Inventory Data Ingestion - Supplier inventory updates
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-Inventory-Ingestion") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def ingest_inventory_data(spark):
    """Ingest supplier inventory sample data"""
    inventory_data = [
        ("SUP_001", "prod_dragon_figure", "2024-07-15", 150, 12.50, "2024-07-10"),
        ("SUP_002", "prod_mystic_poster", "2024-07-15", 200, 5.25, "2024-07-12"),
        ("SUP_003", "prod_cyber_keychain", "2024-07-16", 500, 3.00, "2024-07-14"),
        ("SUP_001", "prod_dragon_bluray", "2024-07-16", 75, 45.00, "2024-07-11"),
        ("SUP_004", "prod_fantasy_manga", "2024-07-17", 300, 4.50, "2024-07-13"),
        ("SUP_002", "prod_action_tshirt", "2024-07-18", 120, 8.75, "2024-07-15"),
        ("SUP_003", "prod_mystic_nendoroid", "2024-07-18", 80, 22.00, "2024-07-16"),
        ("SUP_001", "prod_space_model", "2024-07-19", 60, 35.50, "2024-07-17"),
        ("SUP_004", "prod_samurai_sword", "2024-07-19", 25, 120.00, "2024-07-18"),
        ("SUP_002", "prod_cafe_mug", "2024-07-20", 200, 6.25, "2024-07-19")
    ]
    
    schema = StructType([
        StructField("supplier_code", StringType()),
        StructField("product_sku", StringType()),
        StructField("report_date", StringType()),
        StructField("quantity_on_hand", IntegerType()),
        StructField("unit_cost", FloatType()),
        StructField("shipment_date", StringType())
    ])
    
    df = spark.createDataFrame(inventory_data, schema) \
        .withColumn("report_date", to_date("report_date")) \
        .withColumn("shipment_date", to_date("shipment_date"))
    
    df.writeTo("my_catalog.bronze.bronze_supplier_inventory").overwritePartitions()
    print(f"✅ Ingested {df.count()} inventory records")

def main():
    spark = create_spark_session()
    try:
        ingest_inventory_data(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
