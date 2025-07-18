#!/usr/bin/env python3
"""
Pricing Data Ingestion - Product pricing for SCD2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-Pricing-Ingestion") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def ingest_pricing_data(spark):
    """Ingest product pricing sample data"""
    pricing_data = [
        # Historical pricing for SCD2
        ("prod_dragon_figure", 29.99, "2024-01-01", "2024-06-30"),
        ("prod_dragon_figure", 34.99, "2024-07-01", "9999-12-31"),
        ("prod_mystic_poster", 15.50, "2024-01-01", "9999-12-31"),
        ("prod_cyber_keychain", 7.99, "2024-01-01", "2024-05-31"),
        ("prod_cyber_keychain", 8.99, "2024-06-01", "9999-12-31"),
        ("prod_dragon_bluray", 89.99, "2024-01-01", "2024-05-31"),
        ("prod_dragon_bluray", 79.99, "2024-06-01", "9999-12-31"),
        ("prod_fantasy_manga", 12.50, "2024-01-01", "9999-12-31"),
        ("prod_action_tshirt", 22.00, "2024-01-01", "2024-04-30"),
        ("prod_action_tshirt", 25.00, "2024-05-01", "9999-12-31"),
        ("prod_mystic_nendoroid", 42.00, "2024-01-01", "2024-06-15"),
        ("prod_mystic_nendoroid", 45.00, "2024-06-16", "9999-12-31"),
        ("prod_space_model", 55.00, "2024-01-01", "9999-12-31"),
        ("prod_samurai_sword", 150.00, "2024-01-01", "2024-03-31"),
        ("prod_samurai_sword", 135.00, "2024-04-01", "9999-12-31"),
        ("prod_cafe_mug", 9.99, "2024-01-01", "9999-12-31")
    ]
    
    schema = StructType([
        StructField("product_id", StringType()),
        StructField("price", FloatType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType())
    ])
    
    df = spark.createDataFrame(pricing_data, schema) \
        .withColumn("start_date", to_date("start_date")) \
        .withColumn("end_date", to_date("end_date"))
    
    df.writeTo("my_catalog.bronze.bronze_product_pricing").overwritePartitions()
    print(f"✅ Ingested {df.count()} pricing records")

def main():
    spark = create_spark_session()
    try:
        ingest_pricing_data(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
