#!/usr/bin/env python3
"""
POS Data Ingestion - Point of sale transactions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-POS-Ingestion") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def ingest_pos_data(spark):
    """Ingest POS transaction sample data"""
    pos_data = [
        ("txn_001", "prod_dragon_figure", 2, 29.99, "2024-07-15 10:30:00", "purchase", "online", "cust_001"),
        ("txn_002", "prod_mystic_poster", 1, 15.50, "2024-07-15 14:20:00", "purchase", "store", "cust_002"),
        ("txn_003", "prod_cyber_keychain", 5, 8.99, "2024-07-16 09:15:00", "purchase", "online", "cust_003"),
        ("txn_004", "prod_dragon_bluray", 1, 89.99, "2024-07-16 16:45:00", "purchase", "store", "cust_001"),
        ("txn_005", "prod_fantasy_manga", 3, 12.50, "2024-07-17 12:00:00", "purchase", "mobile", "cust_004"),
        ("txn_006", "prod_action_tshirt", 2, 22.00, "2024-07-17 15:30:00", "purchase", "online", "cust_005"),
        ("txn_007", "prod_mystic_nendoroid", 1, 42.00, "2024-07-18 11:45:00", "purchase", "store", "cust_006"),
        ("txn_008", "prod_space_model", 1, 55.00, "2024-07-18 13:20:00", "purchase", "online", "cust_007"),
        ("txn_009", "prod_samurai_sword", 1, 135.00, "2024-07-19 16:10:00", "purchase", "store", "cust_002"),
        ("txn_010", "prod_cafe_mug", 3, 9.99, "2024-07-19 18:25:00", "purchase", "mobile", "cust_003")
    ]
    
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", FloatType()),
        StructField("timestamp", StringType()),
        StructField("event_type", StringType()),
        StructField("channel", StringType()),
        StructField("customer_id", StringType())
    ])
    
    df = spark.createDataFrame(pos_data, schema) \
        .withColumn("timestamp", to_timestamp("timestamp"))
    
    df.writeTo("my_catalog.bronze.bronze_pos_transactions").overwritePartitions()
    print(f"✅ Ingested {df.count()} POS transactions")

def main():
    spark = create_spark_session()
    try:
        ingest_pos_data(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()