#!/usr/bin/env python3
"""
Reviews Data Ingestion - Customer reviews (late arrival data)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-Reviews-Ingestion") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def ingest_reviews_data(spark):
    """Ingest customer reviews sample data"""
    reviews_data = [
        ("rev_001", "cust_001", "prod_dragon_figure", "cinema_001", 5, "Amazing quality figure! Highly detailed and well made.", "2024-07-16"),
        ("rev_002", "cust_002", "prod_mystic_poster", "online_store", 4, "Beautiful artwork, fast shipping. Love it!", "2024-07-17"),
        ("rev_003", "cust_003", "prod_cyber_keychain", "online_store", 5, "Perfect gift for anime fans. Great price too.", "2024-07-18"),
        ("rev_004", "cust_004", "prod_dragon_bluray", "cinema_001", 5, "Excellent video quality and bonus features.", "2024-07-19"),
        ("rev_005", "cust_005", "prod_fantasy_manga", "store_001", 4, "Great story, recommended for fantasy fans!", "2024-07-20"),
        ("rev_006", "cust_001", "prod_action_tshirt", "online_store", 3, "Good quality but sizing runs small.", "2024-07-21"),
        ("rev_007", "cust_006", "prod_mystic_nendoroid", "store_001", 5, "Adorable figure with great articulation!", "2024-07-22"),
        ("rev_008", "cust_007", "prod_space_model", "online_store", 4, "Detailed model kit, fun to build.", "2024-07-23"),
        ("rev_009", "cust_002", "prod_samurai_sword", "cinema_001", 5, "Incredible replica! Display worthy.", "2024-07-24"),
        ("rev_010", "cust_003", "prod_cafe_mug", "store_001", 4, "Nice design, good for daily use.", "2024-07-25")
    ]
    
    schema = StructType([
        StructField("review_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("cinema_id", StringType()),
        StructField("rating", IntegerType()),
        StructField("review_text", StringType()),
        StructField("review_date", StringType())
    ])
    
    df = spark.createDataFrame(reviews_data, schema) \
        .withColumn("review_date", to_date("review_date"))
    
    df.writeTo("my_catalog.bronze.bronze_customer_reviews").overwritePartitions()
    print(f"✅ Ingested {df.count()} customer reviews")

def main():
    spark = create_spark_session()
    try:
        ingest_reviews_data(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
