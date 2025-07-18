#!/usr/bin/env python3
"""
Customer Data Ingestion - Customer profiles for SCD2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-Customer-Ingestion") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def ingest_customer_data(spark):
    """Ingest customer profile sample data"""
    customer_data = [
        ("cust_001", "Akira Tanaka", "akira@anime.com", "Action,Fantasy", "online,store", "2024-01-15"),
        ("cust_002", "Sakura Yamamoto", "sakura@manga.jp", "Fantasy,Romance", "store,cinema", "2024-02-20"),
        ("cust_003", "Hiroshi Sato", "hiroshi@otaku.net", "Sci-Fi,Action", "online,mobile", "2024-03-10"),
        ("cust_004", "Emi Watanabe", "emi@cosplay.org", "Fantasy,Drama", "online,cinema", "2024-04-05"),
        ("cust_005", "Kenji Nakamura", "kenji@studio.co", "Mecha,Action", "store,cinema", "2024-05-12"),
        ("cust_006", "Yuki Takahashi", "yuki@anime.fan", "Romance,Slice of Life", "mobile,online", "2024-06-18"),
        ("cust_007", "Ryo Matsuda", "ryo@manga.store", "Horror,Thriller", "store", "2024-07-02")
    ]
    
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("preferred_genres", StringType()),
        StructField("preferred_channels", StringType()),
        StructField("signup_date", StringType())
    ])
    
    df = spark.createDataFrame(customer_data, schema) \
        .withColumn("signup_date", to_date("signup_date"))
    
    df.writeTo("my_catalog.bronze.bronze_customer_profiles").overwritePartitions()
    print(f"✅ Ingested {df.count()} customer profiles")

def main():
    spark = create_spark_session()
    try:
        ingest_customer_data(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
