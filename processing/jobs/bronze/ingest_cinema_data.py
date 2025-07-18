#!/usr/bin/env python3
"""
Cinema Data Ingestion - Cinema sales and screenings
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-Cinema-Ingestion") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def ingest_cinema_data(spark):
    """Ingest cinema sales sample data"""
    cinema_data = [
        ("screen_001", "cust_001", "Dragon Warrior Chronicles", "premium", "2024-07-15 19:00:00", 2, 15.50),
        ("screen_002", "cust_002", "Mystic Academy", "standard", "2024-07-15 21:30:00", 1, 8.00),
        ("screen_003", "cust_003", "Cyber Knights", "premium", "2024-07-16 18:00:00", 3, 22.75),
        ("screen_004", "cust_004", "Dragon Warrior Chronicles", "imax", "2024-07-16 20:15:00", 2, 35.00),
        ("screen_005", "cust_005", "Mystic Academy", "standard", "2024-07-17 15:30:00", 1, 7.50),
        ("screen_006", "cust_001", "Space Odyssey", "premium", "2024-07-17 20:00:00", 2, 18.00),
        ("screen_007", "cust_006", "Demon Hunter Academy", "imax", "2024-07-18 19:45:00", 3, 42.00),
        ("screen_008", "cust_002", "Slice of Life Cafe", "standard", "2024-07-18 14:15:00", 1, 6.75),
        ("screen_009", "cust_007", "Mecha Revolution", "premium", "2024-07-19 21:15:00", 2, 25.50),
        ("screen_010", "cust_003", "Samurai Chronicles", "imax", "2024-07-19 18:30:00", 4, 56.00)
    ]
    
    schema = StructType([
        StructField("screening_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("title_name", StringType()),
        StructField("ticket_type", StringType()),
        StructField("showtime", StringType()),
        StructField("seats_sold", IntegerType()),
        StructField("concession_amt", FloatType())
    ])
    
    df = spark.createDataFrame(cinema_data, schema) \
        .withColumn("showtime", to_timestamp("showtime"))
    
    df.writeTo("my_catalog.bronze.bronze_cinema_sales").overwritePartitions()
    print(f"✅ Ingested {df.count()} cinema sales records")

def main():
    spark = create_spark_session()
    try:
        ingest_cinema_data(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()