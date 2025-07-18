#!/usr/bin/env python3
"""
Build Fact Cinema Attendance - Cinema analytics fact table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Gold-Fact-Cinema") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def build_fact_cinema(spark):
    """Build cinema attendance fact table"""
    screenings = spark.table("my_catalog.silver.silver_cinema_screenings")
    
    fact_cinema = screenings.select(
        concat(lit("att_"), col("screening_id")).alias("attendance_id"),
        col("customer_id"),
        col("anime_title_id"),
        col("cinema_id"),
        col("date_key"),
        col("ticket_type"),
        col("seats_sold"),
        col("concessions_sales").alias("concession_sales")
    )
    
    fact_cinema.writeTo("my_catalog.gold.gold_fact_cinema_attendance").overwritePartitions()
    print(f"✅ Built cinema fact: {fact_cinema.count()} records")

def main():
    spark = create_spark_session()
    try:
        build_fact_cinema(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
