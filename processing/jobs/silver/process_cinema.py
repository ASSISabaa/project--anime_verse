#!/usr/bin/env python3
"""
Process Cinema - Transform cinema sales to Silver
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Process-Cinema") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_cinema(spark):
    """Clean and transform cinema sales data"""
    cinema_bronze = spark.table("my_catalog.bronze.bronze_cinema_sales")
    
    cinema_silver = cinema_bronze.select(
        col("screening_id"),
        col("customer_id"),
        regexp_replace(lower(col("title_name")), " ", "_").alias("anime_title_id"),
        col("ticket_type"),
        col("showtime"),
        col("seats_sold"),
        col("concession_amt").alias("concessions_sales"),
        # Business logic transformations
        date_format(col("showtime"), "yyyyMMdd").alias("date_key"),
        # Calculate ticket revenue based on type
        when(col("ticket_type") == "premium", col("seats_sold") * 25.0)
        .when(col("ticket_type") == "imax", col("seats_sold") * 35.0)
        .otherwise(col("seats_sold") * 15.0).alias("ticket_revenue"),
        # Screening period classification
        when(hour(col("showtime")).between(18, 22), "prime_time")
        .when(hour(col("showtime")).between(14, 17), "matinee")
        .otherwise("off_peak").alias("screening_period"),
        # Add cinema location
        lit("cinema_001").alias("cinema_id")
    ).filter(
        col("screening_id").isNotNull() &
        (col("seats_sold") > 0)
    )
    
    cinema_silver.writeTo("my_catalog.silver.silver_cinema_screenings").overwritePartitions()
    print(f"✅ Processed {cinema_silver.count()} cinema screening records")

def main():
    spark = create_spark_session()
    try:
        process_cinema(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()