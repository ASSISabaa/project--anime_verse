#!/usr/bin/env python3
"""
Build Calendar Dimension - Date dimension for analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import calendar as cal

def create_spark_session():
    return SparkSession.builder \
        .appName("Gold-Build-Calendar") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def build_calendar(spark):
    """Build calendar dimension for 2024"""
    
    # Generate calendar data
    dates = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    current = start_date
    while current <= end_date:
        date_key = current.strftime("%Y%m%d")
        year = current.year
        month = current.month
        day = current.day
        quarter = (month - 1) // 3 + 1
        reason = cal.day_name[current.weekday()]
        is_holiday = current.weekday() >= 5  # Weekend
        
        dates.append((date_key, year, month, day, quarter, reason, is_holiday))
        current += timedelta(days=1)
    
    calendar_schema = StructType([
        StructField("date_key", StringType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
        StructField("day", IntegerType()),
        StructField("quarter", IntegerType()),
        StructField("reason", StringType()),
        StructField("is_holiday", BooleanType())
    ])
    
    calendar_df = spark.createDataFrame(dates, calendar_schema)
    calendar_df.writeTo("my_catalog.gold.gold_dim_calendar").overwritePartitions()
    print(f"✅ Built calendar dimension: {calendar_df.count()} records")

def main():
    spark = create_spark_session()
    try:
        build_calendar(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
