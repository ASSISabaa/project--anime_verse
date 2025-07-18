#!/usr/bin/env python3
"""
Process Theaters - Transform anime releases to Silver theaters
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver-Process-Theaters") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_theaters(spark):
    """Transform anime releases to theater data"""
    anime_bronze = spark.table("my_catalog.bronze.bronze_anime_releases")
    
    theaters_silver = anime_bronze.select(
        regexp_replace(lower(col("title_name")), " ", "_").alias("anime_title_id"),
        col("title_name"),
        split(col("genres"), ",")[0].alias("genre"),
        # Determine season based on release date
        when(col("release_date").between("2024-01-01", "2024-03-31"), "Winter2024")
        .when(col("release_date").between("2024-04-01", "2024-06-30"), "Spring2024")
       .when(col("release_date").between("2024-07-01", "2024-09-30"), "Summer2024")
       .otherwise("Fall2024").alias("season"),
       col("release_date"),
       # Calculate popularity score based on genre and platform
       when(col("genres").contains("Action"), 85.0)
       .when(col("genres").contains("Fantasy"), 80.0)
       .when(col("genres").contains("Sci-Fi"), 75.0)
       .when(col("genres").contains("Romance"), 70.0)
       .when(col("platform") == "Netflix", col("popularity_score") + 10)
       .otherwise(65.0).alias("popularity_score")
   ).filter(col("title_name").isNotNull())
   
   theaters_silver.writeTo("my_catalog.silver.silver_theaters").overwritePartitions()
   print(f"✅ Processed {theaters_silver.count()} theater records")

def main():
   spark = create_spark_session()
   try:
       process_theaters(spark)
   finally:
       spark.stop()

if __name__ == "__main__":
   main()
