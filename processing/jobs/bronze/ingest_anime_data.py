#!/usr/bin/env python3
"""
Anime Data Ingestion - Anime releases and catalog
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Bronze-Anime-Ingestion") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def ingest_anime_data(spark):
    """Ingest anime releases sample data"""
    anime_data = [
        ("Dragon Warrior Chronicles", "2024-04-01", "Action,Adventure,Fantasy", "Netflix", "Epic tale of dragon warriors seeking ancient artifacts"),
        ("Mystic Academy", "2024-07-01", "Fantasy,School,Magic", "Crunchyroll", "Students learn magic at mystical academy"),
        ("Cyber Knights", "2024-10-01", "Sci-Fi,Mecha,Action", "Funimation", "Future warriors pilot giant mechs against alien invasion"),
        ("Fantasy Realm", "2024-01-15", "Fantasy,Adventure", "Hulu", "Heroes explore magical kingdoms and ancient mysteries"),
        ("Space Odyssey", "2024-11-01", "Sci-Fi,Space,Drama", "Netflix", "Crew explores distant galaxies searching for new home"),
        ("Demon Hunter Academy", "2024-03-20", "Action,Supernatural", "Crunchyroll", "Students train to become elite demon hunters"),
        ("Slice of Life Cafe", "2024-06-15", "Slice of Life,Romance", "Funimation", "Daily life at a cozy neighborhood cafe"),
        ("Mecha Revolution", "2024-09-10", "Mecha,Action,War", "Netflix", "Giant robots battle in post-apocalyptic world"),
        ("Magic School Adventures", "2024-05-05", "Fantasy,School,Comedy", "Hulu", "Comedic adventures at wizard academy"),
        ("Samurai Chronicles", "2024-08-25", "Historical,Action,Drama", "Crunchyroll", "Epic tale of honor and sword fighting")
    ]
    
    schema = StructType([
        StructField("title_name", StringType()),
        StructField("release_date", StringType()),
        StructField("genres", StringType()),
        StructField("platform", StringType()),
        StructField("synopsis", StringType())
    ])
    
    df = spark.createDataFrame(anime_data, schema) \
        .withColumn("release_date", to_date("release_date"))
    
    df.writeTo("my_catalog.bronze.bronze_anime_releases").overwritePartitions()
    print(f"✅ Ingested {df.count()} anime releases")

def main():
    spark = create_spark_session()
    try:
        ingest_anime_data(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
