import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("Kafka-to-Bronze-Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def ingest_kafka_to_bronze(spark, topic, table_name):
    """Ingest data from Kafka topic to Bronze Iceberg table"""
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("message_key"),
        from_json(col("value").cast("string"), get_schema_for_topic(topic)).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    ).select(
        "message_key",
        "data.*",
        "kafka_timestamp",
        "partition",
        "offset",
        current_timestamp().alias("ingestion_timestamp")
    )
    
    # Write to Bronze Iceberg table
    query = parsed_df.writeStream \
        .format("iceberg") \
        .option("table", f"local.bronze.{table_name}") \
        .option("checkpointLocation", f"s3a://bronze-layer/checkpoints/{table_name}") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

def get_schema_for_topic(topic):
    """Return the appropriate schema for each Kafka topic"""
    
    schemas = {
        "pos-transactions": StructType([
            StructField("transaction_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("timestamp", StringType()),
            StructField("channel", StringType()),
            StructField("event_type", StringType()),
            StructField("store_location", StringType())
        ]),
        
        "cinema-sales": StructType([
            StructField("booking_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("anime_title", StringType()),
            StructField("screening_time", StringType()),
            StructField("ticket_type", StringType()),
            StructField("seats_booked", IntegerType()),
            StructField("ticket_price", DoubleType()),
            StructField("concession_amount", DoubleType()),
            StructField("theater_id", StringType()),
            StructField("payment_method", StringType())
        ]),
        
        "inventory-updates": StructType([
            StructField("update_id", StringType()),
            StructField("product_id", StringType()),
            StructField("supplier_id", StringType()),
            StructField("quantity_received", IntegerType()),
            StructField("unit_cost", DoubleType()),
            StructField("shipment_date", StringType()),
            StructField("expected_delivery", StringType()),
            StructField("warehouse_location", StringType()),
            StructField("quality_status", StringType())
        ]),
        
        "customer-reviews": StructType([
            StructField("review_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("anime_title", StringType()),
            StructField("rating", IntegerType()),
            StructField("review_text", StringType()),
            StructField("review_date", StringType()),
            StructField("verified_purchase", BooleanType()),
            StructField("helpful_votes", IntegerType())
        ])
    }
    
    return schemas.get(topic, StructType([]))

def main():
    parser = argparse.ArgumentParser(description='Ingest Kafka data to Bronze layer')
    parser.add_argument('--topic', required=True, help='Kafka topic name')
    parser.add_argument('--table', required=True, help='Bronze table name')
    parser.add_argument('--duration', default=300, type=int, help='Run duration in seconds')
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    try:
        print(f"Starting ingestion from topic {args.topic} to table {args.table}")
        
        query = ingest_kafka_to_bronze(spark, args.topic, args.table)
        
        # Run for specified duration
        query.awaitTermination(timeout=args.duration)
        
        print(f"Completed ingestion for {args.topic}")
        
    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()