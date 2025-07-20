import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from iceberg_utils import IcebergUtils

def process_scd_customers(spark):
    """Process SCD Type 2 for customers dimension"""
    print("Processing SCD Type 2 for customers...")
    
    # Get current customers data from source
    current_customers = spark.table("local.silver.transactions") \
        .select("customer_id") \
        .distinct() \
        .withColumn("name", concat(lit("Customer_"), col("customer_id"))) \
        .withColumn("email", concat(col("customer_id"), lit("@animeverse.com"))) \
        .withColumn("segment", 
                   when(rand() < 0.3, "VIP")
                   .when(rand() < 0.6, "Premium")
                   .otherwise("Regular")) \
        .withColumn("loyalty_tier",
                   when(col("segment") == "VIP", "Platinum")
                   .when(col("segment") == "Premium", "Gold")
                   .otherwise("Silver"))
    
    # Get existing dimension data
    existing_customers = spark.table("local.gold.dim_customers")
    
    # Find changed records
    changed_customers = current_customers.alias("curr") \
        .join(existing_customers.alias("existing"), "customer_id") \
        .where(
            (col("existing.is_current") == True) & 
            ((col("curr.segment") != col("existing.segment")) | 
             (col("curr.loyalty_tier") != col("existing.loyalty_tier")))
        )
    
    if changed_customers.count() > 0:
        # Close expired records
        spark.sql("""
            UPDATE local.gold.dim_customers 
            SET effective_end = current_timestamp(), is_current = false
            WHERE customer_id IN (
                SELECT DISTINCT customer_id FROM changed_customers_temp
            ) AND is_current = true
        """)
        
        # Insert new records
        new_records = changed_customers.select("curr.*") \
            .withColumn("customer_key", monotonically_increasing_id()) \
            .withColumn("effective_start", current_timestamp()) \
            .withColumn("effective_end", lit(None).cast(TimestampType())) \
            .withColumn("is_current", lit(True))
        
        new_records.writeTo("local.gold.dim_customers") \
            .option("write-mode", "append") \
            .create()
        
        print(f"Updated {changed_customers.count()} customer records")

def process_scd_anime_schedule(spark):
    """Process SCD Type 2 for anime schedule changes"""
    print("Processing SCD Type 2 for anime schedule...")
    
    # Simulate schedule changes
    schedule_changes = spark.sql("""
        SELECT 
            anime_title_id,
            title_name,
            genre,
            case 
                when rand() < 0.1 then date_add(broadcast_start_date, 30)
                else broadcast_start_date 
            end as new_broadcast_start,
            case 
                when rand() < 0.1 then date_add(broadcast_end_date, 60)
                else broadcast_end_date 
            end as new_broadcast_end
        FROM local.gold.dim_anime_titles
        WHERE is_current = true
    """)
    
    # Find records with schedule changes
    changed_schedules = schedule_changes.alias("new") \
        .join(spark.table("local.gold.dim_anime_titles").alias("existing"), "anime_title_id") \
        .where(
            (col("existing.is_current") == True) & 
            ((col("new.new_broadcast_start") != col("existing.broadcast_start_date")) | 
             (col("new.new_broadcast_end") != col("existing.broadcast_end_date")))
        )
    
    if changed_schedules.count() > 0:
        print(f"Found {changed_schedules.count()} anime schedule changes")
        
        # Implementation would close old records and create new ones
        # Similar to customer SCD processing

def main():
    parser = argparse.ArgumentParser(description='Process SCD Type 2 updates')
    parser.add_argument('--table', required=True, choices=['customers', 'anime_schedule'], 
                       help='Table to process SCD for')
    
    args = parser.parse_args()
    
    spark = IcebergUtils.create_spark_session("SCD-Type2-Processing")
    
    try:
        if args.table == 'customers':
            process_scd_customers(spark)
        elif args.table == 'anime_schedule':
            process_scd_anime_schedule(spark)
            
    except Exception as e:
        print(f"Error in SCD processing: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()