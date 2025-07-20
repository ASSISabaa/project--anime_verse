from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from iceberg_utils import IcebergUtils

def handle_late_arrivals(spark):
    """Handle late arriving data in inventory updates"""
    print("Processing late arriving inventory data...")
    
    # Read current inventory data
    inventory_df = spark.table("local.bronze.inventory_updates")
    
    # Identify late arrivals (more than 24 hours old)
    current_time = current_timestamp()
    cutoff_time = current_time - expr("INTERVAL 24 HOURS")
    
    late_arrivals = inventory_df.filter(
        to_timestamp(col("shipment_date")) < cutoff_time
    )
    
    print(f"Found {late_arrivals.count()} late arrival records")
    
    if late_arrivals.count() > 0:
        # Process late arrivals with special handling
        processed_late_arrivals = late_arrivals \
            .withColumn("processing_type", lit("late_arrival")) \
            .withColumn("late_arrival_hours", 
                       (unix_timestamp(current_time) - unix_timestamp(to_timestamp(col("shipment_date")))) / 3600) \
            .withColumn("priority", lit("high")) \
            .withColumn("processed_at", current_timestamp())
        
        # Update Silver layer with late arrivals
        processed_late_arrivals.writeTo("local.silver.inventory_late_arrivals") \
            .option("write-mode", "append") \
            .createOrReplace()
        
        print(f"Processed {processed_late_arrivals.count()} late arrival records")
        
        # Update main inventory table
        spark.sql("""
            MERGE INTO local.silver.inventory AS target
            USING local.silver.inventory_late_arrivals AS source
            ON target.update_id = source.update_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

def handle_transaction_late_arrivals(spark):
    """Handle late arriving transaction data"""
    print("Processing late arriving transaction data...")
    
    transactions_df = spark.table("local.bronze.pos_transactions")
    
    # Check for transactions older than 2 hours
    current_time = current_timestamp()
    cutoff_time = current_time - expr("INTERVAL 2 HOURS")
    
    late_transactions = transactions_df.filter(
        to_timestamp(col("timestamp")) < cutoff_time
    )
    
    if late_transactions.count() > 0:
        print(f"Found {late_transactions.count()} late transaction records")
        
        # Process with reconciliation
        processed_transactions = late_transactions \
            .withColumn("is_late_arrival", lit(True)) \
            .withColumn("reconciliation_required", lit(True)) \
            .withColumn("processed_at", current_timestamp())
        
        # Store for manual review if needed
        processed_transactions.writeTo("local.silver.transactions_reconciliation") \
            .option("write-mode", "append") \
            .createOrReplace()

def main():
    spark = IcebergUtils.create_spark_session("Late-Arrival-Handler")
    
    try:
        handle_late_arrivals(spark)
        handle_transaction_late_arrivals(spark)
        print("Late arrival processing completed")
        
    except Exception as e:
        print(f"Error in late arrival processing: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()