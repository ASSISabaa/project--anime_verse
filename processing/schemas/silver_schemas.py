from pyspark.sql.types import *

class SilverSchemas:
    
    @staticmethod
    def transactions_schema():
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("final_amount", DoubleType(), True),
            StructField("sale_ts", TimestampType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("channel", StringType(), True),
            StructField("store_location", StringType(), True),
            StructField("processed_at", TimestampType(), True)
        ])
    
    @staticmethod
    def cinema_screenings_schema():
        return StructType([
            StructField("booking_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("anime_title", StringType(), True),
            StructField("screening_ts", TimestampType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("ticket_type", StringType(), True),
            StructField("seats_booked", IntegerType(), True),
            StructField("ticket_price", DoubleType(), True),
            StructField("total_ticket_amount", DoubleType(), True),
            StructField("concession_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("theater_id", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("processed_at", TimestampType(), True)
        ])
    
    @staticmethod
    def inventory_schema():
        return StructType([
            StructField("update_id", StringType(), False),
            StructField("product_id", StringType(), True),
            StructField("supplier_id", StringType(), True),
            StructField("quantity_received", IntegerType(), True),
            StructField("unit_cost", DoubleType(), True),
            StructField("total_cost", DoubleType(), True),
            StructField("shipment_ts", TimestampType(), True),
            StructField("delivery_ts", TimestampType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("warehouse_location", StringType(), True),
            StructField("is_late_arrival", BooleanType(), True),
            StructField("processed_at", TimestampType(), True)
        ])
    
    @staticmethod
    def reviews_schema():
        return StructType([
            StructField("review_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("anime_title", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("review_text", StringType(), True),
            StructField("sentiment_score", StringType(), True),
            StructField("review_ts", TimestampType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("verified_purchase", BooleanType(), True),
            StructField("helpful_votes", IntegerType(), True),
            StructField("processed_at", TimestampType(), True)
        ])