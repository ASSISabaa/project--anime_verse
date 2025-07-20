from pyspark.sql.types import *

class BronzeSchemas:
    
    @staticmethod
    def pos_transactions_schema():
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("store_location", StringType(), True)
        ])
    
    @staticmethod
    def cinema_sales_schema():
        return StructType([
            StructField("booking_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("anime_title", StringType(), True),
            StructField("screening_time", StringType(), True),
            StructField("ticket_type", StringType(), True),
            StructField("seats_booked", IntegerType(), True),
            StructField("ticket_price", DoubleType(), True),
            StructField("concession_amount", DoubleType(), True),
            StructField("theater_id", StringType(), True),
            StructField("payment_method", StringType(), True)
        ])
    
    @staticmethod
    def inventory_updates_schema():
        return StructType([
            StructField("update_id", StringType(), False),
            StructField("product_id", StringType(), True),
            StructField("supplier_id", StringType(), True),
            StructField("quantity_received", IntegerType(), True),
            StructField("unit_cost", DoubleType(), True),
            StructField("shipment_date", StringType(), True),
            StructField("expected_delivery", StringType(), True),
            StructField("warehouse_location", StringType(), True),
            StructField("quality_status", StringType(), True)
        ])
    
    @staticmethod
    def customer_reviews_schema():
        return StructType([
            StructField("review_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("anime_title", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("review_text", StringType(), True),
            StructField("review_date", StringType(), True),
            StructField("verified_purchase", BooleanType(), True),
            StructField("helpful_votes", IntegerType(), True)
        ])