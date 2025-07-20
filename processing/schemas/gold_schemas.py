from pyspark.sql.types import *

class GoldSchemas:
    
    @staticmethod
    def dim_customers_schema():
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("signup_date", DateType(), True),
            StructField("segment", StringType(), True),
            StructField("loyalty_tier", StringType(), True),
            StructField("preferred_genres", StringType(), True),
            StructField("effective_start", TimestampType(), True),
            StructField("effective_end", TimestampType(), True),
            StructField("is_current", BooleanType(), True)
        ])
    
    @staticmethod
    def dim_products_schema():
        return StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("format", StringType(), True),
            StructField("anime_title_id", StringType(), True),
            StructField("base_price", DoubleType(), True),
            StructField("supplier_id", StringType(), True),
            StructField("created_date", DateType(), True)
        ])
    
    @staticmethod
    def dim_anime_titles_schema():
        return StructType([
            StructField("anime_title_id", StringType(), False),
            StructField("title_name", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("season", StringType(), True),
            StructField("studio", StringType(), True),
            StructField("release_date", DateType(), True),
            StructField("popularity_score", IntegerType(), True),
            StructField("status", StringType(), True),
            StructField("broadcast_start_date", DateType(), True),
            StructField("broadcast_end_date", DateType(), True),
            StructField("effective_start", TimestampType(), True),
            StructField("effective_end", TimestampType(), True),
            StructField("is_current", BooleanType(), True)
        ])
    
    @staticmethod
    def fact_sales_schema():
        return StructType([
            StructField("sales_key", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("anime_title_id", StringType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("final_amount", DoubleType(), True),
            StructField("channel", StringType(), True),
            StructField("store_location", StringType(), True),
            StructField("sale_ts", TimestampType(), True),
            StructField("processed_at", TimestampType(), True)
        ])