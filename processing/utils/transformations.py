from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DataTransformations:
    """Common data transformation utilities"""
    
    @staticmethod
    def add_date_dimensions(df: DataFrame, timestamp_col: str) -> DataFrame:
        """Add date dimension columns from timestamp"""
        return df.withColumn("date_key", date_format(col(timestamp_col), "yyyyMMdd").cast(IntegerType())) \
                 .withColumn("year", year(col(timestamp_col))) \
                 .withColumn("month", month(col(timestamp_col))) \
                 .withColumn("day", dayofmonth(col(timestamp_col))) \
                 .withColumn("hour", hour(col(timestamp_col))) \
                 .withColumn("day_of_week", dayofweek(col(timestamp_col)))
    
    @staticmethod
    def calculate_business_metrics(df: DataFrame) -> DataFrame:
        """Calculate common business metrics"""
        return df.withColumn("total_amount", col("quantity") * col("unit_price")) \
                 .withColumn("discount_amount", lit(0.0)) \
                 .withColumn("final_amount", col("total_amount") - col("discount_amount"))
    
    @staticmethod
    def clean_string_columns(df: DataFrame, columns: list) -> DataFrame:
        """Clean string columns - trim and handle nulls"""
        for col_name in columns:
            df = df.withColumn(col_name, 
                              when(col(col_name).isNull(), "Unknown")
                              .otherwise(trim(col(col_name))))
        return df
    
    @staticmethod
    def handle_late_arrivals(df: DataFrame, timestamp_col: str, hours_threshold: int = 48) -> DataFrame:
        """Mark records that arrived late"""
        current_time = current_timestamp()
        cutoff_time = current_time - expr(f"INTERVAL {hours_threshold} HOURS")
        
        return df.withColumn("is_late_arrival", 
                           when(col(timestamp_col) < cutoff_time, True)
                           .otherwise(False))
    
    @staticmethod
    def standardize_phone_numbers(df: DataFrame, phone_col: str) -> DataFrame:
        """Standardize phone number format"""
        return df.withColumn(phone_col,
                           regexp_replace(col(phone_col), "[^0-9]", ""))
    
    @staticmethod
    def categorize_by_amount(df: DataFrame, amount_col: str) -> DataFrame:
        """Categorize transactions by amount"""
        return df.withColumn("amount_category",
                           when(col(amount_col) < 10, "Low")
                           .when(col(amount_col) < 50, "Medium")
                           .when(col(amount_col) < 100, "High")
                           .otherwise("Premium"))