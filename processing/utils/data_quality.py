from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class DataQualityChecker:
    
    def __init__(self, spark):
        self.spark = spark
    
    def check_null_values(self, df: DataFrame, columns: list):
        """Check for null values in specified columns"""
        results = {}
        for col in columns:
            null_count = df.filter(col(col).isNull()).count()
            total_count = df.count()
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
            results[col] = {
                "null_count": null_count,
                "null_percentage": round(null_percentage, 2)
            }
        return results
    
    def check_duplicates(self, df: DataFrame, key_columns: list):
        """Check for duplicate records based on key columns"""
        total_count = df.count()
        unique_count = df.dropDuplicates(key_columns).count()
        duplicate_count = total_count - unique_count
        
        return {
            "total_records": total_count,
            "unique_records": unique_count,
            "duplicate_records": duplicate_count,
            "duplicate_percentage": round((duplicate_count / total_count) * 100, 2) if total_count > 0 else 0
        }
    
    def check_data_freshness(self, df: DataFrame, timestamp_column: str, threshold_hours: int = 24):
        """Check data freshness based on timestamp column"""
        current_time = current_timestamp()
        threshold_time = current_time - expr(f"INTERVAL {threshold_hours} HOURS")
        
        fresh_count = df.filter(col(timestamp_column) >= threshold_time).count()
        total_count = df.count()
        stale_count = total_count - fresh_count
        
        return {
            "total_records": total_count,
            "fresh_records": fresh_count,
            "stale_records": stale_count,
            "fresh_percentage": round((fresh_count / total_count) * 100, 2) if total_count > 0 else 0
        }
    
    def validate_ranges(self, df: DataFrame, column: str, min_val: float, max_val: float):
        """Validate that numeric values are within expected ranges"""
        valid_count = df.filter((col(column) >= min_val) & (col(column) <= max_val)).count()
        total_count = df.count()
        invalid_count = total_count - valid_count
        
        return {
            "total_records": total_count,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "valid_percentage": round((valid_count / total_count) * 100, 2) if total_count > 0 else 0
        }