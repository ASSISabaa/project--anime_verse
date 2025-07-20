from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

class IcebergUtils:
    
    @staticmethod
    def create_spark_session(app_name="AnimeVerse-DataProcessing"):
        conf = SparkConf() \
            .setAppName(app_name) \
            .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .set("spark.sql.catalog.spark_catalog.type", "hive") \
            .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .set("spark.sql.catalog.local.type", "hadoop") \
            .set("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
            .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .set("spark.hadoop.fs.s3a.access.key", "admin") \
            .set("spark.hadoop.fs.s3a.secret.key", "password123") \
            .set("spark.hadoop.fs.s3a.path.style.access", "true") \
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        return spark
    
    @staticmethod
    def create_bronze_table(spark, table_name, schema, location):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS local.bronze.{table_name} (
                {IcebergUtils._schema_to_ddl(schema)}
            ) USING iceberg
            LOCATION '{location}'
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)
    
    @staticmethod
    def _schema_to_ddl(schema):
        ddl_parts = []
        for field in schema.fields:
            ddl_parts.append(f"{field.name} {IcebergUtils._spark_type_to_sql(field.dataType)}")
        return ", ".join(ddl_parts)
    
    @staticmethod
    def _spark_type_to_sql(spark_type):
        type_mapping = {
            "StringType": "string",
            "IntegerType": "int",
            "DoubleType": "double",
            "BooleanType": "boolean",
            "TimestampType": "timestamp"
        }
        return type_mapping.get(type(spark_type).__name__, "string")