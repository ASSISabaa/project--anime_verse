"""
Script to set up Airflow connections and variables for AnimeVerse project
"""

import os
from airflow import settings
from airflow.models import Connection, Variable
from airflow.utils.db import provide_session

@provide_session
def create_connections(session=None):
    """Create necessary connections for the AnimeVerse project"""
    
    # Spark connection
    spark_conn = Connection(
        conn_id='spark_default',
        conn_type='spark',
        host='spark-master',
        port=7077,
        extra={
            'master': 'spark://spark-master:7077',
            'deploy-mode': 'client'
        }
    )
    
    # Kafka connection
    kafka_conn = Connection(
        conn_id='kafka_default',
        conn_type='generic',
        host='kafka',
        port=9092,
        extra={
            'bootstrap.servers': 'kafka:9092',
            'security.protocol': 'PLAINTEXT'
        }
    )
    
    # MinIO S3 connection
    minio_conn = Connection(
        conn_id='minio_default',
        conn_type='s3',
        host='minio',
        port=9000,
        login='minioadmin',
        password='minioadmin',
        extra={
            'endpoint_url': 'http://minio:9000',
            'aws_access_key_id': 'minioadmin',
            'aws_secret_access_key': 'minioadmin',
            'region_name': 'us-east-1'
        }
    )
    
    # PostgreSQL connection (for metadata)
    postgres_conn = Connection(
        conn_id='postgres_default',
        conn_type='postgres',
        host='postgres',
        port=5432,
        login='airflow',
        password='airflow',
        schema='airflow'
    )
    
    # Email connection for notifications
    email_conn = Connection(
        conn_id='email_default',
        conn_type='smtp',
        host='smtp.gmail.com',
        port=587,
        login='airflow@animeverse.com',
        password='your_app_password',
        extra={
            'use_tls': True,
            'use_ssl': False
        }
    )
    
    connections = [spark_conn, kafka_conn, minio_conn, postgres_conn, email_conn]
    
    for conn in connections:
        existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        if existing:
            session.delete(existing)
        session.add(conn)
    
    session.commit()
    print("Connections created successfully!")

@provide_session
def create_variables(session=None):
    """Create necessary variables for the AnimeVerse project"""
    
    variables = {
        # S3/MinIO paths
        'BRONZE_BUCKET': 'bronze',
        'SILVER_BUCKET': 'silver',
        'GOLD_BUCKET': 'gold',
        'BACKUP_BUCKET': 'backups',
        
        # Kafka topics
        'POS_TOPIC': 'pos-transactions',
        'CINEMA_TOPIC': 'cinema-tickets',
        'CONCESSION_TOPIC': 'concession-sales',
        
        # Processing settings
        'BATCH_SIZE': '1000',
        'CHECKPOINT_INTERVAL': '60',
        'LATE_DATA_THRESHOLD': '48',  # hours
        'RETENTION_PERIOD': '365',    # days
        
        # Data quality settings
        'QUALITY_CHECK_INTERVAL': '6',  # hours
        'ALERT_THRESHOLD': '5',         # percentage
        'MAX_NULL_PERCENTAGE': '10',
        'MAX_DUPLICATE_PERCENTAGE': '1',
        
        # Spark configuration
        'SPARK_MASTER': 'spark://spark-master:7077',
        'SPARK_DRIVER_MEMORY': '2g',
        'SPARK_EXECUTOR_MEMORY': '2g',
        'SPARK_EXECUTOR_CORES': '2',
        'SPARK_MAX_EXECUTORS': '4',
        
        # Iceberg configuration
        'ICEBERG_CATALOG': 'animeverse_catalog',
        'ICEBERG_WAREHOUSE': 's3a://warehouse/',
        
        # Business settings
        'BUSINESS_HOURS_START': '09:00',
        'BUSINESS_HOURS_END': '22:00',
        'PEAK_HOURS_START': '18:00',
        'PEAK_HOURS_END': '21:00',
        
        # Notification settings
        'NOTIFICATION_EMAIL': 'admin@animeverse.com',
        'SLACK_WEBHOOK': 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK',
        'ALERT_EMAIL': 'alerts@animeverse.com',
        
        # ML model settings
        'MODEL_TRAINING_INTERVAL': '168',  # hours (weekly)
        'MODEL_PREDICTION_BATCH_SIZE': '500',
        'MODEL_ACCURACY_THRESHOLD': '0.85',
        
        # Dashboard settings
        'DASHBOARD_REFRESH_INTERVAL': '15',  # minutes
        'DASHBOARD_CACHE_TTL': '300',        # seconds
        
        # External API settings
        'ANIME_API_BASE_URL': 'https://api.jikan.moe/v4',
        'ANIME_API_RATE_LIMIT': '60',  # requests per minute
        'WEATHER_API_KEY': 'your_weather_api_key',
        
        # Security settings
        'ENCRYPTION_KEY': 'your_encryption_key_here',
        'JWT_SECRET': 'your_jwt_secret_here',
        'SESSION_TIMEOUT': '3600',  # seconds
    }
    
    for key, value in variables.items():
        existing = session.query(Variable).filter(Variable.key == key).first()
        if existing:
            existing.val = value
        else:
            var = Variable(key=key, val=value)
            session.add(var)
    
    session.commit()
    print("Variables created successfully!")

if __name__ == "__main__":
    create_connections()
    create_variables()
    print("Airflow setup completed successfully!")