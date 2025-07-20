from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'animeverse-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'bronze_ingestion_pipeline',
    default_args=default_args,
    description='Bronze layer data ingestion from Kafka streams',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes for real-time
    catchup=False,
    tags=['bronze', 'ingestion', 'kafka', 'real-time']
)

def check_kafka_connectivity(**context):
    """Check Kafka broker connectivity and topics"""
    import logging
    
    logging.info("Checking Kafka connectivity...")
    
    kafka_config = {
        'bootstrap_servers': 'kafka:9092',
        'topics': ['pos-transactions', 'cinema-sales', 'inventory-updates', 'customer-reviews'],
        'consumer_group': 'animeverse-bronze-ingestion'
    }
    
    # Simulate Kafka health check
    broker_status = {
        'broker_count': 3,
        'healthy_brokers': 3,
        'topic_partitions': {
            'pos-transactions': 4,
            'cinema-sales': 2,
            'inventory-updates': 3,
            'customer-reviews': 2
        },
        'consumer_lag': {
            'pos-transactions': 12,
            'cinema-sales': 5,
            'inventory-updates': 8,
            'customer-reviews': 3
        },
        'status': 'healthy'
    }
    
    for topic, lag in broker_status['consumer_lag'].items():
        logging.info(f"Topic {topic}: {lag} messages behind")
    
    logging.info("Kafka connectivity check completed successfully")
    context['task_instance'].xcom_push(key='kafka_status', value=broker_status)
    
    return broker_status

def ingest_pos_transactions(**context):
    """Ingest POS transaction data from Kafka"""
    import logging
    
    logging.info("Starting POS transactions ingestion...")
    
    # Simulate Kafka consumption
    ingestion_stats = {
        'topic': 'pos-transactions',
        'messages_consumed': 156,
        'bytes_processed': 45600,
        'processing_time_ms': 1250,
        'errors': 0,
        'data_sample': {
            'transaction_id': 'TXN-20240120-001',
            'customer_id': 'CUST-12345',
            'product_id': 'MANGA-AOT-VOL45',
            'quantity': 2,
            'unit_price': 12.99,
            'total_amount': 25.98,
            'timestamp': datetime.now().isoformat()
        },
        'status': 'completed'
    }
    
    logging.info(f"POS ingestion: {ingestion_stats['messages_consumed']} messages, {ingestion_stats['bytes_processed']} bytes")
    context['task_instance'].xcom_push(key='pos_ingestion_result', value=ingestion_stats)
    
    return ingestion_stats

def ingest_cinema_sales(**context):
    """Ingest cinema sales data from Kafka"""
    import logging
    
    logging.info("Starting cinema sales ingestion...")
    
    # Simulate Kafka consumption
    ingestion_stats = {
        'topic': 'cinema-sales',
        'messages_consumed': 89,
        'bytes_processed': 32400,
        'processing_time_ms': 890,
        'errors': 0,
        'data_sample': {
            'booking_id': 'BOOK-20240120-001',
            'customer_id': 'CUST-67890',
            'movie_id': 'ANIME-AOT-FINAL',
            'theater_id': 'THR-001',
            'seats': 2,
            'show_time': '19:30',
            'ticket_price': 15.50,
            'total_amount': 31.00,
            'timestamp': datetime.now().isoformat()
        },
        'status': 'completed'
    }
    
    logging.info(f"Cinema ingestion: {ingestion_stats['messages_consumed']} messages, {ingestion_stats['bytes_processed']} bytes")
    context['task_instance'].xcom_push(key='cinema_ingestion_result', value=ingestion_stats)
    
    return ingestion_stats

def ingest_inventory_updates(**context):
    """Ingest inventory update data from Kafka"""
    import logging
    
    logging.info("Starting inventory updates ingestion...")
    
    # Simulate Kafka consumption
    ingestion_stats = {
        'topic': 'inventory-updates',
        'messages_consumed': 67,
        'bytes_processed': 18900,
        'processing_time_ms': 650,
        'errors': 1,
        'data_sample': {
            'update_id': 'INV-20240120-001',
            'product_id': 'FIGURE-NARUTO-001',
            'warehouse_id': 'WH-TOKYO-01',
            'stock_change': -3,
            'current_stock': 47,
            'reorder_level': 20,
            'supplier_id': 'SUP-BANDAI',
            'timestamp': datetime.now().isoformat()
        },
        'status': 'completed'
    }
    
    logging.info(f"Inventory ingestion: {ingestion_stats['messages_consumed']} messages, {ingestion_stats['bytes_processed']} bytes")
    context['task_instance'].xcom_push(key='inventory_ingestion_result', value=ingestion_stats)
    
    return ingestion_stats

def ingest_customer_reviews(**context):
    """Ingest customer review data from Kafka"""
    import logging
    
    logging.info("Starting customer reviews ingestion...")
    
    # Simulate Kafka consumption
    ingestion_stats = {
        'topic': 'customer-reviews',
        'messages_consumed': 34,
        'bytes_processed': 15600,
        'processing_time_ms': 420,
        'errors': 0,
        'data_sample': {
            'review_id': 'REV-20240120-001',
            'customer_id': 'CUST-99999',
            'product_id': 'MANGA-DS-VOL23',
            'rating': 5,
            'review_text': 'Amazing artwork and story progression!',
            'helpful_votes': 12,
            'verified_purchase': True,
            'timestamp': datetime.now().isoformat()
        },
        'status': 'completed'
    }
    
    logging.info(f"Reviews ingestion: {ingestion_stats['messages_consumed']} messages, {ingestion_stats['bytes_processed']} bytes")
    context['task_instance'].xcom_push(key='reviews_ingestion_result', value=ingestion_stats)
    
    return ingestion_stats

def write_to_bronze_layer(**context):
    """Write ingested data to Bronze layer (MinIO/Iceberg)"""
    import logging
    
    logging.info("Writing data to Bronze layer...")
    
    # Get ingestion results
    ti = context['task_instance']
    pos_result = ti.xcom_pull(task_ids='ingest_pos_transactions')
    cinema_result = ti.xcom_pull(task_ids='ingest_cinema_sales')
    inventory_result = ti.xcom_pull(task_ids='ingest_inventory_updates')
    reviews_result = ti.xcom_pull(task_ids='ingest_customer_reviews')
    
    # Calculate totals
    total_messages = sum([
        pos_result['messages_consumed'] if pos_result else 0,
        cinema_result['messages_consumed'] if cinema_result else 0,
        inventory_result['messages_consumed'] if inventory_result else 0,
        reviews_result['messages_consumed'] if reviews_result else 0
    ])
    
    total_bytes = sum([
        pos_result['bytes_processed'] if pos_result else 0,
        cinema_result['bytes_processed'] if cinema_result else 0,
        inventory_result['bytes_processed'] if inventory_result else 0,
        reviews_result['bytes_processed'] if reviews_result else 0
    ])
    
    # Simulate Bronze layer write
    bronze_write_result = {
        'bronze_tables_created': [
            'bronze_pos_transactions',
            'bronze_cinema_sales', 
            'bronze_inventory_updates',
            'bronze_customer_reviews'
        ],
        'total_messages_written': total_messages,
        'total_bytes_written': total_bytes,
        'storage_location': 'minio://bronze-layer/',
        'iceberg_tables': {
            'bronze_pos_transactions': {'partitions': 4, 'files_created': 12},
            'bronze_cinema_sales': {'partitions': 2, 'files_created': 6},
            'bronze_inventory_updates': {'partitions': 3, 'files_created': 8},
            'bronze_customer_reviews': {'partitions': 2, 'files_created': 4}
        },
        'write_performance': {
            'avg_write_speed_mb_s': 45.2,
            'total_write_time_ms': 2850
        },
        'data_freshness': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Bronze write completed: {total_messages} messages, {total_bytes} bytes")
    context['task_instance'].xcom_push(key='bronze_write_result', value=bronze_write_result)
    
    return bronze_write_result

def validate_bronze_data(**context):
    """Validate data quality in Bronze layer"""
    import logging
    
    logging.info("Validating Bronze layer data quality...")
    
    # Get write results
    ti = context['task_instance']
    bronze_result = ti.xcom_pull(task_ids='write_to_bronze_layer')
    
    # Simulate data validation
    validation_result = {
        'validation_checks': {
            'schema_compliance': {'passed': True, 'score': 100.0},
            'null_values': {'passed': True, 'score': 98.5},
            'data_types': {'passed': True, 'score': 99.2},
            'duplicate_records': {'passed': True, 'score': 99.8},
            'referential_integrity': {'passed': True, 'score': 97.9}
        },
        'overall_quality_score': 99.1,
        'records_validated': bronze_result['total_messages_written'] if bronze_result else 0,
        'quality_issues': {
            'minor_warnings': 3,
            'critical_errors': 0
        },
        'validation_timestamp': datetime.now().isoformat(),
        'status': 'passed'
    }
    
    logging.info(f"Bronze validation: {validation_result['overall_quality_score']}% quality score")
    context['task_instance'].xcom_push(key='validation_result', value=validation_result)
    
    return validation_result

def handle_late_arrivals(**context):
    """Handle late arriving data and reprocessing"""
    import logging
    
    logging.info("Checking for late arriving data...")
    
    # Simulate late arrival detection
    late_arrival_result = {
        'late_arrivals_detected': 5,
        'reprocessing_required': True,
        'affected_partitions': [
            'bronze_pos_transactions/year=2024/month=01/day=19',
            'bronze_cinema_sales/year=2024/month=01/day=19'
        ],
        'reprocessing_strategy': 'incremental_update',
        'estimated_reprocessing_time_minutes': 8,
        'status': 'handled'
    }
    
    if late_arrival_result['late_arrivals_detected'] > 0:
        logging.info(f"Late arrivals handled: {late_arrival_result['late_arrivals_detected']} records")
    else:
        logging.info("No late arrivals detected")
    
    context['task_instance'].xcom_push(key='late_arrival_result', value=late_arrival_result)
    
    return late_arrival_result

# Task definitions
check_kafka_task = PythonOperator(
    task_id='check_kafka_connectivity',
    python_callable=check_kafka_connectivity,
    dag=dag
)

ingest_pos_task = PythonOperator(
    task_id='ingest_pos_transactions',
    python_callable=ingest_pos_transactions,
    dag=dag
)

ingest_cinema_task = PythonOperator(
    task_id='ingest_cinema_sales',
    python_callable=ingest_cinema_sales,
    dag=dag
)

ingest_inventory_task = PythonOperator(
    task_id='ingest_inventory_updates',
    python_callable=ingest_inventory_updates,
    dag=dag
)

ingest_reviews_task = PythonOperator(
    task_id='ingest_customer_reviews',
    python_callable=ingest_customer_reviews,
    dag=dag
)

write_bronze_task = PythonOperator(
    task_id='write_to_bronze_layer',
    python_callable=write_to_bronze_layer,
    dag=dag
)

validate_bronze_task = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    dag=dag
)

handle_late_arrivals_task = PythonOperator(
    task_id='handle_late_arrivals',
    python_callable=handle_late_arrivals,
    dag=dag
)

# Cleanup tasks
cleanup_kafka_connections = BashOperator(
    task_id='cleanup_kafka_connections',
    bash_command='''
    echo "Cleaning up Kafka connections..."
    echo "Connection pool cleaned at $(date)"
    ''',
    dag=dag
)

send_bronze_completion_notification = BashOperator(
    task_id='send_bronze_completion_notification',
    bash_command='''
    echo "=== Bronze Ingestion Pipeline Completed ==="
    echo "Timestamp: $(date)"
    echo "Data successfully ingested to Bronze layer"
    echo "Ready for Silver layer processing"
    echo "============================================="
    ''',
    dag=dag
)

# Task dependencies
check_kafka_task >> [ingest_pos_task, ingest_cinema_task, ingest_inventory_task, ingest_reviews_task]

[ingest_pos_task, ingest_cinema_task, ingest_inventory_task, ingest_reviews_task] >> write_bronze_task

write_bronze_task >> validate_bronze_task >> handle_late_arrivals_task

handle_late_arrivals_task >> cleanup_kafka_connections >> send_bronze_completion_notification