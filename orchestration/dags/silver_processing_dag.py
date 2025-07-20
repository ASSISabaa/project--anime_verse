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
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'silver_processing_pipeline',
    default_args=default_args,
    description='Silver layer data cleaning and transformation',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    catchup=False,
    tags=['silver', 'transformation', 'data-quality', 'cleaning']
)

def wait_for_bronze_completion(**context):
    """Wait for Bronze layer completion"""
    import logging
    import time
    
    logging.info("Waiting for Bronze layer processing to complete...")
    
    # Simulate waiting for Bronze completion
    time.sleep(3)
    
    bronze_status = {
        'bronze_processing_completed': True,
        'last_bronze_update': datetime.now().isoformat(),
        'tables_available': [
            'bronze_pos_transactions',
            'bronze_cinema_sales', 
            'bronze_inventory_updates',
            'bronze_customer_reviews'
        ],
        'total_records_available': 346,
        'data_freshness_minutes': 2
    }
    
    logging.info("Bronze layer processing confirmed complete")
    context['task_instance'].xcom_push(key='bronze_status', value=bronze_status)
    
    return bronze_status

def clean_pos_transactions(**context):
    """Clean and validate POS transaction data"""
    import logging
    
    logging.info("Starting POS transactions cleaning...")
    
    # Simulate data cleaning operations
    cleaning_stats = {
        'table': 'silver_pos_transactions_clean',
        'input_records': 156,
        'output_records': 154,
        'cleaning_operations': {
            'duplicate_removal': 2,
            'null_value_handling': 3,
            'data_type_conversion': 156,
            'business_rule_validation': 154,
            'outlier_detection': 1
        },
        'data_quality_improvements': {
            'completeness': '99.2%',
            'accuracy': '98.7%',
            'consistency': '99.5%'
        },
        'enrichments': {
            'product_category_mapping': 154,
            'customer_segment_tagging': 154,
            'seasonal_flags': 154
        },
        'processing_time_ms': 2340,
        'status': 'completed'
    }
    
    logging.info(f"POS cleaning: {cleaning_stats['input_records']} → {cleaning_stats['output_records']} records")
    context['task_instance'].xcom_push(key='pos_cleaning_result', value=cleaning_stats)
    
    return cleaning_stats

def clean_cinema_sales(**context):
    """Clean and enrich cinema sales data"""
    import logging
    
    logging.info("Starting cinema sales cleaning...")
    
    # Simulate data cleaning and enrichment
    cleaning_stats = {
        'table': 'silver_cinema_sales_enriched',
        'input_records': 89,
        'output_records': 89,
        'cleaning_operations': {
            'show_time_standardization': 89,
            'seat_number_validation': 89,
            'pricing_tier_assignment': 89,
            'theater_capacity_check': 89
        },
        'enrichments': {
            'movie_metadata_join': 89,
            'customer_history_enrichment': 89,
            'popularity_scoring': 89,
            'demographic_tagging': 89
        },
        'data_quality_improvements': {
            'completeness': '100%',
            'accuracy': '99.1%',
            'consistency': '98.9%'
        },
        'business_insights': {
            'peak_hours_identified': ['19:00-21:00', '14:00-16:00'],
            'popular_genres': ['Action', 'Adventure', 'Drama'],
            'avg_customer_age': 24.5
        },
        'processing_time_ms': 1890,
        'status': 'completed'
    }
    
    logging.info(f"Cinema cleaning: {cleaning_stats['input_records']} → {cleaning_stats['output_records']} records")
    context['task_instance'].xcom_push(key='cinema_cleaning_result', value=cleaning_stats)
    
    return cleaning_stats

def normalize_inventory_data(**context):
    """Normalize inventory update data"""
    import logging
    
    logging.info("Starting inventory data normalization...")
    
    # Simulate normalization operations
    normalization_stats = {
        'table': 'silver_inventory_normalized',
        'input_records': 67,
        'output_records': 66,
        'normalization_operations': {
            'stock_level_calculation': 66,
            'reorder_point_optimization': 66,
            'supplier_standardization': 66,
            'warehouse_consolidation': 66,
            'seasonal_adjustment': 66
        },
        'data_transformations': {
            'unit_standardization': 66,
            'currency_normalization': 66,
            'date_format_standardization': 66,
            'location_geocoding': 66
        },
        'inventory_insights': {
            'low_stock_alerts': 8,
            'overstock_warnings': 3,
            'reorder_recommendations': 12,
            'fast_moving_items': 15,
            'slow_moving_items': 5
        },
        'processing_time_ms': 1560,
        'status': 'completed'
    }
    
    logging.info(f"Inventory normalization: {normalization_stats['input_records']} → {normalization_stats['output_records']} records")
    context['task_instance'].xcom_push(key='inventory_normalization_result', value=normalization_stats)
    
    return normalization_stats

def analyze_customer_sentiment(**context):
    """Analyze customer review sentiment"""
    import logging
    
    logging.info("Starting customer sentiment analysis...")
    
    # Simulate sentiment analysis
    sentiment_stats = {
        'table': 'silver_reviews_sentiment',
        'input_records': 34,
        'output_records': 34,
        'sentiment_analysis': {
            'positive_reviews': 26,
            'negative_reviews': 4,
            'neutral_reviews': 4,
            'avg_sentiment_score': 0.72,
            'confidence_threshold': 0.85
        },
        'text_processing': {
            'language_detection': 34,
            'spam_filtering': 34,
            'keyword_extraction': 34,
            'topic_modeling': 34,
            'emotion_detection': 34
        },
        'insights_generated': {
            'trending_topics': ['quality', 'delivery', 'packaging', 'value'],
            'improvement_areas': ['customer_service', 'shipping_speed'],
            'product_satisfaction': {
                'manga': 4.5,
                'figures': 4.2,
                'clothing': 4.0,
                'accessories': 4.1
            }
        },
        'processing_time_ms': 980,
        'status': 'completed'
    }
    
    logging.info(f"Sentiment analysis: {sentiment_stats['input_records']} reviews processed")
    context['task_instance'].xcom_push(key='sentiment_analysis_result', value=sentiment_stats)
    
    return sentiment_stats

def perform_data_quality_checks(**context):
    """Perform comprehensive data quality validation"""
    import logging
    
    logging.info("Performing Silver layer data quality checks...")
    
    # Get processing results
    ti = context['task_instance']
    pos_result = ti.xcom_pull(task_ids='clean_pos_transactions')
    cinema_result = ti.xcom_pull(task_ids='clean_cinema_sales')
    inventory_result = ti.xcom_pull(task_ids='normalize_inventory_data')
    sentiment_result = ti.xcom_pull(task_ids='analyze_customer_sentiment')
    
    # Aggregate quality metrics
    quality_report = {
        'tables_processed': 4,
        'total_input_records': sum([
            pos_result['input_records'] if pos_result else 0,
            cinema_result['input_records'] if cinema_result else 0,
            inventory_result['input_records'] if inventory_result else 0,
            sentiment_result['input_records'] if sentiment_result else 0
        ]),
        'total_output_records': sum([
            pos_result['output_records'] if pos_result else 0,
            cinema_result['output_records'] if cinema_result else 0,
            inventory_result['output_records'] if inventory_result else 0,
            sentiment_result['output_records'] if sentiment_result else 0
        ]),
        'quality_metrics': {
            'overall_completeness': 99.1,
            'overall_accuracy': 98.8,
            'overall_consistency': 99.0,
            'data_freshness_score': 99.5
        },
        'quality_issues': {
            'critical_errors': 0,
            'warnings': 2,
            'informational': 5
        },
        'performance_metrics': {
            'avg_processing_time_ms': 1692,
            'throughput_records_per_second': 245,
            'resource_utilization': '45%'
        },
        'recommendations': [
            'Monitor POS transaction duplicate patterns',
            'Enhance inventory supplier data quality',
            'Implement real-time sentiment scoring'
        ],
        'status': 'passed'
    }
    
    quality_score = quality_report['quality_metrics']['overall_accuracy']
    logging.info(f"Silver quality check completed: {quality_score}% overall accuracy")
    context['task_instance'].xcom_push(key='quality_report', value=quality_report)
    
    return quality_report

def create_silver_aggregations(**context):
    """Create aggregated views for Gold layer preparation"""
    import logging
    
    logging.info("Creating Silver layer aggregations...")
    
    # Simulate aggregation creation
    aggregation_stats = {
        'aggregations_created': [
            'silver_daily_sales_summary',
            'silver_hourly_cinema_metrics',
            'silver_inventory_snapshots',
            'silver_customer_behavior_patterns'
        ],
        'aggregation_details': {
            'daily_sales_summary': {
                'records': 30,
                'metrics': ['total_revenue', 'transaction_count', 'avg_basket_size'],
                'dimensions': ['date', 'product_category', 'customer_segment']
            },
            'hourly_cinema_metrics': {
                'records': 168,  # 7 days * 24 hours
                'metrics': ['tickets_sold', 'revenue', 'occupancy_rate'],
                'dimensions': ['hour', 'theater_id', 'movie_genre']
            },
            'inventory_snapshots': {
                'records': 450,  # Number of products
                'metrics': ['current_stock', 'stock_movement', 'reorder_status'],
                'dimensions': ['product_id', 'warehouse_id', 'supplier_id']
            },
            'customer_behavior_patterns': {
                'records': 1250,  # Number of active customers
                'metrics': ['purchase_frequency', 'avg_order_value', 'satisfaction_score'],
                'dimensions': ['customer_id', 'segment', 'registration_cohort']
            }
        },
        'performance_optimizations': {
            'partition_strategy': 'date-based',
            'compression_ratio': 0.35,
            'index_creation': 'completed'
        },
        'status': 'completed'
    }
    
    total_agg_records = sum(agg['records'] for agg in aggregation_stats['aggregation_details'].values())
    logging.info(f"Silver aggregations created: {len(aggregation_stats['aggregations_created'])} views, {total_agg_records} total records")
    context['task_instance'].xcom_push(key='aggregation_stats', value=aggregation_stats)
    
    return aggregation_stats

def handle_schema_evolution(**context):
    """Handle schema evolution and backward compatibility"""
    import logging
    
    logging.info("Handling schema evolution...")
    
    # Simulate schema evolution handling
    schema_evolution_result = {
        'schema_changes_detected': 2,
        'changes': [
            {
                'table': 'silver_pos_transactions_clean',
                'change_type': 'column_addition',
                'new_column': 'payment_method_category',
                'backward_compatible': True
            },
            {
                'table': 'silver_cinema_sales_enriched',
                'change_type': 'data_type_change',
                'column': 'show_duration',
                'old_type': 'string',
                'new_type': 'integer',
                'migration_required': True
            }
        ],
        'migration_strategy': 'incremental_update',
        'compatibility_maintained': True,
        'version_info': {
            'previous_version': '1.2.0',
            'current_version': '1.3.0',
            'breaking_changes': False
        },
        'status': 'completed'
    }
    
    logging.info(f"Schema evolution handled: {schema_evolution_result['schema_changes_detected']} changes")
    context['task_instance'].xcom_push(key='schema_evolution_result', value=schema_evolution_result)
    
    return schema_evolution_result

# Task definitions
wait_for_bronze_task = PythonOperator(
    task_id='wait_for_bronze_completion',
    python_callable=wait_for_bronze_completion,
    dag=dag
)

clean_pos_task = PythonOperator(
    task_id='clean_pos_transactions',
    python_callable=clean_pos_transactions,
    dag=dag
)

clean_cinema_task = PythonOperator(
    task_id='clean_cinema_sales',
    python_callable=clean_cinema_sales,
    dag=dag
)

normalize_inventory_task = PythonOperator(
    task_id='normalize_inventory_data',
    python_callable=normalize_inventory_data,
    dag=dag
)

analyze_sentiment_task = PythonOperator(
    task_id='analyze_customer_sentiment',
    python_callable=analyze_customer_sentiment,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='perform_data_quality_checks',
    python_callable=perform_data_quality_checks,
    dag=dag
)

create_aggregations_task = PythonOperator(
    task_id='create_silver_aggregations',
    python_callable=create_silver_aggregations,
    dag=dag
)

handle_schema_evolution_task = PythonOperator(
    task_id='handle_schema_evolution',
    python_callable=handle_schema_evolution,
    dag=dag
)

# Maintenance tasks
optimize_silver_tables = BashOperator(
    task_id='optimize_silver_tables',
    bash_command='''
    echo "Optimizing Silver layer tables..."
    echo "Compacting small files and updating statistics..."
    echo "Table optimization completed at $(date)"
    ''',
    dag=dag
)

send_silver_completion_notification = BashOperator(
    task_id='send_silver_completion_notification',
    bash_command='''
    echo "=== Silver Processing Pipeline Completed ==="
    echo "Timestamp: $(date)"
    echo "Data cleaned and transformed successfully"
    echo "Ready for Gold layer analytics"
    echo "============================================="
    ''',
    dag=dag
)

# Task dependencies
wait_for_bronze_task >> [clean_pos_task, clean_cinema_task, normalize_inventory_task, analyze_sentiment_task]

[clean_pos_task, clean_cinema_task, normalize_inventory_task, analyze_sentiment_task] >> quality_check_task

quality_check_task >> create_aggregations_task >> handle_schema_evolution_task

handle_schema_evolution_task >> optimize_silver_tables >> send_silver_completion_notification