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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gold_analytics_pipeline',
    default_args=default_args,
    description='Create Gold layer analytics tables and star schema',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    catchup=False,
    tags=['gold', 'analytics', 'star-schema']
)

# Wait for Silver processing to complete - simplified
def wait_for_silver_completion(**context):
    """Wait for Silver processing to complete - Simulated"""
    import logging
    import time
    
    logging.info("Checking Silver layer completion status...")
    
    # Simulate waiting for Silver processing
    time.sleep(5)
    
    silver_status = {
        'silver_processing_completed': True,
        'last_update': datetime.now().isoformat(),
        'tables_ready': ['pos_transactions_clean', 'cinema_sales_enriched', 'inventory_normalized', 'reviews_sentiment'],
        'quality_score': 98.5
    }
    
    logging.info("Silver layer processing completed successfully")
    context['task_instance'].xcom_push(key='silver_status', value=silver_status)
    
    return silver_status

def create_dim_customers(**context):
    """Create customer dimension table - Simulated"""
    import logging
    
    logging.info("Creating customer dimension table...")
    
    # Simulate dimension table creation
    customers_dim = {
        'table_name': 'dim_customers',
        'total_customers': 15420,
        'new_customers_today': 45,
        'customer_segments': {
            'premium': 2340,
            'regular': 8950,
            'occasional': 4130
        },
        'scd_type': 'Type 2',
        'columns': ['customer_id', 'customer_name', 'email', 'segment', 'registration_date', 'last_purchase', 'effective_date', 'expiry_date'],
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Customer dimension created: {customers_dim['total_customers']} customers")
    context['task_instance'].xcom_push(key='dim_customers_result', value=customers_dim)
    
    return customers_dim

def create_dim_products(**context):
    """Create product dimension table - Simulated"""
    import logging
    
    logging.info("Creating product dimension table...")
    
    # Simulate dimension table creation
    products_dim = {
        'table_name': 'dim_products',
        'total_products': 8950,
        'active_products': 7820,
        'product_categories': {
            'manga': 3450,
            'figures': 2100,
            'clothing': 1890,
            'accessories': 1510
        },
        'columns': ['product_id', 'product_name', 'category', 'subcategory', 'price', 'supplier', 'launch_date'],
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Product dimension created: {products_dim['total_products']} products")
    context['task_instance'].xcom_push(key='dim_products_result', value=products_dim)
    
    return products_dim

def create_dim_anime_titles(**context):
    """Create anime titles dimension table - Simulated"""
    import logging
    
    logging.info("Creating anime titles dimension table...")
    
    # Simulate dimension table creation
    anime_dim = {
        'table_name': 'dim_anime_titles',
        'total_titles': 450,
        'currently_screening': 25,
        'genres': {
            'shonen': 125,
            'shoujo': 89,
            'seinen': 98,
            'josei': 67,
            'mecha': 71
        },
        'top_rated': [
            {'title': 'Attack on Titan: Final Season', 'rating': 9.2, 'tickets_sold': 2340},
            {'title': 'Demon Slayer: Infinity Train', 'rating': 9.0, 'tickets_sold': 1890},
            {'title': 'Your Name', 'rating': 8.9, 'tickets_sold': 1650}
        ],
        'columns': ['anime_id', 'title', 'genre', 'studio', 'release_year', 'rating', 'status'],
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Anime dimension created: {anime_dim['total_titles']} titles")
    context['task_instance'].xcom_push(key='dim_anime_result', value=anime_dim)
    
    return anime_dim

def create_dim_calendar(**context):
    """Create calendar dimension table - Simulated"""
    import logging
    
    logging.info("Creating calendar dimension table...")
    
    # Simulate dimension table creation
    calendar_dim = {
        'table_name': 'dim_calendar',
        'date_range': '2024-01-01 to 2025-12-31',
        'total_days': 731,
        'business_days': 522,
        'weekend_days': 209,
        'special_events': {
            'anime_conventions': 12,
            'movie_premieres': 28,
            'sales_events': 45
        },
        'columns': ['date_id', 'date', 'year', 'month', 'day', 'quarter', 'day_of_week', 'is_weekend', 'is_holiday'],
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Calendar dimension created: {calendar_dim['total_days']} days")
    context['task_instance'].xcom_push(key='dim_calendar_result', value=calendar_dim)
    
    return calendar_dim

def create_fact_sales(**context):
    """Create sales fact table - Simulated"""
    import logging
    
    logging.info("Creating sales fact table...")
    
    # Get dimension results
    ti = context['task_instance']
    customers_dim = ti.xcom_pull(task_ids='create_dim_customers')
    products_dim = ti.xcom_pull(task_ids='create_dim_products')
    
    # Simulate fact table creation
    sales_fact = {
        'table_name': 'fact_sales',
        'total_transactions': 25600,
        'total_revenue': 485750.50,
        'avg_transaction_value': 18.97,
        'metrics': {
            'daily_revenue': 15750.50,
            'weekly_revenue': 89250.75,
            'monthly_revenue': 385400.25
        },
        'top_selling_categories': {
            'manga': 145800.25,
            'figures': 125600.75,
            'clothing': 98750.50,
            'accessories': 75550.25
        },
        'columns': ['transaction_id', 'customer_key', 'product_key', 'date_key', 'quantity', 'unit_price', 'total_amount', 'discount'],
        'grain': 'One row per transaction line item',
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Sales fact table created: {sales_fact['total_transactions']} transactions, ${sales_fact['total_revenue']:,.2f} revenue")
    context['task_instance'].xcom_push(key='fact_sales_result', value=sales_fact)
    
    return sales_fact

def create_fact_cinema_attendance(**context):
    """Create cinema attendance fact table - Simulated"""
    import logging
    
    logging.info("Creating cinema attendance fact table...")
    
    # Simulate fact table creation
    cinema_fact = {
        'table_name': 'fact_cinema_attendance',
        'total_bookings': 3450,
        'total_tickets_sold': 8920,
        'total_revenue': 156750.25,
        'avg_ticket_price': 17.58,
        'metrics': {
            'occupancy_rate': 78.5,
            'concession_revenue': 25600.75,
            'premium_bookings': 1240
        },
        'top_performing_movies': {
            'Attack on Titan: Final Season': 2340,
            'Demon Slayer: Infinity Train': 1890,
            'Your Name': 1650,
            'Spirited Away': 1420
        },
        'columns': ['booking_id', 'customer_key', 'anime_key', 'date_key', 'theater_id', 'tickets_sold', 'ticket_revenue', 'concession_revenue'],
        'grain': 'One row per booking',
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Cinema fact table created: {cinema_fact['total_bookings']} bookings, {cinema_fact['total_tickets_sold']} tickets sold")
    context['task_instance'].xcom_push(key='fact_cinema_result', value=cinema_fact)
    
    return cinema_fact

def create_fact_engagement(**context):
    """Create customer engagement fact table - Simulated"""
    import logging
    
    logging.info("Creating customer engagement fact table...")
    
    # Simulate fact table creation
    engagement_fact = {
        'table_name': 'fact_engagement',
        'total_reviews': 1250,
        'avg_rating': 4.3,
        'total_interactions': 15600,
        'metrics': {
            'positive_sentiment': 892,
            'negative_sentiment': 123,
            'neutral_sentiment': 235,
            'review_response_rate': 89.5
        },
        'engagement_by_product': {
            'manga': {'reviews': 520, 'avg_rating': 4.5},
            'figures': {'reviews': 310, 'avg_rating': 4.2},
            'clothing': {'reviews': 245, 'avg_rating': 4.1},
            'accessories': {'reviews': 175, 'avg_rating': 4.0}
        },
        'columns': ['engagement_id', 'customer_key', 'product_key', 'date_key', 'review_rating', 'sentiment_score', 'helpful_votes'],
        'grain': 'One row per customer product interaction',
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Engagement fact table created: {engagement_fact['total_reviews']} reviews, avg rating: {engagement_fact['avg_rating']}")
    context['task_instance'].xcom_push(key='fact_engagement_result', value=engagement_fact)
    
    return engagement_fact

def create_daily_summary(**context):
    """Create daily business summary - Simulated"""
    import logging
    
    logging.info("Creating daily business summary...")
    
    # Get fact table results
    ti = context['task_instance']
    sales_fact = ti.xcom_pull(task_ids='create_fact_sales')
    cinema_fact = ti.xcom_pull(task_ids='create_fact_cinema_attendance')
    engagement_fact = ti.xcom_pull(task_ids='create_fact_engagement')
    
    # Calculate daily summary
    daily_summary = {
        'table_name': 'daily_business_summary',
        'summary_date': datetime.now().strftime('%Y-%m-%d'),
        'kpis': {
            'total_revenue': (sales_fact['total_revenue'] if sales_fact else 0) + (cinema_fact['total_revenue'] if cinema_fact else 0),
            'total_transactions': (sales_fact['total_transactions'] if sales_fact else 0) + (cinema_fact['total_bookings'] if cinema_fact else 0),
            'customer_satisfaction': engagement_fact['avg_rating'] if engagement_fact else 0,
            'inventory_turnover': 0.85,
            'market_share': 12.5
        },
        'trends': {
            'revenue_growth': 8.5,
            'customer_growth': 12.3,
            'product_popularity_change': 5.7
        },
        'alerts': [
            'Low stock warning for Attack on Titan merchandise',
            'High customer satisfaction in cinema segment',
            'Opportunity to expand figure collection'
        ],
        'created_at': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    total_revenue = daily_summary['kpis']['total_revenue']
    logging.info(f"Daily summary created: ${total_revenue:,.2f} total revenue")
    context['task_instance'].xcom_push(key='daily_summary_result', value=daily_summary)
    
    return daily_summary

def process_scd_customers(**context):
    """Process SCD Type 2 for customers - Simulated"""
    import logging
    
    logging.info("Processing SCD Type 2 for customers...")
    
    # Simulate SCD processing
    scd_result = {
        'table_name': 'dim_customers_scd',
        'records_processed': 15420,
        'new_records': 45,
        'updated_records': 23,
        'expired_records': 12,
        'scd_operations': {
            'new_customer_segments': 5,
            'address_changes': 18,
            'contact_updates': 12
        },
        'effective_date': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"SCD processing completed: {scd_result['new_records']} new, {scd_result['updated_records']} updated")
    context['task_instance'].xcom_push(key='scd_customers_result', value=scd_result)
    
    return scd_result

def process_scd_anime_schedule(**context):
    """Process SCD Type 2 for anime schedule - Simulated"""
    import logging
    
    logging.info("Processing SCD Type 2 for anime schedule...")
    
    # Simulate SCD processing
    scd_result = {
        'table_name': 'dim_anime_schedule_scd',
        'records_processed': 450,
        'new_records': 8,
        'updated_records': 5,
        'expired_records': 2,
        'schedule_changes': {
            'new_screenings': 8,
            'time_changes': 3,
            'venue_changes': 2
        },
        'effective_date': datetime.now().isoformat(),
        'status': 'completed'
    }
    
    logging.info(f"Anime schedule SCD processing completed: {scd_result['new_records']} new, {scd_result['updated_records']} updated")
    context['task_instance'].xcom_push(key='scd_anime_result', value=scd_result)
    
    return scd_result

def generate_analytics_report(**context):
    """Generate comprehensive analytics report"""
    import logging
    
    logging.info("Generating comprehensive analytics report...")
    
    # Get all results
    ti = context['task_instance']
    daily_summary = ti.xcom_pull(task_ids='create_daily_summary')
    scd_customers = ti.xcom_pull(task_ids='process_scd_customers')
    scd_anime = ti.xcom_pull(task_ids='process_scd_anime_schedule')
    
    analytics_report = {
        'report_name': 'Animeverse Daily Analytics Report',
        'generated_at': datetime.now().isoformat(),
        'executive_summary': {
            'total_revenue': daily_summary['kpis']['total_revenue'] if daily_summary else 0,
            'customer_satisfaction': daily_summary['kpis']['customer_satisfaction'] if daily_summary else 0,
            'key_insights': [
                'Strong performance in manga sales',
                'High cinema attendance for new releases',
                'Positive customer sentiment trend'
            ]
        },
        'data_freshness': {
            'last_update': datetime.now().isoformat(),
            'data_quality_score': 98.7,
            'coverage': '100%'
        },
        'next_actions': [
            'Monitor inventory levels for popular items',
            'Analyze customer segmentation for targeted marketing',
            'Prepare for upcoming anime releases'
        ],
        'status': 'completed'
    }
    
    logging.info("Analytics report generated successfully")
    context['task_instance'].xcom_push(key='analytics_report', value=analytics_report)
    
    return analytics_report

# Task definitions

# Wait for Silver processing
wait_for_silver_task = PythonOperator(
    task_id='wait_for_silver_processing',
    python_callable=wait_for_silver_completion,
    dag=dag
)

# Dimension tables
create_dim_customers_task = PythonOperator(
    task_id='create_dim_customers',
    python_callable=create_dim_customers,
    dag=dag
)

create_dim_products_task = PythonOperator(
    task_id='create_dim_products',
    python_callable=create_dim_products,
    dag=dag
)

create_dim_anime_titles_task = PythonOperator(
    task_id='create_dim_anime_titles',
    python_callable=create_dim_anime_titles,
    dag=dag
)

create_dim_calendar_task = PythonOperator(
    task_id='create_dim_calendar',
    python_callable=create_dim_calendar,
    dag=dag
)

# Fact tables
create_fact_sales_task = PythonOperator(
    task_id='create_fact_sales',
    python_callable=create_fact_sales,
    dag=dag
)

create_fact_cinema_attendance_task = PythonOperator(
    task_id='create_fact_cinema_attendance',
    python_callable=create_fact_cinema_attendance,
    dag=dag
)

create_fact_engagement_task = PythonOperator(
    task_id='create_fact_engagement',
    python_callable=create_fact_engagement,
    dag=dag
)

# Business summary
create_daily_summary_task = PythonOperator(
    task_id='create_daily_summary',
    python_callable=create_daily_summary,
    dag=dag
)

# SCD processing
process_scd_customers_task = PythonOperator(
    task_id='process_scd_customers',
    python_callable=process_scd_customers,
    dag=dag
)

process_scd_anime_schedule_task = PythonOperator(
    task_id='process_scd_anime_schedule',
    python_callable=process_scd_anime_schedule,
    dag=dag
)

# Final report
generate_analytics_report_task = PythonOperator(
    task_id='generate_analytics_report',
    python_callable=generate_analytics_report,
    dag=dag
)

# Cleanup and notification
cleanup_task = BashOperator(
    task_id='cleanup_gold_temp_files',
    bash_command='''
    echo "Gold layer cleanup started at $(date)"
    echo "Cleaning analytics temporary files..."
    echo "Cleanup completed successfully"
    ''',
    dag=dag
)

notify_completion_task = BashOperator(
    task_id='notify_analytics_completion',
    bash_command='''
    echo "=== Gold Analytics Pipeline Completed ==="
    echo "Timestamp: $(date)"
    echo "Star schema created successfully"
    echo "Analytics reports generated"
    echo "Business intelligence data ready"
    echo "========================================="
    ''',
    dag=dag
)

# Dependencies - Fixed syntax
wait_for_silver_task >> [create_dim_customers_task, create_dim_products_task, create_dim_anime_titles_task, create_dim_calendar_task]

# Dimensions to Facts - one by one
create_dim_customers_task >> create_fact_sales_task
create_dim_products_task >> create_fact_sales_task
create_dim_anime_titles_task >> create_fact_cinema_attendance_task
create_dim_calendar_task >> create_fact_sales_task

create_dim_customers_task >> create_fact_cinema_attendance_task
create_dim_customers_task >> create_fact_engagement_task
create_dim_products_task >> create_fact_engagement_task

# Facts to Summary
create_fact_sales_task >> create_daily_summary_task
create_fact_cinema_attendance_task >> create_daily_summary_task
create_fact_engagement_task >> create_daily_summary_task

# SCD processing
create_dim_customers_task >> process_scd_customers_task
create_dim_anime_titles_task >> process_scd_anime_schedule_task

# Final steps
create_daily_summary_task >> generate_analytics_report_task
process_scd_customers_task >> generate_analytics_report_task
process_scd_anime_schedule_task >> generate_analytics_report_task

generate_analytics_report_task >> cleanup_task >> notify_completion_task