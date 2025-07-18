from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

default_args = {
    'owner': 'animeverse',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'animeverse_data_quality_dag',
    default_args=default_args,
    description='AnimeVerse Data Quality Monitoring and Validation',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['animeverse', 'data-quality', 'monitoring'],
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

def validate_layer(layer_name):
    def _validate(**context):
        logging.info(f"Validating data quality for: {layer_name}")
        # مثال: check nulls, duplicates, schema, freshness
        return True
    return _validate

bronze_check = PythonOperator(
    task_id="validate_bronze_layer",
    python_callable=validate_layer("bronze"),
    dag=dag
)

silver_check = PythonOperator(
    task_id="validate_silver_layer",
    python_callable=validate_layer("silver"),
    dag=dag
)

start >> bronze_check >> silver_check >> end
