from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

default_args = {{
    'owner': 'animeverse',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    'animeverse_etl_dag',
    default_args=default_args,
    description='AnimeVerse ETL Pipeline for Bronze, Silver, and Gold layers',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['animeverse', 'etl', 'iceberg'],
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


bronze_0 = SparkSubmitOperator(
    task_id='complete_bronze_layer',
    application='/opt/airflow/processing/jobs/bronze/complete_bronze_layer.py',
    conn_id='spark_default',
    dag=dag,
)
bronze_1 = SparkSubmitOperator(
    task_id='dimensions_processing_bronze',
    application='/opt/airflow/processing/jobs/bronze/dimensions_processing_bronze.py',
    conn_id='spark_default',
    dag=dag,
)
bronze_2 = SparkSubmitOperator(
    task_id='streaming_bronze',
    application='/opt/airflow/processing/jobs/bronze/streaming_bronze.py',
    conn_id='spark_default',
    dag=dag,
)

silver_0 = SparkSubmitOperator(
    task_id='complete_silver_layer',
    application='/opt/airflow/processing/jobs/silver/complete_silver_layer.py',
    conn_id='spark_default',
    dag=dag,
)
silver_1 = SparkSubmitOperator(
    task_id='dimensions_processing_silver',
    application='/opt/airflow/processing/jobs/silver/dimensions_processing_silver.py',
    conn_id='spark_default',
    dag=dag,
)
silver_2 = SparkSubmitOperator(
    task_id='streaming_silver',
    application='/opt/airflow/processing/jobs/silver/streaming_silver.py',
    conn_id='spark_default',
    dag=dag,
)

gold_0 = SparkSubmitOperator(
    task_id='complete_gold_layer',
    application='/opt/airflow/processing/jobs/gold/complete_gold_layer.py',
    conn_id='spark_default',
    dag=dag,
)

start >> bronze_0 >> bronze_1 >> bronze_2 >> silver_0 >> silver_1 >> silver_2 >> gold_0 >> end