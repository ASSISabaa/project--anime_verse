from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

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
    'animeverse_scd_dag',
    default_args=default_args,
    description='AnimeVerse Slowly Changing Dimensions (SCD Type 2) Processing',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    tags=['animeverse', 'scd', 'dimensions', 'type2'],
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

process_customer_dim = SparkSubmitOperator(
    task_id="process_customer_dimension",
    application="/opt/spark/jobs/customer_scd.py",
    conn_id="spark_default",
    dag=dag,
)

process_product_dim = SparkSubmitOperator(
    task_id="process_product_dimension",
    application="/opt/spark/jobs/product_scd.py",
    conn_id="spark_default",
    dag=dag,
)

start >> process_customer_dim >> process_product_dim >> end
