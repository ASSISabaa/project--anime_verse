
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'animeverse',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'animeverse_streaming_dag',
    default_args=default_args,
    description='AnimeVerse Real-time Data Processing Pipeline',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['animeverse', 'streaming', 'kafka', 'real-time'],
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

run_stream_job = SparkSubmitOperator(
    task_id="run_stream_processing",
    application="/opt/spark/jobs/stream_processing.py",
    conn_id="spark_default",
    dag=dag,
)

handle_late_data = SparkSubmitOperator(
    task_id="handle_late_data",
    application="/opt/spark/jobs/handle_late_data.py",
    conn_id="spark_default",
    dag=dag,
)

start >> run_stream_job >> handle_late_data >> end
