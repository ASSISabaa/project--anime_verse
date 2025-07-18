from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import logging
import requests

default_args = {
    'owner': 'animeverse',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'animeverse_monitoring_dag',
    default_args=default_args,
    description='AnimeVerse System Monitoring and Health Checks',
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=['animeverse', 'monitoring', 'health-check'],
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

def check_spark_cluster_health(**context):
    logging.info("Checking Spark cluster health...")
    try:
        spark_master_url = Variable.get('SPARK_MASTER', 'spark://spark-master:7077')
        spark_ui_url = spark_master_url.replace('spark://', 'http://').replace(':7077', ':8080')
        response = requests.get(f"{spark_ui_url}/json/", timeout=10)
        if response.status_code == 200:
            logging.info("Spark cluster is healthy.")
        else:
            logging.warning("Spark cluster might be down or unreachable.")
    except Exception as e:
        logging.error(f"Failed to connect to Spark UI: {e}")

spark_health_check = PythonOperator(
    task_id="check_spark_cluster_health",
    python_callable=check_spark_cluster_health,
    dag=dag,
)

start >> spark_health_check >> end
