FROM apache/airflow:2.5.0

USER root
RUN apt-get update && apt-get install -y curl netcat default-jdk

USER airflow
RUN pip install apache-airflow-providers-apache-spark
