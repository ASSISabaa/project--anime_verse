FROM apache/airflow:2.7.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    default-jdk \
    procps \
    curl \
    wget \
    vim \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Create necessary directories
RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/dags \
    && mkdir -p /opt/airflow/plugins \
    && mkdir -p /opt/airflow/spark_apps

USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt

# Copy Airflow configuration files
COPY airflow.cfg /opt/airflow/airflow.cfg

# Copy DAGs and plugins
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

# Set environment variables for Airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true
ENV AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
ENV AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
ENV AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=UTC
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=UTC

# Spark configuration
ENV SPARK_HOME=/opt/bitnami/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Airflow connections
ENV AIRFLOW_CONN_SPARK_DEFAULT='spark://spark-master:7077'
ENV AIRFLOW_CONN_POSTGRES_DEFAULT='postgresql://airflow:airflow@postgres:5432/airflow'

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set proper permissions
USER root
RUN chown -R airflow:airflow /opt/airflow
USER airflow

# Default command
CMD ["airflow", "webserver"]