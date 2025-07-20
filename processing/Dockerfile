FROM bitnami/spark:3.4.1

USER root

# Install Python packages and system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    kafka-python==2.0.2 \
    boto3==1.28.62 \
    pandas==2.1.1 \
    numpy==1.24.3 \
    pyarrow==12.0.1 \
    python-dotenv==1.0.0 \
    great-expectations==0.17.10 \
    sqlalchemy==1.4.49

# Create application directories
RUN mkdir -p /opt/bitnami/spark/work-dir \
    && mkdir -p /opt/bitnami/spark/schemas \
    && mkdir -p /opt/bitnami/spark/utils

# Copy application files
COPY spark_apps/ /opt/bitnami/spark/work-dir/
COPY jars/ /opt/bitnami/spark/jars/
COPY utils/ /opt/bitnami/spark/utils/
COPY schemas/ /opt/bitnami/spark/schemas/

# Download additional JAR files if needed
RUN cd /opt/bitnami/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Set proper permissions
RUN chown -R 1001:1001 /opt/bitnami/spark/
RUN chmod -R 755 /opt/bitnami/spark/work-dir/
RUN chmod -R 755 /opt/bitnami/spark/utils/
RUN chmod -R 755 /opt/bitnami/spark/schemas/

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PYTHONPATH="${SPARK_HOME}/utils:${SPARK_HOME}/schemas:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER 1001

# Default command
CMD ["spark-submit", "--help"]