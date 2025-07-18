FROM bitnami/spark:3.4.2

USER root

# Install Python packages for data engineering
RUN pip install --no-cache-dir \
    great-expectations==0.17.23 \
    kafka-python==2.0.2 \
    pydantic==2.5.0 \
    requests==2.31.0

# Download Iceberg and AWS dependencies
RUN curl -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.4_2.12-1.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar

RUN curl -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.367.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

USER 1001
