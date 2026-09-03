FROM apache/spark:3.4.1

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# DO NOT install pyspark here — the base image already provides it at /opt/spark
RUN pip3 install --no-cache-dir \
    boto3==1.26.13 \
    psycopg2-binary==2.9.7 \
    pyarrow==12.0.1 \
    fastparquet==2024.2.0 \
    redis==5.0.1 \
    python-dotenv==1.0.1

ENV SPARK_JARS_DIR=/opt/spark/jars

# Download all JARs in a single RUN layer (fixes the broken continuation)
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o ${SPARK_JARS_DIR}/postgresql.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o ${SPARK_JARS_DIR}/hadoop-aws.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar \
    -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar \
    -o ${SPARK_JARS_DIR}/kafka-clients.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o ${SPARK_JARS_DIR}/commons-pool2.jar && \
    chmod 644 ${SPARK_JARS_DIR}/*.jar

# Copy config
COPY metrics.properties /opt/spark/conf/metrics.properties

# Create /spark and copy jobs with correct ownership
RUN mkdir -p /spark && chown spark:spark /spark
COPY --chown=spark:spark \
    validation_job.py \
    state_processing_job.py \
    eta_calculator_job.py \
    delay_detector_job.py \
    analytics_aggregator.py \
    /spark/
COPY --chown=spark:spark config/ /spark/config/
RUN chown spark:spark /opt/spark/conf/metrics.properties
RUN mkdir -p /spark/checkpoints && chown -R spark:spark /spark/checkpoints
USER spark
WORKDIR /spark