FROM apache/spark:3.4.1

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir pyspark==3.4.1 \
    && pip3 install --no-cache-dir boto3==1.26.13 \
    && pip3 install --no-cache-dir psycopg2-binary==2.9.7 \
    && pip3 install --no-cache-dir pyarrow==12.0.1 \
    && pip3 install --no-cache-dir fastparquet==2024.2.0 \
    && pip3 install --no-cache-dir confluent-kafka==2.2.0

ENV SPARK_JARS_DIR=/opt/spark/jars

RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o ${SPARK_JARS_DIR}/postgresql.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o ${SPARK_JARS_DIR}/hadoop-aws.jar

RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar \
    -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar \
    -o ${SPARK_JARS_DIR}/kafka-clients.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o ${SPARK_JARS_DIR}/commons-pool2.jar

# Copy Spark metrics configuration into Spark's config directory
COPY metrics.properties /opt/spark/conf/metrics.properties

# Copy project files
COPY . /spark

RUN chown spark:spark /opt/spark/conf/metrics.properties

USER spark

WORKDIR /spark