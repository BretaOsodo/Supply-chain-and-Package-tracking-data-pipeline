FROM apache/spark:3.4.1

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir pyspark==3.4.1\
    && pip3 install --no-cache-dir boto3==1.26.13 \
    && pip3 install --no-cache-dir psycopg2-binary==2.9.7 \
    && pip3 install --no-cache-dir pyarrow==12.0.1 \
    && pip3 install --no-cache-dir fastparquet==2024.6.14 \
    && pip3 install --no-cache-dir confluent-kafka==2.2.0 

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o ${SPARK_JARS_DIR}/postgresql.jar

RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o ${SPARK_JARS_DIR}/hadoop-aws.jar

RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_JARS_DIR}/aws-java-sdk.jar

USER spark

WORKDIR /spark

COPY . .