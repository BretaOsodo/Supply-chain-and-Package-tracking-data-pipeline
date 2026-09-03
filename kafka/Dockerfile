FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir \
    confluent-kafka==2.15.0 \
    python-dotenv==1.2.2

COPY kafka /app/kafka
COPY data_generator /app/data_generator

CMD ["python", "-m", "kafka.streaming"]