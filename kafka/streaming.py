from data_generator.generated_data import stream_package_events
from confluent_kafka import Producer
import json 
import logging 

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

TOPIC ='package_events'

producer_config = {
    "bootstrap.servers": "127.0.0.1:9092,127.0.0.1:9093",
    "client.id": "package-events-producer",
    "acks": "all",
    "retries": 5,
    "enable.idempotence": True,
}

producer = Producer(producer_config)

def delivery_report(err,msg):
    if err:
        logger.error(
            f"Failed to deliver message to {msg.topic()}: {err}"
        )

    else:
        logger.info(
            f"Delivered event to {msg.topic()}"
            f"[partition = {msg.partition()}], offset= {msg.offset()}"
        )

for event in stream_package_events(
    num_packages=10000,
    events_per_batch=1,
    delay_seconds=1.0
):

    data = event.to_dict()
    value= json.dumps(data).encode("utf-8")

    package_id=data['package_id']

    producer.produce(
        topic= TOPIC,
        key=package_id.encode('utf-8'),
        value=value,
        callback= delivery_report
    )

    ##give kafka an opportunity to process delivery callbacks 
    producer.poll(0)

producer.flush()