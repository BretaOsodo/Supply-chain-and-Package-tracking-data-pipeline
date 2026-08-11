"""
Intergration tests for the docker services defined in the docker-compose.yml file.
"""

import socket 
import time 
import pytest
import requests
from confluent_kafka.admin import AdminClient, NewTopic
import os 
import uuid 
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer, KafkaException

load_dotenv()

KAFKA_BOOTSTRAP = "127.0.0.1:9092,127.0.0.1:9093"
SCHEMA_REGISTRY_URL = "http://127.0.0.1:8082"
KAFDROP_URL = "http://127.0.0.1:9000"
SPARK_MASTER_UI = "http://127.0.0.1:9090"
PROMETHEUS_URL = "http://127.0.0.1:9096"
GRAFANA_URL = "http://127.0.0.1:3000"
CADVISOR_URL = "http://127.0.0.1:8080"
KAFKA_EXPORTER_URL = "http://127.0.0.1:9308"
 
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = os.environ.get("HOST_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_SCHEMA")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
 
PGADMIN_URL = f"http://127.0.0.1:{os.environ.get('PGADMIN_PORT', '5050')}"
 
REQUEST_TIMEOUT = 5

#Helpers 
def wait_for_port(host:str , port:int, timeout:float=60) -> bool:

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host,port), timeout=2):
                return True
        except OSError:
            time.sleep(2)
    return False

def wait_for_kafka_topic(admin_client, topic_name, timeout=30):
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            metadata = admin_client.list_topics(
                topic=topic_name,
                timeout=5
            )

            if topic_name in metadata.topics:
                return True

        except Exception:
            pass

        time.sleep(1)

    return False


def wait_for_http(url:str, timeout:float=60, expected_status:int=200) -> bool:

    dealine = time.time() + timeout

    while time.time() < dealine:
        try:
            r=requests.get(url, timeout=5)
            if r.status_code == expected_status:
                return True
        except requests.RequestException:
            pass
        time.sleep(3)
    return False 

@pytest.fixture(scope="session")
def admin_client():
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

@pytest.fixture(scope="session")

def test_topic(admin_client):
    """
    Create a test topic for Kafka integration tests.
    """
    topic_name = f"pytest-snoke-{uuid.uuid4().hex[:8]}"
    new_topic=NewTopic(topic_name, num_partitions=1, replication_factor=2)

    futures = admin_client.create_topics([new_topic])
    for topic , future in futures.items():
        try:
            future.result(timeout=25)
            print(f"Created topic {topic}")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
            pytest.fail(f"Failed to create topic {topic}: {e}")
    yield topic_name

    admin_client.delete_topics([topic_name])

#Test Kafka Servives:
class Testkafka:
    def test_kafka_port_is_open(self):
        """
        kafka broker should be listening on port 9092 and 9093
        """

        assert wait_for_port("127.0.0.1",9092, timeout=60), "Kafka broker is not listening on port 9092"
        assert wait_for_port("127.0.0.1",9093, timeout=60), "Kafka broker is not listening on port 9093"

    def test_brokers_reachable(self,admin_client):
        metadata = admin_client.list_topics(timeout=10)
        assert len(metadata.brokers) >=2,(
            f"Expected 2 brokers, found {len(metadata.brokers)}. Brokers: {metadata.brokers}"
        )

    def test_can_create_topic(self, admin_client, test_topic):
        assert wait_for_kafka_topic(
            admin_client,
            test_topic,
            timeout=30
        ), f"Topic {test_topic} was not created successfully"

    def test_produce_and_consume_roundtrip(self,test_topic):
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        test_key='PKG-TESTKEY'
        test_value = '{"package_id": "PKG-TESTKEY", "scan_type": "picked"}'

        delivery_reports = []

        def on_delivery(err,msg):
            delivery_reports.append((err,msg))

        producer.produce(test_topic, key=test_key, value=test_value, callback=on_delivery
        )
        producer.flush(timeout=10)

        assert len(delivery_reports) == 1, "Expected one delivery report"
        err, msg = delivery_reports[0]
        assert err is None, f"Delivery failed: {err}"

        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"pytest-consumer-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest"
        })

        consumer.subscribe([test_topic])

        recived = None 
        dealine = time.time() + 10
        while time.time() < dealine:
            record = consumer.poll(timeout=1.0)
            if record is None:
                continue
            if record.error():
                raise KafkaException(record.error())    
            received = record
            break
        consumer.close()

        assert received is not None, "Did not receive any message from Kafka"
        assert received.key().decode("utf-8") == test_key, f"Expected key {test_key}, got {received.key().decode('utf-8')}"
        assert received.value().decode("utf-8") == test_value, f"Expected value {test_value}, got {received.value().decode('utf-8')}"

    def test_same_key_lands_on_same_partitions(self,test_topic):
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        partitions_seen = set()

        def on_delivery(err,msg):
            if err is None:
                partitions_seen.add(msg.partition())

        for _ in range(5):
            producer.produce(test_topic, key="PKG-CONSISTENT", value="{}", callback=on_delivery)

        producer.flush(timeout=10)

        assert len(partitions_seen) == 1, f"Messages with the same key landed on different partitions: {partitions_seen}"


#Schema registry tests
class TestSchemaRegistry:
    def test_schema_registry_port_is_open(self):
        """
        Schema registry should be listening on port 8082
        """
        assert wait_for_port("127.0.0.1",8082, timeout=60), "Schema registry is not listening on port 8082"

    def test_schema_registry_http(self):
        """
        Schema registry should respond to HTTP requests
        """
        assert wait_for_http(SCHEMA_REGISTRY_URL, timeout=60), "Schema registry did not respond to HTTP requests"

#Kafdrop tests
class TestKafdrop:
    def test_kafdrop_port_is_open(self):
        """
        Kafdrop should be listening on port 9000
        """
        assert wait_for_port("127.0.0.1",9000, timeout=60), "Kafdrop is not listening on port 9000"

    def test_kafdrop_http(self):
        """
        Kafdrop should respond to HTTP requests
        """
        assert wait_for_http(KAFDROP_URL, timeout=60), "Kafdrop did not respond to HTTP requests"

#Spark tests
class TestSpark:
    def test_spark_master_ui_port_is_open(self):
        """
        Spark master UI should be listening on port 8080
        """
        assert wait_for_port("127.0.0.1",8080, timeout=60), "Spark master UI is not listening on port 8080"

    def test_spark_master_ui_http(self):
        """
        Spark master UI should respond to HTTP requests
        """
        assert wait_for_http(SPARK_MASTER_UI, timeout=60), "Spark master UI did not respond to HTTP requests"

    def test_two_workers_registered(self):
        """
        There should be two Spark workers registered with the master
        """
        r = requests.get(f"{SPARK_MASTER_UI}/json", timeout=5)

        assert r.status_code == 200
        data = r.json()
        alive_workers = [worker for worker in data.get("workers", []) if worker.get("state") == "ALIVE"]

        assert len(alive_workers) == 2, f"Expected 2 alive workers, found {len(alive_workers)}"

#prometheus tests 
class TestPrometheus:
    def test_prometheus_port_is_open(self):
        """
        Prometheus should be listening on port 9090
        """
        assert wait_for_port("127.0.0.1",9090, timeout=60), "Prometheus is not listening on port 9090"

    def test_prometheus_http(self):
        """
        Prometheus should respond to HTTP requests
        """
        assert wait_for_http(PROMETHEUS_URL, timeout=60), "Prometheus did not respond to HTTP requests"

    def test_all_targets_up(self):
        """
        All Prometheus targets should be up
        """
        r = requests.get(f"{PROMETHEUS_URL}/api/v1/targets", timeout=5)
        assert r.status_code == 200

        targets = r.json()['data']['activeTargets']

        down_targets= [
            target for target in targets if target['health'] != 'up'
        ]

        assert not down_targets, f"Some Prometheus targets are down: {down_targets}"


#grafana tests
class TestGrafana:
    def test_grafana_port_is_open(self):
        """
        Grafana should be listening on port 3000
        """
        assert wait_for_port("127.0.0.1",3000, timeout=60), "Grafana is not listening on port 3000"

    def test_grafana_http(self):
        """
        Grafana should respond to HTTP requests
        """
        assert wait_for_http(GRAFANA_URL, timeout=60), "Grafana did not respond to HTTP requests"

#cAdvisor tests
class TestCAdvisor:
    def test_cadvisor_port_is_open(self):
        """
        cAdvisor should be listening on port 8080
        """
        assert wait_for_port("127.0.0.1",8080, timeout=60), "cAdvisor is not listening on port 8080"

    def test_cadvisor_http(self):
        """
        cAdvisor should respond to HTTP requests
        """
        assert wait_for_http(CADVISOR_URL, timeout=60), "cAdvisor did not respond to HTTP requests"

#kafka exporter tests
class TestKafkaExporter:
    def test_kafka_exporter_port_is_open(self):
        """
        Kafka exporter should be listening on port 9308
        """
        assert wait_for_port("127.0.0.1",9308, timeout=60), "Kafka exporter is not listening on port 9308"

    def test_kafka_exporter_http(self):
        """
        Kafka exporter should respond to HTTP requests
        """
        assert wait_for_http(KAFKA_EXPORTER_URL, timeout=60), "Kafka exporter did not respond to HTTP requests"

#Postgres tests
class TestPostgres:
    def test_postgres_port_is_open(self):
        """
        Postgres should be listening on port 5432
        """
        assert wait_for_port(POSTGRES_HOST,int(POSTGRES_PORT), timeout=60), "Postgres is not listening on port 5432"

    def test_can_connect_and_query(self):
        """
        Should be able to connect to Postgres and run a simple query
        """
        import psycopg2
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=5
            )
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            assert result[0] == 1, f"Expected 1, got {result[0]}"
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()


#PGAdmin tests

class TestPGAdmin:
    def test_pgadmin_port_is_open(self):
        """
        PGAdmin should be listening on the configured port
        """
        pgadmin_port = int(os.environ.get("PGADMIN_PORT", "5050"))
        assert wait_for_port("127.0.0.1", pgadmin_port, timeout=60), f"PGAdmin is not listening on port {pgadmin_port}"

    def test_pgadmin_http(self):
        """
        PGAdmin should respond to HTTP requests
        """
        pgadmin_port = int(os.environ.get("PGADMIN_PORT", "5050"))
        assert wait_for_http(f"http://127.0.0.1:{pgadmin_port}", timeout=60), f"PGAdmin did not respond to HTTP requests on port {pgadmin_port}"