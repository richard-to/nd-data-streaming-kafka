"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry! (DONE)
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://127.0.0.1:9092",
            "compression.type": "lz4",
        }
        schema_registry_url = "http://127.0.0.1:8081"

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer (DONE)
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
            schema_registry=CachedSchemaRegistryClient(schema_registry_url),
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker. (DONE)
        #
        #
        client = AdminClient(self.broker_properties)
        futures = client.create_topics(
            [NewTopic(topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)]
        )
        for _, future in futures.items():
            try:
                future.result()
            except Exception:
                pass

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here (DONE)
        #
        #
        self.producer.flush()
        self.producer.close()
