"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry! (DONE)
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://127.0.0.1:9092",
            "group.id": "cta",
        }
        schema_registry_url = "http://172.31.1.20:8081"

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.consumer = AvroConsumer(
                self.broker_properties,
                schema_registry=CachedSchemaRegistryClient(schema_registry_url)
            )
        else:
            self.consumer = Consumer(self.broker_properties)

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked. (DONE)
        #
        #
        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest (DONE)
        for partition in partitions:
            if self.offset_earliest:
                partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved. (DONE)
        #
        #
        message = self.consumer.poll(timeout=1)
        if message is None:
            return 0
        elif message.error() is not None:
            logger.error(message.error())
            return 0
        self.message_handler(message)
        return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer (DONE)
        #
        #
        self.consumer.close()
