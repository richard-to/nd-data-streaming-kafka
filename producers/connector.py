"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://127.0.0.1:8083/connectors"
CONNECTOR_NAME = "stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!) (DONE)
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                # TODO (DONE)
                "connection.url": "jdbc:postgresql://postgres:5432/cta",
                # TODO (DONE)
                "connection.user": "cta_admin",
                # TODO (DONE)
                "connection.password": "chicago",
                # TODO (DONE)
                "table.whitelist": "stations",
                # TODO (DONE)
                "mode": "incrementing",
                # TODO (DONE)
                "incrementing.column.name": "stop_id",
                # TODO (DONE)
                "topic.prefix": "org.chicago.cta.db.",
                # TODO (DONE)
                # Since the stations table shouldn't change that much, we'll use an interval of 1 hour
                "poll.interval.ms": "3600000",
            }
        }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
