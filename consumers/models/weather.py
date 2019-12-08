"""Contains functionality related to Weather"""
import json
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status. (DONE)
        #
        #
        if message.topic() != "org.chicago.cta.weather.v1":
            return
        weather_data = json.loads(message.value())
        self.temperature = weather_data["temperature"]
        self.status = weather_data["status"]
