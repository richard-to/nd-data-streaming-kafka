"""Contains functionality related to Weather"""
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
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status. (DONE)
        #
        #
        if message.topic() != "org.chicago.cta.weather.v1":
            return
        weather_data = message.value()
        self.temperature = weather_data["temperature"]
        self.status = weather_data["status"]
