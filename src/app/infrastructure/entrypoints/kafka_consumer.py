import json
import logging
from kafka import KafkaConsumer
from ...core.entities.event import Event
from ...core.use_cases.process_event import ProcessEventUseCase

logger = logging.getLogger(__name__)

class EventConsumer:
    def __init__(self, consumer: KafkaConsumer, use_case: ProcessEventUseCase):
        self._consumer = consumer
        self._use_case = use_case

    def run(self):
        logger.info("Kafka consumer started. Waiting for messages...")
        for message in self._consumer:
            try:
                event_data = json.loads(message.value)
                logger.info(f"Received message with key: {message.key}")
                event = Event.from_dict(event_data)
                self._use_case.execute(event)
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Failed to decode or validate message. Value: '{message.value}'. Error: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while processing message. Value: '{message.value}'. Error: {e}", exc_info=True)