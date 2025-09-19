import logging
from ..entities.event import Event
from ...presentation.ports.idempotency_repository import IIdempotencyRepository
from ...presentation.ports.event_storage import IEventStorage
from ...presentation.ports.notification_gateway import INotificationGateway

logger = logging.getLogger(__name__)

class ProcessEventUseCase:
    def __init__(
        self,
        idempotency_repo: IIdempotencyRepository,
        event_storage: IEventStorage,
        notification_gateway: INotificationGateway
    ):
        self._idempotency_repo = idempotency_repo
        self._event_storage = event_storage
        self._notification_gateway = notification_gateway

    def execute(self, event: Event) -> None:
        table_key = event.get_table_identifier()
        incoming_hash = event.get_payload_hash()
        
        logger.info(f"Received event '{event.event_type.value}' for table '{table_key}' with state hash '{incoming_hash[:8]}...'")

        last_hash = self._idempotency_repo.get_last_state_hash(key=table_key)
        
        if incoming_hash == last_hash:
            logger.warning(f"Duplicate state detected for table '{table_key}'. Incoming hash matches last stored hash. Skipping.")
            return

        logger.info(f"New state detected for table '{table_key}'. Last hash: '{last_hash[:8] if last_hash else 'None'}'. Processing...")
        
        try:
            stored_info = self._event_storage.save(event)
            self._notification_gateway.notify_event_stored(stored_info)
            self._idempotency_repo.save_state_hash(key=table_key, state_hash=incoming_hash)
            
            logger.info(f"Successfully processed event for '{table_key}'. State updated with new hash '{incoming_hash[:8]}...'.")
        except Exception as e:
            logger.error(f"Failed to process event for '{table_key}': {e}", exc_info=True)
            raise