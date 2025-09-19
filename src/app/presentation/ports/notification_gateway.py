from typing import Protocol
from .event_storage import StoredEventInfo

class INotificationGateway(Protocol):
    def notify_event_stored(self, stored_info: StoredEventInfo) -> None:
        ...