from typing import Protocol, NamedTuple
from ...core.entities.event import Event

class StoredEventInfo(NamedTuple):
    bucket: str
    path: str

class IEventStorage(Protocol):
    def save(self, event: Event) -> StoredEventInfo:
        ...