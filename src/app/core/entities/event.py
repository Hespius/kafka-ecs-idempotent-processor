import hashlib
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List
from enum import Enum

class EventType(Enum):
    UPSERT = "UPSERT"
    DROP = "DROP"

@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    is_nullable: bool

@dataclass
class Event:
    event_id: str
    event_type: EventType
    instance_name: str
    db_name: str
    table_name: str
    columns: List[ColumnMetadata] = field(default_factory=list)

    def get_table_identifier(self) -> str:
        return f"{self.instance_name}:{self.db_name}:{self.table_name}"

    def get_payload_hash(self) -> str:
        if self.event_type == EventType.DROP:
            return "DROPPED"

        payload_data = [asdict(col) for col in self.columns]
        sorted_payload_data = sorted(payload_data, key=lambda x: x['name'])
        canonical_payload = json.dumps(sorted_payload_data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(canonical_payload.encode('utf-8')).hexdigest()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        try:
            event_type = EventType(data['event_type'])
            columns_data = data.get('columns', [])
            columns = [ColumnMetadata(**col) for col in columns_data]
            return cls(
                event_id=data['event_id'],
                event_type=event_type,
                instance_name=data['instance_name'],
                db_name=data['db_name'],
                table_name=data['table_name'],
                columns=columns
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid event data: {e}") from e