import json
import logging
from dataclasses import asdict
from botocore.exceptions import ClientError
from ...core.entities.event import Event
from ...presentation.ports.event_storage import IEventStorage, StoredEventInfo

logger = logging.getLogger(__name__)

class S3EventStorage(IEventStorage):
    def __init__(self, bucket_name: str, s3_client):
        self._bucket_name = bucket_name
        self._s3_client = s3_client

    def save(self, event: Event) -> StoredEventInfo:
        file_path = f"{event.instance_name}/{event.db_name}/{event.table_name}/{event.event_id}.json"
        event_dict = asdict(event)
        event_dict['event_type'] = event.event_type.value # Serialize Enum to string
        
        try:
            self._s3_client.put_object(
                Bucket=self._bucket_name,
                Key=file_path,
                Body=json.dumps(event_dict, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Event {event.event_id} saved to s3://{self._bucket_name}/{file_path}")
            return StoredEventInfo(bucket=self._bucket_name, path=file_path)
        except ClientError as e:
            logger.error(f"Failed to save event {event.event_id} to S3: {e}")
            raise