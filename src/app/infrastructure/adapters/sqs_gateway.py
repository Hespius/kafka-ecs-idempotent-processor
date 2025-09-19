import json
import logging
from botocore.exceptions import ClientError
from ...presentation.ports.notification_gateway import INotificationGateway
from ...presentation.ports.event_storage import StoredEventInfo

logger = logging.getLogger(__name__)

class SQSNotificationGateway(INotificationGateway):
    def __init__(self, queue_url: str, sqs_client):
        self._queue_url = queue_url
        self._sqs_client = sqs_client

    def notify_event_stored(self, stored_info: StoredEventInfo) -> None:
        message_body = {
            'bucket': stored_info.bucket,
            'path': stored_info.path
        }
        try:
            self._sqs_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(message_body)
            )
            logger.info(f"Notification for S3 object {stored_info.path} sent to SQS.")
        except ClientError as e:
            logger.error(f"Failed to send SQS notification for {stored_info.path}: {e}")
            raise