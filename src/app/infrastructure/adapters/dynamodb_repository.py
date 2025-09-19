import logging
from typing import Optional
from botocore.exceptions import ClientError
from ...presentation.ports.idempotency_repository import IIdempotencyRepository

logger = logging.getLogger(__name__)

class DynamoDBIdempotencyRepository(IIdempotencyRepository):
    def __init__(self, table_name: str, dynamodb_resource):
        self._table = dynamodb_resource.Table(table_name)

    def get_last_state_hash(self, key: str) -> Optional[str]:
        try:
            response = self._table.get_item(Key={'tableIdentifier': key})
            if 'Item' in response:
                return response['Item'].get('lastStateHash')
            return None
        except ClientError as e:
            logger.error(f"Error getting state hash from DynamoDB for key {key}: {e}")
            raise

    def save_state_hash(self, key: str, state_hash: str) -> None:
        try:
            self._table.put_item(
                Item={
                    'tableIdentifier': key,
                    'lastStateHash': state_hash
                }
            )
        except ClientError as e:
            logger.error(f"Error saving state hash to DynamoDB for key {key}: {e}")
            raise