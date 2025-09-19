import logging
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv

from app.infrastructure.settings import Settings
from app.infrastructure.adapters.dynamodb_repository import DynamoDBIdempotencyRepository
from app.infrastructure.adapters.s3_storage import S3EventStorage
from app.infrastructure.adapters.sqs_gateway import SQSNotificationGateway
from app.core.use_cases.process_event import ProcessEventUseCase
from app.infrastructure.entrypoints.kafka_consumer import EventConsumer

def main():
    load_dotenv()
    settings = Settings()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting application...")

    boto3_session = boto3.Session(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION
    )
    
    clients_kwargs = {'endpoint_url': settings.AWS_ENDPOINT_URL} if settings.AWS_ENDPOINT_URL else {}

    dynamodb = boto3_session.resource('dynamodb', **clients_kwargs)
    s3_client = boto3_session.client('s3', **clients_kwargs)
    sqs_client = boto3_session.client('sqs', **clients_kwargs)
    
    logger.info("Initializing adapters and use cases...")
    idempotency_repo = DynamoDBIdempotencyRepository(
        table_name=settings.IDEMPOTENCY_TABLE_NAME, 
        dynamodb_resource=dynamodb
    )
    event_storage = S3EventStorage(
        bucket_name=settings.S3_BUCKET_NAME, 
        s3_client=s3_client
    )
    notification_gateway = SQSNotificationGateway(
        queue_url=settings.SQS_QUEUE_URL, 
        sqs_client=sqs_client
    )

    process_event_use_case = ProcessEventUseCase(
        idempotency_repo, 
        event_storage, 
        notification_gateway
    )

    kafka_consumer = KafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(','),
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset='earliest',
        value_deserializer=lambda v: v.decode('utf-8')
    )

    consumer = EventConsumer(kafka_consumer, process_event_use_case)
    consumer.run()

if __name__ == "__main__":
    main()