import os

class Settings:
    def __init__(self):
        self.KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
        self.KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
        self.KAFKA_CONSUMER_GROUP = os.environ['KAFKA_CONSUMER_GROUP']
        
        self.AWS_REGION = os.environ['AWS_REGION']
        self.AWS_ENDPOINT_URL = os.getenv('AWS_ENDPOINT_URL') # Optional
        self.AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
        self.AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
        
        self.IDEMPOTENCY_TABLE_NAME = os.environ['IDEMPOTENCY_TABLE_NAME']
        self.S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
        self.SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']