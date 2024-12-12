import json
import random
import threading
import time
import uuid
from typing import Optional

from confluent_kafka import Producer, KafkaError, Message
from confluent_kafka.admin import AdminClient, NewTopic
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

# Configuration
KAFKA_BROKERS = "localhost:19092,localhost:29092,localhost:39092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = 'financial_transactions'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Producer configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 1000000,  # Increased for higher throughput
    'queue.buffering.max.kbytes': 1048576,  # 1GB buffer
    'batch.num.messages': 10000,  # Larger batch size
    'linger.ms': 100,  # Slightly higher linger for better batching
    'acks': 1,
    'compression.type': 'lz4'
}

producer = Producer(producer_conf)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def create_topic(topic_name: str) -> None:
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                future.result()
                logger.info(f"Topic '{topic_name}' created successfully!")
        else:
            logger.info(f"Topic '{topic_name}' already exists!")
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        raise


def generate_transactions():
    return {
        'transactionId': str(uuid.uuid4()),
        'userId': f"user_{random.randint(1, 100)}",
        'amount': round(random.uniform(50000, 150000), 2),
        'transactionTime': int(time.time()),
        'merchantId': random.choice(['merchant_1', 'merchant_2', 'merchant_3']),
        'transactionType': random.choice(['purchase', 'refund']),
        'location': f'location_{random.randint(1, 50)}',
        'paymentMethod': random.choice(['credit_card', 'paypal', 'bank_transfer']),
        'isInternational': random.choice(['True', 'False']),
        'currency': random.choice(['USD', 'EUR', 'GBP'])
    }


def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    if err:
        logger.error(f'Delivery failed for record {msg.key()}: {err}')
    else:
        logger.info(f'Record {msg.key()} successfully produced')


def produce_transactions(thread_id):
    count = 0
    while True:
        transaction = generate_transactions()
        producer.produce(
            topic=TOPIC_NAME,
            key=transaction['userId'],
            value=json.dumps(transaction).encode('utf-8'),
            on_delivery=delivery_report
        )
        count += 1

        # Reduced logging overhead: log only every 10,000 messages
        if count % 10000 == 0:
            logger.info(f'Thread {thread_id} produced {count} messages so far')

        # Poll periodically to handle delivery reports without blocking.
        if count % 50000 == 0:
            producer.poll(0)


def produce_data_parallel(num_thread):
    threads = []
    try:
        for i in range(num_thread):
            thread = threading.Thread(target=produce_transactions, args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    except Exception as e:
        logger.error(f'Error message: {e}')


if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    produce_data_parallel(8)
