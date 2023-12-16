import cv2
import logging
import sys

from time import sleep

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from src.entities.kafka_producer_params import KafkaProducerParams
from src.entities.read_params import read_spark_kafka_params

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def init_kafka_producer(params: KafkaProducerParams) -> KafkaProducer:
    producer = KafkaProducer(bootstrap_servers=params.bootstrap_servers)
    return producer


def init_admin_client(params: KafkaProducerParams):
    admin_client = KafkaAdminClient(
        bootstrap_servers=params.bootstrap_servers
    )
    return admin_client


def create_topic(producer_params: KafkaProducerParams, admin_client: KafkaAdminClient):
    existing_topics = admin_client.list_topics()

    if producer_params.topic not in existing_topics:
        topic_list = [NewTopic(name=producer_params.topic, num_partitions=producer_params.num_partitions,
                               replication_factor=producer_params.replication_factor)]
        admin_client.create_topics(new_topics=topic_list, validate_only=producer_params.validate_only)


def send_video_message(message_params: KafkaProducerParams, producer: KafkaProducer):
    video = cv2.VideoCapture(message_params.video_file)

    if not video.isOpened():
        print("Connection error with video stream")
        exit()

#    while video.isOpened():
    for i in range(10):
        is_read, video_frame = video.read()
        if not is_read:
            print("There are problem with reading!")
            break

        ret, buffer = cv2.imencode('.jpg', video_frame)
        data_bytes = buffer.tobytes()

        future = producer.send(message_params.topic, value=data_bytes)
        result = future.get(timeout=5)

        print(f"Message sended: partition {result.partition}, offset {result.offset}")
        sleep(0.03)

    video.release()
    logger.info("Reading finished")


if __name__ == '__main__':
    kafka_producer_params = read_spark_kafka_params("../configs/spark_kafka_config.yaml")

    kafka_producer = init_kafka_producer(kafka_producer_params)
    kafka_admin_client = init_admin_client(kafka_producer_params)

    create_topic(kafka_producer_params, kafka_admin_client)
    send_video_message(kafka_producer_params, kafka_producer)
