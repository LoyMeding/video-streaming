import cv2
import logging
import sys
import time
import json

from time import sleep

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def publish_video(video_file, topic, producer):
    video = cv2.VideoCapture(video_file)

    if not video.isOpened():
        print("Ошибка соединения с видео потоком")
        exit()

#    while video.isOpened():
    for i in range(10):
        success, frame = video.read()
        if not success:
            print("There are problem with reading!")
            break
        ret, buffer = cv2.imencode('.jpg', frame)
        num_bytes = bytes(str(12345), encoding='utf-8')
        data = {'num': i}
        data_bytes = json.dumps(data).encode('utf-8')
        future = producer.send(topic, value=data_bytes)
        result = future.get(timeout=5)  # Подождите подтверждения успешной отправки (необязательно)
        print(f"Сообщение отправлено на partition {result.partition}, offset {result.offset}")
        sleep(1)
    video.release()
    logger.info("Reading finished")


if __name__ == '__main__':
    admin_client = KafkaAdminClient(
        bootstrap_servers="172.18.0.4:9092"
    )
    producer = KafkaProducer(bootstrap_servers='172.18.0.4:9092')

    topic = 'detect-video'

    existing_topics = admin_client.list_topics()

    if topic not in existing_topics:
        topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    video_file = 'http://185.137.146.14/mjpg/video.mjpg'

    publish_video(video_file, topic, producer)
