from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time
import sys
import cv2


def publish_video(video_file, topic, producer):
    # Открытие файла
    video = cv2.VideoCapture(video_file)

    # Проверить, успешно ли установлено соединение с видео потоком
    if not video.isOpened():
        print("Ошибка соединения с видео потоком")
        exit()

    print('Передаём кадр...')

    #    while (video.isOpened()):
    for i in range(20):
        success, frame = video.read()

        # Проверка успешности чтения кадра
        if not success:
            print("Ошибка чтения кадра!")
            break

        # Перевод изображения в jpg
        ret, buffer = cv2.imencode('.jpg', frame)
        # Получение текущего времени в битовом представлении
        time_bytes = bytes(str(time.time()), encoding='utf-8')
        # Перевод изображения в битовое представление и отправка в Kafka
        producer.send(topic, key=time_bytes, value=buffer.tobytes())
        print("ret, buffer", ret, buffer)
        time.sleep(0.2)
    video.release()
    print('Отправка завершена')


if __name__ == '__main__':
    admin_client = KafkaAdminClient(
        bootstrap_servers="172.25.0.12:9092"
    )
    producer = KafkaProducer(bootstrap_servers='172.25.0.13:9092')

    topic = 'detect-video'

    existing_topics = admin_client.list_topics()

    if topic not in existing_topics:
        topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    video_file = 'http://185.137.146.14/mjpg/video.mjpg'
    publish_video(video_file, topic, producer)
