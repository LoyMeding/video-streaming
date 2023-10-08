import torch
import time
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from io import BytesIO
import os
import cv2
import numpy as np

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

# Создаем SparkContext
sc = SparkContext(appName="KafkaConsumer").getOrCreate()

# Создаем StreamingContext с интервалом 2 секунды
ssc = StreamingContext(sc, 0.2)

# Установка параметров Kafka
kafka_params = {
    "bootstrap.servers": "172.25.0.12:9092,172.25.0.13:9092",  # адрес и порт для подключения к Kafka broker'у
    "startingOffsets": "earliest"
}

# Указываем топик, из которого будем читать сообщения
topic = ['detect-video']

# Создание DStream для чтения из Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, topic, kafka_params)

# Загрузить модель YOLOv7
# model = torch.hub.load('WongKinYiu/yolov7', 'custom', path='yolov7-tiny.pt', trust_repo=True)

# Создаём счётчики кадров и времени
frames_count = 0
start_time = time.time()


def process_image(message):
    if message.count() == 0:
        print("-------------------------------------------")
        print('No message')
        print("-------------------------------------------")
    # Если RDD пустое, то игнорируем
    #

    else:
        # message = rdd.map(lambda x: (x[0], x[1]))
        # print('Message:', message[0])
        key = message[0]
        image_bytes = message[1]

        #images = message.map(lambda x: Image.open(BytesIO(x[1])))
        # Декодирование байтового массива в изображение
        image = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), cv2.IMREAD_COLOR)

        # Вывод ключа в консоль
        print("Ключ сообщения: ", key)


"""
    # Декодируем бинарные данные в изображение
    

    # Преобразуем изображение в формат jpg
    jpg_image = images.map(lambda img: img.convert('RGB'))

    # Применить модель к изображению
    results = model(jpg_image)
    # Посчитать FPS
    frames_count += 1
    end_time = time.time()
    total_time = end_time - start_time
    fps = frame_count / total_time

    # Вывести координаты ограничивающего прямоугольника, класс объекта и вероятность
    for i, det in enumerate(results.xyxy[0]):
        print("-------------------------------------------")
        print("FPS: {}".format(fps))
        print("Box {}: {}, {}, {}, {}".format(i, det[0], det[1], det[2], det[3]))
        print("Class: {}".format(int(det[5])))
        print("Confidence: {}".format(det[4]))
        print("-------------------------------------------")
"""

# Обработка стрима
# kafka_stream.foreachRDD(process_image)
kafka_stream.foreachRDD(lambda rdd: rdd.foreach(process_image))
kafka_stream.pprint()
# Запуск Spark Streaming
ssc.start()
ssc.awaitTermination()
