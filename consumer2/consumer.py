import torch
import time
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from io import BytesIO
from PIL import Image
import os
import numpy as np

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

# Создаем SparkContext
sc = SparkContext(appName="KafkaConsumer").getOrCreate()

# Создаем StreamingContext с интервалом 2 секунды
ssc = StreamingContext(sc, 0.3)

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


def process_image(message):
    # Если RDD пустое, то игнорируем
    if message.count() == 0:
        print("-------------------------------------------")
        print('No message')
        print("-------------------------------------------")


    else:
        # message = rdd.map(lambda x: (x[0], x[1]))
        # Начало обработки кадра
        start_time = time.process_time()
        images = message.map(lambda x: x[1]).map(lambda x: Image.open(BytesIO(x)))
        # Завершение обработки кадра
        end_time = time.process_time()
        # Вычисление времени обработки одного кадра
        processing_time = end_time - start_time
        # Вычисление скорости обработки кадров в секунду
        frames_per_second = round(1 / processing_time, 2)
        key = 1
        # Вывод ключа в консоль
        if images is not None:
            print("-------------------------------------------")
            print("Изображение успешно открыто!")
            print("Время обработки одного кадра:", processing_time)
            print("Скорость обработки кадров в секунду:", frames_per_second)
            print("-------------------------------------------")
        else:
            print("-------------------------------------------")
            print("Ключ сообщения: ", key)
            print("Не удалось открыть изображение.")
            print("-------------------------------------------")






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
kafka_stream.foreachRDD(process_image)

# Запуск Spark Streaming
ssc.start()
ssc.awaitTermination()
