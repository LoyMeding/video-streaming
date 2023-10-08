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
    "bootstrap.servers": "localhost:9092",  # адрес и порт для подключения к Kafka broker'у
    "startingOffsets": "earliest"
}

# Указываем топик, из которого будем читать сообщения
topic = ['detect-video']

# Создание DStream для чтения из Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, topic, kafka_params)

# Загрузить модель YOLOv7
# model = torch.hub.load('WongKinYiu/yolov7', path='yolov7-tiny.pt', trust_repo=True)
# Загрузка модели
#model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)


def process_image(message):
    # Если RDD пустое, то игнорируем
    if message.count() == 0:
        print("-------------------------------------------")
        print('No message')
        print("-------------------------------------------")
    else:
        # Начало обработки кадра
        start_time = time.process_time()
        # Открытие изображения из сообщения
        images = message.map(lambda x: x[1]).map(lambda x: Image.open(BytesIO(x)))
        # images = message.map(lambda x: x[1]).map(
        # subprocess.call(['python', 'detect.py', '--source', '-', '--weights', 'yolov7.pt'], stdin=img_str)
        # results = model(images)
        # Завершение обработки кадра
        end_time = time.process_time()
        # Вычисление времени обработки одного кадра
        processing_time = round(end_time - start_time, 2)
        # Вычисление скорости обработки кадров в секунду
        frames_per_second = round(1 / processing_time, 2)
        key = 1
        # Вывод ключа в консоль
        if images is not None:
            print("-------------------------------------------")
            print("Successful image opening!")
            print("Processing time of one frame:", processing_time)
            print("FPS:", frames_per_second)
            print("-------------------------------------------")
        else:
            print("-------------------------------------------")
            print("Image key: ", key)
            print("Failed to open image.")
            print("-------------------------------------------")


"""
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
