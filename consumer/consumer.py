import logging

from pyspark.sql import SparkSession
from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import *
from pyspark.sql.functions import col


logging.basicConfig(level=logging.INFO)


def process_image(bytes_arr):
    # Декодируйте изображение из байтового массива
    image = ImageSchema.decodeImage(bytes_arr)

    # Примените модель YOLO к изображению и получите предсказания
    predictions = True

    return predictions


#process_image_udf = udf(process_image, ArrayType(StringType()))

spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "detect-video") \
    .load()
'''
output_df = kafka_df \
    .selectExpr("CAST(value AS BINARY)") \
    .withColumn("predictions", lit(True))

output_query = output_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
'''

# Преобразование данных Kafka в строку
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Преобразование строковых данных в DataFrame с числами
df = kafka_df.select(
    col("value").cast("int").alias("number")
)
# Вывод DataFrame в терминал
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Ожидание выполнения операции
query.awaitTermination()
