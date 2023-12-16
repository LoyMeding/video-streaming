from pyspark.sql import SparkSession
from pyspark.ml.image import ImageSchema

from pyspark.sql.functions import col


def process_image_message(bytes_arr):
    image = ImageSchema.decodeImage(bytes_arr)

    predictions = True

    return predictions


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
    .option("startingOffsets", "latest")  \
    .load()

kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

df = kafka_df.select(
    col("value").cast("int").alias("image")
)

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()
