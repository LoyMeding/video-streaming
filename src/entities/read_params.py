import yaml
import logging
import sys

from dataclasses import dataclass, field
from marshmallow_dataclass import class_schema

from src.entities.train_params import TrainingParams
from src.entities.kafka_producer_params import KafkaProducerParams

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


PIPELINE_CONFIG_PATH = "../configs/train_config.yaml"
SPARK_KAFKA_CONFIG_PATH = "../configs/spark_kafka_config.yaml"


@dataclass()
class TrainingPipelineParams:
    train_params: TrainingParams


@dataclass()
class SparkKafkaParams:
    kafka_producer_params: KafkaProducerParams


TrainingPipelineParamsSchema = class_schema(TrainingPipelineParams)
SparkKafkaParamsSchema = class_schema(SparkKafkaParams)


def read_training_pipeline_params(path: str):
    with open(path, "r") as input_stream:
        config_dict = yaml.safe_load(input_stream)
        schema = TrainingPipelineParamsSchema().load(config_dict)
        logger.info("Check schema: %s", schema)
        return schema


def read_spark_kafka_params(path: str):
    with open(path, "r") as input_stream:
        config_dict = yaml.safe_load(input_stream)
        schema = SparkKafkaParamsSchema().load(config_dict)
        logger.info("Check schema: %s", schema)
        return schema


if __name__ == "__main__":
    read_training_pipeline_params(PIPELINE_CONFIG_PATH)
    read_spark_kafka_params(SPARK_KAFKA_CONFIG_PATH)