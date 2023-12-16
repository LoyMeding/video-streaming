from ultralytics import YOLO
from src.entities.train_params import TrainingParams


def init_pretrained_yolo(models_weight: str) -> YOLO:
    pretrained_detect_model = YOLO(models_weight)
    return pretrained_detect_model


def train_yolo(model: YOLO, training_params: TrainingParams):
    model.train(
        data=training_params.dataset_yaml_config,
        imgsz=training_params.image_size,
        epochs=training_params.epochs,
        batch=training_params.batch_size,
        freeze = training_params.num_frozen_layers,
        name=training_params.result_dir)
