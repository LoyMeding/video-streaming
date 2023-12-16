from dataclasses import dataclass, field


@dataclass()
class TrainingParams:
    pretrained_models_weight: str = field(default='yolov8m.pt')
    dataset_yaml_config: str = field(default='/content/datasets/dataset/data.yaml')
    image_size: int = field(default=640)
    epochs: int = field(default=100)
    batch_size: int = field(default=8)
    num_frozen_layers: int = field(default=10)
    result_dir: str = field(default='detection_yolo8m_100epochs')
