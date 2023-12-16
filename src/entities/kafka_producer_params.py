from dataclasses import dataclass, field


@dataclass()
class KafkaProducerParams:
    bootstrap_servers: str = field(default="172.18.0.4:9092")
    topic: str = field(default='detect-video')
    video_file: str = field(default='http://185.137.146.14/mjpg/video.mjpg')
    num_partitions: int = field(default=1)
    replication_factor: int = field(default=1)
    validate_only: bool = field(default=False)