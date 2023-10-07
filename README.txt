Для того, чтобы собрать и запустить данный проект необходимо выполнить следующие команды:
1. docker-compose up -d
2. docker-compose exec spark bash
3. cd /app
4. pip install --upgrade pip
5. pip install -r requirements.txt
6. python producer/producer.py & spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --jars consumer/kafka-clients-2.2.0.jar --driver-class-path kafka-clients-2.2.0.jar consumer/consumer.py


python producer/producer.py & spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars consumer/kafka-clients-2.2.0.jar --driver-class-path consumer/kafka-clients-2.2.0.jar consumer/consumer.py
