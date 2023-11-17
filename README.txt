cd /app
mkdir myenv
cd myenv
virtualenv venv
source venv/bin/activate
cd ..
pip install -r requirements.txt
apt-get update && apt-get install libgl1
apt-get install libglib2.0-0
python producer/producer.py & spark-submit --master spark://spark:7070 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 consumer/consumer.py