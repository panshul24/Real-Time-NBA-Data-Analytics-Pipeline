from kafka import KafkaProducer
import json
import time
from nba_api.live.nba.endpoints import scoreboard
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
kafka_broker = 'localhost:9092'  # Adjust as necessary
kafka_topic = 'nba-scoreboard-1'  # Ensure this topic exists in your Kafka setup

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_broker],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_nba_scores():
    try:
        # Fetch today's NBA scoreboard
        games = scoreboard.ScoreBoard()
        data = games.get_dict()  # Fetch the games data as a dictionary
        return data
    except Exception as e:
        logger.error(f"Failed to fetch NBA scores: {e}")
        return None

def send_data_to_kafka(data):
    try:
        if data:
            producer.send(kafka_topic, data)
            producer.flush()
            logger.info("Data sent to Kafka successfully.")
        else:
            logger.info("No data to send.")
    except Exception as e:
        logger.error(f"Failed to send data to Kafka: {e}")

def fetch_and_send():
    data = fetch_nba_scores()  # Fetch data from NBA API
    send_data_to_kafka(data)   # Send the data to Kafka

def main():
    scheduler = BlockingScheduler()
    scheduler.add_job(fetch_and_send, 'interval', minutes=1)  # Adjust the interval as needed
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == "__main__":
    main()
