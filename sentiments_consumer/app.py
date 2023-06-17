from kafka import KafkaConsumer
import csv
import logging
import os
from textblob import TextBlob
import time
from multiprocessing.context import Process

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
TOPIC = os.getenv("TOPIC")
SENTIMENTS_CONSUMER_NUMBER = int(os.getenv("SENTIMENTS_CONSUMER_NUMBER"))

def get_polarity(tweet: str) -> float:
	return TextBlob(tweet).sentiment.polarity

def get_sentiment(tweet_polarity: int) -> str:
	if tweet_polarity < 0:
		return 'Negative'
	elif tweet_polarity == 0:
		return 'Neutral'
	else:
		return 'Positive'

def run(consumer_id: int):
	messages_count = 0
	sentiments = set()
	consumer = KafkaConsumer(TOPIC,
							 bootstrap_servers=[f'{HOST}:{PORT}'],
							 consumer_timeout_ms=10000,
							 auto_offset_reset='earliest', #'latest'
							 enable_auto_commit=True,
							 group_id="sentiment")


	with open(f'logs/log_{consumer_id}.csv', 'w') as f:
		writer = csv.writer(f)
		logging.info("Consumer [%s] is reading messages from topic [%s]...", consumer_id, TOPIC)
		for count, message in enumerate(consumer):
			message_value = message.value.decode('utf-8')
			logging.info("%d:%s:%d:%d:%d value=%s", consumer_id, message.topic, message.partition,
						 message.offset, message.timestamp, message_value)
			time.sleep(1)
			message_text = message_value.split(',')[2]
			defined_polarity = get_polarity(message_text)
			defined_sentiment = get_sentiment(defined_polarity)
			sentiments.add(defined_sentiment)
			# logging.info(" is %s", defined_sentiment)
			logging.info("%s message id has %s sentiment.", count, defined_sentiment)
			writer.writerow([int(message.timestamp/1000), int(message.partition),
							 int(message.offset), str(defined_sentiment)] )
			messages_count+=1
		logging.info(f"Consumer [%s] read %d messages", consumer_id, messages_count)
		logging.info(f"Sentiments list is [%s].", sentiments)
		logging.info("Finished.")


if __name__ == '__main__':
	# Create and start multiple consumer processes
	processes = []
	process = Process(target=run, args=(SENTIMENTS_CONSUMER_NUMBER,))
	process.start()
	processes.append(process)

	# Wait for all processes to finish
	for process in processes:
		process.join()
