from kafka import KafkaConsumer
import csv
import logging
import os
import time
from multiprocessing.context import Process

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
TOPIC = os.getenv("TOPIC")
USERS_CONSUMER_NUMBER = int(os.getenv("USERS_CONSUMER_NUMBER"))


def run(consumer_id: int):
	messages_count = 0
	users = set()
	consumer = KafkaConsumer(TOPIC,
							 bootstrap_servers=[f'{HOST}:{PORT}'],
							 consumer_timeout_ms=10000,
							 auto_offset_reset='earliest', #'latest'
							 enable_auto_commit=True,
							 group_id="user")


	with open(f'logs/log_{consumer_id}.csv', 'w') as f:
		writer = csv.writer(f)
		logging.info("Consumer [%s] is reading messages from topic [%s]...", consumer_id, TOPIC)
		for count, message in enumerate(consumer):
			message_value = message.value.decode('utf-8')
			logging.info("%d:%s:%d:%d:%d value=%s", consumer_id, message.topic, message.partition,
						 message.offset, message.timestamp, message_value)
			time.sleep(1)
			user = message_value.split(',')[3]
			users.add(user)
			logging.info("User is %s", user)
			logging.info("%s message id was posted by %s user.", count, user)
			writer.writerow([int(message.timestamp/1000), int(message.partition),
							 int(message.offset), str(user)] )
			messages_count+=1
		logging.info(f"Consumer [%s] read %d messages", consumer_id, messages_count)
		logging.info(f"Users are [%s].", users)
		logging.info("Finished.")


if __name__ == '__main__':
	# Create and start multiple consumer processes
	processes = []
	process = Process(target=run, args=(USERS_CONSUMER_NUMBER,))
	process.start()
	processes.append(process)

	# Wait for all processes to finish
	for process in processes:
		process.join()
