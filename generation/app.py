import logging
import os
import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("HOST", 'localhost')
PORT = os.getenv("PORT", '8097')
TOPIC = os.getenv("TOPIC", '8part-topic')
DATA_RECORDS = int(os.getenv("DATA_RECORDS", '30'))


def on_send_success(record_metadata, producer_id):
    logging.info('Producer [%s] sent message to topic:partition:offset - %s:%d:%d', producer_id,
                 record_metadata.topic, record_metadata.partition, record_metadata.offset)


def produce_data(producer_id, start_record, end_record):
    producer = KafkaProducer(bootstrap_servers=[f'{HOST}:{PORT}'])
    data = pd.read_csv('chatgpt1.csv', encoding='utf-8',
                       usecols=['Datetime', 'Tweet Id', 'Text', 'Username', 'Language'])
    data.iloc[:, 2] = data.iloc[:, 2].str.replace(',', '')
    data = data[start_record:end_record]
    print(data.head())

    for index, row in data.iterrows():
        message = ','.join([str(elem) for elem in list(row)])
        producer.send(TOPIC, str(message).encode('utf-8')).add_callback(on_send_success, producer_id=producer_id)

    producer.flush()


if __name__ == '__main__':
    produce_data(1, 0, DATA_RECORDS)
