import logging
import os

import faust
import pandas as pd
from textblob import TextBlob

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
TOPIC = os.getenv("TOPIC")
kafka_broker = f'{HOST}:{PORT}'


def get_polarity(tweet: str) -> float:
	return TextBlob(tweet).sentiment.polarity


def get_sentiment(tweet_polarity: int) -> str:
	if tweet_polarity < 0:
		return 'Negative'
	elif tweet_polarity == 0:
		return 'Neutral'
	else:
		return 'Positive'


class AggRecord(faust.Record):
	defined_sentiment: str
	message_language: str
	message_user: str


app = faust.App('statistics_report', broker=kafka_broker)
app.producer.buffer.max_messages = 10000

raw_topic = app.topic(f'{TOPIC}', key_type=str, value_type=str, value_serializer='raw', partitions=8)
total_agg_topic = app.topic('total_agg_topic', key_type=str, value_type=AggRecord, partitions=8, internal=True)

users = app.Table("users", key_type=str, value_type=int, partitions=8, default=int)
languages = app.Table("languages", key_type=str, value_type=int, partitions=8, default=int)
sentiments = app.Table("sentiments", key_type=str, value_type=int, partitions=8, default=int)


@app.agent(raw_topic)
async def statistic_processing(messages):
	async for message in messages:
		print(f'{TOPIC}, {message}')
		logging.info("%d:%s message=%s", TOPIC, message)
		message_user = message.split(',')[3]
		message_language = message.split(',')[4]
		message_text = message.split(',')[2]
		defined_polarity = get_polarity(message_text)
		defined_sentiment = get_sentiment(defined_polarity)
		await total_agg_topic.send(value=AggRecord(
			defined_sentiment=defined_sentiment,
			message_language=message_language,
			message_user=message_user))


@app.agent(total_agg_topic)
async def aggregate_data(aggregated_data):
	async for data in aggregated_data:
		language = data.message_language
		languages[str(language)] += 1

		sentiment = data.defined_sentiment
		sentiments[str(sentiment)] += 1

		user = data.message_user
		users[str(user)] += 1


def create_df(col_name, top10=False):
	tables = {"Languages": languages, "Sentiments": sentiments, "Users": users}
	language_list, count_list = [], []
	data = {col_name: language_list, "Count": count_list}

	for key, value in tables.get(col_name).items():
		language_list.append(key)
		count_list.append(value)

	df = pd.DataFrame(data)
	return df.sort_values(by=['Count'], ascending=False).head(10) if top10 else df


@app.page('/languages')
async def languages_statistic_view(web, request):
	return web.html(create_df("Languages").to_html(col_space=50, index=False))


@app.page('/sentiments')
async def sentiments_statistic_view(web, request):
	return web.html(create_df("Sentiments").to_html(col_space=50, index=False))


@app.page('/users')
async def sentiments_statistic_view(web, request):
	return web.html(create_df("Users", top10=True).to_html(col_space=50, index=False))


if __name__ == '__main__':
	app.main()
