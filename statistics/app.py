import logging
import os
import time
import faust
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

raw_topic = app.topic(f'{TOPIC}', key_type=str, value_type=str, value_serializer='raw', partitions=8)

sentiments_agg_topic = app.topic('sentiments_agg_topic', key_type=str, value_type=str,
								 partitions=8, internal=True)
languages_agg_topic = app.topic('languages_agg_topic', key_type=str, value_type=str,
								partitions=8, internal=True)
users_agg_topic = app.topic('users_agg_topic', key_type=str, value_type=str,
							partitions=8, internal=True)
total_agg_topic = app.topic('total_agg_topic', key_type=str, value_type=AggRecord,
								partitions=8, internal=True)

users = app.Table("users", key_type=str, value_type=int, partitions=8, default=int)
languages = app.Table("languages", key_type=str, value_type=int, partitions=8, default=int)
sentiments = app.Table("sentiments", key_type=str, value_type=int, partitions=8, default=int)


@app.agent(raw_topic)
async def statistic_processing(messages):
	async for message in messages:
		print(f'{TOPIC}, {message}')
		logging.info("%d:%s message=%s", TOPIC, message)
		time.sleep(1)
		message_user = message.split(',')[3]
		print(message_user)  # remove after debugging
		message_language = message.split(',')[4]
		print(message_language)  # remove after debugging
		message_text = message.split(',')[2]
		defined_polarity = get_polarity(message_text)
		defined_sentiment = get_sentiment(defined_polarity)
		print(defined_sentiment)  # remove after debugging
		sentiments_agg_topic.send(value=defined_sentiment)
		languages_agg_topic.send(value=message_language)
		# total_agg_topic.send(value=defined_sentiment)
		total_agg_topic.send(value=AggRecord(
				defined_sentiment=defined_sentiment,
				message_language=message_language,
				message_user=message_user))
		await sentiments_agg_topic.send(value=defined_sentiment)
		await languages_agg_topic.send(value=message_language)
		await total_agg_topic.send(value=AggRecord(
			defined_sentiment=defined_sentiment,
			message_language=message_language,
			message_user=message_user))

		# await agg_topic.send(value=AggRecord(
		# 	defined_sentiment=defined_sentiment,
		# 	message_language=message_language,
		# 	message_user=message_user)
		# )
		# await agg_topic.send(value={
		# 	'defined_sentiment': defined_sentiment,
		# 	'message_language': message_language,
		# 	'message_user': message_user})
		# await agg_topic.send(value=defined_sentiment)



# @app.agent(sentiments_agg_topic)
# async def aggregate_data(sentiments_data):
# 	async for data in sentiments_data:
# 		print(data)  # remove after debugging
# 		sentiments[str(data)] += 1
# 		print(f'{str(data)} sentiment detected {sentiments[str(data)]} times.')
#
#
# @app.agent(languages_agg_topic)
# async def aggregate_data(languages_data):
# 	async for language in languages_data:
# 		print(language)  # remove after debugging
# 		languages[str(language)] += 1
# 		print(f'{str(language)} language detected {languages[str(language)]} times.')


@app.agent(total_agg_topic)
async def aggregate_data(aggregated_data):
	async for data in aggregated_data:
		print('AGGREGATED: ', data)  # remove after debugging
		language = data.message_language
		languages[str(language)] += 1
		print(f'{str(language)} language detected {languages[str(language)]} times.')
		sentiment = data.defined_sentiment
		sentiments[str(sentiment)] += 1
		print(f'{str(sentiment)} sentiment detected {sentiments[str(sentiment)]} times.')


# to do users
# @app.agent(users_agg_topic)
# async def aggregate_data(users_data):
# 	async for user in users_data:


if __name__ == '__main__':
	app.main()
