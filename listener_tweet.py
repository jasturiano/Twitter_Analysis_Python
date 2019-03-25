import config_twitter
from kafka import SimpleClient, SimpleProducer
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

#Authentication
consumer_key = config_twitter.consumer_key
consumer_secret = config_twitter.consumer_secret
access_token = config_twitter.access_token
access_secret = config_twitter.access_secret

# Setting up Topic Name
topic = 'twitter-stream'

# Setting up Kafka producer
kafka = SimpleClient(hosts=['localhost:9092'])
producer = SimpleProducer(kafka)

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages(topic, data.encode('utf-8'))
        print(data)
        return True
    def on_error(self, status):
        print (status)


l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

# Keep this process always going
while True:
    try:
        stream = Stream(auth, l)
        stream.filter(languages=["en"],track=['#Hashtag'])
    except:
        pass
