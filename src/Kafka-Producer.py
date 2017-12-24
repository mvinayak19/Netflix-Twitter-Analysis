from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

# Twitter API Credentials
consumer_key= "####"
consumer_secret = "####"
access_token = "####"
access_token_secret = "####"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("tweets", data.encode('utf-8')) # sending messages to "tweets" kafka topic.
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient(XXXXXXXXXXXXXXXXXXXXXXXX)
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
# Enter the terms to track
stream.filter(track=["netflix","StrangerThings2","RiverDale","bingewatching"])