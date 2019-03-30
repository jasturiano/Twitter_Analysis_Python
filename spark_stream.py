# Execute as: ./spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 spark_stream.py /
#             localhost:2181 topic_name  (Use zookeeper port, not Kafka one)

import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json

if __name__ == "__main__":

    #Create Context
    sc = SparkContext(appName="SparkStreaming")
    ssc = StreamingContext(sc, 10) # 10 second window
    sc.setLogLevel("ERROR")

    broker, topic = sys.argv[1:]
    kafkaStream = KafkaUtils.createStream(ssc, broker, 'spark-streaming',{topic:1})

    #Pull Kafka topic into json and get desired info 
    raw_tweets=kafkaStream.map(lambda x: json.loads(x[1]))
    createdAt_dstream = raw_tweets.map(lambda tweet: (tweet['created_at'],1)).\
    reduceByKey(lambda x,y: x + y)
    
    author_dstream = raw_tweets.map(lambda tweet: tweet['user']['screen_name'],1)
    text_dstream = raw_tweets.map(lambda tweet: tweet['text'],1)
    
    location_dstream = raw_tweets.map(lambda tweet: tweet['user']['location'],1).\
    reduceByKey(lambda x,y: x + y)

    count_this_batch = kafkaStream.count().map(lambda x:('Tweets in this batch: %s' % x)) #Count by batch
    
    count_this_batch.pprint()
    #raw_tweets.pprint()
    createdAt_dstream.pprint()
    author_dstream.pprint()
    text_dstream.pprint()
    #ocation_dstream.pprint()
    
    
    ssc.start()
    ssc.awaitTermination()
