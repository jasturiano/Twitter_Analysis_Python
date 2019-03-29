# Execute as: ./spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 spark_stream.py /
#             localhost:2181 twitter-stream  (Use zookeeper port, not Kafka one)

import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

if __name__ == "__main__":

    
    sc = SparkContext(appName="SparkStreaming")
    ssc = StreamingContext(sc, 10) # 10 second window
    sc.setLogLevel("ERROR")

    broker, topic = sys.argv[1:]
    kafkaStream = KafkaUtils.createStream(ssc, broker, 'spark-streaming',{topic:1})
    
    #Extract tweets
    lines = kafkaStream.map(lambda v: v[1])
    createdAt_dstream = lines.map(lambda tweet: tweet['created_at'])
    author_dstream = lines.map(lambda tweet: tweet['user']['screen_name'])
    location_dstream = lines.map(lambda tweet: tweet['user']['location'])

    count_this_batch = kafkaStream.count().map(lambda x:('Tweets in this batch: %s' % x)) #Count by batch
    
    count_this_batch.pprint()
    createdAt_dstream.pprint()
    author_dstream.pprint()
    location_dstream.pprint()
    
    ssc.start()
    ssc.awaitTermination()
