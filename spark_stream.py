import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell"
os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Create Spark context
def createContext():
    sc = SparkContext(appName="SparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5) # Create streaming context along with the batch duration in seconds

    #Connect to Kafka as a consumer
    #https://spark.apache.org/docs/latest/streaming-kafka-0-8-integration.html
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:9092', 'spark-streaming', {'twitter-stream':1})

    # Extract tweets

    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    count_this_batch = kafkaStream.count().map(lambda x:('Tweets in this batch: %s' % x)) #Count by batch
    count_windowed = kafkaStream.countByWindow(60,5).map(lambda x:('Tweets total (One minute rolling count): %s' % x)) #Count by Window

    #Get required fields for the tweets

    createdAt_dstream = parsed.map(lambda tweet: tweet['created_at'])
    author_dstream = parsed.map(lambda tweet: tweet['user']['screen_name'])
    location_dstream = parsed.map(lambda tweet: tweet['user']['location'])

    count_this_batch.union(count_windowed).pprint()

    return ssc

ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v01',lambda: createContext())
ssc.start()
ssc.awaitTermination()

