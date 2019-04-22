# Twitter Data Analysis Python/Spark

This project is a demo to analyze data using tweets as Data Source and Python to create the streaming via tweepy

The goal is create a Kafka producer to get all the data and then processing with Spark Streaming.

1. listener_tweet.py pulls tweets into a Kafka topic given a specific keywork
2. spark_stream uses pyspark to create a spark streaming to get the kafka topic and cleanising some data.

In Progress...

