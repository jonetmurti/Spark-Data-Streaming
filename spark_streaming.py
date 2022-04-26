import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import mysql.connector
import json

app_name = "PysparkStreaming"
master = "spark://project2-bigdata:7077"

conf = SparkConf().setMaster(master).setAppName(app_name)
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 30)

topic = 'test-topic'
bootstrap_server = 'localhost:9092'
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': bootstrap_server})

def process_rdd(rdd):
	result = rdd.map(lambda x : json.loads(x[1]))

	if result:
		conn = mysql.connector.connect(user='root', database='social_media', password='root', host="localhost", port=3306)
		cursor = conn.cursor(prepared=True)
		query = ('INSERT INTO summary_test ' + 
		        '   (timestamp, social_media, stream_count, unique_user_count) ' + 
		        'VALUES (%s, %s, %s, %s) ' + 
		        'ON DUPLICATE KEY UPDATE ' +
		        '   stream_count = %s, unique_user_count = %s')

		bulk_data = [(
		        row['timestamp'], row['social_media'], 
		        row['stream_count'], row['unique_user_count'],
		        row['stream_count'], row['unique_user_count']
		    ) for row in result.collect()]
		cursor.executemany(query, bulk_data)

		conn.commit()
		cursor.close()
		conn.close()

kafkaStream.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()
