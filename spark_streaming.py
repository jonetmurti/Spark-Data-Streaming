import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import mysql.connector
import json
from datetime import datetime

app_name = "PysparkStreaming"
master = "spark://project2-bigdata:7077"

conf = SparkConf().setMaster(master).setAppName(app_name)
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 60)

topic = 'social_media_topic'
bootstrap_server = 'localhost:9092'
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': bootstrap_server})

def get_json_value(data, key):
  keys = key.split(".")
  if len(keys) == 1:
    if keys[0] in data.keys():
      return data[keys[0]]
    else:
      return None
  else:
    currentVal = data
    for key in keys:
      if key in currentVal.keys():
        currentVal = currentVal[key]
      else:
        return None
    return currentVal

def parser(row):
	extracted_data = {}
	key_list_map = {
		"facebook": "id-from.name",
		"instagram": "id-user.username",
		"twitter": "id-user.screen_name",
		"youtube": "id-snippet.channelTitle"
	}
	field_list = ["original_id",  "from_name", "social_media"]
	row_json = json.loads(row)
	social_media_type = row_json["crawler_target"]["specific_resource_type"]
	if social_media_type in key_list_map.keys(): # Ignore other type
		key_list = key_list_map[social_media_type].split("-")
		for i in range(len(key_list)):
			extracted_data[field_list[i]] = str(get_json_value(row_json, key_list[i]))
		extracted_data[field_list[2]] = social_media_type
	return extracted_data

def process_rdd(rdd):
	# TODO: Process data
	result = rdd.map(lambda x : parser(x[1]).take(10))

	# Stream count for each social media
	sm = result.map(lambda x: (x['social_media'], 1))
	stream_count = sm.reduceByKey(lambda a,b: a+b)
	# Result: [('facebook', 10), ('twitter', 20)]

	# Unique user count for each social media
	# uc = result.map(lambda x: ((x["from_name"], x["social_media"]), 1))
	# unique_user = uc.reduceByKey(lambda a,b: 1)) #distict
	# count_user = unique_user.map(lambda x: (x[0][1], 1))
	# count_user_per_sosmed = count_user.reduceByKey(lambda x, y: x + y)
	
	unique_user = result.map(lambda x: (x['social_media'], x['from_name'])).distinct()
	user_sm = unique_user.map(lambda x: (x[0], 1))
	unique_user_count = user_sm.reduceByKey(lambda a,b: a+b)
	# Result: [('facebook', 10), ('twitter', 20)]

	res = stream_count.join(unique_user_count).map(lambda x: {'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'social_media': x[0], 'stream_count': x[1][0], 'unique_user_count': x[1][1]})

	# Add index for each social media
	
	if res:
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
		    ) for row in res.collect()]
		cursor.executemany(query, bulk_data)

		conn.commit()
		cursor.close()
		conn.close()

kafkaStream.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()
