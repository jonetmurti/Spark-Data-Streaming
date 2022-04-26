from pyspark.sql import SparkSession
import mysql.connector
import pyspark.sql.functions as sf

spark = SparkSession.builder.master('spark://project2-bigdata:7077') \
                    .appName('test') \
                    .getOrCreate()

df = spark.read.format('avro').load('hdfs://localhost:9000/social_media.avro')

df_rename = df.withColumnRenamed('created_at', 'timestamp')

df_group = df_rename.groupBy(['timestamp', 'social_media']) \
             .agg(
                sf.count('original_id').alias('stream_count'),
                sf.countDistinct('from_name').alias('unique_user_count')
              )

# df_timestamp = df_group.withColumn('created_at', sf.current_timestamp().cast('string')) \
#                        .withColumn('updated_at', sf.current_timestamp().cast('string'))

print('HELOO_1')
conn = mysql.connector.connect(user='root', database='social_media', password='root', host="localhost", port=3306)
cursor = conn.cursor(prepared=True)
query = ('INSERT INTO summary_test ' + 
        '   (timestamp, social_media, stream_count, unique_user_count) ' + 
        'VALUES (%s, %s, %s, %s) ' + 
        'ON DUPLICATE KEY UPDATE ' +
        '   stream_count = %s, unique_user_count = %s')
print('HELOO_2')

bulk_data = [(
        row.timestamp, row.social_media, 
        row.stream_count, row.unique_user_count,
        row.stream_count, row.unique_user_count
    ) for row in df_group.rdd.collect()]
# for row in df_group.rdd.collect():
#     val = (
#         row.timestamp, row.social_media, 
#         row.stream_count, row.unique_user_count,
#         row.stream_count, row.unique_user_count
#     )
cursor.executemany(query, bulk_data)

print('HELOO_3')
conn.commit()
cursor.close()
conn.close()
print('HELOO_4')
