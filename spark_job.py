from pyspark.sql import SparkSession

import pyspark.sql.functions as sf

spark = SparkSession.builder.master('spark://hadoop-master:7077') \
                    .appName('test') \
                    .getOrCreate()

df = spark.read.format('avro').load('hdfs://hadoop-master:9000/social_media.avro')

df_rename = df.withColumnRenamed('created_at', 'timestamp')

df_group = df_rename.groupBy(['timestamp', 'social_media']) \
             .agg(
                sf.count('original_id').alias('stream_count'),
                sf.countDistinct('from_name').alias('unique_user_count')
              )

df_timestamp = df_group.withColumn('created_at', sf.current_timestamp().cast('string')) \
                       .withColumn('updated_at', sf.current_timestamp().cast('string'))

host = "jdbc:mysql://localhost:3306"

conn_properties = {
    "user": "newuser",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

query = "SELECT * FROM social_media.summary"
push_down_query = "(" + query + ") temp_tab"
df_db = spark.read.jdbc(url=host, table=push_down_query, properties=conn_properties)
df_drop_id = df_db.drop("id")

def map_condition(row):
    if row[2] and row[6]:
        return (row[0], row[1], row[6], row[7], row[4], row[9])
    elif not row[2] and row[6]:
        return (row[0], row[1], row[6], row[7], row[8], row[9])
    else:
        return (row[0], row[1], row[2], row[3], row[4], row[5])

if len(df_drop_id.head(1)) == 0:
  df_order = df_timestamp.orderBy(['social_media', 'timestamp'])

  df_id = df_order.withColumn('id', sf.monotonically_increasing_id()) \
                .select('id', 'timestamp', 'social_media', 'stream_count', 'unique_user_count', 'created_at', 'updated_at')

  df_id.write.mode("overwrite").jdbc(url=host, table="social_media.summary", properties=conn_properties)
else:
  df_join = df_drop_id.join(df_timestamp, ['timestamp', 'social_media'], 'outer')

  rdd_map = df_join.rdd.map(map_condition)

  new_df = rdd_map.toDF(['timestamp', 'social_media', 'stream_count', 'unique_user_count', 'created_at', 'updated_at'])

  df_order = new_df.orderBy(['social_media', 'timestamp'])

  df_id = df_order.withColumn('id', sf.monotonically_increasing_id()) \
                .select('id', 'timestamp', 'social_media', 'stream_count', 'unique_user_count', 'created_at', 'updated_at')

  df_id.write.mode("overwrite").jdbc(url=host, table="social_media.summary", properties=conn_properties)


