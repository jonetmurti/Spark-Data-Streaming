import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf


spark = SparkSession.builder.master('spark://hadoop-master:7077') \
                    .appName('social_media_job') \
                    .getOrCreate()

df = spark.read.format('avro').load('hdfs://hadoop-master:9000/social_media.avro')
df_rename = df.withColumnRenamed('created_at', 'timestamp')
df_group = df_rename.groupBy(['timestamp', 'social_media']) \
             .agg(
                sf.count('from_name').alias('stream_count'),
                sf.countDistinct('from_name').alias('unique_user_count')
              )
df_order = df_group.orderBy(['social_media', 'timestamp'])

df_timestamp = df_order.withColumn('created_at', sf.current_timestamp()) \
                       .withColumn('updated_at', sf.lit(None))
df_id = df_timestamp.withColumn('id', sf.monotonically_increasing_id()) \
                    .select('id', 'timestamp', 'social_media', 'stream_count', 'unique_user_count', 'created_at', 'updated_at')

df_id.write.option('header', True).csv('file:///home/hadoopuser/project-folder/project-2/spark-output')
