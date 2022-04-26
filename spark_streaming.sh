source ./venv/bin/activate
export PYSPARK_PYTHON=./venv/bin/python
spark-submit --archives venv.tar.gz --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 --master spark://project2-bigdata:7077 /home/project2-bigdata/Projects/project-2-tbd/spark_streamig.py
