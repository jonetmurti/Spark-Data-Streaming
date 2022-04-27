source ./venv/bin/activate
export SPARK_HOME=/home/project2-bigdata/Frameworks/spark-2.4.7-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh
start-slave.sh spark://project2-bigdata:7077
export PYSPARK_PYTHON=/home/project2-bigdata/Projects/project-2-tbd/venv/bin/python3
echo $SPARK_HOME
echo $PYSPARK_PYTHON
spark-submit --archives venv.tar.gz --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 --master spark://project2-bigdata:7077 spark_streaming.py
