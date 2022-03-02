import json
from hdfs import InsecureClient
from hdfs.ext.avro import AvroWriter
import sys

if len(sys.argv) < 4:
    sys.exit('usage: python3 hdfs_write.py [avro_schema] [json_input] [avro_output]')

python_file, avro_schema, json_input, avro_output = sys.argv

schema = json.load(open(avro_schema))

records = json.load(open(json_input))

# TODO: organize records according to schema

client = InsecureClient('http://hadoop-master:9870')

with AvroWriter(client, avro_output, schema) as writer:
    for record in records:
        writer.write(record)

