import json
from hdfs import InsecureClient
from hdfs.ext.avro import AvroWriter
import sys
import os

if len(sys.argv) < 3:
    sys.exit('usage: python3 hdfs_write.py [avro_schema] [json_input]')

avro_schema = sys.argv[1]
json_inputs = sys.argv[2:]

if not os.path.isfile(avro_schema):
    sys.exit('avro schema does not exists')

schema = None
records = []

try:
    schema = json.load(open(avro_schema, encoding='utf8'))
    for json_input in json_inputs:
        if os.path.isfile(json_input):
            records += json.load(open(json_input, encoding='utf8'))
except:
    sys.exit('invalid json input or schema format')

avro_output = '/social_media.avro'

client = InsecureClient('http://hadoop-master:9870')

client.delete(avro_output)

with AvroWriter(client, avro_output, schema) as writer:
    for record in records:
        writer.write(record)
