import json
from hdfs import InsecureClient
from hdfs.ext.avro import AvroWriter
import sys
import os

if len(sys.argv) < 3:
    sys.exit('usage: python3 hdfs_write.py [avro_schema] [json_input]')

python_file, avro_schema, json_input = sys.argv

if not os.path.isfile(avro_schema):
    sys.exit('avro schema does not exists')

if not os.path.isfile(json_input):
    sys.exit('json input does not exists')

schema = None
records = None

try:
    schema = json.load(open(avro_schema))
    records = json.load(open(json_input))
except:
    sys.exit('invalid json input or schema format')

filename = os.path.splitext(os.path.basename(json_input))[0]
avro_output = '/' + filename + '.avro'

# TODO: parse data

client = InsecureClient('http://hadoop-master:9870')

client.delete(avro_output)

with AvroWriter(client, avro_output, schema) as writer:
    for record in records:
        writer.write(record)
