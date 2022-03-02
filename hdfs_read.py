import json
from hdfs import InsecureClient
from hdfs.ext.avro import AvroReader
import sys
import csv

if len(sys.argv) < 3:
    sys.exit('usage: python3 hdfs_read.py [avro_input] [csv_output]')

python_file, avro_input, csv_output = sys.argv

client = InsecureClient('http://hadoop-master:9870')

with AvroReader(client, avro_input) as reader:
    fields = [field['name'] for field in reader.writer_schema['fields']]
    with open(csv_output, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for record in reader:
            writer.writerow(record)
