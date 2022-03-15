import json
from hdfs import InsecureClient
from hdfs.ext.avro import AvroReader
import sys
import csv
import os

if len(sys.argv) < 2:
    sys.exit('usage: python3 hdfs_read.py [avro_input] [field_1] [field_2] ...')

avro_input = sys.argv[1]
input_fields = sys.argv[2:] if len(sys.argv) > 2 else []

client = InsecureClient('http://hadoop-master:9870')

with AvroReader(client, avro_input) as reader:
    actual_fields = [field['name'] for field in reader.writer_schema['fields']]
    selected_fields = [field for field in input_fields if field in actual_fields]
    if len(selected_fields) == 0:
        selected_fields = actual_fields
    filename = os.path.splitext(os.path.basename(avro_input))[0]
    csv_output = filename + '.csv'
    with open(csv_output, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=selected_fields)
        writer.writeheader()
        for record in reader:
            writer.writerow({key: record[key] for key in selected_fields})
