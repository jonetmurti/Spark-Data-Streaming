import json
from hdfs import InsecureClient
from hdfs.ext.avro import AvroReader
import sys
import csv
import os
import argparse
import pandas as pd

parser = argparse.ArgumentParser(
    description='Extract read input data')
parser.add_argument('--avro_input', type=str, default='/social_media.avro')
parser.add_argument('--select_field', type=str, default='')
parser.add_argument('--social_media', type=str, default='')
parser.add_argument('--search_phrase', type=str, default='')
args = parser.parse_args()

avro_input = args.avro_input
input_fields = args.select_field.split('-') if args.select_field else []
social_media = args.social_media
search_phrase = args.search_phrase

client = InsecureClient('http://localhost:9870')

with AvroReader(client, avro_input) as reader:
    actual_fields = [field['name'] for field in reader.writer_schema['fields']]
    selected_fields = [field for field in input_fields if field in actual_fields]
    if len(selected_fields) == 0:
        selected_fields = actual_fields
    filename = os.path.splitext(os.path.basename(avro_input))[0]
    csv_output = filename + '.csv'
    df = pd.DataFrame(reader)

    if social_media:
        df = df[df['social_media'] == social_media]

    if search_phrase:
        df = df[df['content'].str.contains(search_phrase)]

    df = df[selected_fields]
    
    df.to_csv(csv_output, index=False)
