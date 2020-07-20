"""
Insert CSV data into Elasticsearch
https://github.com/depklyon/es-insert-tool

MIT License
Copyright (c) 2020 Patrick Palma

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import csv
import json
import yaml
from os import listdir
from os.path import isfile, join
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Map table of es mapping to Python types, used to convert values.
types = {"byte": int, "double": float, "long": int, "integer": int, "date": int}
doc_id = 0

# Get the current directory
root = str(Path(__file__).parent.absolute())

# Load config file
with open(root + '/config.yml', 'r') as config_stream:
    cfg = yaml.load(config_stream, Loader=yaml.CLoader)

es = Elasticsearch([cfg['connection']])

# Load fields mapping json
with open(cfg['index']['mapping_file']) as json_file:
    mapping = json.load(json_file)

# Check if the index exists and delete it if needed
if cfg['delete_index'] and es.indices.exists(cfg['index']['name']):
    es.indices.delete(cfg['index']['name'])
    print('Index "%s" deleted.' % cfg['index']['name'])

# Create the index if not exists
if not es.indices.exists(cfg['index']['name']):
    print('Creating index "%s"...' % cfg['index']['name'], end='')
    es.indices.create(cfg['index']['name'], {
        "mappings": {"properties": mapping},
        "settings": cfg['index']['settings']
    })
    print("OK")


# Extracts data from all csv files in the specified directory
def extract_csv_files(directory):
    print("Extracting CSV data...")
    csv_files = [f for f in listdir(directory) if isfile(join(directory, f)) and f.lower().endswith('.csv')]
    for file in csv_files:
        with open(join(directory, file)) as csvfile:
            content = csv.DictReader(csvfile, delimiter=";")
            for row in content:
                yield row


# Convert the value to the type specified in the mapping file
def convert_value(key, value):
    if isinstance(value, str) and key in mapping and mapping[key]['type'] in types:
        try:
            num_value = float(value.replace(',', '.'))
            return types[mapping[key]['type']](num_value)
        except ValueError:
            pass

    return None if value == 'null' else value


# Generate data to be imported into CS
def gen_data():
    global doc_id
    for data in extract_csv_files(cfg['csv_directory']):
        for field in data:
            data[field] = convert_value(field, data[field])

        doc_id += 1
        yield {
            "_op_type": "create",
            "_id": doc_id,
            "_index": cfg['index']['name'],
            "_source": data
        }


# let's run the bulk insert!
summary = [0, 0]  # Compute number of succeed vs failed inserts
for result in streaming_bulk(es, gen_data()):
    summary[not result[0]] += 1
    print("Succeed: %d | Failed: %d" % tuple(summary), end="\r")

print()
print("DONE!")
