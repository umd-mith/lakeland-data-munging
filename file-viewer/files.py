#!/usr/bin/env python3

import io
import os
import csv
import json

from schema import Base
from dotenv import load_dotenv

load_dotenv()
airtable_key = os.environ.get('AIRTABLE_KEY')


lda = Base("app9sKntqCyBwawhA", airtable_key, {
    "Items": {
        "Files": "Files"
    },
    "Files": {}
})

fs = {}
for item in lda.tables['Files'].data:
    files = item['fields'].get('File Path')
    if not files:
        continue

    # value could be multiple paths in quoted csv notation
    if files.startswith('"'):
        paths = next(csv.reader(io.StringIO(files))) 
    else:
        paths = [files]

    for path in paths:
        node = fs
        for part in path.split('/'):
            if part not in node:
                node[part] = {}
            node = node[part]

json.dump(fs, open('files.json', 'w'), indent=2)
