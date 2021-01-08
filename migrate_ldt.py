#!/usr/bin/env python3

import os
import json
import pathlib

from dotenv import load_dotenv
from airtable import Airtable

load_dotenv()
airtable_key = os.environ.get('AIRTABLE_KEY')

data = pathlib.Path('data')

def get_base(base_id, schema, force=False):
    base = {}

    # get the raw tables
    for table_name in schema.keys():
        cached_data = data / (table_name + '.json')
        if cached_data.exists():
            table = json.load(cached_data.open())
        else:
            airtable = Airtable(base_id, table_name, airtable_key)
            table = airtable.get_all()
            json.dump(table, cached_data.open('w'), indent=2)
        base[table_name] = table

    # replace record ids with the relevant objects
    for table_name, foreign_keys in schema.items():
        table = base[table_name]
        for col, foreign_table_name in foreign_keys.items():
            for row in table:
                if col not in row['fields']:
                    continue
                vals = row['fields'][col]
                index = get_index(base[foreign_table_name])
                vals = list(map(index, vals))
                row['fields'][col] = vals

    return base

def get_index(table):
    m = {}
    for rec in table:
        m[rec['id']] = rec

    def f(id):
        return m.get(id)

    return f

ldt = get_base("appkzHtR3oryuaKfm", {
    "Folder": {
        "Linked Items": "Items",
        "Linked Images": "Images"
    },
    "Items": {
        "Images in Item": "Images",
        "People": "People",
        "Subjects": "Subjects",
        "Places/Organizations": "Locations"
    },
    "Images": {
        "People (Image Level)": "People",
        "Subjects (Image Level)": "Subjects",
        "Places (Image Level)": "Locations"
    },
    "People": {},
    "Subjects": {},
    "Locations": {},
    "QA": {}
})

for folder in ldt['Folder']:
    print(json.dumps(folder, indent=2))
    break

#l = get_base(
#    "appqn0kIOXRo00kdN",
#    key,
#    []
#)



