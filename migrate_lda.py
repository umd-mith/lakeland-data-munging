#!/usr/bin/env python3

import os

from schema import Base
from dotenv import load_dotenv

load_dotenv()
airtable_key = os.environ.get('AIRTABLE_KEY')

# Lakeland Digitization Archive 

lda = Base("app9sKntqCyBwawhA", airtable_key, {
    "Items": {
        "Files": "Files"
    },
    "Files": {}
})


# Lakeland Temp Airtable

lak = Base('appqn0kIOXRo00kdN', airtable_key, {
    "Accessions": {},
    "Files": {},
    "Items": {},
    "People": {},
    "Places": {},
    "Subjects": {},
    "Organizations": {}
})

for item in lda.tables['Files'].data:
    path = item['fields'].get('File Path')

