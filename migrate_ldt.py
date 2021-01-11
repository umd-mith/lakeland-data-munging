#!/usr/bin/env python3

import os
import re

from schema import Base
from dotenv import load_dotenv

load_dotenv()
airtable_key = os.environ.get('AIRTABLE_KEY')

# Lakeland Digitization Tracking Airtable

ldt = Base("appkzHtR3oryuaKfm", airtable_key, {
    "Folder": {
        "Donor Name": "People"
    },
    "Items": {},
    "Images": {},
    "People": {},
    "Subjects": {},
    "Locations": {},
    "QA": {}
})


# Lakeland Airtable Airtable

lak = Base('appqn0kIOXRo00kdN', airtable_key, {
    "Accessions": {},
    "Files": {},
    "Items": {},
    "People": {},
    "Places": {},
    "Subjects": {},
    "Organizations": {}
})

# start with a clean slate
lak.wipe()

def parse_name(s):
    """
    Parse a name string into its parts.
    """
    parts = s.split(' ')
    f = parts.pop(0)
    l = parts.pop()
    s = None
    if re.match(r'^(sr)|jr|[iv]+$', l, re.IGNORECASE):
        s = l
        l = parts.pop()
    m = ' '.join(parts) if len(parts) > 0 else None
    return f, m, l, s

# Folders -> Accessions, Files, People
for f in ldt.tables['Folder'].data:
    donors = []
    for person in f['fields']['Donor Name']:
        first, middle, last, suffix = parse_name(person['fields']['Name'])
        d = lak.tables['People'].get_or_insert({
            "First Name": first,
            "Middle Name": middle,
            "Last Name": last,
            "Suffix Name": suffix
        })
        donors.append(d['id'])

    lak.tables['Accessions'].insert({
        "Donor": donors,
        "Date of Donation": f["fields"].get("Date of donation"),
        "Description": f["fields"].get("Accession Notes")
    })

# Images -> Items, Subjects, People, Places
for f in ldt.tables['Images'].data:
    pass