#!/usr/bin/env python3

import os
import re
import magic
import pathlib
import hashlib

from schema import Base
from dotenv import load_dotenv
from ldt_images import get_images

load_dotenv()
airtable_key = os.environ.get('AIRTABLE_KEY')

# Lakeland Digitization Data S3 bucket
# s3://mith-lastclass-raw
media = pathlib.Path("mith-lastclass-raw")

# Lakeland Digitization Tracking Airtable

ldt = Base("appkzHtR3oryuaKfm", airtable_key, {
    "Folder": {
        "Donor Name": "People",
        "Linked Images": "Images"
    },
    "Items": {},
    "Images": {},
    "People": {},
    "Subjects": {},
    "Locations": {},
    "QA": {}
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


def get_sha256(f):
    d = hashlib.sha256()
    fh = open(f, 'rb')
    while True:
        chunk = fh.read(512 * 1024)
        if not chunk:
            break
        d.update(chunk)
    return d.hexdigest()

# Folders -> Accessions, Files, People

for f in ldt.tables['Folder'].data:

    donors = []

    # get or add the donor
    for person in f['fields']['Donor Name']:
        first, middle, last, suffix = parse_name(person['fields']['Name'])
        d = lak.tables['People'].get_or_insert({
            "First Name": first,
            "Middle Name": middle,
            "Last Name": last,
            "Suffix Name": suffix
        })
        donors.append(d['id'])

    # add each of the accessions
    accession = lak.tables['Accessions'].insert({
        "Donor": donors,
        "Date of Donation": f["fields"].get("Date of donation"),
        "Description": f["fields"].get("Accession Notes"),
    })

    # make sure the folder is on disk
    folder_id = f['fields']['Folder ID']
    if not (media / folder_id).is_dir():
        print("missing folder for {}".format(folder_id))
        continue

    # add the accession folder images as files
    for image in f['fields']['Linked Images']:
        print(image)

    '''
    for image in sorted(get_images(f['fields']['Folder ID']), key=lambda r: r['id']):
        mimetype = magic.from_file(image['path'], mime=True)
        sha256 = get_sha256(image['path'])

        print(image['id'], image['path'], mimetype, sha256) 
        # use accession date for created
    '''

# Images -> Items, Subjects, People, Places
for f in ldt.tables['Images'].data:
    pass
