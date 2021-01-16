#!/usr/bin/env python3

import os
import magic
import pathlib

from dotenv import load_dotenv
from ldt_images import get_orig
from schema import Base, parse_name, get_sha256, get_ext

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
    "Items": {
        "People": "People",
        "Places/Organizations": "Locations",
        "Subjects": "Subjects",
        "Images in Item": "Images"
    },
    "Images": {
        "People (Image Level)": "People",
        "Places (Image Level)": "Locations",
        "Subjects (Image Level)": "Subjects",
    },
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

# First wipe the slate clean.

print('Resetting base')
lak.wipe()

# Populate authorities

print('Adding People')
for p in ldt.tables['People'].data:
    if 'Name' not in p['fields']:
        continue
    first, middle, last, suffix = parse_name(p['fields']['Name'])
    lak.tables['People'].insert({
        "First Name": first,
        "Middle Name": middle,
        "Last Name": last,
        "Suffix Name": suffix
    })

print('Adding Places')
for p in ldt.tables['Locations'].data:
    if 'Name' not in p['fields']:
        continue
    lak.tables['Places'].insert({
        'Name': p['fields']['Name']
    })

print('Adding Subjects')
for s in ldt.tables['Subjects'].data:
    if 'Name' not in s['fields']:
        continue
    lak.tables['Subjects'].insert({
        'Name': s['fields']['Name']
    })


# Folders -> Accessions, Files

print('Adding Accessions & Files')

# image id -> file id mapping for use later
image_file_map = {}

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

    # add any attachments
    docs = []
    for a in f['fields'].get('Inventory Form', []):
        docs.append({'url': a['url']})
    for a in f['fields'].get('Consent Form', []):
        docs.append({'url': a['url']})

    # add each of the accessions
    accession = lak.tables['Accessions'].insert({
        "Donor": donors,
        "Date of Donation": f["fields"].get("Date of donation"),
        "Description": f["fields"].get("Accession Notes"),
        "Documentation": docs, 
        "Legacy Folder ID": f["fields"].get("Folder ID")
    })

    # make sure the folder is on disk
    folder_id = f['fields']['Folder ID']
    if not (media / folder_id).is_dir():
        print("missing folder for {}".format(folder_id))
        continue

    # get the original file for each linked image
    files = []
    for image in f['fields']['Linked Images']:
        image_id = image['fields']['Image ID']
        orig = get_orig(image_id)
        if orig:
            files.append(orig)
        else:
            print("couldn't find image for {}".format(image_id))

    # sort them so they appear in sequence
    for image in sorted(files, key=lambda r: r['id']):
        image_path = pathlib.Path(image['path'])
        mimetype = magic.from_file(image_path.as_posix(), mime=True)
        sha256 = get_sha256(image_path)
        size = image_path.stat().st_size
        ext = get_ext(image_path, mimetype)
        img = lak.tables['Files'].insert({
            "Accession": [accession['id']],
            "SHA256": sha256,
            "Format": mimetype,
            "Size": size,
            "Extension": ext,
            "Original Filenames": image_path.as_posix(),
            "Legacy Image ID": image['id']
        })
        image_file_map[image['id']] = img['id']

# Images -> Items

print('Addding Images to Items')

for image in ldt.tables['Images'].data:

    # see if there is descriptive metadata about the image, if so add it as an item
    # "Image Description", "People (Image Level)", "Places (Image Level)", 
    # "Subjects (Image Level)"

    title = image['fields'].get('Image Description')

    subjects = set()
    for s in image['fields'].get('Subjects (Image Level)', []):
        if 'Name' not in s['fields']:
            continue
        subject = lak.tables['Subjects'].get_or_insert({
            'Name': s['fields']['Name']
        })
        subjects.add(subject['id'])

    places = set()
    for p in image['fields'].get('Places (Image Level)', []):
        if 'Name' not in p['fields']:
            continue
        place = lak.tables['Places'].get_or_insert({
            'Name': p['fields']['Name']
        })
        places.add(place['id'])

    people = set()
    for p in image['fields'].get('People (Image Level)', []):
        if 'Name' not in p['fields']:
            continue
        first, middle, last, suffix = parse_name(p['fields']['Name'])
        person = lak.tables['People'].get_or_insert({
            "First Name": first,
            "Middle Name": middle,
            "Last Name": last,
            "Suffix Name": suffix
        })
        people.add(person['id'])

    file_id = image_file_map.get(image['fields']['Image ID'])

    if file_id and (title or subjects or people or places):
        lak.tables['Items'].insert({
            "Title": title,
            "Subjects": list(subjects),
            "People": list(people),
            "Places": list(places),
            "Files": [file_id]
        })

# Items -> Items

print('Adding Items')

for item in ldt.tables['Items'].data:

    title = item['fields'].get('Title')
    item_type = item['fields'].get('Object Type')

    subjects = set()
    for s in item['fields'].get('Subjects', []):
        subject = lak.tables['Subjects'].get_or_insert({
            "Name": s['fields']["Name"]
        })
        subjects.add(subject['id'])

    places = set()
    for p in item['fields'].get('Places', []):
        place = lak.tables['Places'].get_or_insert({
            "Name": p['fields']["Name"]
        })
        places.add(place['id'])

    people = set()
    for p in item['fields'].get('People', []):
        if 'Name' not in p['fields']:
            continue
        first, middle, last, suffix = parse_name(p['fields']['Name'])
        person = lak.tables['People'].get_or_insert({
            "First Name": first,
            "Middle Name": middle,
            "Last Name": last,
            "Suffix Name": suffix
        })
        people.add(person['id'])

    files = []
    for image in item['fields'].get('Images in Item', []):
        image_id = image['fields'].get('Image ID')
        file_id = image_file_map.get(image_id)
        if file_id:
            files.append({"image_id": image_id, "file_id": file_id})
        else:
            print('unable to find file for {}'.format(image['fields']['Image ID']))

    # sort the files by their image_id (includes their sequence number)
    # the order of files is their sequence in the item
    files = sorted(files, key=lambda f: f["image_id"])

    if files:
        lak.tables['Items'].insert({
            "Title": title,
            "Type": item_type,
            "Subjects": list(subjects),
            "People": list(people),
            "Places": list(places),
            "Files": list(files),
            "Legacy Item ID": item['fields'].get('Readable Item ID')
        })
