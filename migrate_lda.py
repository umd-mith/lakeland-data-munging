#!/usr/bin/env python3

import os
import re
import csv
import magic
import pathlib
import requests
import tempfile

from dotenv import load_dotenv
from urllib.parse import urlparse
from urllib.request import urlretrieve
from schema import Base, parse_name, get_sha256, get_ext, csv_str, csv_list, save_file

load_dotenv()
airtable_key = os.environ.get('AIRTABLE_KEY')
omeka_key = os.environ.get('OMEKA_KEY')
google_key = os.environ.get('GOOGLE_KEY')
asa_password = os.environ.get('ASA_PASSWORD')

# nfs mount of mith NAS
drive = pathlib.Path('/mnt/storage.mith.us-projects/lakeland-digital-archive/object files/Files by Object Type/')

# Lakeland Digitization Archive 
lda = Base("app9sKntqCyBwawhA", airtable_key, {
    "Items": {
        "Files": "Files",
        "Creator": "Entities",
        "Interviewer": "Entities",
        "People": "Entities",
        "Places/Organizations": "Entities",
        "Subjects": "Subjects",
        "Source/Provenance": "Entities",
        "Interviewer": "Entities",
        "Interviewee": "Entities"
    },
    "Files": {},
    "Subjects": {},
    "Entities": {},
    "Relationships": {}
})

# Lakeland Temp Airtable
lak = Base('appqn0kIOXRo00kdN', airtable_key, {
    "Accessions": {},
    "Files": {},
    "Items": {},
    "People": {},
    "Places": {},
    "Subjects": {},
    "Organizations": {},
    "Events": {},
    "Families": {}
})

print('migrate_lda: adding Subjects')
for s in lda.tables['Subjects'].data:
    s_type = s['fields'].get('Subject Category')
    if s_type == 'Concept':
        table_name = 'Subjects'
    elif s_type == 'Event':
        table_name = 'Events'
    else:
        print('migrate_lda: unknown subject type:', s)
        continue
    lak.tables[table_name].get_or_insert({
        "Name": s['fields']['Name']
    })

print('migrate_lda: adding Entities')
for e in lda.tables['Entities'].data:
    obj = {'Name': e['fields'].get('Name')}
    extra = None

    e_type = e['fields']['Entity Category']
    if e_type in ['Person', 'Person (LCHP Team)']:
        f, m, l, s = parse_name(obj['Name'])
        table_name = 'People'
        obj = {
            "First Name": f,
            "Middle Name": m,
            "Last Name": l,
            "Suffix Name": s
        }
        if e_type == 'Person (LCHP Team)':
            extra = {'LCHP Staff': True}
    elif e_type == 'Corporate Body':
        table_name = 'Organizations'
    elif e_type == 'Family':
        table_name = 'Families'
    elif e_type == 'Place':
        table_name = 'Places'
    else:
        print('migrate_lda: unknown entity type:', e)
        continue

    lak.tables[table_name].get_or_insert(obj, extra)

def localize(url):
    if 'lakeland.umd.edu/asa' in url or 'reclaim.hosting/asa/' in url:
        auth = ('lakeland', asa_password)
    else:
        auth = None

    local_filename = pathlib.Path('tmp') / (url.split('/')[-1] or 'file')
    with requests.get(url, stream=True, auth=auth) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

    return pathlib.Path(local_filename)

def get_asa(url):
    name = url.split('/')[-1]
    return 'https://protected.lakeland.reclaim.hosting/asa/audio/{}.mp3'.format(name)

def get_omeka_meta(url):
    omeka_id = url.split('/')[-1]
    url = 'https://lakeland.umd.edu/api/items/{}?key={}'.format(omeka_id, omeka_key)
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    else:
        return None

def get_omeka_originals(url):
    omeka_id = url.split('/')[-1]
    url = 'https://lakeland.umd.edu/api/files?item={}&key={}'.format(omeka_id, omeka_key)
    resp = requests.get(url)
    results = []
    if resp.status_code == 200:
        files = resp.json()
        results = [f['file_urls']['original'] for f in files]
    return results

def get_google_drive(url):
    m = re.match(r'.+drive.google.com/.*file/./(.+)/view.+', url)
    if not m:
        print('migrate_lda: unknown Google Drive URL', url)
        return None
    file_id = m.group(1)
    return "https://www.googleapis.com/drive/v3/files/" + file_id + "?supportsAllDrives=true&alt=media&key=" + google_key

def get_accessions(f):
    accessions = []

    if 'lakeland.umd.edu/asa' in f['fields'].get('Virtual Location', ''):
        accessions.append(
            lak.tables['Accessions'].get_or_insert({
                "Description": "ASA"
            })
        )
    elif 'lakeland.umd.edu' in f['fields'].get('Virtual Location', ''):
        accessions.append(
            lak.tables['Accessions'].get_or_insert({
                "Description": "Lakeland Omeka Instance Images"
            })
        )
   
    files = f['fields'].get('File Path', '')

    if 'Mary Sies Hard Drive' in files:
        accessions.append(
            lak.tables['Accessions'].get_or_insert({
                "Description": "Mary Sies Hard Drive: Images"
            })
        )

    if "Maxine Hard Drive" in files:
        accessions.append(
            lak.tables['Accessions'].get_or_insert({
                "Description": "Maxine Gross Hard Drive: Images"
            })
        )

    if "College Park Photos" in files:
        accessions.append(
            lak.tables['Accessions'].get_or_insert({
                "Description": "College Park Photos"
            })
        )

    if len(accessions) == 0:
        print('migrate_lda: unknown accession for files', f)

    return [a['id'] for a in accessions]

def get_paths(f):

    loc = f['fields'].get('Virtual Location')
    file_paths = f['fields'].get('File Path')

    paths = []

    if file_paths:
        # get the first original path and look for it in nfs mount
        orig_paths = csv_list(file_paths)
        path = drive / orig_paths[0]
        if not path.is_file():
            print("migrate_lda: file {} doesn't exist for {}".format(path, f))
        else:
            paths = [path]

    elif loc:
        uri = urlparse(loc)
        host = uri.netloc

        try:
            if host == 'mith-lakeland-media.s3-website-us-east-1.amazonaws.com':
                paths = [localize(loc)]
            elif host == 'raw.githubusercontent.com':
                paths = [localize(loc)]
            elif host == 'lakeland.umd.edu' and uri.path.startswith('/asa/'):
                paths = [localize(get_asa(loc))]
            elif host == 'lakeland.umd.edu':
                paths = list(map(localize, get_omeka_originals(loc)))
            elif host == "drive.google.com":
                paths = [localize(get_google_drive(loc))]
        except requests.exceptions.HTTPError as e:
            print("migrate_lda: encountered HTTP error when downloading loc: {}".format(e))

    if len(paths) == 0:
        print("migrate_lda: couldn't determine file(s) for:", f)

    return paths

def get_files(f):
    files = []
    for path in get_paths(f):
        accessions = get_accessions(f)
        mimetype = magic.from_file(path.as_posix(), mime=True)
        sha256 = get_sha256(path)
        size = path.stat().st_size
        ext = get_ext(path, mimetype)
        duration = f['fields'].get('Duration')

        obj = {
            "Accession": accessions,
            "SHA256": sha256,
            "Format": mimetype,
            "Size": size,
            "Extension": ext,
            "Original Filenames": f['fields'].get('File Path'),
            "Duration": duration
        }

        # look for an existing file with the same sha256
        # if one is found it needs to be updated

        afile = lak.tables['Files'].find({"SHA256": sha256}, first=True)
        if afile and 'File Path' in f['fields']:
            # merge the csv lists of original filenames
            filenames = csv_list(afile['fields'].get('Original Filenames', ''))
            filenames.extend(csv_list(f['fields']['File Path']))
            obj['Original Filenames'] = csv_str(filenames)
            afile = lak.tables['Files'].update(afile['id'], obj)

        # otherwise we need to insert a new row
        else:
            # only delete things that were downloaded to tmp
            delete = True if str(path).startswith("tmp") else False
            save_file(path, sha256, ext, delete=delete)
            afile = lak.tables['Files'].insert(obj)

        files.append(afile)
    return [f['id'] for f in files]

def get_entities(entities, lak_table):
    entity_ids = []
    for entity in entities:
        fields = {"Name": entity['fields']['Name']}
        if lak_table == 'People':
            f, l, m, s = parse_name(entity['fields']['Name'])
            fields = {
                "First Name": f,
                "Last Name": l,
                "Middle Name": m,
                "Suffix Name": s
            }
        lak_obj = lak.tables[lak_table].get_or_insert(fields)
        entity_ids.append(lak_obj['id'])
    return entity_ids

def replace(s1, s2, l):
    """Replace all occurrences of string s1 in list l with string s2
    """
    new_l = []
    for s in l:
        if s == s1:
            new_l.append(s2)
        else:
            new_l.append(s)
    return list(set(new_l))

print("migrate_lda: adding Items & Files")
for i in lda.tables['Items'].data:

    files = []
    for f in i['fields']['Files']:
        files.extend(get_files(f))

    if not files:
        print("migrate_lda: skipping, no files for item", i)
        continue

    creators = get_entities(i['fields'].get('Creator', []), 'People')
    interviewers = get_entities(i['fields'].get('Interviewer', []), 'People')
    interviewees = get_entities(i['fields'].get('Interviewee', []), 'People')
    people = get_entities(i['fields'].get('People', []), 'People')
    subjects = get_entities(i['fields'].get('Subjects', []), 'Subjects')

    # disentangle places and organizations
    places = filter(lambda p: p['fields']['Entity Category'] == 'Place', i['fields'].get('Places/Organizations', []))
    places = get_entities(places, 'Places')
    orgs = filter(lambda p: p['fields']['Entity Category'] == 'Corporate Body', i['fields'].get('Places/Organizations', []))
    orgs = get_entities(orgs, 'Organizations')

    # collect the Object Type and Object Category values
    otypes = [
        i['fields'].get('Object Type'),
        i['fields'].get('Object Category')
    ]

    # remove any None values
    otypes = list(filter(lambda o: o is not None, otypes))

    # normalize some of them
    otypes = replace("Photos", "Photo", otypes)
    otypes = replace("OralHistory", "Oral History", otypes)
    otypes = replace("Publications", "Publication", otypes)

    lak.tables['Items'].insert({
        "Title": i['fields'].get('Title'),
        "Description": i['fields'].get('Description'),
        "Legacy UMD ID": i['fields'].get('Legacy ID-UMD'),
        "Type": otypes,
        "Files": files,
        "Created": i['fields'].get('Creation Date'),
        "Creator": creators,
        "Interviewers": interviewers,
        "Interviewees": interviewees,
        "People": people,
        "Places": places,
        "Organizations": orgs,
        "Subjects": subjects
    })

