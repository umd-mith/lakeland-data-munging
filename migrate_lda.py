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
from schema import Base, parse_name, get_sha256, csv_str, csv_list, save_file, get_ext

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
        "Source (Provenance)": "Entities",
        "Interviewer": "Entities",
        "Interviewee": "Entities",
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
            "Suffix Name": s,
            "Source Code": e["fields"].get("Source Code"),
            "Alternate Name": e["fields"].get("Alternate Name"),
        }
        if e_type == 'Person (LCHP Team)':
            extra = {'LCHP Staff': True}
    elif e_type == 'Corporate Body':
        table_name = 'Organizations'
        for col in ["Source Code", "Address", "Latitude", "Longitude"]:
            obj[col] = e["fields"].get(col)
    elif e_type == 'Family':
        table_name = 'Families'
    elif e_type == 'Place':
        table_name = 'Places'
        for col in ["Source Code", "Address", "Latitude", "Longitude"]:
            obj[col] = e["fields"].get(col)
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

    virtual_loc = f['fields'].get('Virtual Location', '')
    if 'lakeland.umd.edu/asa' in virtual_loc:
        accessions.append(
            lak.tables['Accessions'].get_or_insert({
                "Description": "ASA"
            })
        )
    elif 'lakeland.umd.edu' in virtual_loc:
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

    if "Maxine Hard Drive" in files or "Maxine Gross Hard Drive" in files:
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

    if 'LCHP Accession 2021' in files:
        accessions.append(
            lak.tables['Accessions'].get_or_insert({
                "Description": "LCHP Accession 2021"
            })
        )

    return accessions

def get_paths(f):

    loc = f['fields'].get('Virtual Location')
    file_paths = f['fields'].get('File Path')

    paths = []

    if file_paths:
        # get the first original path and look for it in nfs mount
        orig_path = csv_list(file_paths)[0]

        # these are relative to a different base directory one level up to keep
        # things interesting I guess. thankfully all this code can be jettisoned
        orig_path = orig_path.replace('/Projects/lakeland-digital-archive/object files/LCHP Accession 2021', '../LCHP Accession 2021')

        # get the full path on the nfs share
        path = drive / orig_path

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

    return paths

def get_files(f):

    # get the accession that the file is a part of (could be more than one)
    # because of duplication within hard drives that have been processed
    # if there aren't any we can't save the file so we need to stop

    accessions = get_accessions(f)
    if len(accessions) == 0:
        return []

    files = []
    for path in get_paths(f):
        mimetype = magic.from_file(path.as_posix(), mime=True)
        sha256 = get_sha256(path)
        size = path.stat().st_size
        ext = get_ext(mimetype)
        duration = f['fields'].get('Duration')

        obj = {
            "Accession": [a['id'] for a in accessions],
            "SHA256": sha256,
            "Format": mimetype,
            "Size": size,
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

            # save the file to storage using the first accession, 
            # checksum, sha256, and file extension to determine the path
            # the relative path to a storage root is returned for saving
            # in the Airtable Files table as Location.
            #
            # Note: a file can be part of more than one accession, so 
            # but the file should only live at one physical location and 
            # not be duplicated so we pick the first one.

            obj['Location'] = save_file(
                path,
                accessions[0]['fields']['ID'], # can be more than one
                sha256,
                ext
            )

            afile = lak.tables['Files'].insert(obj)

            # only delete things that were downloaded to tmp
            if str(path).startswith("tmp"):
                os.remove(path)

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
                "Suffix Name": s,
                "Source Code": entity['fields'].get('Source Code'),
                "Alternate Name": entity['fields'].get('Alternate Name')
            }
        elif lak_table in ['Places', 'Organizations']:
            fields['Source Code'] = entity['fields'].get('Source Code')
            fields['Address'] = entity['fields'].get('Address')
            fields['Latitude'] = entity['fields'].get('Latitude')
            fields['Longitude'] = entity['fields'].get('Longitude')

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

for i in lda.tables['Items'].data:
    files = []
    for f in i['fields']['Files']:
        new_files = get_files(f)

        # if files can't be found stop right away
        if len(new_files) == 0:
            files = []
            break

        files.extend(new_files)

    creators = get_entities(i['fields'].get('Creator', []), 'People')
    interviewers = get_entities(i['fields'].get('Interviewer', []), 'People')
    interviewees = get_entities(i['fields'].get('Interviewee', []), 'People')
    people = get_entities(i['fields'].get('People', []), 'People')
    subjects = get_entities(i['fields'].get('Subjects', []), 'Subjects')
    events = get_entities(i['fields'].get('Events', []), 'Events')

    # disentangle places and organizations

    places = filter(lambda p: p['fields']['Entity Category'] == 'Place', i['fields'].get('Places/Organizations', []))
    places = get_entities(places, 'Places')

    orgs = filter(lambda p: p['fields']['Entity Category'] == 'Corporate Body', i['fields'].get('Places/Organizations', []))
    orgs = get_entities(orgs, 'Organizations')

    # disentangle Source (Provenance)

    source_people = filter(lambda s: s['fields']['Entity Category'] == 'Person', i['fields'].get('Source (Provenance)', []))
    source_people = get_entities(source_people, 'People')

    source_orgs = filter(lambda s: s['fields']['Entity Category'] == 'Corporate Body', i['fields'].get('Source (Provenance)', []))
    source_orgs = get_entities(source_orgs, 'Organizations')

    source_fams = filter(lambda s: s['fields']['Entity Category'] == 'Family', i['fields'].get('Source (Provenance)', []))
    source_fams = get_entities(source_fams, 'Families')

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
        "In Lakeland Book?": i['fields'].get('In Lakeland Book?'),
        "Lakeland Book Chapter": i['fields'].get('Lakeland Book Chapter'),
        "Lakeland Book Page #": i['fields'].get('Lakeland Book Page #'),
        "Used in Lakeland Video?": i['fields'].get('Used in Lakeland Video?'),
        "Flag for Removal?": i['fields'].get('Flag for Removal?'),
        "Creator": creators,
        "Interviewers": interviewers,
        "Interviewees": interviewees,
        "Events": events,
        "People": people,
        "Places": places,
        "Organizations": orgs,
        "Subjects": subjects,
        "Source (People)": source_people,
        "Source (Organizations)": source_orgs,
        "Source (Families)": source_fams
    })

# add interview transcripts as files attached to items

for item in lda.tables['Items'].data:
    files = []
    lak_item = None

    for interview in item['fields'].get('Interview Summary', []):
        old_id = item['fields'].get('Legacy ID-UMD')
        lak_item = lak.tables['Items'].find({'Legacy UMD ID': old_id}, first=True)

        orig_filename = interview['filename']
        path = localize(interview['url'])
        mimetype = magic.from_file(path.as_posix(), mime=True)
        sha256 = get_sha256(path)
        size = path.stat().st_size
        ext = get_ext(mimetype)

        accession = lak.tables['Accessions'].find({"Description": "LCHP Accession 2021"}, first=True)

        location = save_file(
            path,
            accession['fields']['ID'],
            sha256,
            ext
        )

        afile = lak.tables['Files'].insert({
            "Accession": [accession['id']],
            "SHA256": sha256,
            "Format": mimetype,
            "Size": size,
            "Original Filenames": orig_filename,
            "Location": location
        })
 
        # only delete things that were downloaded to tmp
        if str(path).startswith("tmp"):
            os.remove(path)

        files.append(afile['id'])

    if lak_item is not None:
        lak.tables['Items'].update(lak_item['id'], {"Files": files})

