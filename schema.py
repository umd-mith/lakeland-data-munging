import io
import re
import csv
import magic
import shutil
import hashlib
import pathlib
import mimetypes

from airtable import Airtable


class Base:
    """
    Represents an Airtable Base and all its Tables.
    """

    def __init__(self, base_id, api_key, schema):
        self.id = base_id
        self.tables = {}
        self.api_key = api_key
        self.schema = schema
        self.load()

    def load(self):
        for table_name, relations in self.schema.items():
            self.tables[table_name] = Table(self, table_name, relations)
        for table in self.tables.values():
            table.link()
        
    def wipe(self):
        """
        Empty all the tables but leave the schema intact.
        """
        # safety to only ever wipe this airtable base
        assert self.id == 'appqn0kIOXRo00kdN'

        # first get the latest data
        self.load()

        for table in self.tables.values():
            table.wipe()

        self.load()


class Table:
    """
    Represents an Airbase table.
    """

    def __init__(self, base, table_name, relations):
        self.base = base
        self.table_name = table_name
        self.relations = relations
        self.airtable = Airtable(base.id, table_name, base.api_key)
        self.load()

    def load(self):
        self.data = self.airtable.get_all()

    def insert(self, row):
        result = self.airtable.insert(row)
        self.data.append(result)
        self.link()
        return result

    def update(self, id, row):
        return self.airtable.update(id, row)

    def get(self, id):
        for row in self.data:
            if row['id'] == id:
                return row
        return None

    def find(self, fields, first=False):
        results = []
        for row in self.data:
            match = True
            for k, v in fields.items():
                if k not in row['fields']:
                    if v is not None:
                        match = False
                elif row['fields'][k] != v:
                    match = False
            if match:
                results.append(row)
        if first:
            if len(results) == 0:
                return None
            else:
                return results[0]
        return results

    def get_or_insert(self, fields, extra=None):
        """
        Get or insert and get the first record that matches the fields.
        When inserting you can add additional things using the extras value
        which should be a dictionary of names and values to set in addition
        to the supplied fields.
        """
        r = self.find(fields, first=True)
        if not r:
            f = fields
            if extra:
                f.update(extra)
            r = self.insert(f)
        return r

    def link(self):
        """
        Use the table's schema relations to turn IDs into objects.
        """
        for row in self.data:
            for prop, other_table_name in self.relations.items():
                other_table = self.base.tables[other_table_name]
                if prop in row['fields']:
                    value = row['fields'][prop]
                    if type(value) == list:
                        new_value = []
                        for v in value:
                            new_value.append(other_table.get(v))
                        row['fields'][prop] = new_value
                    else:
                        row['fields'][prop] = other_table.get(value)

    def wipe(self):
        """
        Remove all rows from the table.
        """
        self.load()
        ids = [row['id'] for row in self.data]
        self.airtable.batch_delete(ids)


def parse_name(s):
    """
    Parse a name string into its parts.
    """
    parts = s.split(' ')
    f = parts.pop(0)
    l = parts.pop() if len(parts) > 0 else None
    s = None
    if l and re.match(r'^(sr)|jr|[iv]+$', l, re.IGNORECASE):
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

def csv_list(s):
    "Parse a CSV row into a list"
    if not s or s == '':
        return []
    elif "," in s and '"' not in s:
        return [s]
    else:
        return next(csv.reader(io.StringIO(s)))

def csv_str(l):
    "Turn a list into a CSV row"
    out = io.StringIO()
    csv.writer(out).writerow(l)
    return out.getvalue().strip()

def save_file(src, accession_dir, sha256, ext):
    ext = ext.lstrip('.')
    filename = "{}.{}".format(sha256, ext)
    rel_path = pathlib.Path(str(accession_dir)) / filename
    abs_path = pathlib.Path("/mnt/data") / rel_path

    # make the directory if needed
    if not abs_path.parent.is_dir():
        abs_path.parent.mkdir(parents=True)

    # copy the file
    shutil.copyfile(src, abs_path)

    return str(rel_path)

def get_ext(mimetype):
    result = mimetypes.guess_extension(mimetype)
    if not result and mimetype == 'image/vnd.adobe.photoshop':
        result = '.psd'
    elif result is None:
        result = ''
    return result
