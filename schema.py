import json
import pathlib

from airtable import Airtable


# location of the project and data files
ROOT_DIR = pathlib.Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"


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
        self.cache_file = DATA_DIR / (base.id + "-" + table_name + '.json')

        if self.cache_file.exists():
            self.data = json.load(self.cache_file.open())
        else:
            self.data = self.airtable.get_all()
            json.dump(self.data, self.cache_file.open('w'), indent=2)

    def insert(self, row):
        result = self.airtable.insert(row)
        self.data.append(result)
        self.link()
        return result

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

    def get_or_insert(self, fields):
        r = self.find(fields, first=True)
        if not r:
            r = self.insert(fields)
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
        Remove all rows from the table, and remove any cached data.
        """
        ids = [row['id'] for row in self.data]
        self.airtable.batch_delete(ids)
        self.cache_file.unlink()
