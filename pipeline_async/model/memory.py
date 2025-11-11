# Standard library modules.
import dataclasses

# Third party modules.

# Local modules.
from .base import ModelBase, keyfields

# Globals and constants variables.

class MemoryModel(ModelBase):

    def __init__(self):
        self.storage = {}

    def exists(self, data):
        table = self._get_table_name(data)
        key = self._create_key(data)
        return key in self.storage.get(table, {})

    def _create_key(self, data):
        keys = []

        for field in keyfields(data):
            value = getattr(data, field.name)

            if dataclasses.is_dataclass(field.type):
                keys.append(self._create_key(value))
            else:
                keys.append(value)

        return hash(tuple(keys))

    def add(self, data, check_exists=True):
        if check_exists and self.exists(data):
            return False

        # Add data from fields
        for field in dataclasses.fields(data):
            name = field.name
            value = getattr(data, name)

            if dataclasses.is_dataclass(value):
                self.add(value, check_exists)

        # Add actual data
        table = self._get_table_name(data)
        key = self._create_key(data)
        self.storage.setdefault(table, {})[key] = data
        return True

    def get_alldata(self, dataclass):
        table = self._get_table_name(dataclass)
        return tuple(self.storage.get(table, {}).values())

    def clear(self):
        self.storage.clear()
