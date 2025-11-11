# Standard library modules.
import re
import abc
import dataclasses
import inspect

# Third party modules.

# Local modules.

# Globals and constants variables.

def iskeyfield(field):
    return field.name.startswith('key') or field.metadata.get('key', False)

def keyfields(dataclass):
    return tuple(field for field in dataclasses.fields(dataclass) if iskeyfield(field))

def camelcase_to_words(text):
    return re.sub("([a-z0-9])([A-Z])", r"\1 \2", text)

class ModelBase(metaclass=abc.ABCMeta):

    def _get_table_name(self, data_or_dataclass):
        if not inspect.isclass(data_or_dataclass):
            data_or_dataclass = type(data_or_dataclass)

        name = data_or_dataclass.__name__.lower()
        return "_".join(camelcase_to_words(name).split())

    @abc.abstractmethod
    def exists(self, data):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, data, check_exists=True):  # pragma: no cover
        """
        Adds a data object to the model.
        Returns ``True`` if the data is added, ``False`` if it already exists.
        """
        raise NotImplementedError
