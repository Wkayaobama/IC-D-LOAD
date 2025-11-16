# Standard library modules.

# Third party modules.

# Local modules.
from .base import ModelBase

# Globals and constants variables.

class ChainModel(ModelBase):

    def __init__(self, *models):
        if len(models) <= 1:
            raise ValueError('Specify at least 2 models')
        self.models = models

    def exists(self, data):
        """
        Only check if the data exists in the first model.
        """
        return self.models[0].exists(data)

    def add(self, data, check_exists=True):
        """
        Adds data to all models. Exists as soon as one model returns ``False``.
        """
        for model in self.models:
            success = model.add(data, check_exists)
            if not success:
                return False

        return True
