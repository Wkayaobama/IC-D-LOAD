# Standard library modules.

# Third party modules.

# Local modules.
from .base import ModelBase

# Globals and constants variables.

class PassThroughModel(ModelBase):
    """Model that does nothing - for testing/debugging"""

    def exists(self, data):
        return False

    def add(self, data, check_exists=True):
        return True
