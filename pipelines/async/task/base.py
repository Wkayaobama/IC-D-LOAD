__all__ = ['Task']

# Standard library modules.
import abc

# Third party modules.
from loguru import logger

# Local modules.
from ..model.passthrough import PassThroughModel

# Globals and constants variables.

class Task(metaclass=abc.ABCMeta):

    def __init__(self, model=None):
        if model is None:
            model = PassThroughModel()
        self.model = model

    @abc.abstractmethod
    async def run(self, progress=True):
        """
        Executes the task
        Returns ``True`` if the task was executed, ``False`` if skipped.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError
