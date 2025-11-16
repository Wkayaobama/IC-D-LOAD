"""
Model Package
=============

Data storage models for the extraction pipeline.

Available models:
- ModelBase: Abstract base class
- MemoryModel: In-memory storage
- SqlModel: SQL database persistence
- ChainModel: Chain multiple models together
- PassThroughModel: No-op model for testing
"""

from .base import ModelBase
from .memory import MemoryModel
from .passthrough import PassThroughModel

try:
    from .sql import SqlModel
except ImportError:
    SqlModel = None

try:
    from .chain import ChainModel
except ImportError:
    ChainModel = None

__all__ = [
    'ModelBase',
    'MemoryModel',
    'PassThroughModel',
    'SqlModel',
    'ChainModel'
]
