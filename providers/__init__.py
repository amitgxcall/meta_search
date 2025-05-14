"""
Update providers package to include the enhanced providers.
"""

from .base import DataProvider
from .csv_provider import CSVProvider
from .enhanced_csv_provider import EnhancedCSVProvider
from .sqlite_provider import SQLiteProvider

# Import extensions
from . import csv_provider_extension

# Import hybrid providers
from .hybrid_provider import HybridProvider
from .sequential_hybrid_provider import SequentialHybridProvider
from .enhanced_sequential_provider import EnhancedSequentialHybridProvider

__all__ = [
    'DataProvider',
    'CSVProvider',
    'EnhancedCSVProvider',
    'SQLiteProvider',
    'HybridProvider',
    'SequentialHybridProvider',
    'EnhancedSequentialHybridProvider'
]

# Import optional providers if available
try:
    from .json_provider import JSONProvider
    __all__.append('JSONProvider')
except ImportError:
    pass