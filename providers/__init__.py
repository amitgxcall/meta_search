"""
Data providers for meta_search.
"""

from .base import DataProvider
from .csv_provider import CSVProvider
from .sqlite_provider import SQLiteProvider
from .hybrid_provider import HybridProvider

__all__ = [
    'DataProvider',
    'CSVProvider',
    'SQLiteProvider', 
    'HybridProvider'
]

# Conditionally import JSON provider if available
try:
    from .json_provider import JSONProvider
    __all__.append('JSONProvider')
except ImportError:
    pass