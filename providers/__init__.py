"""
Providers package for meta_search.
Contains implementations for different data sources.
"""

from .base import DataProvider

__all__ = ['DataProvider']

# Import and expose specific provider implementations
try:
    from .csv_provider import CSVProvider
    __all__.append('CSVProvider')
except ImportError:
    pass

try:
    from .sqlite_provider import SQLiteProvider
    __all__.append('SQLiteProvider')
except ImportError:
    pass

try:
    from .json_provider import JSONProvider
    __all__.append('JSONProvider')
except ImportError:
    pass