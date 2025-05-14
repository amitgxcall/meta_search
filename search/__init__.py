"""
Search package for meta_search.
Contains the search engine and related functionality.
"""

from .engine import SearchEngine

__all__ = ['SearchEngine']

# Import and expose other search module components
try:
    from .query_classifier import QueryClassifier
    __all__.append('QueryClassifier')
except ImportError:
    pass

try:
    from .query_patterns import QueryPattern
    __all__.append('QueryPattern')
except ImportError:
    pass

try:
    from .vector_search import VectorSearchEngine
    __all__.append('VectorSearchEngine')
except ImportError:
    pass

try:
    from .result_formatter import format_results
    __all__.append('format_results')
except ImportError:
    pass