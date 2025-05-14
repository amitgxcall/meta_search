"""
Utils package for meta_search.
Contains utility functions and classes.
"""

# Import and expose utility components
try:
    from .field_mapping import FieldMapping
    __all__ = ['FieldMapping']
except ImportError:
    __all__ = []

try:
    from .cache import Cache
    __all__.append('Cache')
except ImportError:
    pass

try:
    from .text_processing import (
        normalize_text,
        tokenize,
        remove_stopwords,
        stem_words
    )
    __all__.extend(['normalize_text', 'tokenize', 'remove_stopwords', 'stem_words'])
except ImportError:
    pass