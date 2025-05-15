"""
Hybrid provider compatibility wrapper.

This module provides backward compatibility for code that imports from
the original hybrid_provider module. It re-exports classes and functions
from the refactored hybrid package.

Example:
    # Old import style (still works but deprecated)
    from meta_search.providers.hybrid_provider import HybridProvider
    
    # New import style (preferred)
    from meta_search.providers.hybrid import HybridProvider
"""

import warnings

# Import from the refactored structure
from .hybrid.provider import HybridProvider
from .hybrid.strategies import (
    WeightedCombinationStrategy,
    SequentialCombinationStrategy,
    RankBoostCombinationStrategy,
    get_strategy
)

# Show deprecation warning
warnings.warn(
    "Importing from hybrid_provider.py is deprecated and will be removed in a future version. "
    "Please update your imports to use 'from meta_search.providers.hybrid import HybridProvider' instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export for backward compatibility
__all__ = [
    'HybridProvider',
    'WeightedCombinationStrategy',
    'SequentialCombinationStrategy',
    'RankBoostCombinationStrategy',
    'get_strategy'
]