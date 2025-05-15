"""
Hybrid provider package for meta_search.

This package provides the hybrid provider implementation, which combines
structured data search with vector search for improved results.

Example:
    # Create a hybrid provider
    from meta_search.providers.hybrid import HybridProvider
    
    # Initialize with a data source
    provider = HybridProvider('job_details.csv')
    
    # Set field mapping
    provider.set_field_mapping(field_mapping)
    
    # Search
    results = provider.search('failed database jobs')
"""

from .provider import HybridProvider
from .strategies import (
    CombinationStrategy,
    WeightedCombinationStrategy,
    SequentialCombinationStrategy,
    RankBoostCombinationStrategy,
    get_strategy
)

__all__ = [
    'HybridProvider',
    'CombinationStrategy',
    'WeightedCombinationStrategy',
    'SequentialCombinationStrategy',
    'RankBoostCombinationStrategy',
    'get_strategy'
]