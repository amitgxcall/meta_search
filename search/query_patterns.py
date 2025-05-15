"""
Results formatter compatibility wrapper.

This module provides backward compatibility for code that imports from
the original resultsformatter module. It re-exports functions from
the refactored results package.

Example:
    # Old import style (still works but deprecated)
    from meta_search.search.resultsformatter import format_for_llm, display_results
    
    # New import style (preferred)
    from meta_search.search.results import format_for_llm, display_results
"""

import warnings

# Import from the refactored structure
from .results.formatter import (
    format_for_llm,
    display_results,
    format_as_json,
    format_as_csv,
    count_results_by_field,
    summarize_results
)

# Show deprecation warning
warnings.warn(
    "Importing from resultsformatter.py is deprecated and will be removed in a future version. "
    "Please update your imports to use 'from meta_search.search.results import ...' instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export for backward compatibility
__all__ = [
    'format_for_llm',
    'display_results',
    'format_as_json',
    'format_as_csv',
    'count_results_by_field',
    'summarize_results'
]