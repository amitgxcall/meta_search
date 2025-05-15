"""
Search results module for meta_search.

This module provides functionality for formatting, displaying, and processing
search results in various formats and contexts.

Example:
    # Format search results for display
    from meta_search.search.results import display_results
    
    # Display search results
    display_results(results)
"""

from .formatter import (
    format_for_llm,
    display_results,
    format_as_json,
    format_as_csv,
    count_results_by_field,
    summarize_results
)

__all__ = [
    'format_for_llm',
    'display_results',
    'format_as_json',
    'format_as_csv',
    'count_results_by_field',
    'summarize_results'
]