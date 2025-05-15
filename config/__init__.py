"""
Configuration module for meta_search.

This module provides configuration functionality for the meta_search system,
including settings loading, validation, and access.

Example:
    # Load configuration
    from meta_search.config import settings
    
    # Use configuration settings
    max_results = settings.get_search_setting('max_results', 10)
"""

from .settings import SearchConfig, load_config

# Create default configuration
default_config = load_config()

__all__ = ['SearchConfig', 'load_config', 'default_config']