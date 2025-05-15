"""
Command-line interface package for meta_search.

This package provides a command-line interface for interacting with the
meta_search system, allowing users to search, query, and manage data
from various sources through a consistent interface.

Example:
    # Import the CLI
    from meta_search.cli import main
    
    # Run the CLI
    main()
"""

# These imports need to be absolute, not relative
from cli.commands import main, run_search, run_metadata
from cli.parsers import create_parser

__all__ = [
    'main',
    'run_search',
    'run_metadata',
    'create_parser'
]