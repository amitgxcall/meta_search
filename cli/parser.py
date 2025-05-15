def _add_config_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add configuration-specific arguments to a parser.
    
    Args:
        parser: ArgumentParser to add arguments to
    """
    parser.add_argument(
        '--show',
        action='store_true',
        help='Show the current configuration'
    )
    
    parser.add_argument(
        '--create',
        metavar='PATH',
        help='Create a default configuration file at the specified path'
    )
    
    parser.add_argument(
        '--locations',
        action='store_true',
        help='Show all possible configuration file locations'
    )
    
    parser.add_argument(
        '--validate',
        metavar='PATH',
        help='Validate a configuration file'
    )"""
Command-line interface argument parsing for meta_search.

This module provides the argument parsing functionality for the meta_search
command-line interface, including command definitions, argument specifications,
and help text.

Example:
    # Create a parser
    parser = create_parser()
    
    # Parse arguments
    args = parser.parse_args()
"""

import argparse
import os
import sys
from typing import Optional


def create_parser() -> argparse.ArgumentParser:
    """
    Create an argument parser for the meta_search CLI.
    
    Returns:
        Configured ArgumentParser instance
    """
    # Create main parser
    parser = argparse.ArgumentParser(
        description='Meta Search: Search across different data sources',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Add global arguments
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--config', '-c',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--create-config',
        metavar='PATH',
        help='Create a default configuration file at the specified path'
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(
        dest='command',
        title='commands',
        help='Command to run'
    )
    
    # Create the 'search' command
    search_parser = subparsers.add_parser(
        'search',
        help='Search for items',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    _add_search_arguments(search_parser)
    
    # Create the 'metadata' command
    metadata_parser = subparsers.add_parser(
        'metadata',
        help='Get metadata about a data source',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    _add_metadata_arguments(metadata_parser)
    
    # Create the 'config' command
    config_parser = subparsers.add_parser(
        'config',
        help='Manage configuration',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    _add_config_arguments(config_parser)
    
    return parser


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add common arguments to a parser.
    
    Args:
        parser: ArgumentParser to add arguments to
    """
    parser.add_argument(
        '--data-source',
        required=True,
        help='Path to the data source file'
    )
    
    parser.add_argument(
        '--id-field',
        default='id',
        help='Field to use as ID'
    )
    
    parser.add_argument(
        '--name-field',
        default='name',
        help='Field to use as name'
    )
    
    parser.add_argument(
        '--provider',
        choices=['csv', 'sqlite', 'json', 'hybrid'],
        default='hybrid',
        help='Provider type to use'
    )
    
    parser.add_argument(
        '--vector-index',
        help='Path to vector index file (for hybrid provider)'
    )
    
    parser.add_argument(
        '--build-index',
        action='store_true',
        help='Force rebuild of vector index (for hybrid provider)'
    )
    
    parser.add_argument(
        '--table-name',
        help='Table name for SQLite provider'
    )


def _add_search_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add search-specific arguments to a parser.
    
    Args:
        parser: ArgumentParser to add arguments to
    """
    # Add common arguments
    _add_common_arguments(parser)
    
    # Add search-specific arguments
    parser.add_argument(
        '--query',
        required=True,
        help='Search query'
    )
    
    parser.add_argument(
        '--vector-weight',
        type=float,
        default=0.5,
        help='Weight for vector search when using hybrid provider (0-1)'
    )
    
    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Use sequential combination for hybrid search'
    )
    
    parser.add_argument(
        '--max-results',
        type=int,
        default=10,
        help='Maximum number of results to return'
    )
    
    parser.add_argument(
        '--max-width',
        type=int,
        help='Maximum width for console output (auto-detect if not specified)'
    )
    
    parser.add_argument(
        '--output-format',
        choices=['console', 'json', 'csv'],
        default='console',
        help='Output format'
    )
    
    parser.add_argument(
        '--include-metadata',
        action='store_true',
        help='Include metadata fields (starting with _) in output'
    )


def _add_metadata_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add metadata-specific arguments to a parser.
    
    Args:
        parser: ArgumentParser to add arguments to
    """
    # Add common arguments
    _add_common_arguments(parser)


def parse_args(args: Optional[list] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Args:
        args: Arguments to parse (if None, use sys.argv)
        
    Returns:
        Parsed arguments
    """
    parser = create_parser()
    return parser.parse_args(args)


def validate_args(args: argparse.Namespace) -> bool:
    """
    Validate parsed arguments.
    
    Args:
        args: Parsed arguments
        
    Returns:
        True if arguments are valid, False otherwise
    """
    # Check if data source exists
    if not os.path.exists(args.data_source):
        print(f"Error: Data source not found: {args.data_source}")
        return False
    
    # Validate vector weight
    if hasattr(args, 'vector_weight') and (args.vector_weight < 0 or args.vector_weight > 1):
        print(f"Error: Vector weight must be between 0 and 1, got {args.vector_weight}")
        return False
    
    # Validate max results
    if hasattr(args, 'max_results') and args.max_results <= 0:
        print(f"Error: Max results must be positive, got {args.max_results}")
        return False
    
    # Validate max width
    if hasattr(args, 'max_width') and args.max_width is not None and args.max_width <= 0:
        print(f"Error: Max width must be positive, got {args.max_width}")
        return False
    
    return True