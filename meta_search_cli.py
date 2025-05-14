#!/usr/bin/env python
"""
Command-line interface for meta_search.
"""

import argparse
import sys
import os

# Add the parent directory to sys.path to allow absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now use absolute imports
from meta_search.utils.field_mapping import FieldMapping
from meta_search.search.engine import SearchEngine
from meta_search.providers.base import DataProvider

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Meta Search CLI')
    parser.add_argument('query', help='Search query')
    parser.add_argument('--provider', '-p', default='csv', 
                        choices=['csv', 'sqlite', 'json'],
                        help='Data provider to use')
    parser.add_argument('--source', '-s', required=True,
                        help='Data source path (file or directory)')
    parser.add_argument('--output', '-o', default='console',
                        choices=['console', 'json', 'csv'],
                        help='Output format')
    return parser.parse_args()

def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Initialize the search engine
    engine = SearchEngine()
    
    # Set up the data provider based on args
    if args.provider == 'csv':
        from meta_search.providers.csv_provider import CSVProvider
        provider = CSVProvider(args.source)
    elif args.provider == 'sqlite':
        from meta_search.providers.sqlite_provider import SQLiteProvider
        provider = SQLiteProvider(args.source)
    elif args.provider == 'json':
        from meta_search.providers.json_provider import JSONProvider
        provider = JSONProvider(args.source)
    else:
        print(f"Unknown provider: {args.provider}")
        sys.exit(1)
    
    # Register the provider with the engine
    engine.register_provider(provider)
    
    # Run the search
    results = engine.search(args.query)
    
    # Format the output
    if args.output == 'console':
        for idx, result in enumerate(results, 1):
            print(f"Result {idx}:")
            for key, value in result.items():
                print(f"  {key}: {value}")
            print()
    elif args.output == 'json':
        import json
        print(json.dumps(results, indent=2))
    elif args.output == 'csv':
        import csv
        import sys
        writer = csv.DictWriter(sys.stdout, fieldnames=results[0].keys() if results else [])
        writer.writeheader()
        writer.writerows(results)
    else:
        print(f"Unknown output format: {args.output}")
        sys.exit(1)

if __name__ == '__main__':
    main()