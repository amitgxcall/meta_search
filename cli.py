#!/usr/bin/env python3
"""
Command-line interface for meta_search.
"""

import argparse
import sys
import os

# Import directly from local directories
try:
    from utils.field_mapping import FieldMapping
    from search.engine import SearchEngine
except ImportError:
    # Fallback: add the current directory to path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from utils.field_mapping import FieldMapping
    from search.engine import SearchEngine

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Meta Search CLI')
    parser.add_argument('--data-source', required=True,
                        help='Path to the data source file')
    parser.add_argument('--id-field', required=True,
                        help='Field to use as ID')
    parser.add_argument('--name-field', required=True,
                        help='Field to use as name')
    parser.add_argument('--query', required=True,
                        help='Search query')
    return parser.parse_args()

def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    try:
        print(f"Searching for '{args.query}' in {args.data_source}")
        print(f"Using {args.id_field} as ID field and {args.name_field} as name field")
        
        # Initialize the search engine
        engine = SearchEngine()
        
        # Set up the data provider based on the file extension
        if args.data_source.endswith('.csv'):
            from providers.csv_provider import CSVProvider
            provider = CSVProvider(args.data_source)
        elif args.data_source.endswith('.sqlite') or args.data_source.endswith('.db'):
            from providers.sqlite_provider import SQLiteProvider
            provider = SQLiteProvider(args.data_source)
        elif args.data_source.endswith('.json'):
            from providers.json_provider import JSONProvider
            provider = JSONProvider(args.data_source)
        else:
            print(f"Unknown file type for: {args.data_source}")
            print("Supported formats: .csv, .sqlite, .db, .json")
            sys.exit(1)
        
        # Create field mapping
        field_mapping = FieldMapping()
        field_mapping.add_mapping('id', args.id_field)
        field_mapping.add_mapping('name', args.name_field)
        
        # Configure the provider with the field mapping
        provider.set_field_mapping(field_mapping)
        
        # Register the provider with the engine
        engine.register_provider(provider)
        
        # Run the search
        results = engine.search(args.query)
        
        # Display results
        if not results:
            print("No results found.")
        else:
            print(f"Found {len(results)} results:")
            for idx, result in enumerate(results, 1):
                print(f"\nResult {idx}:")
                for key, value in result.items():
                    print(f"  {key}: {value}")
        
    except ImportError as e:
        print(f"Import error: {e}")
        print("This could be due to missing module files. Make sure all required files exist.")
        print(f"Current directory: {os.getcwd()}")
        print(f"Python path: {sys.path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()