"""
Command-line interface for meta_search with basic but functional providers.
"""

import argparse
import sys
import os

# Add the current directory to sys.path to allow imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import necessary modules
from search.engine import SearchEngine
from utils.field_mapping import FieldMapping
from providers.csv_provider import CSVProvider

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
        
        # Create field mapping
        field_mapping = FieldMapping()
        field_mapping.add_mapping('id', args.id_field)
        field_mapping.add_mapping('name', args.name_field)
        
        # Using simple CSV provider for now
        provider = CSVProvider(args.data_source)
        
        # Set field mapping
        provider.set_field_mapping(field_mapping)
        
        # Check if the query looks like an ID search and handle it specially
        if "id" in args.query.lower() and any(char.isdigit() for char in args.query):
            # Extract the ID from the query (simple version)
            digits = ''.join(char for char in args.query if char.isdigit())
            if digits:
                print(f"Detected ID search for: {digits}")
                # Look for exact ID match
                for item in provider.data:
                    if item.get(args.id_field) == digits:
                        result = provider.map_fields(item.copy())
                        print(f"\nExact match found for ID {digits}:")
                        for key, value in result.items():
                            print(f"  {key}: {value}")
                        sys.exit(0)
        
        # Register the provider with the engine for normal search
        engine.register_provider(provider)
        
        # Execute the search
        results = engine.search(args.query)
        
        # Display results
        if not results:
            print("No results found.")
        else:
            print(f"\nFound {len(results)} results:")
            for idx, result in enumerate(results, 1):
                print(f"\nResult {idx}:")
                for key, value in result.items():
                    if not key.startswith("_"):  # Skip internal fields
                        print(f"  {key}: {value}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()