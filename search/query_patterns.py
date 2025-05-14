"""
Command-line interface for meta_search with enhanced providers.
"""

import argparse
import sys
import os

# Add the current directory to sys.path to allow imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import necessary modules
from search.engine import SearchEngine
from utils.field_mapping import FieldMapping


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
    parser.add_argument('--provider', 
                        choices=['csv', 'enhanced-csv', 'sqlite', 'json', 'hybrid', 'sequential', 'enhanced'],
                        default='enhanced',
                        help='Provider type to use (default: enhanced)')
    parser.add_argument('--vector-weight', type=float, default=0.5,
                        help='Weight for vector search when using hybrid provider (0-1)')
    parser.add_argument('--vector-index', 
                        help='Path to vector index file (for hybrid provider)')
    parser.add_argument('--build-index', action='store_true',
                        help='Force rebuild of vector index (for hybrid provider)')
    parser.add_argument('--table-name',
                        help='Table name for SQLite provider')
    return parser.parse_args()

def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    try:
        print(f"Searching for '{args.query}' in {args.data_source}")
        print(f"Using {args.id_field} as ID field and {args.name_field} as name field")
        print(f"Provider: {args.provider}")
        
        # Initialize the search engine
        engine = SearchEngine()
        
        # Create field mapping
        field_mapping = FieldMapping()
        field_mapping.add_mapping('id', args.id_field)
        field_mapping.add_mapping('name', args.name_field)
        
        # Set up the appropriate provider
        if args.provider == 'csv':
            from providers.csv_provider import CSVProvider
            provider = CSVProvider(args.data_source)
        elif args.provider == 'enhanced-csv':
            from providers.enhanced_csv_provider import EnhancedCSVProvider
            provider = EnhancedCSVProvider(args.data_source)
        elif args.provider == 'sqlite':
            from providers.sqlite_provider import SQLiteProvider
            provider = SQLiteProvider(args.data_source, args.table_name)
        elif args.provider == 'json':
            from providers.json_provider import JSONProvider
            provider = JSONProvider(args.data_source)
        elif args.provider == 'sequential':
            from providers.hybrid_provider import HybridProvider
            provider = HybridProvider(args.data_source, args.vector_index, args.table_name)
            print(f"Using sequential hybrid provider (structured results first, then vector results)")
        elif args.provider == 'enhanced':
            from providers.hybrid_provider import HybridProvider
            provider = HybridProvider(args.data_source, args.vector_index, args.table_name)
            print(f"Using enhanced sequential provider with smart query parsing")
        elif args.provider == 'hybrid':
            from providers.hybrid_provider import HybridProvider
            provider = HybridProvider(args.data_source, args.vector_index, args.table_name)
            print(f"Using hybrid provider with vector weight: {args.vector_weight}")
        else:
            print(f"Unknown provider type: {args.provider}")
            sys.exit(1)
        
        # Set field mapping
        provider.set_field_mapping(field_mapping)
        
        # For hybrid providers, rebuild index if requested
        if args.provider in ('hybrid', 'sequential', 'enhanced') and args.build_index:
            print("Rebuilding vector index...")
            provider.build_vector_index()
        
        # Register the provider with the engine
        engine.register_provider(provider)
        
        # Run the search with appropriate parameters
        if args.provider == 'hybrid':
            # We need to pass the hybrid_weight to the search method
            # First get the search method's reference
            original_search = provider.search
            
            # Override the search method to include the weight
            def search_with_weight(query):
                return original_search(query, args.vector_weight)
            
            # Replace the method
            provider.search = search_with_weight
        
        # Execute the search
        results = engine.search(args.query)
        
        # Display results
        if not results:
            print("No results found.")
        else:
            print(f"\nFound {len(results)} results:")
            
            # Handle separators for the sequential hybrid provider
            result_index = 1
            for result in results:
                # Check if this is a separator item
                if result.get("_separator", False):
                    print(f"\n--- {result.get('_message', 'Vector search results below:')} ---\n")
                    continue
                
                print(f"\nResult {result_index}:")
                result_index += 1
                
                # Format scores for better readability if they exist
                if "_score" in result:
                    result["_score"] = f"{result['_score']:.4f}"
                if "_structured_score" in result:
                    result["_structured_score"] = f"{result['_structured_score']:.4f}"
                if "_vector_score" in result:
                    result["_vector_score"] = f"{result['_vector_score']:.4f}"
                if "_combined_score" in result:
                    result["_combined_score"] = f"{result['_combined_score']:.4f}"
                
                # Print information about the result type and matched terms
                result_type = result.get("_result_type", "unknown")
                if result_type == "structured":
                    match_info = f"[EXACT MATCH] {result.get('name', '')}"
                    if "_matched_terms" in result:
                        match_info += f" (Matched: {', '.join(result['_matched_terms'])})"
                    print(f"  {match_info}")
                elif result_type == "vector":
                    print(f"  [SEMANTIC MATCH] {result.get('name', '')}")
                
                # Print all fields
                for key, value in result.items():
                    # Skip internal fields starting with underscore
                    if not key.startswith("_"):
                        print(f"  {key}: {value}")
        
    except ImportError as e:
        print(f"Import error: {e}")
        print("This could be due to missing module files. Make sure all required files exist.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()