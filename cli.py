"""
Command-line interface for the job search system.
"""

import argparse
import json
import sys
import os
from typing import Dict, List, Any, Optional

from .utils.field_mapping import FieldMapping
from .unified_search import UnifiedJobSearch

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Job Search System CLI")
    
    # Required arguments
    parser.add_argument('--data-source', required=True, help='Path to the data source file')
    
    # Optional arguments
    parser.add_argument('--source-type', choices=['csv', 'sqlite', 'hybrid'], 
                      help='Data source type (auto-detected if not specified)')
    parser.add_argument('--query', help='Search query (enter interactive mode if not provided)')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of results to return')
    parser.add_argument('--llm', action='store_true', help='Format output for LLM consumption')
    parser.add_argument('--explain', action='store_true', help='Explain how the query would be processed')
    parser.add_argument('--stats', action='store_true', help='Show statistics about the data source')
    parser.add_argument('--cache-dir', default='.cache', help='Directory for caching search data')
    parser.add_argument('--use-hybrid', action='store_true', help='Use hybrid search with SQLite + vector search')
    parser.add_argument('--no-hybrid', action='store_false', dest='use_hybrid', help='Disable hybrid search')
    parser.add_argument('--convert-csv-to-sqlite', action='store_true', help='Convert CSV to SQLite (one-time operation)')
    
    # Field mapping options
    parser.add_argument('--id-field', help='Field name for the primary identifier')
    parser.add_argument('--name-field', help='Field name for the name/title')
    parser.add_argument('--status-field', help='Field name for status information')
    parser.add_argument('--timestamp-fields', help='Comma-separated list of fields containing timestamps')
    parser.add_argument('--numeric-fields', help='Comma-separated list of fields containing numeric values')
    parser.add_argument('--text-fields', help='Comma-separated list of fields containing searchable text')
    
    # Set default for hybrid search
    parser.set_defaults(use_hybrid=True)
    
    return parser.parse_args()

def create_field_mapping(args) -> Optional[FieldMapping]:
    """Create field mapping from command line arguments."""
    # Check if any mapping arguments are provided
    mapping_args = [args.id_field, args.name_field, args.status_field, 
                   args.timestamp_fields, args.numeric_fields, args.text_fields]
    
    if not any(arg is not None for arg in mapping_args):
        return None
    
    # Parse list arguments
    timestamp_fields = args.timestamp_fields.split(',') if args.timestamp_fields else []
    numeric_fields = args.numeric_fields.split(',') if args.numeric_fields else []
    text_fields = args.text_fields.split(',') if args.text_fields else []
    
    # Create field mapping
    return FieldMapping(
        id_field=args.id_field or 'job_id',
        name_field=args.name_field or 'job_name',
        status_field=args.status_field or 'status',
        timestamp_fields=timestamp_fields,
        numeric_fields=numeric_fields,
        text_fields=text_fields
    )

def convert_csv_to_sqlite(csv_path: str, sqlite_path: Optional[str] = None):
    """Convert CSV file to SQLite database."""
    import pandas as pd
    import sqlite3
    
    # Default SQLite path if not provided
    if sqlite_path is None:
        sqlite_path = os.path.splitext(csv_path)[0] + '.db'
    
    print(f"Converting {csv_path} to SQLite database at {sqlite_path}")
    
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Create parent directory if needed
    os.makedirs(os.path.dirname(os.path.abspath(sqlite_path)), exist_ok=True)
    
    # Write to SQLite
    conn = sqlite3.connect(sqlite_path)
    df.to_sql('jobs', conn, if_exists='replace', index=False)
    
    # Create basic indexes
    cursor = conn.cursor()
    
    # Try to identify and index key fields
    for potential_id in ['job_id', 'id', 'task_id']:
        if potential_id in df.columns:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{potential_id} ON jobs ({potential_id})")
            break
    
    for potential_name in ['job_name', 'name', 'title']:
        if potential_name in df.columns:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{potential_name} ON jobs ({potential_name})")
            break
    
    for potential_status in ['status', 'state']:
        if potential_status in df.columns:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{potential_status} ON jobs ({potential_status})")
            break
    
    # Index timestamp fields
    for ts_field in ['created_at', 'updated_at', 'execution_time', 'start_time', 'end_time']:
        if ts_field in df.columns:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{ts_field} ON jobs ({ts_field})")
    
    conn.commit()
    conn.close()
    
    print(f"Conversion complete. Created database with {len(df)} records.")
    return sqlite_path

def interactive_mode(search: UnifiedJobSearch):
    """Enter interactive search mode."""
    print("\n=== Job Search System - Interactive Mode ===")
    print("Enter search queries or commands:")
    print("  - Type 'exit' or 'quit' to exit")
    print("  - Type 'stats' to show data source statistics")
    print("  - Type 'explain <query>' to explain how a query would be processed")
    print("  - Type 'llm <query>' to format results for LLM consumption")
    print("  - Type 'id <value>' to look up a record by ID")
    print("  - Type anything else to search")
    
    while True:
        try:
            user_input = input("\nSearch> ").strip()
            
            if not user_input:
                continue
                
            if user_input.lower() in ['exit', 'quit', 'q']:
                break
                
            if user_input.lower() == 'stats':
                # Show statistics
                stats = search.get_statistics()
                print(json.dumps(stats, indent=2))
                continue
                
            if user_input.lower().startswith('explain '):
                # Explain query
                query = user_input[8:].strip()
                explanation = search.explain_search(query)
                print(json.dumps(explanation, indent=2))
                continue
                
            if user_input.lower().startswith('llm '):
                # Format for LLM
                query = user_input[4:].strip()
                results = search.search(query)
                llm_format = search.format_for_llm(results, query)
                print(json.dumps(llm_format, indent=2))
                continue
                
            if user_input.lower().startswith('id '):
                # Look up by ID
                id_value = user_input[3:].strip()
                
                # Try to convert to appropriate type
                try:
                    id_value = int(id_value)
                except ValueError:
                    pass  # Keep as string
                
                record = search.get_record_by_id(id_value)
                if record:
                    print(json.dumps(record, indent=2))
                else:
                    print(f"No record found with ID: {id_value}")
                continue
            
            # Default: search
            print(f"Searching for: {user_input}")
            results = search.search(user_input)
            search.display_results(results)
            print(f"\nFound {len(results)} results")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {str(e)}")

def main():
    """Main function."""
    args = parse_args()
    
    try:
        # Handle CSV to SQLite conversion if requested
        if args.convert_csv_to_sqlite:
            if not args.data_source.lower().endswith('.csv'):
                print("Error: --convert-csv-to-sqlite requires a CSV file as data source")
                sys.exit(1)
                
            sqlite_path = convert_csv_to_sqlite(args.data_source)
            print(f"CSV converted to SQLite. You can now use: --data-source {sqlite_path}")
            return
        
        # Create field mapping
        field_mapping = create_field_mapping(args)
        
        # Create search interface
        search = UnifiedJobSearch(
            data_source=args.data_source,
            source_type=args.source_type,
            field_mapping=field_mapping,
            cache_dir=args.cache_dir,
            use_hybrid=args.use_hybrid
        )
        
        # Show statistics if requested
        if args.stats:
            stats = search.get_statistics()
            print(json.dumps(stats, indent=2))
            return
        
        # Process query or enter interactive mode
        if args.query:
            # Explain query if requested
            if args.explain:
                explanation = search.explain_search(args.query)
                print(json.dumps(explanation, indent=2))
                return
            
            # Execute search
            results = search.search(args.query, args.limit)
            
            # Display results
            if args.llm:
                llm_format = search.format_for_llm(results, args.query)
                print(json.dumps(llm_format, indent=2))
            else:
                search.display_results(results)
                print(f"\nFound {len(results)} results")
        else:
            # Enter interactive mode
            interactive_mode(search)
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()