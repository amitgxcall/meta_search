#!/usr/bin/env python3
"""
Command-line interface for meta_search.
"""

import argparse
import sys
import os
import re
from typing import Dict, List, Any, Optional

# Add the parent directory to sys.path to allow absolute imports
# This lets you run the script directly with python3 cli.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Now use package imports without the meta_search prefix
from utils.field_mapping import FieldMapping
from search.engine import SearchEngine
from providers.base import DataProvider
from providers.csv_provider import CSVProvider
from providers.sqlite_provider import SQLiteProvider
from providers.hybrid_provider import HybridProvider

def extract_id_from_query(query):
    """
    Extracts an ID from a query string if it appears to be an ID search.
    
    Args:
        query: The search query
        
    Returns:
        The ID string if found, None otherwise
    """
    # Pattern for "id X", "ID: X", "job id X", etc.
    id_patterns = [
        r'(?:^|\s)id\s*[:=]?\s*(\d+)',
        r'(?:^|\s)job\s+id\s*[:=]?\s*(\d+)',
        r'(?:^|\s)job[-_]id\s*[:=]?\s*(\d+)',
        r'(?:^|\s)#(\d+)',
        r'(?:^|\s)number\s*[:=]?\s*(\d+)',
        r'(?:^|\s)(\d{4,})\s*$'  # Standalone number (at least 4 digits)
    ]
    
    for pattern in id_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None

def is_counting_query(query):
    """
    Determine if a query is asking for a count.
    
    Args:
        query: Query string
        
    Returns:
        True if the query is about counting, False otherwise
    """
    query_lower = query.lower()
    
    # Keywords that indicate counting queries
    counting_keywords = [
        'how many', 'count', 'total', 'number of', 'tally', 
        'sum of', 'sum up', 'calculate', 'compute'
    ]
    
    # Check for counting keywords
    if any(keyword in query_lower for keyword in counting_keywords):
        return True
        
    # Advanced pattern matching for counting queries
    counting_patterns = [
        r'\bhow\s+many\b',
        r'\bcount(?:ing)?\b',
        r'\btotal\s+(?:number|amount|count)?\b',
        r'\bnumber\s+of\b',
    ]
    
    return any(re.search(pattern, query_lower) for pattern in counting_patterns)

def extract_count_target(query):
    """
    Extract what we're counting from the query.
    
    Args:
        query: Query string
        
    Returns:
        String describing what's being counted
    """
    query_lower = query.lower()
    
    # Try to extract the target object being counted
    # Simple pattern: "how many X" or "count X" or "total X"
    patterns = [
        r'how\s+many\s+(.*?)(?:\s+are|\s+with|\s+in|\s+is|\s+do|\?|$)',
        r'count\s+(?:of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)',
        r'total\s+(?:number\s+of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)',
        r'number\s+of\s+(.*?)(?:\s+in|\s+with|\s+that|\?|$)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, query_lower)
        if match:
            return match.group(1).strip()
    
    # Fallback: look for keywords related to jobs
    job_related_words = ['job', 'jobs', 'task', 'tasks', 'process', 'processes']
    for word in job_related_words:
        if word in query_lower:
            return word
            
    return "items"  # Default if we can't determine what to count

def extract_filters_from_query(query):
    """
    Extract filter criteria from the query.
    
    Args:
        query: Query string
        
    Returns:
        Dictionary of field:value filters
    """
    # Extract explicit field:value patterns
    filters = {}
    field_value_pattern = r'(\w+)[:=]"([^"]+)"|(\w+)[:=](\S+)'
    
    for match in re.finditer(field_value_pattern, query):
        field1, value1, field2, value2 = match.groups()
        field = field1 if field1 else field2
        value = value1 if value1 else value2
        filters[field] = value
    
    # Look for "with [field] [value]" patterns
    with_pattern = r'with\s+(\w+(?:\s+\w+)*)\s+(\w+(?:\s+\w+)*)'
    query_lower = query.lower()
    for match in re.finditer(with_pattern, query_lower):
        field_name, field_value = match.groups()
        
        # Handle multi-word field names
        if field_name == "job name":
            field_name = "job_name"
        elif field_name == "job id":
            field_name = "job_id"
        
        # Normalize field names (convert spaces to underscores)
        field_name = field_name.replace(" ", "_")
        
        # Add to filters
        filters[field_name] = field_value
    
    # Extract special keywords
    keyword_mapping = {
        'failed': {'status': 'failed'},
        'success': {'status': 'success'},
        'running': {'status': 'running'},
        'completed': {'status': 'completed'},
        'pending': {'status': 'pending'},
        'high': {'priority': 'high'},
        'medium': {'priority': 'medium'},
        'low': {'priority': 'low'},
        'critical': {'priority': 'critical'}
    }
    
    for keyword, filter_dict in keyword_mapping.items():
        if re.search(r'\b' + keyword + r'\b', query_lower):
            filters.update(filter_dict)
    
    return filters

def preprocess_counting_query(query):
    """
    Preprocess a counting query to create a standard search query.
    
    Args:
        query: The counting query
        
    Returns:
        A modified query for standard search
    """
    # Remove counting keywords
    counting_keywords = [
        'how many', 'count', 'total', 'number of', 'tally', 
        'sum of', 'sum up', 'calculate', 'compute'
    ]
    
    search_query = query.lower()
    for keyword in counting_keywords:
        search_query = search_query.replace(keyword, '').strip()
    
    # Remove question marks
    search_query = search_query.replace('?', '').strip()
    
    # Remove filler words
    filler_words = ['are', 'is', 'there', 'do', 'we', 'have', 'the']
    for word in filler_words:
        search_query = re.sub(r'\b' + word + r'\b', '', search_query)
    
    return search_query.strip()

def filter_results_by_criteria(results, filters, id_field, name_field):
    """
    Filter results based on extracted criteria.
    
    Args:
        results: List of search results
        filters: Dictionary of field:value filters
        id_field: Field name for ID
        name_field: Field name for name
        
    Returns:
        Filtered results
    """
    if not filters:
        return results
    
    filtered_results = []
    for result in results:
        match = True
        for field, value in filters.items():
            # Map field names if needed
            if field == "job_name":
                field = name_field
            elif field == "job_id":
                field = id_field
            
            if field in result:
                # Try exact match first
                if str(result[field]).lower() != str(value).lower():
                    # Also try contains for text fields
                    if not isinstance(value, str) or value.lower() not in str(result[field]).lower():
                        match = False
                        break
            else:
                match = False
                break
        
        if match:
            filtered_results.append(result)
    
    return filtered_results

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
                        choices=['csv', 'sqlite', 'json', 'hybrid'],
                        default='hybrid',
                        help='Provider type to use (default: hybrid)')
    parser.add_argument('--vector-weight', type=float, default=0.5,
                        help='Weight for vector search when using hybrid provider (0-1)')
    parser.add_argument('--vector-index', 
                        help='Path to vector index file (for hybrid provider)')
    parser.add_argument('--build-index', action='store_true',
                        help='Force rebuild of vector index (for hybrid provider)')
    parser.add_argument('--table-name',
                        help='Table name for SQLite provider')
    parser.add_argument('--max-results', type=int, default=10,
                        help='Maximum number of results to return (default: 10)')
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
            provider = CSVProvider(args.data_source)
        elif args.provider == 'sqlite':
            provider = SQLiteProvider(args.data_source, args.table_name)
        elif args.provider == 'json':
            # Use local import without meta_search prefix
            from providers.json_provider import JSONProvider
            provider = JSONProvider(args.data_source)
        elif args.provider == 'hybrid':
            provider = HybridProvider(args.data_source, args.vector_index, args.table_name)
            print(f"Using hybrid provider with vector weight: {args.vector_weight}")
        else:
            print(f"Unknown provider type: {args.provider}")
            sys.exit(1)
        
        # Set field mapping
        provider.set_field_mapping(field_mapping)
        
        # For hybrid providers, rebuild index if requested
        if args.provider == 'hybrid' and args.build_index:
            print("Rebuilding vector index...")
            provider.build_vector_index()
        
        # Register the provider with the engine
        engine.register_provider(provider)
        
        # Check if this is an ID query
        id_value = extract_id_from_query(args.query)
        if id_value:
            print(f"Detected ID search for: {id_value}")
            # Try to get the item directly by ID
            item = provider.get_by_id(id_value)
            if item:
                print(f"Found exact match for ID {id_value}")
                # Format and display the result
                print("\nDirect ID match:")
                for key, value in item.items():
                    if not key.startswith("_"):
                        print(f"  {key}: {value}")
                sys.exit(0)
            else:
                print(f"No exact match found for ID {id_value}, falling back to standard search")
        
        # Check if this is a counting query
        if is_counting_query(args.query):
            print(f"Detected counting query: '{args.query}'")
            
            # Extract what we're counting and any filters
            count_target = extract_count_target(args.query)
            filters = extract_filters_from_query(args.query)
            
            # Adjust vector weight for exact matching
            vector_weight = args.vector_weight
            if filters and any(field.lower() in ["job_name", args.name_field.lower()] for field in filters):
                # If searching for specific job name, prioritize exact matches
                vector_weight = 0.2  # Lower weight for vector component
                print(f"Adjusting vector weight to {vector_weight} for field-specific search")
            
            # Get search terms by removing counting keywords
            search_query = preprocess_counting_query(args.query)
            
            # Run the search with appropriate parameters
            if args.provider == 'hybrid':
                results = provider.search(search_query, vector_weight)
            else:
                results = provider.search(search_query)
            
            # Filter results by criteria
            filtered_results = filter_results_by_criteria(results, filters, args.id_field, args.name_field)
            
            # Handle specific query for "with job name X"
            job_name_value = None
            if 'job_name' in filters:
                job_name_value = filters['job_name']
                # More precise filtering for job name
                filtered_results = [r for r in filtered_results if job_name_value.lower() in str(r.get(args.name_field, '')).lower()]
                
                print(f"\nFound {len(filtered_results)} total jobs with job name containing '{job_name_value}'")
                
                if filtered_results:
                    print("\nMatches:")
                    for i, result in enumerate(filtered_results[:5]):
                        job_name = result.get(args.name_field, 'Unknown')
                        job_id = result.get(args.id_field, 'Unknown')
                        print(f"  {i+1}. {job_name} (ID: {job_id})")
                
                sys.exit(0)
            
            # Display count results
            print(f"\nFound {len(filtered_results)} total {count_target}")
            
            if filters:
                print(f"Filters applied: {filters}")
            
            if filtered_results:
                print("\nSample matches:")
                for i, result in enumerate(filtered_results[:5]):
                    job_name = result.get(args.name_field, 'Unknown')
                    job_id = result.get(args.id_field, 'Unknown')
                    print(f"  {i+1}. {job_name} (ID: {job_id})")
            
            # Exit after showing count result
            sys.exit(0)
        
        # Regular search (non-counting, non-ID search)
        if args.provider == 'hybrid':
            # Use standard hybrid weight
            results = provider.search(args.query, args.vector_weight)
        else:
            results = provider.search(args.query)
        
        # Display results
        if not results:
            print("No results found.")
        else:
            print(f"\nFound {len(results)} results:")
            
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