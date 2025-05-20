#!/usr/bin/env python3
"""
Super simple CSV search script with explicit argument handling.
This script doesn't use argparse to avoid any issues with argument parsing.

Usage:
    python cli.py --data-source=PATH_TO_CSV --query=SEARCH_QUERY
    
Example:
    python cli.py --data-source=./meta-data/products.csv --query="organic"
    python cli.py --data-source=./meta-data/products.csv --query="price>100"
"""

import os
import sys
import csv
import re
import logging
import time
from typing import Dict, List, Any, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pre-compile regular expressions for better performance
ID_PATTERNS = [
    re.compile(r'(?:^|\s)id\s*[:=]?\s*(\d+)', re.IGNORECASE),
    re.compile(r'(?:^|\s)item\s+id\s*[:=]?\s*(\d+)', re.IGNORECASE),
    re.compile(r'(?:^|\s)item[-_]id\s*[:=]?\s*(\d+)', re.IGNORECASE),
    re.compile(r'(?:^|\s)#(\d+)', re.IGNORECASE),
    re.compile(r'(?:^|\s)number\s*[:=]?\s*(\d+)', re.IGNORECASE),
    re.compile(r'(?:^|\s)(\d{4,})\s*$', re.IGNORECASE)
]

FIELD_VALUE_PATTERN = re.compile(r'(\w+)[:=]"([^"]+)"|(\w+)[:=](\S+)')
COMPARISON_PATTERN = re.compile(r'(\w+)\s*(<=|>=|<|>|=|!=)\s*(\d+(?:\.\d+)?)')
COUNTING_PATTERNS = [
    re.compile(r'\bhow\s+many\b', re.IGNORECASE),
    re.compile(r'\bcount(?:ing)?\b', re.IGNORECASE),
    re.compile(r'\btotal\s+(?:number|amount|count)?\b', re.IGNORECASE),
    re.compile(r'\bnumber\s+of\b', re.IGNORECASE),
]
COUNT_TARGET_PATTERNS = [
    re.compile(r'how\s+many\s+(.*?)(?:\s+are|\s+with|\s+in|\s+is|\s+do|\?|$)', re.IGNORECASE),
    re.compile(r'count\s+(?:of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)', re.IGNORECASE),
    re.compile(r'total\s+(?:number\s+of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)', re.IGNORECASE),
    re.compile(r'number\s+of\s+(.*?)(?:\s+in|\s+with|\s+that|\?|$)', re.IGNORECASE)
]


def parse_arguments():
    """Parse command line arguments manually."""
    args = {}
    
    # Process each argument
    for arg in sys.argv[1:]:
        if '=' in arg:
            # Handle --key=value format
            key, value = arg.split('=', 1)
            key = key.lstrip('-')
            args[key] = value
        elif arg.startswith('--') and len(sys.argv) > sys.argv.index(arg) + 1:
            # Handle --key value format
            key = arg.lstrip('-')
            next_arg = sys.argv[sys.argv.index(arg) + 1]
            if not next_arg.startswith('--'):
                args[key] = next_arg
    
    # Check required arguments
    if 'data-source' not in args:
        logger.error("Missing required argument --data-source")
        print(f"Usage: {sys.argv[0]} --data-source=PATH_TO_CSV --query=SEARCH_QUERY")
        sys.exit(1)
    
    if 'query' not in args:
        logger.error("Missing required argument --query")
        print(f"Usage: {sys.argv[0]} --data-source=PATH_TO_CSV --query=SEARCH_QUERY")
        sys.exit(1)
    
    return args


def search_csv(csv_path, query, limit=10):
    """
    Search a CSV file for matching rows with improved performance.
    
    Args:
        csv_path: Path to the CSV file
        query: The search query
        limit: Maximum number of results to return
        
    Returns:
        List of matching rows
    """
    if not os.path.exists(csv_path):
        logger.error(f"Error: File not found: {csv_path}")
        return []
    
    try:
        # Start timing for performance measurement
        start_time = time.time()
        
        # Read the CSV file efficiently
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames or []
            
            # Use list comprehension for more efficient loading
            rows = [row for row in reader]
        
        logger.info(f"Loaded CSV with {len(rows)} rows and {len(headers)} columns in {time.time() - start_time:.4f} seconds")
        start_time = time.time()
        
        # Detect field types based on headers
        id_field = find_best_match(headers, ['id', 'uuid', 'key', 'item_id', 'product_id', 'user_id', 'customer_id', 'document_id', 'event_id'])
        name_field = find_best_match(headers, ['name', 'title', 'label', 'summary', 'description', 'product_name', 'full_name', 'event_name'])
        status_field = find_best_match(headers, ['status', 'state', 'condition', 'type', 'inventory_status', 'account_status', 'severity'])
        
        logger.info(f"Detected fields - ID: {id_field}, Name: {name_field}, Status: {status_field}")
        
        # Check if query is structured using compiled patterns
        is_structured = bool(FIELD_VALUE_PATTERN.search(query) or COMPARISON_PATTERN.search(query))
        
        if is_structured:
            logger.info(f"Detected structured query: '{query}'")
            results = parse_structured_query(rows, query, id_field, name_field, status_field)
        else:
            logger.info(f"Performing text search for: '{query}'")
            results = search_text(rows, query, id_field, name_field, status_field)
        
        # Sort by score (use key function for better performance)
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        logger.info(f"Search completed in {time.time() - start_time:.4f} seconds, found {len(results)} results")
        
        # Apply limit
        return results[:limit]
        
    except Exception as e:
        logger.error(f"Error searching CSV: {e}", exc_info=True)
        return []


def find_best_match(headers, candidates):
    """Find the best match for a field from a list of candidates."""
    # Try exact matches first
    for candidate in candidates:
        if candidate in headers:
            return candidate
    
    # Try case-insensitive matches
    headers_lower = [h.lower() for h in headers]
    for candidate in candidates:
        if candidate.lower() in headers_lower:
            idx = headers_lower.index(candidate.lower())
            return headers[idx]
    
    # Try partial matches
    for header in headers:
        for candidate in candidates:
            if candidate.lower() in header.lower():
                return header
    
    # No match found
    return None


def search_text(rows, query, id_field, name_field, status_field):
    """Optimized text search implementation."""
    query_lower = query.lower()
    query_words = set(query_lower.split())
    results = []
    
    # Precompute field weights for better performance
    field_weights = {}
    if name_field:
        field_weights[name_field] = 3.0  # Higher weight for name field
    if status_field:
        field_weights[status_field] = 2.0  # Medium weight for status field
    # Default weight is 1.0 for other fields
    
    for row in rows:
        # Use more efficient scoring system
        score = 0
        matched_fields = set()
        
        for field, value in row.items():
            if not value:
                continue
            
            # Convert to string and lowercase only once
            value_str = str(value).lower()
            field_weight = field_weights.get(field, 1.0)
            
            # Exact match gets highest score
            if query_lower == value_str:
                score += 10 * field_weight
                matched_fields.add(field)
            # Partial match gets medium score
            elif query_lower in value_str:
                score += 5 * field_weight
                matched_fields.add(field)
            # Word match gets lowest score - use efficient set intersection
            elif query_words.intersection(set(value_str.split())):
                score += 1 * field_weight
                matched_fields.add(field)
        
        if score > 0:
            result = row.copy()
            result['_score'] = score
            result['_match_type'] = 'text'
            result['_matched_fields'] = list(matched_fields)
            
            # Add standard field names
            if id_field and id_field in row:
                result['id'] = row[id_field]
            if name_field and name_field in row:
                result['name'] = row[name_field]
            if status_field and status_field in row:
                result['status'] = row[status_field]
                
            results.append(result)
    
    return results


def parse_structured_query(rows, query, id_field, name_field, status_field):
    """Optimized structured query parsing."""
    # Start with all rows
    filtered_rows = rows.copy()
    
    # Process field:value patterns
    for match in FIELD_VALUE_PATTERN.finditer(query):
        field1, value1, field2, value2 = match.groups()
        field = field1 if field1 else field2
        value = value1 if value1 else value2
        
        # Filter data efficiently with list comprehension
        filtered_rows = [
            row for row in filtered_rows 
            if field in row and str(row[field]).lower() == value.lower()
        ]
    
    # Process comparison patterns
    for match in COMPARISON_PATTERN.finditer(query):
        field, operator, value = match.groups()
        
        try:
            # Convert value to appropriate type
            num_value = float(value)
            
            # Apply appropriate operator
            if operator == '>':
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) > num_value
                ]
            elif operator == '<':
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) < num_value
                ]
            elif operator == '>=':
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) >= num_value
                ]
            elif operator == '<=':
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) <= num_value
                ]
            elif operator == '=':
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) == num_value
                ]
            elif operator == '!=':
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) != num_value
                ]
        except (ValueError, TypeError):
            # Skip this condition if conversion fails
            logger.warning(f"Could not convert value '{value}' to number for field '{field}'")
            continue
    
    # Format results
    results = []
    for row in filtered_rows:
        result = row.copy()
        result['_score'] = 1.0  # Base score for exact matches
        result['_match_type'] = 'structured'
        
        # Add standard field names
        if id_field and id_field in row:
            result['id'] = row[id_field]
        if name_field and name_field in row:
            result['name'] = row[name_field]
        if status_field and status_field in row:
            result['status'] = row[status_field]
            
        results.append(result)
    
    return results


def extract_id_from_query(query):
    """
    Extracts an ID from a query string if it appears to be an ID search.
    Uses pre-compiled regex patterns for better performance.
    """
    for pattern in ID_PATTERNS:
        match = pattern.search(query)
        if match:
            return match.group(1)
    
    return None


def is_counting_query(query):
    """
    Determine if a query is asking for a count.
    Uses pre-compiled regex patterns for better performance.
    """
    query_lower = query.lower()
    
    # Keywords that indicate counting queries
    counting_keywords = [
        'how many', 'count', 'total', 'number of', 'tally', 
        'sum of', 'sum up', 'calculate', 'compute'
    ]
    
    # Check for counting keywords - faster to do a simple check first
    if any(keyword in query_lower for keyword in counting_keywords):
        return True
    
    # Use pre-compiled patterns for more complex checks
    return any(pattern.search(query_lower) for pattern in COUNTING_PATTERNS)


def extract_count_target(query):
    """
    Extract what we're counting from the query.
    Uses pre-compiled regex patterns for better performance.
    """
    query_lower = query.lower()
    
    # Try to extract the target object being counted using pre-compiled patterns
    for pattern in COUNT_TARGET_PATTERNS:
        match = pattern.search(query_lower)
        if match:
            return match.group(1).strip()
    
    # Fallback: look for keywords related to common items
    common_items = ['item', 'items', 'record', 'records', 'entry', 'entries', 
                   'document', 'documents', 'result', 'results']
    for word in common_items:
        if word in query_lower:
            return word
            
    return "items"  # Default if we can't determine what to count


def extract_filters(query):
    """Extract filter criteria from the query."""
    # Extract explicit field:value patterns
    filters = {}
    
    # Use pre-compiled pattern for better performance
    for match in FIELD_VALUE_PATTERN.finditer(query):
        field1, value1, field2, value2 = match.groups()
        field = field1 if field1 else field2
        value = value1 if value1 else value2
        filters[field] = value
    
    # Extract comparison operators using pre-compiled pattern
    for match in COMPARISON_PATTERN.finditer(query):
        field, operator, value = match.groups()
        
        # Convert numeric value
        try:
            if '.' in value:
                value = float(value)
            else:
                value = int(value)
        except ValueError:
            continue
        
        # Create operator mapping
        op_map = {
            '<': 'lt',
            '>': 'gt',
            '<=': 'lte',
            '>=': 'gte',
            '=': 'eq',
            '!=': 'neq'
        }
        
        if operator in op_map:
            # Format for filter
            if field not in filters:
                filters[field] = {}
            
            if isinstance(filters[field], dict):
                filters[field][op_map[operator]] = value
            else:
                # Convert to dict if it's a simple value
                filters[field] = {op_map[operator]: value}
    
    return filters


def preprocess_counting_query(query):
    """Optimize preprocessing of counting queries."""
    # Create set of counting keywords for faster checking
    counting_keywords = {
        'how many', 'count', 'total', 'number of', 'tally', 
        'sum of', 'sum up', 'calculate', 'compute'
    }
    
    search_query = query.lower()
    
    # Remove counting keywords
    for keyword in counting_keywords:
        search_query = search_query.replace(keyword, '').strip()
    
    # Remove question marks and other noise with a single operation
    search_query = re.sub(r'[?.,]', '', search_query).strip()
    
    # Remove filler words efficiently with a single regex
    search_query = re.sub(r'\b(are|is|there|do|we|have|the)\b', '', search_query, flags=re.IGNORECASE)
    
    # Remove "group by" clause
    search_query = re.sub(r'group by\s+\w+', '', search_query, flags=re.IGNORECASE)
    
    return search_query.strip()


def filter_results_by_criteria(results, filters, id_field, name_field):
    """Filter results based on extracted criteria - optimized implementation."""
    if not filters:
        return results
    
    filtered_results = []
    
    # Fast path for common case of single field-value match
    if len(filters) == 1 and isinstance(next(iter(filters.values())), str):
        field, value = next(iter(filters.items()))
        
        # Map field names if needed
        if field == "job_name":
            field = name_field
        elif field == "job_id":
            field = id_field
        
        value_lower = str(value).lower()
        
        # Use list comprehension for better performance
        return [
            r for r in results if 
            field in r and 
            (str(r[field]).lower() == value_lower or value_lower in str(r[field]).lower())
        ]
    
    # More complex filtering
    for result in results:
        match = True
        
        for field, value in filters.items():
            # Map field names if needed
            if field == "job_name":
                field = name_field
            elif field == "job_id":
                field = id_field
            
            # Handle results with potentially nested structure
            current_result = result.get('job_details', result)
            
            if field in current_result:
                field_value = current_result[field]
                
                # Handle different value types
                if isinstance(value, dict):
                    # Operators (gt, lt, etc.)
                    for op, op_value in value.items():
                        if not _apply_operator(op, field_value, op_value):
                            match = False
                            break
                else:
                    # Direct comparison
                    value_str = str(value).lower()
                    field_str = str(field_value).lower()
                    
                    if value_str != field_str and value_str not in field_str:
                        match = False
                        break
            else:
                # Field not found in result
                match = False
                break
        
        if match:
            filtered_results.append(result)
    
    return filtered_results


def _apply_operator(op, field_value, op_value):
    """Apply a comparison operator - optimized implementation."""
    try:
        # Convert values to numbers if possible
        if isinstance(field_value, str) and field_value.replace('.', '', 1).isdigit():
            field_value = float(field_value) if '.' in field_value else int(field_value)
        
        # Direct comparison lookup instead of multiple if/elif
        operators = {
            'gt': lambda a, b: a > b,
            'lt': lambda a, b: a < b,
            'gte': lambda a, b: a >= b,
            'lte': lambda a, b: a <= b,
            'eq': lambda a, b: a == b,
            'neq': lambda a, b: a != b,
            'contains': lambda a, b: str(b).lower() in str(a).lower() if isinstance(a, str) else False
        }
        
        if op in operators:
            return operators[op](field_value, op_value)
        
        return False
    except (ValueError, TypeError):
        return False


def display_results(results, max_width=120):
    """Display search results with optimized formatting."""
    if not results:
        print("No results found.")
        return
    
    # Calculate column widths more efficiently
    id_width = 10
    name_width = max(20, (max_width // 4))  # Allow more space for name
    status_width = 15
    
    # Calculate details width
    details_width = max_width - id_width - name_width - status_width - 8
    
    # Print header
    header = f"{'ID':<{id_width}} | {'Name':<{name_width}} | {'Status':<{status_width}} | Details"
    print("\n" + header)
    print("-" * max_width)
    
    # Print each result more efficiently
    for result in results:
        # Check if this is a separator item
        if result.get("_separator", False):
            print(f"\n--- {result.get('_message', 'Additional results:')} ---\n")
            continue
        
        # Extract primary fields first - prefer result's normalized fields
        id_value = result.get('id', '')
        name_value = result.get('name', '')
        status_value = result.get('status', '')
        
        # If normalized fields not available, try to extract based on type
        if not id_value and 'id' in result:
            id_value = str(result['id'])
        
        if not name_value and 'name' in result:
            name_value = str(result['name'])
            
        if not status_value and 'status' in result:
            status_value = str(result['status'])
        
        # Truncate safely
        id_value = id_value[:id_width] if id_value else ''
        name_value = name_value[:name_width] if name_value else ''
        status_value = status_value[:status_width] if status_value else ''
        
        # Build details text efficiently
        details = []
        
        # Skip fields already displayed and metadata fields
        skip_fields = {'id', 'name', 'status', '_score', '_match_type', '_separator', '_message'}
        
        # Add match info first if available
        match_type = result.get('_match_type')
        score = result.get('_score')
        
        if match_type and score is not None:
            score_str = f"{score:.2f}" if isinstance(score, float) else str(score)
            details.append(f"match: {match_type} ({score_str})")
        
        # Add selected fields (limited to 5 for readability)
        field_count = 0
        for field, value in sorted(result.items()):
            if field in skip_fields or field.startswith('_'):
                continue
                
            field_count += 1
            if field_count > 5:  # Limit number of fields shown
                details.append("...")
                break
                
            # Format value based on type
            if isinstance(value, (int, float)):
                value_str = f"{value}"
            elif value is None:
                value_str = "null"
            else:
                value_str = str(value)
                # Truncate long values
                if len(value_str) > 30:
                    value_str = value_str[:27] + "..."
                    
            details.append(f"{field}: {value_str}")
        
        # Join details efficiently
        details_text = ", ".join(details)
        if len(details_text) > details_width:
            details_text = details_text[:details_width-3] + "..."
        
        # Print row
        print(f"{id_value:<{id_width}} | {name_value:<{name_width}} | {status_value:<{status_width}} | {details_text}")


def handle_id_query(query, csv_path):
    """Handle an ID-based query efficiently."""
    item_id = extract_id_from_query(query)
    logger.info(f"Detected ID search for: {item_id}")
    
    # Read the CSV file
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames or []
            
            # Detect ID field
            id_field = find_best_match(headers, ['id', 'uuid', 'key', 'item_id', 'product_id', 'user_id', 'customer_id', 'document_id', 'event_id'])
            
            if not id_field:
                logger.warning("Could not detect ID field in CSV")
                return None
                
            # Iterate through CSV to find matching ID
            for row in reader:
                if str(row.get(id_field, '')) == str(item_id):
                    logger.info(f"Found exact match for ID {item_id}")
                    row['_match_type'] = 'exact_id'
                    row['_score'] = 1.0
                    return row
                    
    except Exception as e:
        logger.error(f"Error searching by ID: {e}", exc_info=True)
    
    logger.info(f"No exact match found for ID {item_id}")
    return None


def handle_counting_query(query, csv_path):
    """Handle a counting query efficiently."""
    logger.info(f"Detected counting query: '{query}'")
    
    # Extract what we're counting and any filters
    count_target = extract_count_target(query)
    filters = extract_filters(query)
    
    # Get search terms by removing counting keywords
    search_query = preprocess_counting_query(query)
    
    # Run the search
    results = search_csv(csv_path, search_query)
    
    # Filter results by criteria
    id_field = None
    name_field = None
    
    # Try to determine fields from the first result
    if results:
        first_result = results[0]
        # Look for ID field
        for field in first_result:
            if 'id' in field.lower():
                id_field = field
                break
                
        # Look for name field
        for field in first_result:
            if any(name_term in field.lower() for name_term in ['name', 'title', 'label']):
                name_field = field
                break
    
    filtered_results = filter_results_by_criteria(results, filters, id_field, name_field)
    
    # Display count results
    print(f"\nFound {len(filtered_results)} total {count_target}")
    
    if filters:
        print(f"Filters applied: {filters}")
    
    if filtered_results:
        print("\nSample matches:")
        for i, result in enumerate(filtered_results[:5]):
            # Get name with fallbacks
            name = result.get('name', result.get(name_field, 'Unknown')) if name_field else 'Unknown'
            # Get ID with fallbacks
            id_val = result.get('id', result.get(id_field, 'Unknown')) if id_field else 'Unknown'
            print(f"  {i+1}. {name} (ID: {id_val})")
    
    return {"count": len(filtered_results), "target": count_target, "filters": filters}


def main():
    """Main entry point for the script with performance tracking."""
    start_time = time.time()
    
    # Parse arguments
    args = parse_arguments()
    
    csv_path = args['data-source']
    query = args['query']
    
    print(f"Searching for '{query}' in {csv_path}")
    
    # Check if this is an ID query
    item_id = extract_id_from_query(query)
    if item_id:
        item = handle_id_query(query, csv_path)
        if item:
            print("\nDirect ID match:")
            for key, value in item.items():
                if not key.startswith("_"):
                    print(f"  {key}: {value}")
            print(f"\nSearch completed in {time.time() - start_time:.4f} seconds")
            return
    
    # Check if this is a counting query
    if is_counting_query(query):
        handle_counting_query(query, csv_path)
        print(f"\nSearch completed in {time.time() - start_time:.4f} seconds")
        return
    
    # Standard search
    results = search_csv(csv_path, query)
    
    # Display results
    if results:
        print(f"Found {len(results)} results:")
        display_results(results)
    else:
        print("No results found.")
    
    print(f"\nSearch completed in {time.time() - start_time:.4f} seconds")


if __name__ == "__main__":
    main()