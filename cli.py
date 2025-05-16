#!/usr/bin/env python3
"""
Super simple CSV search script with explicit argument handling.
This script doesn't use argparse to avoid any issues with argument parsing.

Usage:
    python super_simple_search.py --data-source=PATH_TO_CSV --query=SEARCH_QUERY
    
Example:
    python super_simple_search.py --data-source=./meta-data/products.csv --query="organic"
    python super_simple_search.py --data-source=./meta-data/products.csv --query="price>100"
"""

import os
import sys
import csv
import re


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
        print("Error: Missing required argument --data-source")
        print(f"Usage: {sys.argv[0]} --data-source=PATH_TO_CSV --query=SEARCH_QUERY")
        sys.exit(1)
    
    if 'query' not in args:
        print("Error: Missing required argument --query")
        print(f"Usage: {sys.argv[0]} --data-source=PATH_TO_CSV --query=SEARCH_QUERY")
        sys.exit(1)
    
    return args


def search_csv(csv_path, query, limit=10):
    """Search a CSV file for matching rows."""
    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        return []
    
    try:
        # Read the CSV file
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames or []
            rows = list(reader)
        
        print(f"Loaded CSV with {len(rows)} rows and {len(headers)} columns")
        
        # Detect field types based on headers
        id_field = find_best_match(headers, ['id', 'uuid', 'key', 'item_id', 'product_id', 'user_id', 'customer_id', 'document_id', 'event_id'])
        name_field = find_best_match(headers, ['name', 'title', 'label', 'summary', 'description', 'product_name', 'full_name', 'event_name'])
        status_field = find_best_match(headers, ['status', 'state', 'condition', 'type', 'inventory_status', 'account_status', 'severity'])
        
        print(f"Detected fields - ID: {id_field}, Name: {name_field}, Status: {status_field}")
        
        # Check if query is structured
        is_structured = ":" in query or ">" in query or "<" in query or ">=" in query or "<=" in query
        
        if is_structured:
            print(f"Detected structured query: '{query}'")
            results = parse_structured_query(rows, query, id_field, name_field, status_field)
        else:
            print(f"Performing text search for: '{query}'")
            results = search_text(rows, query, id_field, name_field, status_field)
        
        # Sort by score
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        # Apply limit
        return results[:limit]
        
    except Exception as e:
        print(f"Error searching CSV: {e}")
        import traceback
        traceback.print_exc()
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
    """Perform a simple text search on CSV rows."""
    query = query.lower()
    results = []
    
    for row in rows:
        # Simple search: check if query is in any field
        score = 0
        for field, value in row.items():
            if not value:
                continue
            
            value_str = str(value).lower()
            
            # Exact match gets higher score
            if query == value_str:
                score += 10
            # Partial match gets lower score
            elif query in value_str:
                score += 5
            # Word match gets even lower score
            elif any(word in value_str for word in query.split()):
                score += 1
        
        if score > 0:
            result = row.copy()
            result['_score'] = score
            result['_match_type'] = 'text'
            
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
    """Parse a structured query and filter rows accordingly."""
    results = []
    
    # Start with all rows
    filtered_rows = rows.copy()
    
    # Split the query into tokens, preserving quoted strings
    parts = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")++', query)
    
    for part in parts:
        if ":" in part:
            # Field:value pattern
            field, value = part.split(":", 1)
            value = value.strip('"')
            
            # Filter data to only include rows where field matches value
            filtered_rows = [
                row for row in filtered_rows 
                if field in row and str(row[field]).lower() == value.lower()
            ]
        
        elif ">" in part and not ">=" in part:
            # Greater than
            field, value = part.split(">", 1)
            value = value.strip('"')
            
            # Try to convert to number if possible
            try:
                num_value = float(value)
                # Filter data
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) > num_value
                ]
            except (ValueError, TypeError):
                # Skip this condition if conversion fails
                continue
        
        elif "<" in part and not "<=" in part:
            # Less than
            field, value = part.split("<", 1)
            value = value.strip('"')
            
            # Try to convert to number if possible
            try:
                num_value = float(value)
                # Filter data
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) < num_value
                ]
            except (ValueError, TypeError):
                # Skip this condition if conversion fails
                continue
        
        elif ">=" in part:
            # Greater than or equal
            field, value = part.split(">=", 1)
            value = value.strip('"')
            
            # Try to convert to number if possible
            try:
                num_value = float(value)
                # Filter data
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) >= num_value
                ]
            except (ValueError, TypeError):
                # Skip this condition if conversion fails
                continue
        
        elif "<=" in part:
            # Less than or equal
            field, value = part.split("<=", 1)
            value = value.strip('"')
            
            # Try to convert to number if possible
            try:
                num_value = float(value)
                # Filter data
                filtered_rows = [
                    row for row in filtered_rows 
                    if field in row and row[field] and float(row[field]) <= num_value
                ]
            except (ValueError, TypeError):
                # Skip this condition if conversion fails
                continue
    
    # Format results
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


def display_results(results, max_width=120):
    """Display search results in a readable format."""
    if not results:
        print("No results found.")
        return
    
    # Calculate column widths
    id_width = 10
    name_width = max(20, (max_width // 4))  # Allow more space for name
    status_width = 15
    
    # Calculate details width
    details_width = max_width - id_width - name_width - status_width - 8
    
    # Print header
    header = f"{'ID':<{id_width}} | {'Name':<{name_width}} | {'Status':<{status_width}} | Details"
    print("\n" + header)
    print("-" * max_width)
    
    # Print each result
    for result in results:
        # Extract ID 
        id_value = result.get('id', '')
        if not id_value:
            for field in result:
                if 'id' in field.lower():
                    id_value = str(result.get(field, ''))
                    break
                
        # Truncate if needed
        id_value = id_value[:id_width]
        
        # Extract Name
        name_value = result.get('name', '')
        if not name_value:
            for field in result:
                if 'name' in field.lower() or 'title' in field.lower():
                    name_value = str(result.get(field, ''))
                    break
                
        # Truncate if needed
        name_value = name_value[:name_width]
        
        # Extract Status
        status_value = result.get('status', '')
        if not status_value:
            for field in result:
                if 'status' in field.lower() or 'state' in field.lower():
                    status_value = str(result.get(field, ''))
                    break
                
        # Truncate if needed
        status_value = status_value[:status_width]
        
        # Get other fields for details
        details = []
        # Skip fields already displayed and metadata fields
        skip_fields = ['id', 'name', 'status']
        skip_fields.extend([field for field in result if 'id' in field.lower()])
        skip_fields.extend([field for field in result if 'name' in field.lower() or 'title' in field.lower()])
        skip_fields.extend([field for field in result if 'status' in field.lower() or 'state' in field.lower()])
        skip_fields.extend(['_score', '_match_type'])
        
        # Add match type and score if available
        match_type = result.get('_match_type', None)
        score = result.get('_score', None)
        
        if match_type and score:
            score_str = f"{score:.2f}" if isinstance(score, float) else str(score)
            details.append(f"match: {match_type} ({score_str})")
        
        # Add a few key fields
        key_fields = []
        for field in result:
            if field not in skip_fields and not field.startswith('_'):
                # Only include a few key fields for readability
                if len(key_fields) < 3:
                    key_fields.append(field)
        
        for field in key_fields:
            value = result[field]
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
        
        # Format details with better spacing and truncation
        details_text = ", ".join(details)
        if len(details_text) > details_width:
            details_text = details_text[:details_width-3] + "..."
        
        # Print row
        print(f"{id_value:<{id_width}} | {name_value:<{name_width}} | {status_value:<{status_width}} | {details_text}")


def main():
    """Main entry point for the script."""
    # Parse arguments
    args = parse_arguments()
    
    csv_path = args['data-source']
    query = args['query']
    
    print(f"Searching for '{query}' in {csv_path}")
    
    # Search the CSV
    results = search_csv(csv_path, query)
    
    # Display results
    if results:
        print(f"Found {len(results)} results:")
        display_results(results)
    else:
        print("No results found.")


if __name__ == "__main__":
    main()