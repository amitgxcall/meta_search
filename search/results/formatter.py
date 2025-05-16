"""
Search result formatting utilities.

This module provides functions for formatting search results in various ways,
including for display in the console, for use in language models, or for
export to different formats.
"""

import json
import shutil
import logging
from typing import Dict, List, Any, Optional, Tuple, Union
import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def format_for_llm(results: List[Dict[str, Any]], 
                  query: str,
                  id_field: str = 'id',
                  name_field: str = 'name',
                  status_field: Optional[str] = 'status') -> Dict[str, Any]:
    """
    Format search results for an LLM.
    
    Args:
        results: List of search result dictionaries
        query: Original user query
        id_field: Field name for ID
        name_field: Field name for name
        status_field: Field name for status
        
    Returns:
        Dictionary formatted for LLM consumption
    """
    # Helper function to convert any value to JSON-serializable format
    def json_serializable(obj):
        if obj is None:
            return None
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif hasattr(obj, 'tolist'):  # For numpy arrays
            return obj.tolist()
        elif isinstance(obj, (list, tuple)):
            return [json_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: json_serializable(v) for k, v in obj.items()}
        else:
            return obj
    
    if not results:
        return {
            "query": query,
            "result_count": 0,
            "results": [],
            "suggested_response": f"I couldn't find any items matching '{query}'. Would you like to try a different search?"
        }
    
    # Create a summary of results 
    result_types = {}
    
    # Try to extract item types or statuses
    for result in results:
        # Get status if available
        status = None
        if status_field and status_field in result:
            status = str(result.get(status_field, 'unknown'))
        elif 'status' in result:
            status = str(result.get('status', 'unknown'))
        else:
            # Try to infer from fields
            for field_name in result:
                if 'status' in field_name.lower() or 'state' in field_name.lower() or 'type' in field_name.lower():
                    status = str(result.get(field_name, 'unknown'))
                    break
        
        # Use 'unknown' if no status found
        if not status:
            status = 'unknown'
            
        # Track count by status
        if status not in result_types:
            result_types[status] = 0
        result_types[status] += 1
    
    # Format results in a clean way
    formatted_items = []
    for result in results:
        # Extract ID
        item_id = None
        if id_field in result:
            item_id = result.get(id_field)
        else:
            # Try common ID field names
            for field in ['id', '_id', 'item_id', 'uuid']:
                if field in result:
                    item_id = result.get(field)
                    break
                
        # Extract name
        item_name = None
        if name_field in result:
            item_name = result.get(name_field)
        else:
            # Try common name field names
            for field in ['name', 'title', 'label', 'description']:
                if field in result:
                    item_name = result.get(field)
                    break
        
        # Extract match info
        match_type = result.get('_match_type', 'unknown')
        match_score = result.get('_score', 0)
        
        # Create a generic representation
        formatted_item = {
            "id": json_serializable(item_id or ''),
            "name": str(item_name or ''),
            "match_type": match_type,
            "match_score": round(float(match_score), 4) if isinstance(match_score, (int, float)) else 0
        }
        
        # Add status if available
        if status_field and status_field in result:
            formatted_item["status"] = result[status_field]
        
        # Add all other fields from result
        for k, v in result.items():
            if (k not in [id_field, name_field, 'id', 'name', '_score', '_match_type', '_separator', '_message'] 
                and not k.startswith('_')):
                formatted_item[k] = json_serializable(v)
        
        formatted_items.append(formatted_item)
    
    # Create a suggested response
    if len(results) == 1:
        # Get ID and name from the single result
        item = results[0]
        
        # Get ID and name, preferring mapped fields but falling back
        item_id = item.get('id', item.get(id_field, 'unknown'))
        item_name = item.get('name', item.get(name_field, 'unknown'))
        
        # Get status with fallbacks
        item_status = None
        if status_field and status_field in item:
            item_status = item[status_field]
        elif 'status' in item:
            item_status = item['status']
        
        # Construct response
        if item_status:
            suggested_response = f"I found one item matching '{query}': {item_name} (ID: {item_id}), which is currently {item_status}."
        else:
            suggested_response = f"I found one item matching '{query}': {item_name} (ID: {item_id})."
    else:
        # Create summary for multiple results
        status_summary = ", ".join([f"{count} {status}" for status, count in result_types.items()])
        
        # Get name of first result with fallbacks
        first_item = results[0]
        first_item_name = first_item.get('name', first_item.get(name_field, 'unknown'))
        
        # Craft response based on query and results
        suggested_response = f"I found {len(results)} items matching '{query}': {status_summary}. The top result is {first_item_name}."
    
    # Make sure the entire result is JSON serializable
    return json_serializable({
        "query": query,
        "result_count": len(results),
        "results": formatted_items,
        "result_types": result_types,
        "suggested_response": suggested_response
    })


def display_results(results: List[Dict[str, Any]], 
                   max_width: Optional[int] = None,
                   id_field: str = 'id',
                   name_field: str = 'name',
                   status_field: Optional[str] = 'status') -> None:
    """
    Display search results in a readable format.
    
    Args:
        results: List of search result dictionaries
        max_width: Maximum width for display (auto-detect if None)
        id_field: Field name for ID
        name_field: Field name for name
        status_field: Field name for status
    """
    if not results:
        print("No results found.")
        return
    
    # Auto-detect terminal width if not specified
    if max_width is None:
        try:
            terminal_width, _ = shutil.get_terminal_size()
            max_width = max(80, min(terminal_width, 160))  # Reasonable bounds
        except Exception:
            max_width = 120  # Default if detection fails
    
    # Calculate column widths
    id_width = 10
    name_width = max(20, (max_width // 4))  # Allow more space for name
    status_width = 15 if status_field else 0
    
    # Calculate details width
    details_width = max_width - id_width - name_width - status_width - 8
    
    # Print header
    if status_field:
        header = f"{'ID':<{id_width}} | {'Name':<{name_width}} | {'Status':<{status_width}} | Details"
    else:
        header = f"{'ID':<{id_width}} | {'Name':<{name_width}} | Details"
    
    print("\n" + header)
    print("-" * max_width)
    
    # Print each result
    for result in results:
        # Check if this is a separator item
        if result.get("_separator", False):
            print(f"\n--- {result.get('_message', 'Additional results:')} ---\n")
            continue
        
        # Extract ID - first try the generic 'id' field, then try original field
        id_value = ''
        for field in ['id', id_field, 'item_id', '_id', 'uuid']:
            if field in result:
                id_value = str(result.get(field, ''))
                break
                
        # Truncate if needed
        id_value = id_value[:id_width]
        
        # Extract Name - first try the generic 'name' field, then try original field
        name_value = ''
        for field in ['name', name_field, 'title', 'label']:
            if field in result:
                name_value = str(result.get(field, ''))
                break
                
        # Truncate if needed
        name_value = name_value[:name_width]
        
        # Extract Status if requested
        status_value = ''
        if status_field:
            for field in ['status', status_field, 'state', 'condition']:
                if field in result:
                    status_value = str(result.get(field, ''))
                    break
                    
            # Truncate if needed
            status_value = status_value[:status_width]
        
        # Get other fields for details
        details = []
        # Skip fields already displayed and metadata fields
        skip_fields = ['id', 'name', 'status', id_field, name_field, status_field]
        skip_fields.extend(['_score', '_structured_score', '_vector_score', '_combined_score', '_result_type'])
        
        # Add match type and score first if available
        match_type = result.get('_match_type', None)
        score = result.get('_score', None)
        
        if match_type and score:
            score_str = f"{score:.4f}" if isinstance(score, float) else str(score)
            details.append(f"match: {match_type} ({score_str})")
        
        for field, value in result.items():
            if field not in skip_fields and not field.startswith('_'):
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
        if status_field:
            print(f"{id_value:<{id_width}} | {name_value:<{name_width}} | {status_value:<{status_width}} | {details_text}")
        else:
            print(f"{id_value:<{id_width}} | {name_value:<{name_width}} | {details_text}")


def format_as_json(results: List[Dict[str, Any]],
                  pretty_print: bool = True,
                  include_metadata: bool = False) -> str:
    """
    Format search results as JSON.
    
    Args:
        results: List of search result dictionaries
        pretty_print: Whether to format the JSON with indentation
        include_metadata: Whether to include metadata fields (starting with _)
        
    Returns:
        JSON string
    """
    # Helper function to convert any value to JSON-serializable format
    def json_serializable(obj):
        if obj is None:
            return None
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif hasattr(obj, 'tolist'):  # For numpy arrays
            return obj.tolist()
        elif isinstance(obj, (list, tuple)):
            return [json_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: json_serializable(v) for k, v in obj.items()}
        else:
            return obj
    
    # Process results
    processed_results = []
    for result in results:
        # Skip separator items
        if result.get("_separator", False):
            continue
        
        # Process fields
        processed_result = {}
        for k, v in result.items():
            # Skip metadata fields if not requested
            if not include_metadata and k.startswith('_'):
                continue
            
            processed_result[k] = json_serializable(v)
        
        processed_results.append(processed_result)
    
    # Convert to JSON
    indent = 2 if pretty_print else None
    return json.dumps(processed_results, indent=indent)


def format_as_csv(results: List[Dict[str, Any]],
                 include_metadata: bool = False) -> str:
    """
    Format search results as CSV.
    
    Args:
        results: List of search result dictionaries
        include_metadata: Whether to include metadata fields (starting with _)
        
    Returns:
        CSV string
    """
    import csv
    from io import StringIO
    
    if not results:
        return ""
    
    # Get all unique fields
    all_fields = set()
    for result in results:
        for field in result.keys():
            # Skip metadata fields if not requested
            if not include_metadata and field.startswith('_'):
                continue
            all_fields.add(field)
    
    # Sort fields for consistent output - prioritize id and name fields
    prioritized_fields = []
    if 'id' in all_fields:
        prioritized_fields.append('id')
        all_fields.remove('id')
    
    if 'name' in all_fields:
        prioritized_fields.append('name')
        all_fields.remove('name')
    
    if 'status' in all_fields:
        prioritized_fields.append('status')
        all_fields.remove('status')
    
    # Add remaining fields in alphabetical order
    sorted_fields = prioritized_fields + sorted(all_fields)
    
    # Create CSV
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=sorted_fields)
    writer.writeheader()
    
    for result in results:
        # Skip separator items
        if result.get("_separator", False):
            continue
        
        # Filter fields
        filtered_result = {}
        for field in sorted_fields:
            # Convert values to strings
            if field in result:
                value = result[field]
                if isinstance(value, (dict, list, tuple)):
                    filtered_result[field] = json.dumps(value)
                else:
                    filtered_result[field] = str(value)
            else:
                filtered_result[field] = ""
        
        writer.writerow(filtered_result)
    
    return output.getvalue()


def count_results_by_field(results: List[Dict[str, Any]], 
                         field: str) -> Dict[str, int]:
    """
    Count results by a specific field value.
    
    Args:
        results: List of search result dictionaries
        field: Field to count by
        
    Returns:
        Dictionary mapping field values to counts
    """
    counts = {}
    
    for result in results:
        # Skip separator items
        if result.get("_separator", False):
            continue
        
        # Get field value
        value = str(result.get(field, 'unknown'))
        
        # Increment count
        if value not in counts:
            counts[value] = 0
        counts[value] += 1
    
    return counts


def summarize_results(results: List[Dict[str, Any]],
                    id_field: str = 'id',
                    name_field: str = 'name',
                    status_field: Optional[str] = 'status') -> Dict[str, Any]:
    """
    Create a summary of search results.
    
    Args:
        results: List of search result dictionaries
        id_field: Field name for ID
        name_field: Field name for name
        status_field: Field name for status
        
    Returns:
        Summary dictionary
    """
    # Filter out separator items
    filtered_results = [r for r in results if not r.get("_separator", False)]
    
    if not filtered_results:
        return {
            "count": 0,
            "message": "No results found."
        }
    
    # Count results by status
    status_counts = {}
    if status_field:
        for result in filtered_results:
            status = str(result.get(status_field, 'unknown'))
            if status not in status_counts:
                status_counts[status] = 0
            status_counts[status] += 1
    
    # Get result types
    result_types = {}
    for result in filtered_results:
        result_type = result.get('_result_type', 'unknown')
        if result_type not in result_types:
            result_types[result_type] = 0
        result_types[result_type] += 1
    
    # Get top result
    top_result = filtered_results[0]
    
    # Get ID and name with fallbacks
    top_id = top_result.get(id_field, top_result.get('id', 'unknown'))
    top_name = top_result.get(name_field, top_result.get('name', 'unknown'))
    
    # Create summary
    return {
        "count": len(filtered_results),
        "status_counts": status_counts,
        "result_types": result_types,
        "top_result": {
            "id": top_id,
            "name": top_name,
            "score": top_result.get('_score', 0)
        }
    }