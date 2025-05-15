"""
Search result formatting utilities.

This module provides functions for formatting search results in various ways,
including for display in the console, for use in language models, or for
export to different formats.

Example:
    # Format search results for a language model
    formatted_results = format_for_llm(results, query)
    
    # Display results in the console
    display_results(results)
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
            "suggested_response": f"I couldn't find any jobs matching '{query}'. Would you like to try a different search?"
        }
    
    # Create a summary of results
    result_types = {}
    for r in results:
        # Get job details from result
        if 'job_details' in r:
            job = r['job_details']
        else:
            job = r
            
        # Look for status in job details
        status = None
        # Try status_field first
        if status_field in job:
            status = str(job.get(status_field, 'unknown'))
        # Then try 'status'
        elif 'status' in job:
            status = str(job.get('status', 'unknown'))
        # Then try other common status fields
        else:
            for field_name in ['state', 'condition', 'job_status']:
                if field_name in job:
                    status = str(job.get(field_name, 'unknown'))
                    break
        
        # Fallback if no status found
        if not status:
            status = 'unknown'
            
        # Track count by status
        if status not in result_types:
            result_types[status] = 0
        result_types[status] += 1
    
    # Format jobs in a clean way
    formatted_jobs = []
    for r in results:
        # Extract job details
        if 'job_details' in r:
            job = r['job_details']
            match_type = r.get('match_type', 'unknown')
            score = r.get('score', 0)
        else:
            job = r
            match_type = r.get('_result_type', 'unknown')
            score = r.get('_score', 0)
        
        # Try various ID fields
        job_id = None
        for field in [id_field, 'id', 'job_id', '_id', 'uuid']:
            if field in job:
                job_id = job.get(field)
                break
                
        # Try various name fields
        job_name = None
        for field in [name_field, 'name', 'job_name', 'title', 'description']:
            if field in job:
                job_name = job.get(field)
                break
        
        # Create a generic representation
        formatted_job = {
            "id": json_serializable(job_id or ''),
            "name": str(job_name or ''),
            "match_type": match_type,
            "match_score": round(float(score), 4) if isinstance(score, (int, float)) else 0
        }
        
        # Add all other fields from job details
        for k, v in job.items():
            if k not in [id_field, name_field, 'id', 'name', '_score', '_result_type', '_separator', '_message']:
                formatted_job[k] = json_serializable(v)
        
        formatted_jobs.append(formatted_job)
    
    # Create a suggested response
    if len(results) == 1:
        job = results[0].get('job_details', results[0])
        
        # Get ID and name, preferring mapped fields but falling back
        job_id = job.get('id', job.get(id_field, 'unknown'))
        job_name = job.get('name', job.get(name_field, 'unknown'))
        
        # Get status with fallbacks
        job_status = job.get('status', job.get(status_field, 'unknown'))
        
        suggested_response = f"I found one job matching '{query}': {job_name} (ID: {job_id}), which is currently {job_status}."
    else:
        status_summary = ", ".join([f"{count} {status}" for status, count in result_types.items()])
        
        # Get name of first result with fallbacks
        first_job = results[0].get('job_details', results[0])
        first_job_name = first_job.get('name', first_job.get(name_field, 'unknown'))
        
        suggested_response = f"I found {len(results)} jobs matching '{query}': {status_summary}. The top result is {first_job_name}."
    
    # Make sure the entire result is JSON serializable
    return json_serializable({
        "query": query,
        "result_count": len(results),
        "results": formatted_jobs,
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
    name_width = 30
    status_width = 15
    score_width = 10
    
    # Calculate details width
    details_width = max_width - id_width - name_width - status_width - score_width - 8
    
    # Print header
    header = f"{'ID':<{id_width}} | {'Name':<{name_width}} | {'Status':<{status_width}} | {'Score':<{score_width}} | Details"
    print("\n" + header)
    print("-" * max_width)
    
    # Print each result
    for result in results:
        # Check if this is a separator item
        if result.get("_separator", False):
            print(f"\n--- {result.get('_message', 'Vector search results below:')} ---\n")
            continue
        
        # Extract job details
        if 'job_details' in result:
            job = result['job_details']
            score = result.get('score', 0)
        else:
            job = result
            score = result.get('_score', 0)
        
        # Extract ID - first try the generic 'id' field, then try original field
        job_id = ''
        for field in ['id', id_field, 'job_id', '_id', 'uuid']:
            if field in job:
                job_id = str(job.get(field, ''))
                break
                
        # Truncate if needed
        job_id = job_id[:id_width]
        
        # Extract Name - first try the generic 'name' field, then try original field
        job_name = ''
        for field in ['name', name_field, 'job_name', 'title']:
            if field in job:
                job_name = str(job.get(field, ''))
                break
                
        # Truncate if needed
        job_name = job_name[:name_width]
        
        # Extract Status with multiple fallbacks
        job_status = ''
        for field in ['status', status_field, 'state', 'condition']:
            if field in job:
                job_status = str(job.get(field, ''))
                break
                
        # Truncate if needed
        job_status = job_status[:status_width]
        
        # Format score with proper precision
        score_str = f"{score:.4f}" if isinstance(score, (int, float)) else str(score)
        score_str = score_str[:score_width]
        
        # Get other fields for details
        details = []
        blacklist_fields = ['id', 'name', 'status', 'state', 'condition', 'job_id', 'job_name']
        blacklist_fields.extend(['_score', '_structured_score', '_vector_score', '_combined_score', '_result_type'])
        
        for field, value in job.items():
            if field not in blacklist_fields:
                # Format value based on type
                if isinstance(value, (int, float)):
                    value_str = f"{value}"
                elif value is None:
                    value_str = "null"
                else:
                    value_str = str(value)
                    # Truncate long values
                    if len(value_str) > 50:
                        value_str = value_str[:47] + "..."
                        
                details.append(f"{field}: {value_str}")
        
        # Format details with better spacing and truncation
        details_text = ", ".join(details)
        if len(details_text) > details_width:
            details_text = details_text[:details_width-3] + "..."
        
        # Print row
        print(f"{job_id:<{id_width}} | {job_name:<{name_width}} | {job_status:<{status_width}} | {score_str:<{score_width}} | {details_text}")


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
    
    # Sort fields for consistent output
    sorted_fields = sorted(all_fields)
    
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
    
    # Create summary
    return {
        "count": len(filtered_results),
        "status_counts": status_counts,
        "result_types": result_types,
        "top_result": {
            "id": filtered_results[0].get(id_field, filtered_results[0].get('id', 'unknown')),
            "name": filtered_results[0].get(name_field, filtered_results[0].get('name', 'unknown')),
            "score": filtered_results[0].get('_score', 0)
        }
    }