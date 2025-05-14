"""
Utilities for formatting search results.
"""

import json
from typing import Dict, List, Any, Optional, Union
import numpy as np
import shutil

def format_for_llm(results: List[Dict[str, Any]], 
                  user_query: str,
                  id_field: str = 'id',
                  name_field: str = 'name',
                  status_field: Optional[str] = 'status') -> Dict[str, Any]:
    """
    Format search results for an LLM.
    
    Args:
        results: List of search result dictionaries
        user_query: Original user query
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
        elif isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32, np.float16)):
            return float(obj)
        elif isinstance(obj, (List, tuple)):
            return [json_serializable(item) for item in obj]
        elif isinstance(obj, Dict):
            return {k: json_serializable(v) for k, v in obj.items()}
        else:
            return obj
    
    if not results:
        return {
            "query": user_query,
            "result_count": 0,
            "results": [],
            "suggested_response": f"I couldn't find any jobs matching '{user_query}'. Would you like to try a different search?"
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
            match_type = 'unknown'
            score = 0
        
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
            "match_score": round(float(score), 4)
        }
        
        # Add all other fields from job details
        for k, v in job.items():
            if k not in [id_field, name_field, 'id', 'name']:
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
        
        suggested_response = f"I found one job matching '{user_query}': {job_name} (ID: {job_id}), which is currently {job_status}."
    else:
        status_summary = ", ".join([f"{count} {status}" for status, count in result_types.items()])
        
        # Get name of first result with fallbacks
        first_job = results[0].get('job_details', results[0])
        first_job_name = first_job.get('name', first_job.get(name_field, 'unknown'))
        
        suggested_response = f"I found {len(results)} jobs matching '{user_query}': {status_summary}. The top result is {first_job_name}."
    
    # Make sure the entire result is JSON serializable
    return json_serializable({
        "query": user_query,
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
        # Extract job details
        if 'job_details' in result:
            job = result['job_details']
            score = result.get('score', 0)
        else:
            job = result
            score = 0
        
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
            if field in job and field in job:
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