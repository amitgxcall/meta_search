"""
Utilities for formatting search results.
"""

import json
from typing import Dict, List, Any, Optional, Union
import numpy as np

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
        job = r.get('job_details', r)
        status = str(job.get(status_field, 'unknown'))
        if status not in result_types:
            result_types[status] = 0
        result_types[status] += 1
    
    # Format jobs in a clean way
    formatted_jobs = []
    for r in results:
        job = r.get('job_details', r)
        
        # Create a generic representation
        formatted_job = {
            "id": json_serializable(job.get(id_field, '')),
            "name": str(job.get(name_field, '')),
            "match_type": r.get('match_type', 'unknown'),
            "match_score": round(float(r.get('score', 0)), 4)
        }
        
        # Add all other fields
        for k, v in job.items():
            if k not in [id_field, name_field]:
                formatted_job[k] = json_serializable(v)
        
        formatted_jobs.append(formatted_job)
    
    # Create a suggested response
    if len(results) == 1:
        job = results[0].get('job_details', results[0])
        job_id = job.get(id_field, 'unknown')
        job_name = job.get(name_field, 'unknown')
        status = job.get(status_field, 'unknown')
        suggested_response = f"I found one job matching '{user_query}': {job_name} (ID: {job_id}), which is currently {status}."
    else:
        status_summary = ", ".join([f"{count} {status}" for status, count in result_types.items()])
        first_job_name = results[0].get('job_details', results[0]).get(name_field, 'unknown')
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
                   max_width: int = 100,
                   id_field: str = 'id',
                   name_field: str = 'name',
                   status_field: Optional[str] = 'status') -> None:
    """
    Display search results in a readable format.
    
    Args:
        results: List of search result dictionaries
        max_width: Maximum width for display
        id_field: Field name for ID
        name_field: Field name for name
        status_field: Field name for status
    """
    if not results:
        print("No results found.")
        return
    
    # Calculate column widths
    id_width = 10
    name_width = 30
    status_width = 15
    score_width = 10
    
    if status_field:
        details_width = max_width - id_width - name_width - status_width - score_width - 8  # 8 for separators
    else:
        details_width = max_width - id_width - name_width - score_width - 6  # 6 for separators
    
    # Print header
    if status_field:
        header = f"{'ID':<{id_width}} | {'Name':<{name_width}} | {'Status':<{status_width}} | {'Score':<{score_width}} | Details"
    else:
        header = f"{'ID':<{id_width}} | {'Name':<{name_width}} | {'Score':<{score_width}} | Details"
    
    print("\n" + header)
    print("-" * max_width)
    
    # Print each result
    for result in results:
        # Get job details
        if 'job_details' in result:
            job = result['job_details']
            score = result.get('score', 0)
        else:
            job = result
            score = 0
        
        # Format fields
        job_id = str(job.get(id_field, ''))[:id_width]
        job_name = str(job.get(name_field, ''))[:name_width]
        
        # Format score
        score_str = f"{score:.4f}" if isinstance(score, (int, float)) else str(score)
        score_str = score_str[:score_width]
        
        # Get other fields for details
        details = []
        for field, value in job.items():
            if field not in [id_field, name_field, status_field]:
                details.append(f"{field}: {value}")
        
        details_text = ", ".join(details)
        if len(details_text) > details_width:
            details_text = details_text[:details_width-3] + "..."
        
        # Print row
        if status_field:
            job_status = str(job.get(status_field, ''))[:status_width]
            print(f"{job_id:<{id_width}} | {job_name:<{name_width}} | {job_status:<{status_width}} | {score_str:<{score_width}} | {details_text}")
        else:
            print(f"{job_id:<{id_width}} | {job_name:<{name_width}} | {score_str:<{score_width}} | {details_text}")