"""
Temporal query handling for time-based searches.
"""

import re
import datetime
from typing import Dict, Any, Optional, Tuple

def extract_temporal_filters(query: str, timestamp_fields: Optional[list] = None) -> Dict[str, Any]:
    """
    Extract temporal filters from a query.
    
    Args:
        query: The query string
        timestamp_fields: Available timestamp fields
        
    Returns:
        Dictionary of temporal filters
    """
    filters = {}
    query_lower = query.lower()
    
    # Default timestamp field (use first available or created_at)
    ts_field = None
    if timestamp_fields and len(timestamp_fields) > 0:
        ts_field = timestamp_fields[0]
    else:
        ts_field = 'created_at'
    
    # Check for "latest" or "recent" indicators
    if any(term in query_lower for term in ['latest', 'recent', 'newest', 'current']):
        if "latest run" in query_lower:
            filters['is_latest_run'] = True
        elif "latest" in query_lower:
            filters['is_latest'] = True
        else:
            # Recent typically means last day
            filters['days_ago'] = 1
    
    # Extract specific time periods
    time_match = re.search(r'(last|past)\s+(\d+)\s+(day|days|hour|hours|week|weeks|month|months)', query_lower)
    if time_match:
        amount = int(time_match.group(2))
        unit = time_match.group(3)
        
        if 'hour' in unit:
            days = amount / 24
        elif 'week' in unit:
            days = amount * 7
        elif 'month' in unit:
            days = amount * 30
        else:  # days
            days = amount
            
        filters['days_ago'] = days
    
    # Handle specific time references
    if 'today' in query_lower:
        filters['days_ago'] = 1
    elif 'yesterday' in query_lower:
        filters['days_ago'] = 2
    elif 'this week' in query_lower:
        filters['days_ago'] = 7
    elif 'last week' in query_lower:
        filters['days_ago'] = 14
    elif 'this month' in query_lower:
        filters['days_ago'] = 30
    elif 'last month' in query_lower:
        filters['days_ago'] = 60
    
    return filters