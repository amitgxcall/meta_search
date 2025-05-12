"""
Query patterns for extracting structured filters from natural language queries.
"""

import re
from typing import Dict, List, Any, Tuple, Pattern, Callable, Match
from datetime import datetime, timedelta

def create_query_patterns() -> List[Tuple[Any, Callable]]:
    """
    Create a list of patterns and extraction functions for query parsing.
    
    Returns:
        List of (pattern, extraction_function) tuples
    """
    patterns = []
    
    # Pattern 1: Status-specific queries
    status_pattern = re.compile(r'(failed|running|success(ful)?|pending|completed|errored|done) jobs?', re.IGNORECASE)
    
    def extract_status(match: Match) -> Dict[str, Any]:
        status = match.group(1).lower()
        if status in ['successful', 'done', 'completed']:
            status = 'success'
        elif status == 'errored':
            status = 'failed'
        return {'status': status}
    
    patterns.append((status_pattern, extract_status))
    
    # Pattern 2: Time-based queries
    time_pattern = re.compile(r'(last|past|recent) (\d+) (hour|day|week|month)s?', re.IGNORECASE)
    
    def extract_time(match: Match) -> Dict[str, Any]:
        amount = int(match.group(2))
        unit = match.group(3).lower()
        
        # Calculate time delta
        now = datetime.now()
        if unit == 'hour':
            cutoff = now - timedelta(hours=amount)
        elif unit == 'day':
            cutoff = now - timedelta(days=amount)
        elif unit == 'week':
            cutoff = now - timedelta(weeks=amount)
        elif unit == 'month':
            cutoff = now - timedelta(days=amount*30)  # Approximation
        
        return {'created_at': {'gt': cutoff}}
    
    patterns.append((time_pattern, extract_time))
    
    # Add more patterns here...
    
    return patterns

def extract_multi_part_filters(query: str, available_fields: List[str]) -> Dict[str, Any]:
    """
    Extract multiple filters from complex queries.
    
    Args:
        query: Query string
        available_fields: List of available field names
    
    Returns:
        Dictionary of field:value filters
    """
    filters = {}
    
    # Field-value pairs with various separators
    patterns = [
        (r'(\w+)\s*[:=]\s*"([^"]+)"', lambda f, v: v),                  # field: "value"
        (r'(\w+)\s*[:=]\s*(\w+)', lambda f, v: v),                      # field: value
        (r'(\w+)\s+is\s+([a-zA-Z0-9_]+)', lambda f, v: v),              # field is value
        (r'(\w+)\s+contains\s+([a-zA-Z0-9_]+)', lambda f, v: {'contains': v}),  # field contains value
        (r'(\w+)\s*>\s*(\d+(?:\.\d+)?)', lambda f, v: {'gt': float(v)}),  # field > value
        (r'(\w+)\s*<\s*(\d+(?:\.\d+)?)', lambda f, v: {'lt': float(v)}),  # field < value
        (r'(\w+)\s*>=\s*(\d+(?:\.\d+)?)', lambda f, v: {'gte': float(v)}),  # field >= value
        (r'(\w+)\s*<=\s*(\d+(?:\.\d+)?)', lambda f, v: {'lte': float(v)})   # field <= value
    ]
    
    for pattern, value_processor in patterns:
        for match in re.finditer(pattern, query, re.IGNORECASE):
            field, value = match.groups()
            # Check if field exists
            if field in available_fields:
                filters[field] = value_processor(field, value)
    
    return filters