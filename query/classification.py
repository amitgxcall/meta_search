"""
Query classification to determine the best search strategy.
"""

import re
from typing import Dict, Any, List, Optional, Tuple

def classify_query(query: str, 
                  available_fields: List[str],
                  extracted_filters: Dict[str, Any]) -> str:
    """
    Classify query to determine best search strategy.
    
    Args:
        query: The query string
        available_fields: Available fields in the data
        extracted_filters: Extracted structured filters
        
    Returns:
        Query type: 'structured', 'vector', or 'hybrid'
    """
    # If we have structured filters, this might be a structured query
    has_structured_component = bool(extracted_filters)
    
    # Keywords suggesting structured search
    structured_keywords = [
        'status:', 'id:', 'name:', 'by:', 'priority:', 'started:',
        'completed:', 'failed', 'success', 'running', 'pending',
        '>', '<', '>=', '<=', '='
    ]
    
    # Check for structured patterns
    for keyword in structured_keywords:
        if keyword in query:
            has_structured_component = True
            break
    
    # Explicit field references
    for field in available_fields:
        if f"{field}:" in query or f"{field}=" in query:
            has_structured_component = True
            break
    
    # Check for temporal indicators
    temporal_indicators = [
        'latest', 'recent', 'today', 'yesterday', 'last week',
        'this month', 'past', 'previous', 'newest'
    ]
    
    has_temporal_component = any(indicator in query.lower() for indicator in temporal_indicators)
    if has_temporal_component:
        has_structured_component = True
    
    # Check for semantic indicators
    semantic_indicators = [
        'like', 'similar', 'about', 'related to', 'find', 
        'search', 'show me', 'concerning', 'regarding'
    ]
    
    has_semantic_component = any(indicator in query.lower() for indicator in semantic_indicators)
    
    # Determine query type
    if has_structured_component and has_semantic_component:
        return 'hybrid'
    elif has_structured_component:
        return 'structured'
    else:
        return 'vector'