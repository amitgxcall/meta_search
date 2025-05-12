"""
Query classifier for determining search strategy.
"""

from typing import Dict, List, Any, Tuple, Pattern
import re

class QueryClassifier:
    """
    Classifies queries to determine the best search strategy.
    """
    
    def __init__(self, patterns: List[Tuple[Any, Any]]):
        """
        Initialize the query classifier.
        
        Args:
            patterns: List of (pattern, extraction_function) tuples
        """
        self.patterns = patterns
        
        # Keywords that indicate structured queries
        self.structured_keywords = [
            'status:', 'id:', 'name:', 'by:', 'priority:', 'started:',
            'completed:', 'failed', 'success', 'running', 'pending',
            '>', '<', '>=', '<=', '='
        ]
        
        # Keywords that indicate semantic queries
        self.semantic_keywords = [
            'like', 'similar', 'about', 'related to', 'concerning',
            'regarding', 'find', 'search for', 'show me'
        ]
    
    def classify(self, query: str) -> str:
        """
        Classify a query as structured, semantic, or hybrid.
        
        Args:
            query: Query string
            
        Returns:
            Classification as 'structured', 'semantic', or 'hybrid'
        """
        query_lower = query.lower()
        
        # Check if any structured patterns match
        has_structured = False
        for pattern, _ in self.patterns:
            pattern_str = pattern if isinstance(pattern, str) else getattr(pattern, 'pattern', str(pattern))
            if re.search(pattern_str, query, re.IGNORECASE):
                has_structured = True
                break
                
        # Check for structured keywords
        if not has_structured:
            has_structured = any(keyword in query_lower for keyword in self.structured_keywords)
        
        # Check for semantic keywords
        has_semantic = any(keyword in query_lower for keyword in self.semantic_keywords)
        
        # Determine classification
        if has_structured and has_semantic:
            return 'hybrid'
        elif has_structured:
            return 'structured'
        else:
            return 'semantic'  # Default to semantic search