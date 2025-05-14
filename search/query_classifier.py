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
        
        # NEW: Keywords that indicate counting queries
        self.counting_keywords = [
            'how many', 'count', 'total', 'number of', 'tally', 
            'sum of', 'sum up', 'calculate', 'compute'
        ]
    
    def classify(self, query: str) -> str:
        """
        Classify a query as structured, semantic, hybrid, or counting.
        
        Args:
            query: Query string
            
        Returns:
            Classification as 'structured', 'semantic', 'hybrid', or 'counting'
        """
        query_lower = query.lower()
        
        # NEW: Check if this is a counting query first
        if self.is_counting_query(query):
            return 'counting'
            
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
    
    def is_counting_query(self, query: str) -> bool:
        """
        Determine if a query is asking for a count.
        
        Args:
            query: Query string
            
        Returns:
            True if the query is about counting, False otherwise
        """
        query_lower = query.lower()
        
        # Check for counting keywords
        if any(keyword in query_lower for keyword in self.counting_keywords):
            return True
            
        # Advanced pattern matching for counting queries
        counting_patterns = [
            r'\bhow\s+many\b',
            r'\bcount(?:ing)?\b',
            r'\btotal\s+(?:number|amount|count)?\b',
            r'\bnumber\s+of\b',
        ]
        
        return any(re.search(pattern, query_lower) for pattern in counting_patterns)
        
    def extract_count_target(self, query: str) -> str:
        """
        Extract what we're counting from the query.
        
        Args:
            query: Query string
            
        Returns:
            String describing what's being counted
        """
        query_lower = query.lower()
        
        # Try to extract the target object being counted
        # Simple pattern: "how many X" or "count X" or "total X"
        patterns = [
            r'how\s+many\s+(.*?)(?:\s+are|\s+with|\s+in|\s+is|\s+do|\?|$)',
            r'count\s+(?:of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)',
            r'total\s+(?:number\s+of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)',
            r'number\s+of\s+(.*?)(?:\s+in|\s+with|\s+that|\?|$)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query_lower)
            if match:
                return match.group(1).strip()
                
        # Fallback: remove counting keywords and return the rest
        for keyword in self.counting_keywords:
            if keyword in query_lower:
                return query_lower.replace(keyword, '').strip()
                
        return "items"  # Default if we can't determine what to count