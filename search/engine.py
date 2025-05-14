"""
Search engine implementation for meta_search.
"""

import os
import sys
import re
from typing import List, Dict, Any, Optional, Union

# Direct imports instead of relative imports
try:
    from providers.base import DataProvider
except ImportError:
    # Add the parent directory to the path for imports
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from providers.base import DataProvider

class SearchEngine:
    """
    Main search engine that coordinates searching across multiple providers.
    """
    
    def __init__(self):
        """Initialize the search engine."""
        self.providers = []
        
    def register_provider(self, provider: DataProvider) -> None:
        """
        Register a data provider with the search engine.
        
        Args:
            provider: The data provider to register
        """
        self.providers.append(provider)
    
    def is_counting_query(self, query: str) -> bool:
        """
        Determine if a query is asking for a count.
        
        Args:
            query: Query string
            
        Returns:
            True if the query is about counting, False otherwise
        """
        query_lower = query.lower()
        
        # Keywords that indicate counting queries
        counting_keywords = [
            'how many', 'count', 'total', 'number of', 'tally', 
            'sum of', 'sum up', 'calculate', 'compute'
        ]
        
        # Check for counting keywords
        if any(keyword in query_lower for keyword in counting_keywords):
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
        
        # Fallback: look for keywords related to jobs
        job_related_words = ['job', 'jobs', 'task', 'tasks', 'process', 'processes']
        for word in job_related_words:
            if word in query_lower:
                return word
                
        return "items"  # Default if we can't determine what to count
    
    def extract_filters(self, query: str) -> Dict[str, Any]:
        """
        Extract filter criteria from the query.
        
        Args:
            query: Query string
            
        Returns:
            Dictionary of field:value filters
        """
        # Extract explicit field:value patterns
        filters = {}
        field_value_pattern = r'(\w+)[:=]"([^"]+)"|(\w+)[:=](\S+)'
        
        for match in re.finditer(field_value_pattern, query):
            field1, value1, field2, value2 = match.groups()
            field = field1 if field1 else field2
            value = value1 if value1 else value2
            filters[field] = value
        
        # Extract comparison operators (<, >, <=, >=)
        comparison_pattern = r'(\w+)\s*(<=|>=|<|>|=)\s*(\d+(?:\.\d+)?)'
        for match in re.finditer(comparison_pattern, query):
            field, operator, value = match.groups()
            
            # Convert numeric value
            try:
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)
            except ValueError:
                continue
            
            # Create operator mapping
            op_map = {
                '<': 'lt',
                '>': 'gt',
                '<=': 'lte',
                '>=': 'gte',
                '=': 'eq'
            }
            
            if operator in op_map:
                # Format for filter
                if field not in filters:
                    filters[field] = {}
                
                if isinstance(filters[field], dict):
                    filters[field][op_map[operator]] = value
                else:
                    # Convert to dict if it's a simple value
                    filters[field] = {op_map[operator]: value}
        
        # Extract special keywords
        keyword_mapping = {
            'failed': {'status': 'failed'},
            'success': {'status': 'success'},
            'running': {'status': 'running'},
            'completed': {'status': 'completed'},
            'pending': {'status': 'pending'},
            'high': {'priority': 'high'},
            'medium': {'priority': 'medium'},
            'low': {'priority': 'low'},
            'critical': {'priority': 'critical'}
        }
        
        query_lower = query.lower()
        for keyword, filter_dict in keyword_mapping.items():
            if re.search(r'\b' + keyword + r'\b', query_lower):
                filters.update(filter_dict)
        
        return filters
    
    def preprocess_counting_query(self, query: str) -> str:
        """
        Preprocess a counting query to create a standard search query.
        
        Args:
            query: The counting query
            
        Returns:
            A modified query for standard search
        """
        # Remove counting keywords
        counting_keywords = [
            'how many', 'count', 'total', 'number of', 'tally', 
            'sum of', 'sum up', 'calculate', 'compute'
        ]
        
        search_query = query.lower()
        for keyword in counting_keywords:
            search_query = search_query.replace(keyword, '').strip()
        
        # Remove question marks
        search_query = search_query.replace('?', '').strip()
        
        # Remove filler words
        filler_words = ['are', 'is', 'there', 'do', 'we', 'have', 'the']
        for word in filler_words:
            search_query = re.sub(r'\b' + word + r'\b', '', search_query)
        
        return search_query.strip()
    
    def search(self, query: str, limit: int = 10) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Search across all registered providers with enhanced counting support.
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of search results or a dictionary with counting results
        """
        # Check if this is a counting query
        if self.is_counting_query(query):
            # Extract what we're counting
            count_target = self.extract_count_target(query)
            
            # Extract filters from the query
            filters = self.extract_filters(query)
            
            # Create a standard search query by removing counting keywords
            search_query = self.preprocess_counting_query(query)
            
            # Get results from all providers
            all_results = []
            for provider in self.providers:
                provider_results = provider.search(search_query)
                all_results.extend(provider_results)
            
            # Sort results by relevance (if available)
            all_results.sort(key=lambda x: x.get('_score', 0), reverse=True)
            
            # Return counting result
            return {
                "query_type": "counting",
                "query": query,
                "search_query": search_query,
                "count": len(all_results),
                "count_target": count_target,
                "filters": filters,
                "sample_results": all_results[:5] if all_results else []
            }
        
        # Standard search (non-counting)
        results = []
        
        for provider in self.providers:
            provider_results = provider.search(query)
            results.extend(provider_results)
            
        # Sort results by relevance (if available)
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        # Limit the number of results
        return results[:limit]