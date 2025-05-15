"""
Search engine implementation for the meta_search system.

This module provides the core search functionality, coordinating searches
across multiple data providers and handling query parsing, execution, and
result formatting.

Example:
    # Create a search engine
    engine = SearchEngine()
    
    # Register data providers
    engine.register_provider(csv_provider)
    
    # Execute a search
    results = engine.search("failed database jobs")
"""

import os
import re
from typing import List, Dict, Any, Optional, Union, Callable, Tuple

from ..providers.base import DataProvider


class SearchEngine:
    """
    Main search engine that coordinates searching across multiple providers.
    
    This class handles query parsing, execution, and result formatting.
    It supports various query types, including standard searches, counting
    queries, and ID-based searches.
    
    Attributes:
        providers: List of registered data providers
        field_weights: Dictionary of field weights for scoring
    """
    
    def __init__(self, data_provider: Optional[DataProvider] = None, cache_dir: Optional[str] = None):
        """
        Initialize the search engine.
        
        Args:
            data_provider: Initial data provider (optional)
            cache_dir: Directory for caching search data (optional)
        """
        self.providers = []
        
        # Default field weights for scoring
        self.field_weights = {
            'name': 2.0,      # Name fields get higher weight
            'description': 1.5,  # Description fields get higher weight
            'status': 1.0,    # Status fields get normal weight
            'error_message': 1.0,  # Error message fields get normal weight
            'default': 0.5    # Default weight for other fields
        }
        
        # Register initial provider if provided
        if data_provider:
            self.register_provider(data_provider)
    
    def register_provider(self, provider: DataProvider) -> None:
        """
        Register a data provider with the search engine.
        
        Args:
            provider: The data provider to register
        """
        self.providers.append(provider)
    
    def search(self, query: str, limit: int = 10) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Search across all registered providers.
        
        This method handles various query types:
        - ID-based queries (e.g., "job id 123")
        - Counting queries (e.g., "how many failed jobs")
        - Standard search queries (e.g., "failed database jobs")
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of search results or a dictionary with counting results
        """
        # Check if this is a counting query
        if self.is_counting_query(query):
            return self._handle_counting_query(query)
        
        # Standard search
        results = []
        
        for provider in self.providers:
            provider_results = provider.search(query)
            results.extend(provider_results)
            
        # Sort results by relevance (if available)
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        # Limit the number of results
        return results[:limit]
    
    def _handle_counting_query(self, query: str) -> Dict[str, Any]:
        """
        Handle a counting query.
        
        Args:
            query: The counting query
            
        Returns:
            Dictionary with counting results
        """
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
    
    def filter_results_by_criteria(self, 
                                 results: List[Dict[str, Any]], 
                                 filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Filter results based on extracted criteria.
        
        Args:
            results: List of search results
            filters: Dictionary of field:value filters
            
        Returns:
            Filtered results
        """
        if not filters:
            return results
        
        filtered_results = []
        for result in results:
            match = True
            for field, value in filters.items():
                if field in result:
                    # Handle different value types
                    if isinstance(value, dict):
                        # Operators (gt, lt, etc.)
                        for op, op_value in value.items():
                            if not self._apply_operator(op, result[field], op_value):
                                match = False
                                break
                    else:
                        # Direct comparison
                        if str(result[field]).lower() != str(value).lower():
                            # Try contains for text fields
                            if not isinstance(value, str) or value.lower() not in str(result[field]).lower():
                                match = False
                                break
                else:
                    match = False
                    break
            
            if match:
                filtered_results.append(result)
        
        return filtered_results
    
    def _apply_operator(self, op: str, field_value: Any, op_value: Any) -> bool:
        """
        Apply a comparison operator.
        
        Args:
            op: Operator name ('gt', 'lt', etc.)
            field_value: Value from the field
            op_value: Value to compare against
            
        Returns:
            True if the comparison is successful, False otherwise
        """
        try:
            # Convert values to numbers if possible
            if isinstance(field_value, str) and field_value.replace('.', '', 1).isdigit():
                field_value = float(field_value) if '.' in field_value else int(field_value)
            
            if op == 'gt':
                return field_value > op_value
            elif op == 'lt':
                return field_value < op_value
            elif op == 'gte':
                return field_value >= op_value
            elif op == 'lte':
                return field_value <= op_value
            elif op == 'eq':
                return field_value == op_value
            elif op == 'neq':
                return field_value != op_value
            elif op == 'contains' and isinstance(field_value, str):
                return str(op_value).lower() in field_value.lower()
            else:
                return False
        except (ValueError, TypeError):
            return False
    
    def format_for_llm(self, 
                      results: List[Dict[str, Any]], 
                      query: str,
                      id_field: str = 'id',
                      name_field: str = 'name',
                      status_field: Optional[str] = 'status') -> Dict[str, Any]:
        """
        Format search results for consumption by a language model.
        
        Args:
            results: Search results
            query: Original query
            id_field: Field name for ID
            name_field: Field name for name
            status_field: Field name for status
            
        Returns:
            Dictionary formatted for LLM consumption
        """
        # Import the result formatter (late import to avoid circular references)
        try:
            from ..search.results.formatter import format_for_llm
            return format_for_llm(results, query, id_field, name_field, status_field)
        except ImportError:
            # Fallback to inline implementation
            if not results:
                return {
                    "query": query,
                    "result_count": 0,
                    "results": [],
                    "suggested_response": f"No results found for '{query}'."
                }
            
            # Basic formatting
            formatted_results = []
            for r in results:
                formatted_result = {}
                for k, v in r.items():
                    if not k.startswith('_'):
                        formatted_result[k] = v
                formatted_results.append(formatted_result)
            
            return {
                "query": query,
                "result_count": len(results),
                "results": formatted_results,
                "suggested_response": f"Found {len(results)} results for '{query}'."
            }
    
    def explain_search(self, query: str) -> Dict[str, Any]:
        """
        Explain how a query will be processed.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with explanation
        """
        is_counting = self.is_counting_query(query)
        filters = self.extract_filters(query)
        
        explanation = {
            "query": query,
            "is_counting_query": is_counting,
            "filters": filters,
        }
        
        if is_counting:
            count_target = self.extract_count_target(query)
            search_query = self.preprocess_counting_query(query)
            explanation.update({
                "count_target": count_target,
                "search_query": search_query
            })
        
        return explanation