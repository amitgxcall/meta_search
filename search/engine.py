"""
Search engine implementation for the meta_search system.

This module provides the core search functionality, coordinating searches
across multiple data providers and handling query parsing, execution, and
result formatting.
"""

import os
import re
import logging
import time
import numpy as np
from typing import List, Dict, Any, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta
from functools import lru_cache
from collections import defaultdict

# Import from base modules
from ..providers.base import DataProvider
from ..utils.field_mapping import FieldMapping

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pre-compile frequently used regular expressions
ID_PATTERN = re.compile(r'(?:^|\s)id\s*[:=]?\s*(\d+)|(?:^|\s)item\s+id\s*[:=]?\s*(\d+)|(?:^|\s)#(\d+)', re.IGNORECASE)
COUNTING_PATTERNS = [
    re.compile(r'\bhow\s+many\b', re.IGNORECASE),
    re.compile(r'\bcount(?:ing)?\b', re.IGNORECASE),
    re.compile(r'\btotal\s+(?:number|amount|count)?\b', re.IGNORECASE),
    re.compile(r'\bnumber\s+of\b', re.IGNORECASE),
]
COUNT_TARGET_PATTERNS = [
    re.compile(r'how\s+many\s+(.*?)(?:\s+are|\s+with|\s+in|\s+is|\s+do|\?|$)', re.IGNORECASE),
    re.compile(r'count\s+(?:of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)', re.IGNORECASE),
    re.compile(r'total\s+(?:number\s+of\s+)?(.*?)(?:\s+in|\s+with|\s+that|\?|$)', re.IGNORECASE),
    re.compile(r'number\s+of\s+(.*?)(?:\s+in|\s+with|\s+that|\?|$)', re.IGNORECASE)
]
FIELD_VALUE_PATTERN = re.compile(r'(\w+)[:=]"([^"]+)"|(\w+)[:=](\S+)')
COMPARISON_PATTERN = re.compile(r'(\w+)\s*(<=|>=|<|>|=|!=)\s*(\d+(?:\.\d+)?)')
TEMPORAL_PATTERN = re.compile(r'(?:in|from|within) the last (\d+)\s+(day|days|week|weeks|month|months|year|years)', re.IGNORECASE)
FILLER_WORDS_PATTERN = re.compile(r'\b(are|is|there|do|we|have|the)\b', re.IGNORECASE)
GROUP_BY_PATTERN = re.compile(r'group by\s+(\w+)', re.IGNORECASE)


class SearchEngine:
    """
    Main search engine that coordinates searching across multiple providers.
    
    This class handles query parsing, execution, and result formatting.
    It supports various query types, including standard searches, counting
    queries, and ID-based searches.
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
        
        # Set cache directory
        self.cache_dir = cache_dir
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
            
        # Performance metrics
        self.metrics = {
            'search_time': 0,
            'parsing_time': 0,
            'total_searches': 0,
            'count_by_type': defaultdict(int)
        }
        
        # Query classification cache
        self._query_type_cache = {}
    
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
        - ID-based queries (e.g., "id 123")
        - Counting queries (e.g., "how many items")
        - Standard search queries (e.g., "important items")
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of search results or a dictionary with counting results
        """
        start_time = time.time()
        self.metrics['total_searches'] += 1
        
        parsing_start = time.time()
        # Check if this is an ID-based query
        id_value = self.extract_id_from_query(query)
        if id_value:
            self.metrics['count_by_type']['id_query'] += 1
            logger.info(f"Detected ID search for: {id_value}")
            
            # Try to get the item directly by ID from any provider
            for provider in self.providers:
                item = provider.get_by_id(id_value)
                if item:
                    # Return as a list with a single item
                    item['_match_type'] = 'exact_id'
                    item['_score'] = 1.0
                    
                    search_time = time.time() - start_time
                    self.metrics['search_time'] += search_time
                    logger.info(f"Found exact ID match in {search_time:.4f} seconds")
                    return [item]
            
            # If no exact match found, continue with standard search
            logger.info(f"No exact match found for ID {id_value}, falling back to standard search")
        
        # Check if this is a counting query
        if self.is_counting_query(query):
            self.metrics['count_by_type']['counting_query'] += 1
            parsing_time = time.time() - parsing_start
            self.metrics['parsing_time'] += parsing_time
            
            count_result = self._handle_counting_query(query)
            
            search_time = time.time() - start_time
            self.metrics['search_time'] += search_time
            logger.info(f"Completed counting query in {search_time:.4f} seconds")
            
            return count_result
        
        # Standard search
        self.metrics['count_by_type']['standard_query'] += 1
        parsing_time = time.time() - parsing_start
        self.metrics['parsing_time'] += parsing_time
        
        results = []
        
        # Use vectorized operations where possible
        for provider in self.providers:
            provider_results = provider.search(query, limit=limit)
            results.extend(provider_results)
        
        # Sort results by relevance using numpy for better performance
        if results:
            # Extract scores as numpy array
            scores = np.array([r.get('_score', 0) for r in results])
            
            # Get sorted indices
            sorted_indices = np.argsort(scores)[::-1]  # Descending order
            
            # Reorder results
            results = [results[i] for i in sorted_indices]
            
            # Limit the number of results
            results = results[:limit]
        
        search_time = time.time() - start_time
        self.metrics['search_time'] += search_time
        logger.info(f"Standard search completed in {search_time:.4f} seconds, found {len(results)} results")
        
        return results
    
    def extract_id_from_query(self, query: str) -> Optional[str]:
        """
        Extract an ID from a query if it appears to be an ID search.
        
        Args:
            query: The search query
            
        Returns:
            The ID string if found, None otherwise
        """
        # Use pre-compiled pattern for better performance
        match = ID_PATTERN.search(query)
        if match:
            # Return the first non-None group
            for group in match.groups():
                if group:
                    return group
        
        return None
    
    def _handle_counting_query(self, query: str) -> Dict[str, Any]:
        """
        Handle a counting query.
        
        Args:
            query: The counting query
            
        Returns:
            Dictionary with counting results
        """
        start_time = time.time()
        
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
        
        # Apply additional filters
        filtered_results = self.filter_results_by_criteria(all_results, filters)
        
        # Check if grouping is requested
        count_by_field = None
        group_by_match = GROUP_BY_PATTERN.search(query.lower())
        if group_by_match:
            # Extract the field to group by
            count_by_field = group_by_match.group(1)
        
        # Create the result structure
        result = {
            "query_type": "counting",
            "query": query,
            "search_query": search_query,
            "count": len(filtered_results),
            "count_target": count_target,
            "filters": filters,
            "sample_results": filtered_results[:5] if filtered_results else [],
            "execution_time": time.time() - start_time
        }
        
        # Add count by field if specified
        if count_by_field:
            result["count_by_field"] = count_by_field
            result["count_by_value"] = self._count_by_field(filtered_results, count_by_field)
        
        return result
    
    def _count_by_field(self, results: List[Dict[str, Any]], field: str) -> Dict[str, int]:
        """
        Count results grouped by a field value.
        
        Args:
            results: List of search results
            field: Field to group by
            
        Returns:
            Dictionary mapping field values to counts
        """
        # Use defaultdict for more efficient counting
        counts = defaultdict(int)
        
        for result in results:
            # Get the field value, using the mapped field if needed
            value = str(result.get(field, 'unknown'))
            
            # Increment count
            counts[value] += 1
        
        return dict(counts)
    
    def is_counting_query(self, query: str) -> bool:
        """
        Determine if a query is asking for a count.
        
        Args:
            query: Query string
            
        Returns:
            True if the query is about counting, False otherwise
        """
        query_lower = query.lower()
        
        # Keywords that indicate counting queries - use set for O(1) lookup
        counting_keywords = {
            'how many', 'count', 'total', 'number of', 'tally', 
            'sum of', 'sum up', 'calculate', 'compute'
        }
        
        # Fast check with set membership first
        if any(keyword in query_lower for keyword in counting_keywords):
            return True
            
        # Use pre-compiled patterns for more complex checks
        return any(pattern.search(query_lower) for pattern in COUNTING_PATTERNS)
    
    def extract_count_target(self, query: str) -> str:
        """
        Extract what we're counting from the query.
        
        Args:
            query: Query string
            
        Returns:
            String describing what's being counted
        """
        query_lower = query.lower()
        
        # Use pre-compiled patterns for better performance
        for pattern in COUNT_TARGET_PATTERNS:
            match = pattern.search(query_lower)
            if match:
                return match.group(1).strip()
        
        # Fallback: look for keywords related to common items
        common_items = ['item', 'items', 'record', 'records', 'entry', 'entries', 
                       'document', 'documents', 'result', 'results']
        for word in common_items:
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
        
        # Use pre-compiled pattern for better performance
        for match in FIELD_VALUE_PATTERN.finditer(query):
            field1, value1, field2, value2 = match.groups()
            field = field1 if field1 else field2
            value = value1 if value1 else value2
            filters[field] = value
        
        # Extract comparison operators using pre-compiled pattern
        for match in COMPARISON_PATTERN.finditer(query):
            field, operator, value = match.groups()
            
            # Convert numeric value
            try:
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)
            except ValueError:
                continue
            
            # Create operator mapping - use dict for O(1) lookup
            op_map = {
                '<': 'lt',
                '>': 'gt',
                '<=': 'lte',
                '>=': 'gte',
                '=': 'eq',
                '!=': 'neq'
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
        
        # Extract temporal filters (e.g., "in the last 7 days")
        temporal_filters = self.extract_temporal_filters(query)
        if temporal_filters:
            filters.update(temporal_filters)
        
        return filters
    
    def extract_temporal_filters(self, query: str) -> Dict[str, Any]:
        """
        Extract temporal filters from the query.
        
        Args:
            query: Query string
            
        Returns:
            Dictionary of temporal filters
        """
        filters = {}
        
        # Use pre-compiled pattern for better performance
        match = TEMPORAL_PATTERN.search(query.lower())
        
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            
            # Calculate start date
            now = datetime.now()
            
            # Use a lookup table for time units
            time_units = {
                'day': lambda x: timedelta(days=x),
                'days': lambda x: timedelta(days=x),
                'week': lambda x: timedelta(weeks=x),
                'weeks': lambda x: timedelta(weeks=x),
                'month': lambda x: timedelta(days=30 * x),  # Approximate
                'months': lambda x: timedelta(days=30 * x),  # Approximate
                'year': lambda x: timedelta(days=365 * x),  # Approximate
                'years': lambda x: timedelta(days=365 * x)   # Approximate
            }
            
            if unit in time_units:
                start_date = now - time_units[unit](amount)
                
                # Format as ISO string
                start_date_str = start_date.isoformat()
                
                # Add to filters using generalized timestamp field (can be mapped by providers)
                filters['timestamp'] = {'gte': start_date_str}
                
                logger.info(f"Extracted temporal filter: last {amount} {unit}, start date: {start_date_str}")
        
        return filters
    
    @lru_cache(maxsize=128)
    def preprocess_counting_query(self, query: str) -> str:
        """
        Preprocess a counting query to create a standard search query.
        Cached for better performance with repeated queries.
        
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
        
        # Remove filler words with a single regex replacement
        search_query = FILLER_WORDS_PATTERN.sub('', search_query)
        
        # Remove "group by" clause
        search_query = GROUP_BY_PATTERN.sub('', search_query)
        
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
        
        # Fast path for common case of single field-value match
        if len(filters) == 1 and isinstance(next(iter(filters.values())), str):
            field, value = next(iter(filters.items()))
            value_lower = str(value).lower()
            
            return [
                r for r in results if 
                field in r and 
                (str(r[field]).lower() == value_lower or value_lower in str(r[field]).lower())
            ]
        
        # More complex filtering
        filtered_results = []
        for result in results:
            match = True
            for field, value in filters.items():
                # Handle results with nested structure
                current_result = result
                if 'job_details' in result:
                    current_result = result['job_details']
                
                # Check if field exists
                if field in current_result:
                    field_value = current_result[field]
                    
                    # Handle different value types
                    if isinstance(value, dict):
                        # Operators (gt, lt, etc.)
                        for op, op_value in value.items():
                            if not self._apply_operator(op, field_value, op_value):
                                match = False
                                break
                    else:
                        # Direct comparison
                        value_str = str(value).lower()
                        field_str = str(field_value).lower()
                        
                        if value_str != field_str and value_str not in field_str:
                            match = False
                            break
                else:
                    # Field not found in result
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
            
            # Lookup table for operators - more efficient than if/elif chain
            operators = {
                'gt': lambda a, b: a > b,
                'lt': lambda a, b: a < b,
                'gte': lambda a, b: a >= b,
                'lte': lambda a, b: a <= b,
                'eq': lambda a, b: a == b,
                'neq': lambda a, b: a != b,
                'contains': lambda a, b: str(b).lower() in str(a).lower() if isinstance(a, str) else False
            }
            
            if op in operators:
                return operators[op](field_value, op_value)
                
            return False
        except (ValueError, TypeError):
            return False
    
    def get_field_weights(self) -> Dict[str, float]:
        """
        Get the current field weights for scoring.
        
        Returns:
            Dictionary of field weights
        """
        return self.field_weights.copy()
    
    def set_field_weights(self, weights: Dict[str, float]) -> None:
        """
        Set the field weights for scoring.
        
        Args:
            weights: Dictionary of field weights
        """
        self.field_weights.update(weights)
    
    def explain_search(self, query: str) -> Dict[str, Any]:
        """
        Explain how a query will be processed.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with explanation details
        """
        explanation = {
            "query": query,
            "is_id_search": self.extract_id_from_query(query) is not None,
            "is_counting_query": self.is_counting_query(query),
            "filters": self.extract_filters(query),
        }
        
        if explanation["is_counting_query"]:
            explanation["count_target"] = self.extract_count_target(query)
            explanation["search_query"] = self.preprocess_counting_query(query)
        
        # Include temporal filters
        explanation["temporal_filters"] = self.extract_temporal_filters(query)
        
        return explanation
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for the search engine.
        
        Returns:
            Dictionary with performance metrics
        """
        if self.metrics['total_searches'] > 0:
            avg_search_time = self.metrics['search_time'] / self.metrics['total_searches']
            avg_parsing_time = self.metrics['parsing_time'] / self.metrics['total_searches']
            
            return {
                **self.metrics,
                'avg_search_time': avg_search_time,
                'avg_parsing_time': avg_parsing_time
            }
        else:
            return self.metrics