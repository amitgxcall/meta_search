"""
Search engine implementation for the meta_search system.

This module provides the core search functionality, coordinating searches
across multiple data providers and handling query parsing, execution, and
result formatting.
"""

import os
import re
import logging
from typing import List, Dict, Any, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta

# Import from base modules
from ..providers.base import DataProvider
from ..utils.field_mapping import FieldMapping

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        
        # Set cache directory
        self.cache_dir = cache_dir
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
    
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
        # Check if this is an ID-based query
        id_value = self.extract_id_from_query(query)
        if id_value:
            logger.info(f"Detected ID search for: {id_value}")
            
            # Try to get the item directly by ID from any provider
            for provider in self.providers:
                item = provider.get_by_id(id_value)
                if item:
                    # Return as a list with a single item
                    item['_match_type'] = 'exact_id'
                    item['_score'] = 1.0
                    return [item]
            
            # If no exact match found, continue with standard search
            logger.info(f"No exact match found for ID {id_value}, falling back to standard search")
        
        # Check if this is a counting query
        if self.is_counting_query(query):
            return self._handle_counting_query(query)
        
        # Standard search
        results = []
        
        for provider in self.providers:
            provider_results = provider.search(query, limit=limit)
            results.extend(provider_results)
            
        # Sort results by relevance (if available)
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        # Limit the number of results
        return results[:limit]
    
    def extract_id_from_query(self, query: str) -> Optional[str]:
        """
        Extract an ID from a query if it appears to be an ID search.
        
        Args:
            query: The search query
            
        Returns:
            The ID string if found, None otherwise
        """
        # Pattern for "id X", "ID: X", "item id X", etc.
        id_patterns = [
            r'(?:^|\s)id\s*[:=]?\s*(\d+)',
            r'(?:^|\s)item\s+id\s*[:=]?\s*(\d+)',
            r'(?:^|\s)item[-_]id\s*[:=]?\s*(\d+)',
            r'(?:^|\s)#(\d+)',
            r'(?:^|\s)number\s*[:=]?\s*(\d+)',
            r'(?:^|\s)(\d{4,})\s*$'  # Standalone number (at least 4 digits)
        ]
        
        for pattern in id_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
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
        
        # Apply additional filters
        filtered_results = self.filter_results_by_criteria(all_results, filters)
        
        # Count by group if specified
        count_by_field = None
        if 'group by ' in query.lower():
            # Extract the field to group by
            match = re.search(r'group by\s+(\w+)', query.lower())
            if match:
                count_by_field = match.group(1)
        
        # Return counting result
        result = {
            "query_type": "counting",
            "query": query,
            "search_query": search_query,
            "count": len(filtered_results),
            "count_target": count_target,
            "filters": filters,
            "sample_results": filtered_results[:5] if filtered_results else []
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
        counts = {}
        
        for result in results:
            # Get the field value, using the mapped field if needed
            value = str(result.get(field, 'unknown'))
            
            # Increment count
            if value not in counts:
                counts[value] = 0
            counts[value] += 1
        
        return counts
    
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
        query_lower = query.lower()
        
        # Check for "last X days/weeks/months/years" pattern
        last_pattern = r'(?:in|from|within) the last (\d+)\s+(day|days|week|weeks|month|months|year|years)'
        match = re.search(last_pattern, query_lower)
        
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            
            # Calculate start date
            now = datetime.now()
            
            if unit in ['day', 'days']:
                start_date = now - timedelta(days=amount)
            elif unit in ['week', 'weeks']:
                start_date = now - timedelta(weeks=amount)
            elif unit in ['month', 'months']:
                # Approximate months as 30 days
                start_date = now - timedelta(days=30 * amount)
            elif unit in ['year', 'years']:
                # Approximate years as 365 days
                start_date = now - timedelta(days=365 * amount)
            
            # Format as ISO string
            start_date_str = start_date.isoformat()
            
            # Add to filters using generalized timestamp field (can be mapped by providers)
            filters['timestamp'] = {'gte': start_date_str}
            
            logger.info(f"Extracted temporal filter: last {amount} {unit}, start date: {start_date_str}")
        
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
        
        # Remove "group by" clause
        search_query = re.sub(r'group by\s+\w+', '', search_query)
        
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
                # Handle results with nested structure
                actual_field = field
                if 'job_details' in result:
                    current_result = result['job_details']
                else:
                    current_result = result
                
                # Check if field exists
                if actual_field in current_result:
                    field_value = current_result[actual_field]
                    
                    # Handle different value types
                    if isinstance(value, dict):
                        # Operators (gt, lt, etc.)
                        for op, op_value in value.items():
                            if not self._apply_operator(op, field_value, op_value):
                                match = False
                                break
                    else:
                        # Direct comparison
                        if str(field_value).lower() != str(value).lower():
                            # Try contains for text fields
                            if not isinstance(value, str) or value.lower() not in str(field_value).lower():
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
            
            # Handle timestamp comparisons
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