"""
Main search engine for the job search system.
"""

import os
import re
from typing import Dict, List, Any, Optional, Tuple
import hashlib

from ..providers.base import DataProvider
from .query_classifier import QueryClassifier
from .query_patterns import create_query_patterns, extract_multi_part_filters
from .vector_search import VectorSearchEngine
from .result_formatter import format_for_llm

class SearchEngine:
    """
    Search engine that coordinates the search process using data providers.
    """
    
    def __init__(self, data_provider: DataProvider, cache_dir: Optional[str] = None):
        """
        Initialize the search engine.
        
        Args:
            data_provider: Data provider to use
            cache_dir: Directory to cache vectors and other search data
        """
        self.data_provider = data_provider
        self.cache_dir = cache_dir or os.path.join(os.getcwd(), 'search_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize field weights
        self.field_weights = {
            'id': 1.0,
            'name': 3.0,
            'status': 2.5,
            'error_message': 2.0,
            'started_by': 1.5,
            'priority': 1.5,
            'duration_minutes': 1.2,
            'days_ago': 2.0,
            'is_latest': 2.5,
            'is_latest_run': 2.5,
            'default': 1.0
        }
        
        # Initialize query patterns and classifier
        self.query_patterns = create_query_patterns()
        self.query_classifier = QueryClassifier(self.query_patterns)
        
        # Initialize vector search
        self.vector_search = self._init_vector_search()
    
    def _init_vector_search(self) -> VectorSearchEngine:
        """
        Initialize vector search engine.
        
        Returns:
            Vector search engine
        """
        # Create vector search engine
        vector_search = VectorSearchEngine(self.cache_dir)
        
        # Get data hash for caching
        data_hash = self._get_data_hash()
        
        # Get all records
        records = self.data_provider.get_all_records()
        
        # Create texts for vectorization
        texts = [
            self.data_provider.get_text_for_vector_search(record, self.field_weights)
            for record in records
        ]
        
        # Initialize with texts
        vector_search.initialize(data_hash, texts)
        
        return vector_search
    
    def _get_data_hash(self) -> str:
        """
        Generate a hash of the data source for caching.
        
        Returns:
            Hash string
        """
        # Get unique identifier for data source
        fields = sorted(self.data_provider.get_all_fields())
        record_count = self.data_provider.get_record_count()
        
        # Create a string representation
        data_repr = f"{type(self.data_provider).__name__}_{record_count}_{','.join(fields)}"
        
        # Generate hash
        return hashlib.md5(data_repr.encode()).hexdigest()
    
    def extract_filters(self, query: str) -> Dict[str, Any]:
        """
        Extract structured filters from a query.
        
        Args:
            query: User query string
            
        Returns:
            Dictionary of field:value pairs for filtering
        """
        # First try the query patterns
        for pattern, extraction_func in self.query_patterns:
            match = pattern.search(query) if hasattr(pattern, 'search') else re.search(pattern, query, re.IGNORECASE)
            if match:
                filters = extraction_func(match)
                if filters:
                    return filters
        
        # Try multi-part filters
        multi_part_filters = extract_multi_part_filters(query, self.data_provider.get_all_fields())
        if multi_part_filters:
            return multi_part_filters
        
        # Extract field:value pairs
        field_filters = {}
        
        # Extract from field:value pattern
        for match in re.finditer(r'(\w+)[:=]"?([^",]+)"?', query):
            field, value = match.groups()
            field = field.strip()
            value = value.strip()
            field_filters[field] = value
        
        # Extract from comparison operators
        for match in re.finditer(r'(\w+)\s*([><]=?)\s*(\d+(?:\.\d+)?)', query):
            field, operator, value = match.groups()
            field = field.strip()
            
            # Map operator to filter syntax
            op_map = {'>': 'gt', '>=': 'gte', '<': 'lt', '<=': 'lte'}
            filter_op = op_map.get(operator)
            
            if filter_op:
                # Convert value to numeric
                try:
                    if '.' in value:
                        numeric_value = float(value)
                    else:
                        numeric_value = int(value)
                    
                    field_filters[field] = {filter_op: numeric_value}
                except ValueError:
                    # Fallback to string
                    field_filters[field] = value
        
        return field_filters
    
    def execute_structured_search(self, filters: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Execute structured search using the data provider.
        
        Args:
            filters: Dictionary of field:value pairs for filtering
            limit: Maximum number of results to return
            
        Returns:
            List of search result dictionaries
        """
        if not filters:
            return []
        
        # Query the data provider
        records = self.data_provider.query_records(filters, limit)
        
        # Format as search results
        results = []
        for record in records:
            processed_record = self.data_provider.prepare_for_output(record)
            results.append({
                'job_details': processed_record,
                'score': 1.0,  # Perfect score for exact matches
                'match_type': 'exact',
                'matched_filters': filters
            })
        
        return results
    
    def execute_vector_search(self, query: str, top_k: int = 5, exclude_ids: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute vector similarity search.
        
        Args:
            query: Query string
            top_k: Maximum number of results to return
            exclude_ids: List of record IDs to exclude from results
            
        Returns:
            List of search result dictionaries
        """
        # Get all records for lookup
        all_records = self.data_provider.get_all_records()
        
        # If vector search is not enabled, return empty results
        if not self.vector_search.enabled:
            return []
        
        # Convert exclude_ids to set for faster lookup
        exclude_ids_set = set(exclude_ids or [])
        id_field = self.data_provider.field_mapping.id_field
        
        # Perform vector search
        indices, distances = self.vector_search.search(query, top_k * 2)  # Get more than needed for filtering
        
        # Process results
        results = []
        
        for i, idx in enumerate(indices):
            if len(results) >= top_k:
                break
                
            if idx < len(all_records):
                record = all_records[idx]
                record_id = record.get(id_field)
                
                # Skip if ID should be excluded
                if record_id in exclude_ids_set:
                    continue
                
                # Calculate a normalized score (0-1, where 1 is best)
                # Lower distance is better, so we invert
                max_distance = 100.0  # Arbitrary scaling factor
                normalized_score = max(0, 1 - (distances[i] / max_distance))
                
                # Format as search result
                processed_record = self.data_provider.prepare_for_output(record)
                results.append({
                    'job_details': processed_record,
                    'score': normalized_score,
                    'match_type': 'semantic',
                    'distance': float(distances[i])
                })
        
        return results
    
    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Main search method combining structured and vector search.
        
        Args:
            query: User query string
            top_k: Maximum number of results to return
            
        Returns:
            List of search result dictionaries
        """
        # 1. Classify query
        query_type = self.query_classifier.classify(query)
        
        # 2. Extract structured filters
        filters = self.extract_filters(query)
        
        all_results = []
        
        # 3. Execute structured search if applicable
        if query_type in ['structured', 'hybrid'] and filters:
            structured_results = self.execute_structured_search(filters, top_k)
            all_results.extend(structured_results)
            
            # For debugging
            if not structured_results:
                print(f"Info: No structured results found for query '{query}' with filters: {filters}")
        
        # 4. Execute vector search if needed
        if query_type in ['semantic', 'hybrid'] or not all_results:
            # Get IDs to exclude (already found in structured results)
            id_field = self.data_provider.field_mapping.id_field
            exclude_ids = [r['job_details'].get(id_field) for r in all_results]
            
            # Calculate how many semantic results we need
            remaining_count = max(0, top_k - len(all_results))
            
            if remaining_count > 0:
                semantic_results = self.execute_vector_search(query, remaining_count, exclude_ids)
                all_results.extend(semantic_results)
                
                # For debugging
                if not semantic_results and query_type == 'semantic':
                    print(f"Info: No semantic results found for query '{query}'")
        
        # 5. Sort results (exact matches first, then by score)
        all_results.sort(key=lambda x: (0 if x['match_type'] == 'exact' else 1, -x['score']))
        
        # 6. For debugging, if no results, provide more information
        if not all_results:
            print(f"Info: No results at all for query '{query}'")
            print(f"- Classification: {query_type}")
            print(f"- Extracted filters: {filters}")
        
        return all_results[:top_k]
    
    def format_for_llm(self, results: List[Dict[str, Any]], user_query: str) -> Dict[str, Any]:
        """
        Format search results for an LLM.
        
        Args:
            results: List of search result dictionaries
            user_query: Original user query
            
        Returns:
            Dictionary formatted for LLM consumption
        """
        return format_for_llm(
            results, 
            user_query,
            id_field=self.data_provider.field_mapping.id_field,
            name_field=self.data_provider.field_mapping.name_field,
            status_field=self.data_provider.field_mapping.status_field
        )
    
    def explain_search(self, query: str) -> Dict[str, Any]:
        """
        Explain how a query would be processed.
        
        Args:
            query: User query string
            
        Returns:
            Dictionary with explanation details
        """
        # Classify query
        query_type = self.query_classifier.classify(query)
        
        # Extract filters
        filters = self.extract_filters(query)
        
        # Check which patterns matched
        matched_patterns = []
        for pattern, _ in self.query_patterns:
            pattern_str = pattern if isinstance(pattern, str) else getattr(pattern, 'pattern', str(pattern))
            match = re.search(pattern_str, query, re.IGNORECASE)
            if match:
                matched_patterns.append({
                    "pattern": pattern_str,
                    "matched_text": match.group(0)
                })
        
        # Create explanation
        explanation = {
            "query": query,
            "classification": query_type,
            "structured_filters": filters,
            "field_weights": self.field_weights,
            "matched_patterns": matched_patterns,
            "data_provider_type": type(self.data_provider).__name__,
            "field_mapping": {
                "id_field": self.data_provider.field_mapping.id_field,
                "name_field": self.data_provider.field_mapping.name_field,
                "status_field": self.data_provider.field_mapping.status_field
            },
            "vector_search_enabled": self.vector_search.enabled
        }
        
        return explanation