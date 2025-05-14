"""
Main search engine for coordinating different search strategies.
"""

import os
from typing import Dict, List, Any, Optional, Tuple

from ..providers.base import DataProvider
from ..providers.hybrid_provider import HybridProvider
from ..query.classification import classify_query
from ..query.filters import extract_filters
from ..query.temporal import extract_temporal_filters

class SearchEngine:
    """
    Search engine that coordinates different search strategies.
    """
    
    def __init__(self, data_provider: DataProvider, cache_dir: Optional[str] = None):
        """
        Initialize the search engine.
        
        Args:
            data_provider: Data provider to use
            cache_dir: Directory for caching search data
        """
        self.data_provider = data_provider
        self.cache_dir = cache_dir or os.path.join(os.getcwd(), 'search_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Field weights for vector search
        self.field_weights = {
            'id': 1.0,
            'job_id': 1.0,
            'name': 5.0,
            'job_name': 5.0,
            'status': 2.0,
            'description': 4.0,
            'details': 4.0,
            'error_message': 3.0,
            'tags': 3.0,
            'owner': 2.0,
            'started_by': 2.0,
            'priority': 1.5,
            'duration_minutes': 1.0,
            'default': 1.0
        }
    
    def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Main search method that automatically chooses the best strategy.
        
        Args:
            query: User query string
            limit: Maximum number of results to return
            
        Returns:
            List of search result dictionaries
        """
        # 1. Extract structured filters
        structured_filters = extract_filters(query, self.data_provider.get_all_fields())
        
        # 2. Extract temporal filters
        timestamp_fields = getattr(self.data_provider.field_mapping, 'timestamp_fields', [])
        temporal_filters = extract_temporal_filters(query, timestamp_fields)
        
        # 3. Combine filters
        combined_filters = {**structured_filters, **temporal_filters}
        
        # 4. Classify the query
        query_type = classify_query(
            query, 
            self.data_provider.get_all_fields(),
            combined_filters
        )
        
        # 5. Execute search based on provider type
        if isinstance(self.data_provider, HybridProvider):
            # Use hybrid provider's specialized method
            return self.data_provider.search_hybrid(
                query, 
                query_type, 
                combined_filters,
                limit
            )
        else:
            # For non-hybrid providers, implement fallback logic
            results = []
            
            # Try structured search first if applicable
            if query_type in ['structured', 'hybrid'] and combined_filters:
                structured_results = self.data_provider.query_records(combined_filters, limit)
                for record in structured_results:
                    results.append({
                        'job_details': self.data_provider.prepare_for_output(record),
                        'score': 1.0,
                        'match_type': 'structured'
                    })
            
            # If needed, add vector search results
            if (query_type in ['vector', 'hybrid'] or not results) and hasattr(self.data_provider, 'execute_vector_search'):
                # Use vector search if available
                vector_limit = max(0, limit - len(results))
                if vector_limit > 0:
                    vector_results = self.data_provider.execute_vector_search(query, vector_limit)
                    results.extend(vector_results)
            
            # Sort and return results
            results.sort(key=lambda x: (
                0 if x['match_type'] == 'structured' else 1,
                -x['score']
            ))
            
            return results[:limit]