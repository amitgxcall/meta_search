"""
Modified hybrid provider that shows structured results first, then vector results.
"""

import os
from typing import List, Dict, Any, Optional, Set

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from providers.hybrid_provider import HybridProvider

class SequentialHybridProvider(HybridProvider):
    """
    Modified hybrid provider that displays structured search results first,
    followed by vector search results, instead of combining them with a weighted score.
    """
    
    def search(self, query: str, hybrid_weight: float = 0.5) -> List[Dict[str, Any]]:
        """
        Search using both structured data and vector search, showing structured results first.
        
        Args:
            query: The search query
            hybrid_weight: Not used in this implementation, kept for API compatibility
            
        Returns:
            List of search results, with structured results first followed by vector results
        """
        # Build vector index if not already built
        if not self.vector_index_built:
            if not self.build_vector_index():
                # If we couldn't build the vector index, just use the data provider
                return self.data_provider.search(query)
        
        # Get data provider results
        structured_results = self.data_provider.search(query)
        
        # Get vector search results
        query_embedding = self.vector_search.get_mock_embedding(query)
        vector_results = self.vector_search.search(query_embedding)
        
        # Convert vector results to same format as structured results
        vector_results_dict = [
            {**item_data, "_score": similarity, "_result_type": "vector"} 
            for item_id, similarity, item_data in vector_results
        ]
        
        # If structured search returns no results, just use vector results
        if not structured_results:
            return vector_results_dict
        
        # Mark structured results
        for item in structured_results:
            item["_result_type"] = "structured"
        
        # Get the sequential combined results
        return self._sequential_combine(structured_results, vector_results_dict)
    
    def _sequential_combine(
        self, 
        structured_results: List[Dict[str, Any]], 
        vector_results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Combine results by placing structured results first, followed by vector results,
        removing duplicates from vector results.
        
        Args:
            structured_results: Results from structured data search
            vector_results: Results from vector search
            
        Returns:
            Combined results with structured results first
        """
        # Determine the ID field
        id_field = 'id'
        if self.field_mapping is not None:
            for standard_name in self.field_mapping.get_mappings().keys():
                if standard_name == 'id':
                    id_field = standard_name
                    break
        
        # Get IDs from structured results to avoid duplicates
        structured_ids = set()
        for item in structured_results:
            if id_field in item:
                structured_ids.add(str(item[id_field]))
        
        # Filter out duplicates from vector results
        filtered_vector_results = [
            item for item in vector_results 
            if id_field in item and str(item[id_field]) not in structured_ids
        ]
        
        # Combine the results (structured first, then vector)
        combined_results = structured_results + filtered_vector_results
        
        # Add a "separator" item between structured and vector results if both exist
        if structured_results and filtered_vector_results:
            separator_index = len(structured_results)
            combined_results.insert(separator_index, {
                "_separator": True,
                "_message": "Vector search results (semantic matches) below:",
                "_result_type": "separator"
            })
        
        return combined_results