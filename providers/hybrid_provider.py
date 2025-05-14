"""
Hybrid data provider implementation that works with different data sources.
"""

import os
import numpy as np
from typing import List, Dict, Any, Optional, Tuple

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from providers.base import DataProvider
from providers.csv_provider import CSVProvider
from providers.sqlite_provider import SQLiteProvider
from search.vector_search import VectorSearchEngine

class HybridProvider(DataProvider):
    """
    Hybrid data provider that combines structured data providers with vector search.
    This provider automatically selects the appropriate data provider based on the file extension.
    """
    
    def __init__(self, source_path: str, vector_index_path: str = None, table_name: str = None):
        """
        Initialize the hybrid provider.
        
        Args:
            source_path: Path to the data source file
            vector_index_path: Path to save/load the vector index (if None, uses source_path + '.vector')
            table_name: Name of the table to use (if None, will try to detect)
        """
        super().__init__(source_path)
        
        # Set vector index path
        if vector_index_path is None:
            self.vector_index_path = source_path + '.vector'
        else:
            self.vector_index_path = vector_index_path
        
        # Determine provider type based on file extension
        self.file_ext = os.path.splitext(source_path)[1].lower()
        
        # Initialize appropriate provider
        if self.file_ext == '.csv':
            print(f"Using CSV provider for {source_path}")
            self.data_provider = CSVProvider(source_path)
        elif self.file_ext in ['.db', '.sqlite', '.sqlite3']:
            print(f"Using SQLite provider for {source_path}")
            self.data_provider = SQLiteProvider(source_path, table_name)
        elif self.file_ext == '.json':
            print(f"Using JSON provider for {source_path}")
            from providers.json_provider import JSONProvider
            self.data_provider = JSONProvider(source_path)
        else:
            print(f"Unknown file type: {self.file_ext}. Defaulting to CSV provider.")
            self.data_provider = CSVProvider(source_path)
        
        # Initialize vector search
        self.vector_search = VectorSearchEngine()
        
        # Keep track of whether the vector index is built
        self.vector_index_built = False
    
    def connect(self) -> bool:
        """
        Connect to the data sources.
        
        Returns:
            True if successful, False otherwise
        """
        # Connect to data provider
        if not self.data_provider.connect():
            return False
        
        # Try to load existing vector index
        if os.path.exists(self.vector_index_path):
            self.vector_index_built = self.vector_search.load_index(self.vector_index_path)
        
        return True
    
    def set_field_mapping(self, field_mapping) -> None:
        """
        Set the field mapping for this provider.
        
        Args:
            field_mapping: FieldMapping object that maps standard field names to source-specific names
        """
        super().set_field_mapping(field_mapping)
        self.data_provider.set_field_mapping(field_mapping)
    
    def build_vector_index(self, text_fields: List[str] = None) -> bool:
        """
        Build the vector index from scratch.
        
        Args:
            text_fields: List of fields to use for generating embeddings
                        (if None, uses all text fields)
            
        Returns:
            True if successful, False otherwise
        """
        print("Building vector index...")
        
        # Get all items from data provider
        if hasattr(self.data_provider, 'get_all_items'):
            items = self.data_provider.get_all_items()
        else:
            # For providers without a get_all_items method, try to use the data attribute
            items = getattr(self.data_provider, 'data', [])
            if items:
                # Map the fields
                items = [self.data_provider.map_fields(item) for item in items]
        
        if not items:
            print("No items found in data source.")
            return False
        
        # If no text fields specified, infer from first item
        if text_fields is None:
            text_fields = []
            for key, value in items[0].items():
                if isinstance(value, str) and len(value) > 5:
                    text_fields.append(key)
            
            print(f"Using text fields for embeddings: {', '.join(text_fields)}")
        
        # Get ID field
        id_field = 'id'
        if self.field_mapping is not None:
            # Look for mapped 'id' field
            for standard_name, source_name in self.field_mapping.get_mappings().items():
                if standard_name == 'id':
                    id_field = standard_name
                    break
        
        # Build vector index
        for item in items:
            if id_field not in item:
                print(f"Warning: Item missing ID field '{id_field}': {item}")
                continue
            
            item_id = str(item[id_field])
            
            # Combine text fields for embedding
            text_values = []
            for field in text_fields:
                if field in item and item[field]:
                    text_values.append(str(item[field]))
            
            text = " ".join(text_values)
            
            # Skip items with no text
            if not text:
                continue
            
            # Generate embedding (using mock function for now)
            embedding = VectorSearchEngine.get_mock_embedding(text)
            
            # Add to vector index
            self.vector_search.add_item(item_id, item, embedding)
        
        print(f"Added {len(self.vector_search.index)} items to vector index")
        
        # Save vector index
        if self.vector_search.save_index(self.vector_index_path):
            self.vector_index_built = True
            print(f"Vector index built with {len(self.vector_search.index)} items and saved to {self.vector_index_path}")
            return True
        else:
            print("Failed to save vector index.")
            return False
    
    def search(self, query: str, hybrid_weight: float = 0.5) -> List[Dict[str, Any]]:
        """
        Search using both structured data and vector search.
        
        Args:
            query: The search query
            hybrid_weight: Weight for combining results (0 = structured only, 1 = vector only)
            
        Returns:
            List of search results
        """
        # Build vector index if not already built
        if not self.vector_index_built:
            if not self.build_vector_index():
                # If we couldn't build the vector index, just use the data provider
                return self.data_provider.search(query)
        
        # Get data provider results
        structured_results = self.data_provider.search(query)
        
        # Get vector search results
        query_embedding = VectorSearchEngine.get_mock_embedding(query)
        vector_results = self.vector_search.search(query_embedding)
        
        # Convert vector results to same format as structured results
        vector_results_dict = [
            {**item_data, "_score": similarity} 
            for item_id, similarity, item_data in vector_results
        ]
        
        # If one of the methods returns no results, just use the other
        if not structured_results:
            return vector_results_dict
        if not vector_results_dict:
            return structured_results
        
        # Combine results based on hybrid_weight
        combined_results = self._combine_results(
            structured_results, 
            vector_results_dict, 
            hybrid_weight
        )
        
        return combined_results
    
    def _combine_results(
        self, 
        structured_results: List[Dict[str, Any]], 
        vector_results: List[Dict[str, Any]], 
        hybrid_weight: float
    ) -> List[Dict[str, Any]]:
        """
        Combine results from structured data and vector search.
        
        Args:
            structured_results: Results from structured data search
            vector_results: Results from vector search
            hybrid_weight: Weight for combining results (0 = structured only, 1 = vector only)
            
        Returns:
            Combined results
        """
        # Create a map of item IDs to items
        all_items = {}
        
        # Add structured results
        id_field = 'id'
        if self.field_mapping is not None:
            # Look for mapped 'id' field
            for standard_name, source_name in self.field_mapping.get_mappings().items():
                if standard_name == 'id':
                    id_field = standard_name
                    break
        
        for item in structured_results:
            if id_field in item:
                item_id = str(item[id_field])
                all_items[item_id] = {
                    **item,
                    "_structured_score": item.get("_score", 0),
                    "_vector_score": 0,
                    "_combined_score": 0
                }
        
        # Add vector results
        for item in vector_results:
            if id_field in item:
                item_id = str(item[id_field])
                if item_id in all_items:
                    # Update existing item
                    all_items[item_id]["_vector_score"] = item.get("_score", 0)
                else:
                    # Add new item
                    all_items[item_id] = {
                        **item,
                        "_structured_score": 0,
                        "_vector_score": item.get("_score", 0),
                        "_combined_score": 0
                    }
        
        # Normalize scores
        max_structured_score = max((item["_structured_score"] for item in all_items.values()), default=1)
        max_vector_score = max((item["_vector_score"] for item in all_items.values()), default=1)
        
        # Compute combined scores
        for item_id, item in all_items.items():
            normalized_structured_score = item["_structured_score"] / max_structured_score if max_structured_score > 0 else 0
            normalized_vector_score = item["_vector_score"] / max_vector_score if max_vector_score > 0 else 0
            
            # Weighted combination
            item["_combined_score"] = (
                (1 - hybrid_weight) * normalized_structured_score + 
                hybrid_weight * normalized_vector_score
            )
            
            # Use combined score as the main score
            item["_score"] = item["_combined_score"]
        
        # Convert to list and sort by combined score
        results = list(all_items.values())
        results.sort(key=lambda x: x["_combined_score"], reverse=True)
        
        return results
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        return self.data_provider.get_by_id(item_id)