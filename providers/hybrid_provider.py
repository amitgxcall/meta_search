"""
Hybrid provider implementation that combines structured data with vector search.

This module provides a provider that intelligently selects between structured
search and vector similarity search, or combines them for optimal results.
"""

import os
import re
import hashlib
import logging
import numpy as np
from typing import List, Dict, Any, Optional, Tuple, Union

# Import from base module
from .base import DataProvider
from .csv_provider import CSVProvider
from ..utils.field_mapping import FieldMapping

# Try to import SQLite provider
try:
    from .sqlite_provider import SQLiteProvider
    from .structured_sqlite_provider import StructuredSQLiteProvider
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

# Try to import JSON provider
try:
    from .json_provider import JSONProvider
    JSON_AVAILABLE = True
except ImportError:
    JSON_AVAILABLE = False

# Import vector search engine
from ..search.vector_search import VectorSearchEngine

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HybridProvider(DataProvider):
    """
    Hybrid data provider that combines structured data with vector search.
    
    This provider intelligently selects between structured data and vector
    similarity search, or combines them for optimal results.
    """
    
    def __init__(self, 
                 data_source: str, 
                 field_mapping: Optional[FieldMapping] = None,
                 vector_index_path: Optional[str] = None,
                 vector_weight: float = 0.5,
                 table_name: Optional[str] = None):
        """
        Initialize the hybrid provider.
        
        Args:
            data_source: Path to the data source file
            field_mapping: Field mapping configuration
            vector_index_path: Path to the vector index file (if None, uses data_source + '.vector')
            vector_weight: Weight for vector search when combining results (0-1)
            table_name: Name of the table to use (for SQLite provider)
        """
        super().__init__(data_source)
        
        # Set vector index path
        if vector_index_path is None:
            self.vector_index_path = data_source + '.vector'
        else:
            self.vector_index_path = vector_index_path
        
        # Set vector weight
        self.vector_weight = vector_weight
        
        # Determine provider type based on file extension
        self.file_ext = os.path.splitext(data_source)[1].lower()
        
        # Initialize appropriate provider
        if self.file_ext == '.csv':
            logger.info(f"Using CSV provider for {data_source}")
            self.data_provider = CSVProvider(data_source)
        elif SQLITE_AVAILABLE and self.file_ext in ['.db', '.sqlite', '.sqlite3']:
            logger.info(f"Using SQLite provider for {data_source}")
            self.data_provider = StructuredSQLiteProvider(data_source, table_name)
        elif JSON_AVAILABLE and self.file_ext == '.json':
            logger.info(f"Using JSON provider for {data_source}")
            self.data_provider = JSONProvider(data_source)
        else:
            logger.warning(f"Unknown file type: {self.file_ext}. Defaulting to CSV provider.")
            self.data_provider = CSVProvider(data_source)
        
        # Initialize vector search
        self.vector_search = VectorSearchEngine()
        
        # Keep track of whether the vector index is built
        self.vector_index_built = False
        
        # Connect to data sources
        self.connect()
        
        # Set field mapping if provided, otherwise use provider's mapping
        if field_mapping:
            self.set_field_mapping(field_mapping)
            self.data_provider.set_field_mapping(field_mapping)
        elif self.data_provider.field_mapping:
            self.set_field_mapping(self.data_provider.field_mapping)
    
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
            if self.vector_index_built:
                logger.info(f"Loaded vector index from {self.vector_index_path}")
        
        return True
    
    def set_field_mapping(self, field_mapping: FieldMapping) -> None:
        """
        Set the field mapping for this provider and the underlying data provider.
        
        Args:
            field_mapping: FieldMapping object
        """
        super().set_field_mapping(field_mapping)
        self.data_provider.set_field_mapping(field_mapping)
    
    def build_vector_index(self) -> bool:
        """
        Build the vector index from scratch.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Building vector index...")
        
        # Get all items from data provider
        items = self._get_all_items_from_provider()
        
        if not items:
            logger.warning("No items found in data source.")
            return False
        
        # Infer text fields from first item
        text_fields = self._infer_text_fields(items[0])
        
        # Get ID field
        id_field = self._get_id_field()
        
        # Build vector index
        for item in items:
            if id_field not in item:
                logger.warning(f"Item missing ID field '{id_field}': {item}")
                continue
            
            item_id = str(item[id_field])
            
            # Combine text fields for embedding
            text = self._combine_text_fields(item, text_fields)
            
            # Skip items with no text
            if not text:
                continue
            
            # Generate embedding
            embedding = VectorSearchEngine.get_mock_embedding(text)
            
            # Add to vector index
            self.vector_search.add_item(item_id, item, embedding)
        
        logger.info(f"Added {len(self.vector_search.id_to_data)} items to vector index")
        
        # Save vector index
        if self.vector_search.save_index(self.vector_index_path):
            self.vector_index_built = True
            logger.info(f"Vector index saved to {self.vector_index_path}")
            return True
        else:
            logger.error("Failed to save vector index.")
            return False
    
    def _get_all_items_from_provider(self) -> List[Dict[str, Any]]:
        """
        Get all items from the data provider.
        
        Returns:
            List of all items
        """
        if hasattr(self.data_provider, 'get_all_records'):
            return self.data_provider.get_all_records()
        
        # Fallback to empty list
        return []
    
    def _infer_text_fields(self, item: Dict[str, Any]) -> List[str]:
        """
        Infer which fields are text fields based on the first item.
        
        Args:
            item: First item from the data source
            
        Returns:
            List of text field names
        """
        # Use field mapping's text fields if available
        if self.field_mapping and hasattr(self.field_mapping, 'text_fields') and self.field_mapping.text_fields:
            return list(self.field_mapping.text_fields)
        
        # Detect text fields
        text_fields = []
        for key, value in item.items():
            if isinstance(value, str) and len(value) > 5:
                text_fields.append(key)
        
        # Add name and status fields if not already included
        if self.field_mapping:
            name_field = self.field_mapping.name_field
            if name_field and name_field not in text_fields and name_field in item:
                text_fields.append(name_field)
            
            status_field = getattr(self.field_mapping, 'status_field', None)
            if status_field and status_field not in text_fields and status_field in item:
                text_fields.append(status_field)
        
        logger.info(f"Inferred text fields for vector search: {', '.join(text_fields)}")
        return text_fields
    
    def _get_id_field(self) -> str:
        """
        Get the ID field name.
        
        Returns:
            ID field name
        """
        id_field = 'id'
        if self.field_mapping is not None:
            id_field = self.field_mapping.id_field
        
        return id_field
    
    def _combine_text_fields(self, item: Dict[str, Any], text_fields: List[str]) -> str:
        """
        Combine multiple text fields into a single text for embedding.
        
        Args:
            item: Item from the data source
            text_fields: List of text field names
            
        Returns:
            Combined text
        """
        text_values = []
        
        # Create field weights for emphasis
        field_weights = {}
        if self.field_mapping:
            name_field = self.field_mapping.name_field
            if name_field in text_fields:
                field_weights[name_field] = 3.0  # Name fields are important
            
            status_field = getattr(self.field_mapping, 'status_field', None)
            if status_field and status_field in text_fields:
                field_weights[status_field] = 2.0  # Status fields are important
        
        # Add each text field
        for field in text_fields:
            if field in item and item[field]:
                value = str(item[field])
                
                # Get field weight
                weight = field_weights.get(field, 1.0)
                
                # Add field with name (prefixed) to make the embedding context-aware
                formatted_value = f"{field}: {value}"
                
                # Add multiple times based on weight
                for _ in range(int(weight)):
                    text_values.append(formatted_value)
        
        return " ".join(text_values)
    
    def detect_query_type(self, query: str) -> str:
        """
        Auto-detect the type of query to determine which search strategy to use.
        
        Args:
            query: The search query
            
        Returns:
            Query type ("structured", "vector", "hybrid")
        """
        # Check for structured query patterns (field:value, field>value, etc.)
        if ":" in query or ">" in query or "<" in query or ">=" in query or "<=" in query:
            return "structured"
        
        # For now, use hybrid search for all other queries
        return "hybrid"
    
    def search(self, query: str, hybrid_weight: Optional[float] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search using both structured data and vector search.
        
        Args:
            query: The search query
            hybrid_weight: Weight for combining results (0 = structured only, 1 = vector only)
            limit: Maximum number of results to return
            
        Returns:
            List of search results
        """
        # Use provided weight or default
        hybrid_weight = hybrid_weight if hybrid_weight is not None else self.vector_weight
        
        # Auto-detect query type
        query_type = self.detect_query_type(query)
        logger.info(f"Detected query type: {query_type}")
        
        # For structured queries, use structured search only
        if query_type == "structured":
            return self.data_provider.search(query, limit=limit)
        
        # Build vector index if not already built
        if not self.vector_index_built:
            if not self.build_vector_index():
                # If we couldn't build the vector index, just use the data provider
                return self.data_provider.search(query, limit=limit)
        
        # Get data provider results
        structured_results = self.data_provider.search(query, limit=limit)
        
        # Get vector search results
        query_embedding = VectorSearchEngine.get_mock_embedding(query)
        vector_results = self.vector_search.search(query_embedding, limit=limit)
        
        # Convert vector results to same format as structured results
        vector_results_dict = [
            {**item_data, "_score": similarity, "_result_type": "vector"} 
            for item_id, similarity, item_data in vector_results
        ]
        
        # Mark structured results
        for item in structured_results:
            item["_result_type"] = "structured"
        
        # If one of the methods returns no results, just use the other
        if not structured_results:
            return vector_results_dict
        if not vector_results_dict:
            return structured_results
        
        # Combine results
        return self._combine_results(structured_results, vector_results_dict, hybrid_weight, limit)
    
    def _combine_results(self, 
                        structured_results: List[Dict[str, Any]], 
                        vector_results: List[Dict[str, Any]], 
                        hybrid_weight: float,
                        limit: int) -> List[Dict[str, Any]]:
        """
        Combine results from structured data and vector search.
        
        Args:
            structured_results: Results from structured data search
            vector_results: Results from vector search
            hybrid_weight: Weight for combining results (0 = structured only, 1 = vector only)
            limit: Maximum number of results to return
            
        Returns:
            Combined results
        """
        # Create a map of item IDs to items
        all_items = {}
        
        # Add structured results
        id_field = self._get_id_field()
        
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
        
        # Limit results
        return results[:limit]
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        return self.data_provider.get_by_id(item_id)
    
    def get_all_fields(self) -> List[str]:
        """
        Get all available fields in the data source.
        
        Returns:
            List of field names
        """
        return self.data_provider.get_all_fields()
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Get all records from the data source.
        
        Returns:
            List of all records
        """
        return self.data_provider.get_all_records()
    
    def get_record_count(self) -> int:
        """
        Get the total number of records in the data source.
        
        Returns:
            Number of records
        """
        return self.data_provider.get_record_count()