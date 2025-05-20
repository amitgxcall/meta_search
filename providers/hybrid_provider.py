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
import time
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
    logging.warning("SQLite support not available. Some functionality will be limited.")

# Try to import JSON provider
try:
    from .json_provider import JSONProvider
    JSON_AVAILABLE = True
except ImportError:
    JSON_AVAILABLE = False
    logging.warning("JSON support not available. Some functionality will be limited.")

# Import vector search engine
from ..search.vector_search import VectorSearchEngine

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pre-compile regex patterns for better performance
STRUCTURED_QUERY_PATTERN = re.compile(r'(\w+)[:=<>]|\b(AND|OR)\b', re.IGNORECASE)
SEMANTIC_TERMS = frozenset([
    'like', 'similar', 'about', 'related', 'search', 'find', 'matching'
])


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
        
        # Cache for query type classification
        self.query_type_cache = {}
        
        # Performance metrics
        self.metrics = {
            'structured_search_time': 0,
            'vector_search_time': 0,
            'combination_time': 0,
            'total_calls': 0
        }
        
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
            start_time = time.time()
            self.vector_index_built = self.vector_search.load_index(self.vector_index_path)
            if self.vector_index_built:
                logger.info(f"Loaded vector index from {self.vector_index_path} in {time.time() - start_time:.4f} seconds")
        
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
        start_time = time.time()
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
        
        # Process items in batches for better performance
        batch_size = 1000
        batch_count = (len(items) + batch_size - 1) // batch_size  # Ceiling division
        
        for batch_idx in range(batch_count):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(items))
            batch_items = items[start_idx:end_idx]
            
            # Process batch
            batch_data = []
            for item in batch_items:
                if id_field not in item:
                    logger.warning(f"Item missing ID field '{id_field}'")
                    continue
                
                item_id = str(item[id_field])
                
                # Combine text fields for embedding
                text = self._combine_text_fields(item, text_fields)
                
                # Skip items with no text
                if not text:
                    continue
                
                # Generate embedding
                embedding = VectorSearchEngine.get_mock_embedding(text)
                
                # Add to batch
                batch_data.append((item_id, item, embedding))
            
            # Add batch to vector index
            self.vector_search.bulk_add_items(batch_data)
            
            logger.info(f"Processed batch {batch_idx+1}/{batch_count} with {len(batch_data)} items")
        
        logger.info(f"Added {len(self.vector_search.id_to_data)} items to vector index in {time.time() - start_time:.4f} seconds")
        
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
        # Optimization: pre-allocate with estimated size
        estimated_size = min(len(text_fields) * 3, 30)  # Conservative estimate
        text_values = [None] * estimated_size
        text_count = 0
        
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
                    if text_count < len(text_values):
                        text_values[text_count] = formatted_value
                        text_count += 1
                    else:
                        # Expand if needed
                        text_values.append(formatted_value)
                        text_count += 1
        
        # Only join the parts that were actually used
        return " ".join(text_values[:text_count])
    
    def detect_query_type(self, query: str) -> str:
        """
        Auto-detect the type of query to determine which search strategy to use.
        Uses regex patterns and caching for better performance.
        
        Args:
            query: The search query
            
        Returns:
            Query type ("structured", "vector", "hybrid")
        """
        # Check cache first
        if query in self.query_type_cache:
            return self.query_type_cache[query]
        
        # Check if query has structured patterns using pre-compiled regex
        has_structured = bool(STRUCTURED_QUERY_PATTERN.search(query))
        
        # Check if query has semantic terms
        query_words = set(query.lower().split())
        has_semantic = bool(query_words.intersection(SEMANTIC_TERMS))
        
        # Determine query type
        if has_structured and has_semantic:
            query_type = "hybrid"
        elif has_structured:
            query_type = "structured"
        else:
            # For short, simple queries - default to hybrid with emphasis on vector search
            if len(query.split()) <= 3 and not has_structured:
                query_type = "hybrid"
                # Adjust weight to favor vector search for natural language
                self.vector_weight = max(0.7, self.vector_weight)
            else:
                query_type = "hybrid"
        
        # Cache the result
        self.query_type_cache[query] = query_type
        
        return query_type
    
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
        start_time = time.time()
        self.metrics['total_calls'] += 1
        
        # Use provided weight or default
        hybrid_weight = hybrid_weight if hybrid_weight is not None else self.vector_weight
        
        # Auto-detect query type
        query_type = self.detect_query_type(query)
        logger.info(f"Detected query type: {query_type}")
        
        # For structured queries, use structured search only
        if query_type == "structured":
            structured_start = time.time()
            results = self.data_provider.search(query, limit=limit)
            self.metrics['structured_search_time'] += time.time() - structured_start
            
            logger.info(f"Completed structured search in {time.time() - start_time:.4f} seconds, found {len(results)} results")
            return results
        
        # Build vector index if not already built
        if not self.vector_index_built:
            if not self.build_vector_index():
                # If we couldn't build the vector index, just use the data provider
                logger.warning("Could not build vector index, falling back to structured search only")
                structured_start = time.time()
                results = self.data_provider.search(query, limit=limit)
                self.metrics['structured_search_time'] += time.time() - structured_start
                return results
        
        # Get data provider results
        structured_start = time.time()
        structured_results = self.data_provider.search(query, limit=limit)
        structured_time = time.time() - structured_start
        self.metrics['structured_search_time'] += structured_time
        
        # Get vector search results
        vector_start = time.time()
        query_embedding = VectorSearchEngine.get_mock_embedding(query)
        vector_results = self.vector_search.search(query_embedding, limit=limit)
        vector_time = time.time() - vector_start
        self.metrics['vector_search_time'] += vector_time
        
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
            logger.info(f"No structured results, using vector results only. Search completed in {time.time() - start_time:.4f} seconds")
            return vector_results_dict
        if not vector_results_dict:
            logger.info(f"No vector results, using structured results only. Search completed in {time.time() - start_time:.4f} seconds")
            return structured_results
        
        # Combine results
        combination_start = time.time()
        combined_results = self._combine_results(structured_results, vector_results_dict, hybrid_weight, limit)
        combination_time = time.time() - combination_start
        self.metrics['combination_time'] += combination_time
        
        logger.info(f"Search completed in {time.time() - start_time:.4f} seconds (structured: {structured_time:.4f}s, vector: {vector_time:.4f}s, combination: {combination_time:.4f}s)")
        
        return combined_results
    
    def _combine_results(self, 
                        structured_results: List[Dict[str, Any]], 
                        vector_results: List[Dict[str, Any]], 
                        hybrid_weight: float,
                        limit: int) -> List[Dict[str, Any]]:
        """
        Combine results from structured data and vector search using NumPy for optimization.
        
        Args:
            structured_results: Results from structured data search
            vector_results: Results from vector search
            hybrid_weight: Weight for combining results (0 = structured only, 1 = vector only)
            limit: Maximum number of results to return
            
        Returns:
            Combined results
        """
        # Create sets of unique item IDs from both result sets
        id_field = self._get_id_field()
        structured_ids = {str(item.get(id_field, '')) for item in structured_results}
        vector_ids = {str(item.get(id_field, '')) for item in vector_results}
        
        # Get all unique IDs
        all_ids = list(structured_ids.union(vector_ids))
        
        # Use NumPy arrays for efficient operations
        structured_scores = np.zeros(len(all_ids), dtype=np.float32)
        vector_scores = np.zeros(len(all_ids), dtype=np.float32)
        
        # Create ID to index mapping
        id_to_index = {item_id: i for i, item_id in enumerate(all_ids)}
        id_to_item = {}
        
        # Fill score arrays and item map
        for item in structured_results:
            item_id = str(item.get(id_field, ''))
            if item_id in id_to_index:
                i = id_to_index[item_id]
                structured_scores[i] = item.get('_score', 0)
                id_to_item[item_id] = item
        
        for item in vector_results:
            item_id = str(item.get(id_field, ''))
            if item_id in id_to_index:
                i = id_to_index[item_id]
                vector_scores[i] = item.get('_score', 0)
                if item_id not in id_to_item:
                    id_to_item[item_id] = item
        
        # Normalize scores using NumPy operations
        max_structured = np.max(structured_scores) if structured_scores.size > 0 and np.max(structured_scores) > 0 else 1.0
        max_vector = np.max(vector_scores) if vector_scores.size > 0 and np.max(vector_scores) > 0 else 1.0
        
        normalized_structured = structured_scores / max_structured
        normalized_vector = vector_scores / max_vector
        
        # Calculate combined scores
        combined_scores = (1 - hybrid_weight) * normalized_structured + hybrid_weight * normalized_vector
        
        # Use argsort to get indices of top scores
        if limit < len(combined_scores):
            top_indices = np.argpartition(combined_scores, -limit)[-limit:]
            top_indices = top_indices[np.argsort(combined_scores[top_indices])[::-1]]
        else:
            top_indices = np.argsort(combined_scores)[::-1]
        
        # Create result list
        results = []
        for i in top_indices:
            item_id = all_ids[i]
            if item_id in id_to_item:
                result = id_to_item[item_id].copy()
                result['_structured_score'] = float(normalized_structured[i])
                result['_vector_score'] = float(normalized_vector[i])
                result['_combined_score'] = float(combined_scores[i])
                result['_score'] = float(combined_scores[i])  # Update main score
                results.append(result)
        
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
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for the hybrid provider.
        
        Returns:
            Dictionary with performance metrics
        """
        if self.metrics['total_calls'] > 0:
            avg_structured = self.metrics['structured_search_time'] / self.metrics['total_calls']
            avg_vector = self.metrics['vector_search_time'] / self.metrics['total_calls']
            avg_combination = self.metrics['combination_time'] / self.metrics['total_calls']
            
            return {
                'avg_structured_search_time': avg_structured,
                'avg_vector_search_time': avg_vector,
                'avg_combination_time': avg_combination,
                'total_calls': self.metrics['total_calls'],
                'total_time': self.metrics['structured_search_time'] + self.metrics['vector_search_time'] + self.metrics['combination_time']
            }
        else:
            return self.metrics