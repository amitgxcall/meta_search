"""
Vector search implementation for meta_search.

This module provides vector-based similarity search functionality, which allows
searching for items based on semantic similarity rather than exact matching.
It supports both in-memory search and integration with more efficient libraries
like FAISS for large-scale search.
"""

import os
import pickle
import numpy as np
import logging
import time
import hashlib
from typing import List, Dict, Any, Optional, Tuple, Union, Callable
from functools import lru_cache

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import FAISS for more efficient vector search
try:
    import faiss
    FAISS_AVAILABLE = True
    logger.info("FAISS is available and will be used for vector search.")
except ImportError:
    FAISS_AVAILABLE = False
    logger.warning("FAISS not available. Using fallback numpy implementation.")


class VectorSearchEngine:
    """
    Vector-based search implementation for semantic similarity search.
    
    This class provides functionality to search for items based on semantic
    similarity using vector embeddings. It supports both a simple numpy-based
    implementation and an optimized FAISS-based implementation when available.
    """
    
    def __init__(self, embedding_dim: int = 768, use_faiss: bool = True):
        """
        Initialize the vector search engine.
        
        Args:
            embedding_dim: Dimension of the embedding vectors
            use_faiss: Whether to use FAISS for vector search (if available)
        """
        self.embedding_dim = embedding_dim
        self.index = {}  # id -> embedding (for numpy implementation)
        self.id_to_data = {}  # id -> original data
        
        # FAISS implementation (if available and requested)
        self.faiss_index = None
        self.id_list = []  # List of IDs for FAISS implementation
        
        # Use FAISS if available and requested
        self.use_faiss = use_faiss and FAISS_AVAILABLE
        
        # Performance metrics
        self.metrics = {
            'index_add_time': 0,
            'search_time': 0,
            'normalize_time': 0,
            'items_added': 0,
            'searches_performed': 0
        }
        
        # Initialize FAISS index if available
        if self.use_faiss:
            self._init_faiss_index()
    
    def _init_faiss_index(self) -> bool:
        """
        Initialize the FAISS index.
        
        Returns:
            True if successful, False otherwise
        """
        if not FAISS_AVAILABLE:
            return False
        
        try:
            # Create a flat L2 index for cosine similarity
            self.faiss_index = faiss.IndexFlatIP(self.embedding_dim)
            return True
        except Exception as e:
            logger.error(f"Error initializing FAISS index: {e}")
            self.use_faiss = False
            return False
    
    def add_item(self, item_id: str, item_data: Dict[str, Any], embedding: Union[List[float], np.ndarray]) -> None:
        """
        Add an item to the vector index.
        
        Args:
            item_id: Unique identifier for the item
            item_data: Original item data
            embedding: Vector embedding for the item (list or numpy array)
        """
        start_time = time.time()
        
        # Convert to numpy array if needed
        if not isinstance(embedding, np.ndarray):
            embedding_array = np.array(embedding, dtype=np.float32)
        else:
            embedding_array = embedding.astype(np.float32)
        
        # Normalize the vector for cosine similarity
        norm = np.linalg.norm(embedding_array)
        if norm > 0:
            embedding_array = embedding_array / norm
        
        # Store the data
        self.id_to_data[item_id] = item_data
        
        # Store differently based on implementation
        if self.use_faiss:
            # Add to FAISS index
            self.id_list.append(item_id)
            self.faiss_index.add(embedding_array.reshape(1, -1))
        else:
            # Add to numpy index
            self.index[item_id] = embedding_array
        
        # Update metrics
        self.metrics['index_add_time'] += time.time() - start_time
        self.metrics['items_added'] += 1
    
    def bulk_add_items(self, items: List[Tuple[str, Dict[str, Any], Union[List[float], np.ndarray]]]) -> None:
        """
        Add multiple items to the index at once for better performance.
        
        Args:
            items: List of (item_id, item_data, embedding) tuples
        """
        if not items:
            return
            
        start_time = time.time()
        
        # Process all embeddings at once
        item_ids = []
        item_data_dict = {}
        
        # Pre-allocate numpy array for embeddings
        embeddings = np.zeros((len(items), self.embedding_dim), dtype=np.float32)
        
        for i, (item_id, item_data, embedding) in enumerate(items):
            # Convert to numpy array if needed
            if not isinstance(embedding, np.ndarray):
                embedding_array = np.array(embedding, dtype=np.float32)
            else:
                embedding_array = embedding.astype(np.float32)
            
            # Store item ID and data
            item_ids.append(item_id)
            item_data_dict[item_id] = item_data
            
            # Store embedding
            embeddings[i] = embedding_array
        
        # Normalize all embeddings at once
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        # Avoid division by zero
        norms[norms == 0] = 1.0
        normalized_embeddings = embeddings / norms
        
        # Update dictionaries
        self.id_to_data.update(item_data_dict)
        
        if self.use_faiss:
            # Add to FAISS index
            self.id_list.extend(item_ids)
            self.faiss_index.add(normalized_embeddings)
        else:
            # Add to numpy index
            for i, item_id in enumerate(item_ids):
                self.index[item_id] = normalized_embeddings[i]
        
        # Update metrics
        self.metrics['index_add_time'] += time.time() - start_time
        self.metrics['items_added'] += len(items)
    
    def search(self, 
              query_embedding: Union[List[float], np.ndarray], 
              limit: int = 10) -> List[Tuple[str, float, Dict[str, Any]]]:
        """
        Search for similar items.
        
        Args:
            query_embedding: Vector embedding for the query
            limit: Maximum number of results to return
            
        Returns:
            List of tuples (item_id, similarity_score, item_data)
        """
        start_time = time.time()
        self.metrics['searches_performed'] += 1
        
        if self.use_faiss:
            results = self._search_faiss(query_embedding, limit)
        else:
            results = self._search_numpy(query_embedding, limit)
        
        self.metrics['search_time'] += time.time() - start_time
        return results
    
    def _search_numpy(self, 
                     query_embedding: Union[List[float], np.ndarray], 
                     limit: int = 10) -> List[Tuple[str, float, Dict[str, Any]]]:
        """
        Search using optimized NumPy implementation.
        
        Args:
            query_embedding: Vector embedding for the query
            limit: Maximum number of results to return
            
        Returns:
            List of tuples (item_id, similarity_score, item_data)
        """
        if not self.index:
            return []
        
        # Convert query to numpy array if needed
        normalize_start = time.time()
        if not isinstance(query_embedding, np.ndarray):
            query_array = np.array(query_embedding, dtype=np.float32)
        else:
            query_array = query_embedding.astype(np.float32)
        
        # Normalize the query vector
        query_norm = np.linalg.norm(query_array)
        if query_norm > 0:
            query_array = query_array / query_norm
        
        self.metrics['normalize_time'] += time.time() - normalize_start
        
        # Get all item IDs and embeddings
        item_ids = list(self.index.keys())
        embeddings = np.array([self.index[item_id] for item_id in item_ids], dtype=np.float32)
        
        # Calculate all similarities at once using matrix multiplication
        similarities = np.dot(embeddings, query_array)
        
        # Get indices of top results
        if limit < len(similarities):
            # Use argpartition for better performance when we only need top K results
            # This is faster than argsort for large arrays when we only need top K
            top_indices = np.argpartition(similarities, -limit)[-limit:]
            
            # Sort the top indices by similarity (descending)
            top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]
        else:
            # If limit >= number of items, just sort all indices
            top_indices = np.argsort(similarities)[::-1]
        
        # Create result tuples with better list comprehension
        results = [
            (item_ids[i], float(similarities[i]), self.id_to_data[item_ids[i]])
            for i in top_indices
        ]
        
        return results
    
    def _search_faiss(self, 
                     query_embedding: Union[List[float], np.ndarray], 
                     limit: int = 10) -> List[Tuple[str, float, Dict[str, Any]]]:
        """
        Search using optimized FAISS implementation.
        
        Args:
            query_embedding: Vector embedding for the query
            limit: Maximum number of results to return
            
        Returns:
            List of tuples (item_id, similarity_score, item_data)
        """
        if not self.faiss_index or not self.id_list:
            return []
        
        # Limit to the actual number of items
        k = min(limit, len(self.id_list))
        if k == 0:
            return []
        
        # Convert to numpy array if needed
        normalize_start = time.time()
        if not isinstance(query_embedding, np.ndarray):
            query_array = np.array(query_embedding, dtype=np.float32)
        else:
            query_array = query_embedding.astype(np.float32)
        
        # Normalize the query vector
        query_norm = np.linalg.norm(query_array)
        if query_norm > 0:
            query_array = query_array / query_norm
        
        # Reshape for FAISS
        query_array = query_array.reshape(1, -1)
        
        self.metrics['normalize_time'] += time.time() - normalize_start
        
        # Search with FAISS
        distances, indices = self.faiss_index.search(query_array, k)
        
        # Format results efficiently using list comprehension
        results = [
            (self.id_list[idx], float(distances[0][i]), self.id_to_data[self.id_list[idx]])
            for i, idx in enumerate(indices[0])
            if idx < len(self.id_list)  # Safety check
        ]
        
        return results
    
    def batch_search(self, 
                    query_embeddings: List[np.ndarray], 
                    limit: int = 10) -> List[List[Tuple[str, float, Dict[str, Any]]]]:
        """
        Perform multiple searches in batch for better performance.
        
        Args:
            query_embeddings: List of query embeddings
            limit: Maximum number of results per query
            
        Returns:
            List of result lists, one per query
        """
        if not query_embeddings:
            return []
        
        start_time = time.time()
        self.metrics['searches_performed'] += len(query_embeddings)
        
        if self.use_faiss:
            # Batch search with FAISS for better performance
            if not self.faiss_index or not self.id_list:
                return [[] for _ in range(len(query_embeddings))]
                
            # Convert to numpy array
            queries_array = np.array([q for q in query_embeddings], dtype=np.float32)
            
            # Normalize queries
            normalize_start = time.time()
            norms = np.linalg.norm(queries_array, axis=1, keepdims=True)
            norms[norms == 0] = 1.0  # Avoid division by zero
            normalized_queries = queries_array / norms
            self.metrics['normalize_time'] += time.time() - normalize_start
            
            # Search with FAISS
            k = min(limit, len(self.id_list)) if self.id_list else 0
            if k == 0:
                return [[] for _ in range(len(query_embeddings))]
                
            distances, indices = self.faiss_index.search(normalized_queries, k)
            
            # Format results
            all_results = []
            for i in range(len(query_embeddings)):
                # Use list comprehension for better performance
                query_results = [
                    (self.id_list[idx], float(distances[i][j]), self.id_to_data[self.id_list[idx]])
                    for j, idx in enumerate(indices[i])
                    if idx < len(self.id_list)  # Safety check
                ]
                all_results.append(query_results)
        else:
            # Batch search with NumPy
            all_results = []
            
            # Get all embeddings at once for better performance
            if not self.index:
                return [[] for _ in range(len(query_embeddings))]
                
            item_ids = list(self.index.keys())
            embeddings = np.array([self.index[item_id] for item_id in item_ids], dtype=np.float32)
            
            # Process all queries
            for query_embedding in query_embeddings:
                # Normalize query
                normalize_start = time.time()
                query_array = np.array(query_embedding, dtype=np.float32)
                query_norm = np.linalg.norm(query_array)
                if query_norm > 0:
                    query_array = query_array / query_norm
                self.metrics['normalize_time'] += time.time() - normalize_start
                
                # Calculate similarities
                similarities = np.dot(embeddings, query_array)
                
                # Get top results
                if limit < len(similarities):
                    top_indices = np.argpartition(similarities, -limit)[-limit:]
                    top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]
                else:
                    top_indices = np.argsort(similarities)[::-1]
                
                # Create results
                query_results = [
                    (item_ids[i], float(similarities[i]), self.id_to_data[item_ids[i]])
                    for i in top_indices
                ]
                
                all_results.append(query_results)
        
        self.metrics['search_time'] += time.time() - start_time
        return all_results
    
    def save_index(self, file_path: str) -> bool:
        """
        Save the vector index to disk.
        
        Args:
            file_path: Path to save the index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            start_time = time.time()
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
            
            data = {
                "embedding_dim": self.embedding_dim,
                "id_to_data": self.id_to_data,
                "use_faiss": self.use_faiss,
                "version": "1.0",  # Add version for future compatibility
                "created": time.time(),
                "item_count": len(self.id_to_data)
            }
            
            if self.use_faiss:
                # Save FAISS index to a separate file
                faiss_path = file_path + ".faiss"
                faiss.write_index(self.faiss_index, faiss_path)
                data["faiss_path"] = faiss_path
                data["id_list"] = self.id_list
                logger.info(f"FAISS index saved to {faiss_path} with {len(self.id_list)} items")
            else:
                # Save numpy index - convert to lists for pickle compatibility
                data["index"] = {k: v.tolist() for k, v in self.index.items()}
                logger.info(f"Numpy index prepared with {len(self.index)} items")
            
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            
            save_time = time.time() - start_time
            logger.info(f"Vector index saved to {file_path} ({len(self.id_to_data)} items) in {save_time:.4f} seconds")
            return True
        except Exception as e:
            logger.error(f"Error saving vector index: {e}", exc_info=True)
            return False
    
    def load_index(self, file_path: str) -> bool:
        """
        Load the vector index from disk.
        
        Args:
            file_path: Path to load the index from
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(file_path):
            logger.warning(f"Vector index file not found: {file_path}")
            return False
        
        try:
            start_time = time.time()
            
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            # Load metadata
            self.embedding_dim = data["embedding_dim"]
            self.id_to_data = data["id_to_data"]
            self.use_faiss = data.get("use_faiss", False) and FAISS_AVAILABLE
            
            version = data.get("version", "0.1")
            created = data.get("created", "unknown")
            item_count = data.get("item_count", len(self.id_to_data))
            
            logger.info(f"Loading vector index: version={version}, items={item_count}")
            
            if self.use_faiss and "faiss_path" in data:
                # Load FAISS index
                faiss_path = data["faiss_path"]
                if os.path.exists(faiss_path):
                    self.faiss_index = faiss.read_index(faiss_path)
                    self.id_list = data["id_list"]
                    logger.info(f"Loaded FAISS index from {faiss_path} with {len(self.id_list)} items")
                else:
                    logger.warning(f"FAISS index file not found: {faiss_path}, falling back to numpy implementation")
                    self.use_faiss = False
            
            if not self.use_faiss and "index" in data:
                # Load numpy index
                try:
                    # Convert lists back to numpy arrays
                    self.index = {k: np.array(v, dtype=np.float32) for k, v in data["index"].items()}
                    logger.info(f"Loaded numpy index with {len(self.index)} items")
                except Exception as e:
                    logger.error(f"Error loading numpy index: {e}")
                    return False
            
            # Verify index integrity
            if not self.verify_index():
                logger.error("Vector index integrity check failed")
                return False
            
            load_time = time.time() - start_time
            logger.info(f"Successfully loaded vector index from {file_path} in {load_time:.4f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Error loading vector index: {e}", exc_info=True)
            return False
    
    def verify_index(self) -> bool:
        """
        Verify the integrity of the loaded index.
        
        Returns:
            True if the index is valid, False otherwise
        """
        try:
            # Basic checks for all index types
            if len(self.id_to_data) == 0:
                logger.warning("Loaded index contains no items")
                return True  # Empty index is still valid
            
            if self.use_faiss:
                # Check FAISS index dimension
                if self.faiss_index.d != self.embedding_dim:
                    logger.error(f"FAISS index dimension ({self.faiss_index.d}) != expected dimension ({self.embedding_dim})")
                    return False
                
                # Check id_list integrity
                if len(self.id_list) != self.faiss_index.ntotal:
                    logger.error(f"ID list count ({len(self.id_list)}) != FAISS index size ({self.faiss_index.ntotal})")
                    return False
                
                # Sample check of IDs in id_list with corresponding data
                for item_id in self.id_list[:min(100, len(self.id_list))]:
                    if item_id not in self.id_to_data:
                        logger.error(f"ID {item_id} in id_list has no data in id_to_data")
                        return False
            else:
                # Check numpy index
                if len(self.index) == 0:
                    logger.warning("Numpy index is empty")
                    return True  # Empty index is still valid
                
                # Check dimensions for a sample of items
                sample_keys = list(self.index.keys())[:min(100, len(self.index))]
                for item_id in sample_keys:
                    embedding = self.index[item_id]
                    if embedding.shape[0] != self.embedding_dim:
                        logger.error(f"Item {item_id} has wrong dimension: {embedding.shape[0]} (expected {self.embedding_dim})")
                        return False
                    
                    if item_id not in self.id_to_data:
                        logger.error(f"ID {item_id} in index has no data in id_to_data")
                        return False
            
            logger.info("Vector index integrity check passed")
            return True
        except Exception as e:
            logger.error(f"Error verifying index: {e}", exc_info=True)
            return False
    
    def clear(self) -> None:
        """
        Clear the vector index.
        """
        self.index = {}
        self.id_to_data = {}
        
        if self.use_faiss:
            self.id_list = []
            self._init_faiss_index()
    
    def get_item_count(self) -> int:
        """
        Get the number of items in the index.
        
        Returns:
            Number of items
        """
        if self.use_faiss:
            return len(self.id_list)
        else:
            return len(self.index)
    
    def get_index_size_bytes(self) -> int:
        """
        Get the approximate size of the index in bytes.
        
        Returns:
            Size in bytes
        """
        import sys
        
        # Approximate size of id_to_data
        data_size = sys.getsizeof(self.id_to_data)
        
        # Size of index depends on implementation
        if self.use_faiss:
            # FAISS doesn't expose size, so estimate based on dimensions
            index_size = len(self.id_list) * self.embedding_dim * 4  # 4 bytes per float32
        else:
            # For numpy implementation, sum up the sizes of arrays
            index_size = sum(sys.getsizeof(embedding) for embedding in self.index.values())
        
        return data_size + index_size
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for the vector search engine.
        
        Returns:
            Dictionary with performance metrics
        """
        if self.metrics['searches_performed'] > 0:
            avg_search = self.metrics['search_time'] / self.metrics['searches_performed']
            avg_normalize = self.metrics['normalize_time'] / self.metrics['searches_performed']
            
            return {
                **self.metrics,
                'avg_search_time': avg_search,
                'avg_normalize_time': avg_normalize,
                'implementation': 'faiss' if self.use_faiss else 'numpy'
            }
        else:
            return self.metrics

    @staticmethod
    @lru_cache(maxsize=1024)
    def get_mock_embedding(text: str, dim: int = 768) -> np.ndarray:
        """
        Generate a mock embedding for text.
        In a real implementation, you would use a proper embedding model.
        This implementation is cached for better performance.
        
        Args:
            text: Text to generate an embedding for
            dim: Dimension of the embedding
            
        Returns:
            Vector embedding (numpy array)
        """
        # Get a deterministic hash of the text
        hash_obj = hashlib.md5(text.encode())
        hash_bytes = hash_obj.digest()
        
        # Use the hash to seed a random number generator
        rng = np.random.RandomState(int.from_bytes(hash_bytes[:4], byteorder='little'))
        
        # Generate a random vector
        embedding = rng.randn(dim).astype(np.float32)
        
        # Normalize to unit length
        embedding = embedding / np.linalg.norm(embedding)
        
        return embedding


# For backward compatibility
VectorSearch = VectorSearchEngine