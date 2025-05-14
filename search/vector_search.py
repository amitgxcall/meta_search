"""
Vector search implementation for meta_search.
"""

import os
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
import json
import pickle

class VectorSearchEngine:
    """
    Vector-based search implementation using simple cosine similarity.
    In a production environment, you might use specialized libraries 
    like FAISS, Annoy, or ScaNN for more efficient similarity search.
    """
    
    def __init__(self, embedding_dim: int = 768):
        """
        Initialize the vector search engine.
        
        Args:
            embedding_dim: Dimension of the embedding vectors
        """
        self.embedding_dim = embedding_dim
        self.index = {}  # id -> embedding
        self.id_to_data = {}  # id -> original data
    
    def add_item(self, item_id: str, item_data: Dict[str, Any], embedding: List[float]) -> None:
        """
        Add an item to the vector index.
        
        Args:
            item_id: Unique identifier for the item
            item_data: Original item data
            embedding: Vector embedding for the item
        """
        # Convert to numpy array for efficient operations
        embedding_array = np.array(embedding, dtype=np.float32)
        
        # Normalize the vector for cosine similarity
        norm = np.linalg.norm(embedding_array)
        if norm > 0:
            embedding_array = embedding_array / norm
        
        self.index[item_id] = embedding_array
        self.id_to_data[item_id] = item_data
    
    def search(self, query_embedding: List[float], limit: int = 10) -> List[Tuple[str, float, Dict[str, Any]]]:
        """
        Search for similar items.
        
        Args:
            query_embedding: Vector embedding for the query
            limit: Maximum number of results to return
            
        Returns:
            List of tuples (item_id, similarity_score, item_data)
        """
        if not self.index:
            return []
        
        # Convert to numpy array and normalize
        query_array = np.array(query_embedding, dtype=np.float32)
        norm = np.linalg.norm(query_array)
        if norm > 0:
            query_array = query_array / norm
        
        # Calculate similarities
        results = []
        for item_id, item_embedding in self.index.items():
            # Cosine similarity is just the dot product of normalized vectors
            similarity = float(np.dot(query_array, item_embedding))
            results.append((item_id, similarity, self.id_to_data[item_id]))
        
        # Sort by similarity (descending)
        results.sort(key=lambda x: x[1], reverse=True)
        
        return results[:limit]
    
    def save_index(self, file_path: str) -> bool:
        """
        Save the vector index to disk.
        
        Args:
            file_path: Path to save the index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            data = {
                "embedding_dim": self.embedding_dim,
                "index": {k: v.tolist() for k, v in self.index.items()},
                "id_to_data": self.id_to_data
            }
            
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            
            return True
        except Exception as e:
            print(f"Error saving index: {e}")
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
            return False
        
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            self.embedding_dim = data["embedding_dim"]
            self.index = {k: np.array(v, dtype=np.float32) for k, v in data["index"].items()}
            self.id_to_data = data["id_to_data"]
            
            return True
        except Exception as e:
            print(f"Error loading index: {e}")
            return False

    @staticmethod
    def get_mock_embedding(text: str, dim: int = 768) -> List[float]:
        """
        Generate a mock embedding for text.
        In a real implementation, you would use a proper embedding model.
        
        Args:
            text: Text to generate an embedding for
            dim: Dimension of the embedding
            
        Returns:
            Vector embedding
        """
        # This is just a simple deterministic mock implementation
        # In a real system, you'd use a proper embedding model
        import hashlib
        
        # Get a deterministic hash of the text
        hash_obj = hashlib.md5(text.encode())
        hash_bytes = hash_obj.digest()
        
        # Use the hash to seed a random number generator
        rng = np.random.RandomState(int.from_bytes(hash_bytes[:4], byteorder='little'))
        
        # Generate a random vector
        embedding = rng.randn(dim).astype(np.float32)
        
        # Normalize to unit length
        embedding = embedding / np.linalg.norm(embedding)
        
        return embedding.tolist()

# For backward compatibility
VectorSearch = VectorSearchEngine