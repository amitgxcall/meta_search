"""
Vector search functionality for the search engine.
"""

import os
import pickle
import numpy as np
from typing import Dict, List, Any, Optional, Tuple

# Import dependencies with fallbacks
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    print("Warning: FAISS is not available. Vector search will be limited.")

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    TFIDF_AVAILABLE = True
except ImportError:
    TFIDF_AVAILABLE = False
    print("Warning: scikit-learn is not available. Vector search will be limited.")

class VectorSearchEngine:
    """
    Vector search engine using TF-IDF and FAISS.
    """
    
    def __init__(self, cache_dir: str):
        """
        Initialize vector search engine.
        
        Args:
            cache_dir: Directory for caching vectors
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        self.vectorizer = None
        self.vectors = None
        self.index = None
        self.enabled = FAISS_AVAILABLE and TFIDF_AVAILABLE
    
    def initialize(self, data_hash: str, texts: List[str]) -> bool:
        """
        Initialize vector search with text data.
        
        Args:
            data_hash: Hash of the data source (for caching)
            texts: List of text representations of records
            
        Returns:
            True if initialization succeeded, False otherwise
        """
        if not self.enabled:
            return False
        
        # Define cache paths
        vectors_path = os.path.join(self.cache_dir, f"vectors_{data_hash}.pkl")
        vectorizer_path = os.path.join(self.cache_dir, f"vectorizer_{data_hash}.pkl")
        
        # Try to load from cache
        if os.path.exists(vectors_path) and os.path.exists(vectorizer_path):
            try:
                with open(vectorizer_path, 'rb') as f:
                    self.vectorizer = pickle.load(f)
                with open(vectors_path, 'rb') as f:
                    self.vectors = pickle.load(f)
                
                # Create FAISS index
                self.index = faiss.IndexFlatL2(self.vectors.shape[1])
                self.index.add(self.vectors)
                
                print("Vector search components loaded from cache")
                return True
            except Exception as e:
                print(f"Error loading vectors from cache: {str(e)}")
                print("Rebuilding vector search components...")
        
        # Create new vectors
        try:
            self.vectorizer = TfidfVectorizer(lowercase=True, stop_words='english')
            self.vectors = self.vectorizer.fit_transform(texts).toarray().astype(np.float32)
            
            # Create FAISS index
            self.index = faiss.IndexFlatL2(self.vectors.shape[1])
            self.index.add(self.vectors)
            
            # Cache vectors and vectorizer
            with open(vectorizer_path, 'wb') as f:
                pickle.dump(self.vectorizer, f)
            with open(vectors_path, 'wb') as f:
                pickle.dump(self.vectors, f)
            
            print("Vector search components cached for future use")
            return True
        except Exception as e:
            print(f"Error initializing vector search: {str(e)}")
            return False
    
    def search(self, query: str, top_k: int = 10) -> Tuple[List[int], List[float]]:
        """
        Search for similar records.
        
        Args:
            query: Query string
            top_k: Maximum number of results
            
        Returns:
            Tuple of (indices, distances)
        """
        if not self.enabled or not self.vectorizer or not self.index:
            return [], []
        
        try:
            # Convert query to vector
            query_vector = self.vectorizer.transform([query]).toarray().astype(np.float32)
            
            # Search with FAISS
            distances, indices = self.index.search(query_vector, top_k)
            
            return indices[0].tolist(), distances[0].tolist()
        except Exception as e:
            print(f"Error in vector search: {str(e)}")
            return [], []