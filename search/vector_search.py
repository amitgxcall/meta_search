"""
Vector search functionality for the search engine.
"""

import os
import pickle
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import re

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
        self.raw_texts = None  # Store original texts for exact matching
        self.record_indices = None  # Store mapping of vector index to record index
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
        texts_path = os.path.join(self.cache_dir, f"texts_{data_hash}.pkl")
        
        # Force cache rebuild (during development - remove this in production)
        force_rebuild = True
        
        # Try to load from cache if not forcing rebuild
        if not force_rebuild and os.path.exists(vectors_path) and os.path.exists(vectorizer_path) and os.path.exists(texts_path):
            try:
                with open(vectorizer_path, 'rb') as f:
                    self.vectorizer = pickle.load(f)
                with open(vectors_path, 'rb') as f:
                    self.vectors = pickle.load(f)
                with open(texts_path, 'rb') as f:
                    cached_data = pickle.load(f)
                    self.raw_texts = cached_data.get('texts', [])
                    self.record_indices = cached_data.get('indices', list(range(len(texts))))
                
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
            print("Building new vector search index...")
            
            # Store original texts for exact matching
            self.raw_texts = texts
            self.record_indices = list(range(len(texts)))
            
            # Create a better vectorizer with custom analyzer
            self.vectorizer = TfidfVectorizer(
                lowercase=True, 
                stop_words='english',
                ngram_range=(1, 2),  # Include both unigrams and bigrams
                max_features=5000,
                min_df=1,
                norm='l2',
                use_idf=True,
                analyzer='word',
                token_pattern=r'\b[a-zA-Z0-9_]+\b'  # Ensure underscores are treated as part of words
            )
            
            # Fit transform the texts
            self.vectors = self.vectorizer.fit_transform(texts).toarray().astype(np.float32)
            
            # Create FAISS index
            self.index = faiss.IndexFlatL2(self.vectors.shape[1])
            self.index.add(self.vectors)
            
            # Cache everything
            with open(vectorizer_path, 'wb') as f:
                pickle.dump(self.vectorizer, f)
            with open(vectors_path, 'wb') as f:
                pickle.dump(self.vectors, f)
            with open(texts_path, 'wb') as f:
                pickle.dump({'texts': self.raw_texts, 'indices': self.record_indices}, f)
            
            print(f"Built and cached vector search index with {len(texts)} documents")
            return True
        except Exception as e:
            print(f"Error initializing vector search: {str(e)}")
            return False
    
    def search(self, query: str, top_k: int = 10) -> Tuple[List[int], List[float]]:
        """
        Search for similar records with improved relevance ranking.
        
        Args:
            query: Query string
            top_k: Maximum number of results
            
        Returns:
            Tuple of (indices, similarities)
        """
        if not self.enabled or not self.vectorizer or not self.index:
            return [], []
        
        try:
            # First prioritize exact matches on job names
            direct_matches = []
            direct_match_scores = []
            
            # Process query to extract key terms
            query_lower = query.lower()
            
            # Add jobs with exact term matches to the name at the top
            # Use a high boost factor for these exact matches
            boost_factor = 0.03  # Adjust this to control importance of direct term matches
            
            for i, text in enumerate(self.raw_texts):
                text_lower = text.lower()
                
                # Calculate a score based on direct term presence in job name
                match_score = 0
                
                # Check for exact job name match (higher boost)
                if 'job_name:' in text_lower:
                    job_name_parts = re.findall(r'job_name:([^\s,;]+)', text_lower)
                    name_matches = 0
                    if job_name_parts:
                        job_name = job_name_parts[0].lower()
                        # Extract individual terms from query
                        query_terms = [term for term in query_lower.split() if len(term) > 2]
                        for term in query_terms:
                            if term in job_name:
                                name_matches += 1
                                # Boost even more if it's an exact word match
                                if term in job_name.split('_'):
                                    name_matches += 0.5
                    
                    # Calculate a significant boost for name matches
                    if name_matches > 0:
                        match_score = boost_factor * name_matches
                
                # If there's any match score, keep track of it
                if match_score > 0:
                    direct_matches.append(i)
                    direct_match_scores.append(match_score)
            
            # Convert query to vector
            query_vector = self.vectorizer.transform([query]).toarray().astype(np.float32)
            
            # Search with FAISS (returns L2 distances - smaller is better)
            faiss_k = top_k * 3  # Get more results than needed for filtering
            distances, indices = self.index.search(query_vector, faiss_k)
            
            # Convert distances to similarity scores (L2 distance to similarity)
            similarities = []
            indexed_similarities = []
            
            for i, dist in enumerate(distances[0]):
                # Map L2 distance to a reasonable similarity score 
                # Lower distance = higher similarity
                base_similarity = max(0.0, 1.0 - (dist / 30.0))
                similarity = min(0.99, base_similarity)  # Cap at 0.99
                
                # Get original record index
                original_idx = self.record_indices[indices[0][i]] if i < len(indices[0]) else -1
                
                if original_idx >= 0:
                    indexed_similarities.append((original_idx, similarity))
            
            # Combine direct matches with vector similarities
            all_matches = {}
            
            # Add direct matches first with their boost
            for i, (idx, score) in enumerate(zip(direct_matches, direct_match_scores)):
                all_matches[idx] = score
            
            # Add vector similarities, potentially boosting existing matches
            for idx, sim in indexed_similarities:
                if idx in all_matches:
                    # Boost existing match
                    all_matches[idx] += sim
                else:
                    all_matches[idx] = sim
            
            # Sort by score (descending) and convert to lists
            sorted_matches = sorted(all_matches.items(), key=lambda x: x[1], reverse=True)
            
            # Return the top_k results
            final_indices = []
            final_scores = []
            
            for idx, score in sorted_matches[:top_k]:
                final_indices.append(idx)
                final_scores.append(min(0.9999, score))  # Cap at 0.9999
            
            return final_indices, final_scores
            
        except Exception as e:
            print(f"Error in vector search: {str(e)}")
            import traceback
            traceback.print_exc()
            return [], []