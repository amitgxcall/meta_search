"""
Search engine implementation for meta_search.
"""

import os
import sys
from typing import List, Dict, Any, Optional

# Direct imports instead of relative imports
try:
    from providers.base import DataProvider
except ImportError:
    # Add the parent directory to the path for imports
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from providers.base import DataProvider

class SearchEngine:
    """
    Main search engine that coordinates searching across multiple providers.
    """
    
    def __init__(self):
        """Initialize the search engine."""
        self.providers = []
        
    def register_provider(self, provider: DataProvider) -> None:
        """
        Register a data provider with the search engine.
        
        Args:
            provider: The data provider to register
        """
        self.providers.append(provider)
        
    def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search across all registered providers.
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of search results
        """
        results = []
        
        for provider in self.providers:
            provider_results = provider.search(query)
            results.extend(provider_results)
            
        # Sort results by relevance (if available)
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        # Limit the number of results
        return results[:limit]