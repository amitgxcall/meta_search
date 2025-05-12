"""
Caching utilities for the search system.
"""

import os
import pickle
import hashlib
import time
from typing import Any, Optional, Dict, Tuple, Callable
import json

class Cache:
    """
    Simple file-based cache for expensive operations.
    """
    
    def __init__(self, cache_dir: str, ttl: int = 86400):
        """
        Initialize the cache.
        
        Args:
            cache_dir: Directory for cache files
            ttl: Time-to-live in seconds (default: 1 day)
        """
        self.cache_dir = cache_dir
        self.ttl = ttl
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key: str) -> str:
        """
        Get the file path for a cache key.
        
        Args:
            key: Cache key
            
        Returns:
            Path to cache file
        """
        # Hash the key to create a filename
        hashed_key = hashlib.md5(str(key).encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hashed_key}.cache")
    
    def get(self, key: str, default: Any = None) -> Tuple[bool, Any]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            default: Default value if not in cache
            
        Returns:
            Tuple of (hit, value) where hit is True if cache hit
        """
        cache_path = self._get_cache_path(key)
        
        # Check if cache file exists
        if not os.path.exists(cache_path):
            return False, default
        
        # Check if cache file is expired
        if self.ttl > 0:
            modified_time = os.path.getmtime(cache_path)
            if time.time() - modified_time > self.ttl:
                # Cache expired
                return False, default
        
        # Load cache data
        try:
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
            return True, data
        except Exception as e:
            print(f"Error loading cache: {str(e)}")
            return False, default
    
    def set(self, key: str, value: Any) -> bool:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            
        Returns:
            True if successful, False otherwise
        """
        cache_path = self._get_cache_path(key)
        
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(value, f)
            return True
        except Exception as e:
            print(f"Error setting cache: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if successful, False otherwise
        """
        cache_path = self._get_cache_path(key)
        
        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
                return True
            except Exception as e:
                print(f"Error deleting cache: {str(e)}")
        
        return False
    
    def clear(self) -> int:
        """
        Clear all items from the cache.
        
        Returns:
            Number of items cleared
        """
        count = 0
        
        for filename in os.listdir(self.cache_dir):
            if filename.endswith('.cache'):
                try:
                    os.remove(os.path.join(self.cache_dir, filename))
                    count += 1
                except Exception as e:
                    print(f"Error clearing cache: {str(e)}")
        
        return count
    
    def cached(self, func: Callable) -> Callable:
        """
        Decorator for caching function results.
        
        Args:
            func: Function to cache
            
        Returns:
            Wrapped function
        """
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            key_parts = [func.__name__]
            key_parts.extend(str(arg) for arg in args)
            key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
            cache_key = ":".join(key_parts)
            
            # Try to get from cache
            hit, value = self.get(cache_key)
            if hit:
                return value
            
            # Call function and cache result
            result = func(*args, **kwargs)
            self.set(cache_key, result)
            return result
        
        return wrapper