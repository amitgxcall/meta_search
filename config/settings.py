"""
Configuration settings for the meta_search system.

This module provides centralized configuration settings for the meta_search
system, including default settings, provider configurations, and search engine
settings.

Example:
    # Load configuration
    config = load_config('config.json')
    
    # Use configuration in search engine
    engine = SearchEngine(config=config)
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SearchConfig:
    """
    Configuration settings for the meta_search system.
    
    This class provides centralized configuration settings for the meta_search
    system, including default settings, provider configurations, and search
    engine settings.
    
    Attributes:
        provider_settings: Dictionary with provider-specific settings
        search_settings: Dictionary with search engine settings
        field_weights: Dictionary with field weight configurations for scoring
        cache_settings: Dictionary with cache-related settings
        debug_mode: Whether to enable debug mode
    """
    
    def __init__(self,
                 provider_settings: Optional[Dict[str, Any]] = None,
                 search_settings: Optional[Dict[str, Any]] = None,
                 field_weights: Optional[Dict[str, float]] = None,
                 cache_settings: Optional[Dict[str, Any]] = None,
                 debug_mode: bool = False):
        """
        Initialize the search configuration.
        
        Args:
            provider_settings: Provider-specific settings
            search_settings: Search engine settings
            field_weights: Field weight configurations
            cache_settings: Cache-related settings
            debug_mode: Whether to enable debug mode
        """
        # Default provider settings
        self.provider_settings = provider_settings or {
            'default_provider': 'hybrid',
            'csv': {
                'delimiter': ',',
                'quotechar': '"',
                'encoding': 'utf-8'
            },
            'sqlite': {
                'timeout': 5.0,
                'detect_types': 0,
                'isolation_level': None
            },
            'hybrid': {
                'vector_weight': 0.5,
                'text_fields': None,
                'sequential': False
            }
        }
        
        # Default search settings
        self.search_settings = search_settings or {
            'max_results': 10,
            'min_score': 0.0,
            'sort_by': '_score',
            'sort_order': 'desc',
            'exclude_fields': ['_internal', '_metadata']
        }
        
        # Default field weights
        self.field_weights = field_weights or {
            'name': 2.0,      # Name fields get higher weight
            'description': 1.5,  # Description fields get higher weight
            'status': 1.0,    # Status fields get normal weight
            'error_message': 1.0,  # Error message fields get normal weight
            'default': 0.5    # Default weight for other fields
        }
        
        # Default cache settings
        self.cache_settings = cache_settings or {
            'enabled': True,
            'directory': os.path.join(os.getcwd(), 'cache'),
            'ttl': 86400,  # 1 day in seconds
            'max_size': 100 * 1024 * 1024  # 100 MB
        }
        
        # Debug mode
        self.debug_mode = debug_mode
        
        # Set up cache directory if enabled
        if self.cache_settings['enabled']:
            os.makedirs(self.cache_settings['directory'], exist_ok=True)
    
    def get_provider_setting(self, provider_type: str, key: str, default: Any = None) -> Any:
        """
        Get a specific provider setting.
        
        Args:
            provider_type: Provider type (csv, sqlite, hybrid, etc.)
            key: Setting key
            default: Default value if not found
            
        Returns:
            Setting value
        """
        provider_dict = self.provider_settings.get(provider_type, {})
        return provider_dict.get(key, default)
    
    def get_search_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a specific search setting.
        
        Args:
            key: Setting key
            default: Default value if not found
            
        Returns:
            Setting value
        """
        return self.search_settings.get(key, default)
    
    def get_field_weight(self, field: str) -> float:
        """
        Get the weight for a specific field.
        
        Args:
            field: Field name
            
        Returns:
            Field weight
        """
        return self.field_weights.get(field, self.field_weights.get('default', 0.5))
    
    def get_cache_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a specific cache setting.
        
        Args:
            key: Setting key
            default: Default value if not found
            
        Returns:
            Setting value
        """
        return self.cache_settings.get(key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of the configuration
        """
        return {
            'provider_settings': self.provider_settings,
            'search_settings': self.search_settings,
            'field_weights': self.field_weights,
            'cache_settings': self.cache_settings,
            'debug_mode': self.debug_mode
        }
    
    def save(self, file_path: str) -> bool:
        """
        Save configuration to a JSON file.
        
        Args:
            file_path: Path to save the configuration
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(file_path, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False
    
    @classmethod
    def load(cls, file_path: str) -> 'SearchConfig':
        """
        Load configuration from a JSON file.
        
        Args:
            file_path: Path to the configuration file
            
        Returns:
            SearchConfig instance
        """
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            return cls(
                provider_settings=data.get('provider_settings'),
                search_settings=data.get('search_settings'),
                field_weights=data.get('field_weights'),
                cache_settings=data.get('cache_settings'),
                debug_mode=data.get('debug_mode', False)
            )
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return cls()  # Return default configuration on error


def load_config(file_path: Optional[str] = None) -> SearchConfig:
    """
    Load configuration from a file or return default configuration.
    
    Args:
        file_path: Path to the configuration file (optional)
        
    Returns:
        SearchConfig instance
    """
    if file_path and os.path.exists(file_path):
        return SearchConfig.load(file_path)
    
    # Try default locations
    default_locations = [
        'config.json',
        os.path.join(os.getcwd(), 'config.json'),
        os.path.join(os.path.dirname(__file__), 'config.json')
    ]
    
    for location in default_locations:
        if os.path.exists(location):
            return SearchConfig.load(location)
    
    # Return default configuration
    logger.info("Using default configuration")
    return SearchConfig()