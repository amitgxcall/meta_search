"""
Data provider registry for the search system.
"""

from typing import Dict, Type, Optional

from .base import DataProvider
from .csv_provider import CSVProvider
from .json_provider import JSONProvider

# Register all available providers
PROVIDER_TYPES = {
    'csv': CSVProvider,
    'json': JSONProvider
}

def get_provider_class(provider_type: str) -> Optional[Type[DataProvider]]:
    """
    Get provider class by type.
    
    Args:
        provider_type: Provider type identifier
        
    Returns:
        Data provider class or None if not found
    """
    return PROVIDER_TYPES.get(provider_type.lower())

def register_provider(provider_type: str, provider_class: Type[DataProvider]) -> None:
    """
    Register a new provider type.
    
    Args:
        provider_type: Provider type identifier
        provider_class: Data provider class
    """
    PROVIDER_TYPES[provider_type.lower()] = provider_class