# providers/__init__.py should expose get_provider_class and PROVIDER_TYPES
from .base import DataProvider
from .csv_provider import CSVProvider
from .json_provider import JSONProvider

# Register all available providers
PROVIDER_TYPES = {
    'csv': CSVProvider,
    'json': JSONProvider
}

def get_provider_class(provider_type: str):
    """Get provider class by type."""
    return PROVIDER_TYPES.get(provider_type.lower())