"""
Additional functionality for CSVProvider to support HybridProvider.
"""

# Add a get_all_items method to CSVProvider
def get_all_items(self):
    """
    Get all items from the CSV.
    
    Returns:
        List of all items with fields mapped
    """
    return [self.map_fields(item.copy()) for item in self.data]

# Monkey patch the CSVProvider class
import sys
import os

# Add the current directory to sys.path to allow imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from providers.csv_provider import CSVProvider
    # Add the method if it doesn't already exist
    if not hasattr(CSVProvider, 'get_all_items'):
        setattr(CSVProvider, 'get_all_items', get_all_items)
except ImportError:
    pass