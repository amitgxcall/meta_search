"""
Base data provider definition.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class DataProvider(ABC):
    """
    Abstract base class for all data providers.
    Each provider should implement the necessary methods for its data source.
    """
    
    def __init__(self, source_path: str):
        """
        Initialize the data provider.
        
        Args:
            source_path: Path to the data source
        """
        self.source_path = source_path
        self.field_mapping = None
        
    def set_field_mapping(self, field_mapping) -> None:
        """
        Set the field mapping for this provider.
        
        Args:
            field_mapping: FieldMapping object that maps standard field names to source-specific names
        """
        self.field_mapping = field_mapping
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Connect to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def search(self, query: str) -> List[Dict[str, Any]]:
        """
        Search the data source.
        
        Args:
            query: The search query
            
        Returns:
            List of search results
        """
        pass
    
    @abstractmethod
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        pass
    
    def map_fields(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map fields from source-specific names to standard names.
        
        Args:
            item: The item with source-specific field names
            
        Returns:
            The item with standard field names
        """
        if self.field_mapping is None:
            return item
        
        mapped_item = {}
        
        # Copy unmapped fields as-is
        for key, value in item.items():
            mapped_item[key] = value
        
        # Apply field mappings
        for standard_name, source_name in self.field_mapping.get_mappings().items():
            if source_name in item:
                mapped_item[standard_name] = item[source_name]
                
        return mapped_item