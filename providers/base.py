"""
Base data provider definition.

This module provides the abstract base class for all data providers,
defining the interface that must be implemented by concrete providers.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple

# Import field mapping
from ..utils.field_mapping import FieldMapping


class DataProvider(ABC):
    """
    Abstract base class for all data providers.
    
    Each provider should implement the necessary methods for its data source.
    The base class provides common functionality and a consistent interface
    for working with different data sources.
    """
    
    def __init__(self, source_path: str):
        """
        Initialize the data provider.
        
        Args:
            source_path: Path to the data source
        """
        self.source_path = source_path
        self.field_mapping = None
        
    def set_field_mapping(self, field_mapping: FieldMapping) -> None:
        """
        Set the field mapping for this provider.
        
        Args:
            field_mapping: FieldMapping object that maps standard field names to source-specific names
        """
        self.field_mapping = field_mapping
        
    def infer_field_mapping(self) -> FieldMapping:
        """
        Infer field mapping from the data source.
        
        Returns:
            Inferred FieldMapping
        """
        # Get a sample record
        records = self.get_sample_records(1)
        if not records:
            return FieldMapping()  # Return default mapping
        
        # Create mapping from sample
        mapping = FieldMapping()
        
        # Infer field types
        mapping.infer_field_types(records[0])
        
        return mapping
    
    def get_sample_records(self, count: int = 5) -> List[Dict[str, Any]]:
        """
        Get sample records for field mapping inference.
        
        Args:
            count: Number of sample records to retrieve
            
        Returns:
            List of sample records
        """
        # Default implementation - can be overridden by subclasses
        all_records = self.get_all_records()
        return all_records[:min(count, len(all_records))]
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Connect to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Search the data source.
        
        Args:
            query: The search query
            **kwargs: Additional search parameters
            
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
    
    def get_all_fields(self) -> List[str]:
        """
        Get all available fields in the data source.
        
        Returns:
            List of field names
        """
        # Default implementation - should be overridden by subclasses
        records = self.get_sample_records()
        if not records:
            return []
        
        # Collect unique fields from all sample records
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        return list(all_fields)
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Get all records from the data source.
        
        Returns:
            List of all records
        """
        # Default implementation - should be overridden by subclasses
        return []
    
    def get_record_count(self) -> int:
        """
        Get the total number of records in the data source.
        
        Returns:
            Number of records
        """
        # Default implementation - should be overridden by subclasses
        return len(self.get_all_records())
    
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
        
        return self.field_mapping.map_record(item)
    
    def prepare_for_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a record for output, ensuring standard field names.
        
        Args:
            record: Record to prepare
            
        Returns:
            Prepared record with standard field names
        """
        if not self.field_mapping:
            return record
        
        output = {}
        
        # Map essential fields (id, name, status)
        id_field = self.field_mapping.id_field
        if id_field in record:
            output['id'] = record[id_field]
        
        name_field = self.field_mapping.name_field
        if name_field in record:
            output['name'] = record[name_field]
        
        status_field = getattr(self.field_mapping, 'status_field', None)
        if status_field and status_field in record:
            output['status'] = record[status_field]
        
        # Copy all other fields
        for field, value in record.items():
            if field not in [id_field, name_field, status_field]:
                output[field] = value
        
        # Include metadata fields (score, etc.)
        for field in record:
            if field.startswith('_'):
                output[field] = record[field]
        
        return output
    
    def get_field_type(self, field_name: str) -> str:
        """
        Get the type of a field.
        
        Args:
            field_name: Name of the field
            
        Returns:
            Field type ('id', 'name', 'status', 'timestamp', 'numeric', 'text', or 'unknown')
        """
        if not self.field_mapping:
            return 'unknown'
        
        return self.field_mapping.get_field_type(field_name)