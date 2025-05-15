"""
Field mapping utilities for normalizing field names across different data sources.

This module provides functionality to map standard field names to source-specific 
field names, allowing the search system to work with different data sources that
may use different naming conventions.

Example:
    # Create field mapping
    mapping = FieldMapping()
    
    # Add mappings for standard fields
    mapping.add_mapping('id', 'job_id')
    mapping.add_mapping('name', 'job_name')
    
    # Get source field for a standard name
    source_field = mapping.get_source_field('id')  # Returns 'job_id'
    
    # Use in data provider
    provider.set_field_mapping(mapping)
"""

from typing import Dict, Optional, List, Set, Any


class FieldMapping:
    """
    Maps standard field names to source-specific field names.
    
    This class allows the search system to work with different data sources
    that may use different field naming conventions. It maintains a mapping
    from standard field names (used throughout the system) to source-specific
    field names (used in the actual data).
    
    Attributes:
        mappings: Dictionary mapping standard names to source-specific names
        id_field: Field name for the ID field in the source
        name_field: Field name for the name field in the source
        status_field: Field name for the status field in the source
        timestamp_fields: Set of field names for timestamp fields
        numeric_fields: Set of field names for numeric fields
        text_fields: Set of field names for text fields
    """
    
    def __init__(self, 
                 id_field: str = 'id', 
                 name_field: str = 'name',
                 status_field: Optional[str] = 'status',
                 timestamp_fields: Optional[List[str]] = None,
                 numeric_fields: Optional[List[str]] = None,
                 text_fields: Optional[List[str]] = None):
        """
        Initialize the field mapping.
        
        Args:
            id_field: Field name for the ID field in the source
            name_field: Field name for the name field in the source
            status_field: Field name for the status field in the source
            timestamp_fields: List of field names for timestamp fields
            numeric_fields: List of field names for numeric fields
            text_fields: List of field names for text fields
        """
        self.mappings: Dict[str, str] = {}  # standard_name -> source_name
        
        # Add standard mappings
        self.mappings['id'] = id_field
        self.mappings['name'] = name_field
        
        if status_field:
            self.mappings['status'] = status_field
        
        # Store field type information
        self.id_field = id_field
        self.name_field = name_field
        self.status_field = status_field
        self.timestamp_fields = set(timestamp_fields or [])
        self.numeric_fields = set(numeric_fields or [])
        self.text_fields = set(text_fields or [])
        
    def add_mapping(self, standard_name: str, source_name: str) -> None:
        """
        Add a mapping from a standard field name to a source-specific field name.
        
        Args:
            standard_name: The standard field name (used throughout the system)
            source_name: The source-specific field name (used in the data source)
        """
        self.mappings[standard_name] = source_name
        
    def get_source_field(self, standard_name: str) -> Optional[str]:
        """
        Get the source-specific field name for a standard field name.
        
        Args:
            standard_name: The standard field name
            
        Returns:
            The source-specific field name if mapped, None otherwise
        """
        return self.mappings.get(standard_name)
    
    def get_mappings(self) -> Dict[str, str]:
        """
        Get all mappings from standard names to source-specific names.
        
        Returns:
            Dictionary mapping standard names to source-specific names
        """
        return self.mappings
    
    def map_field(self, standard_name: str, default: Optional[str] = None) -> str:
        """
        Map a standard field name to a source-specific field name.
        
        Args:
            standard_name: The standard field name
            default: Default value to return if not mapped
            
        Returns:
            The source-specific field name if mapped, default otherwise
        """
        return self.mappings.get(standard_name, default or standard_name)
    
    def map_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a record from source-specific field names to standard field names.
        
        Args:
            record: Record with source-specific field names
            
        Returns:
            Record with standard field names
        """
        mapped_record = {}
        
        # Invert the mapping dictionary for reverse lookup
        reverse_mapping = {v: k for k, v in self.mappings.items()}
        
        # Map each field
        for field_name, value in record.items():
            # Use standard name if available, otherwise keep original
            standard_name = reverse_mapping.get(field_name, field_name)
            mapped_record[standard_name] = value
        
        return mapped_record
    
    def reverse_map_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a record from standard field names back to source-specific field names.
        
        Args:
            record: Record with standard field names
            
        Returns:
            Record with source-specific field names
        """
        mapped_record = {}
        
        # Map each field
        for field_name, value in record.items():
            # Use source name if available, otherwise keep original
            source_name = self.mappings.get(field_name, field_name)
            mapped_record[source_name] = value
        
        return mapped_record
    
    def map_filter(self, filter_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a filter dictionary from standard field names to source-specific field names.
        
        Args:
            filter_dict: Filter dictionary with standard field names
            
        Returns:
            Filter dictionary with source-specific field names
        """
        mapped_filters = {}
        
        for field_name, value in filter_dict.items():
            # Use source name if available, otherwise keep original
            source_name = self.mappings.get(field_name, field_name)
            mapped_filters[source_name] = value
        
        return mapped_filters

    def get_field_type(self, field_name: str) -> str:
        """
        Get the type of a field.
        
        Args:
            field_name: Name of the field
            
        Returns:
            Field type ('id', 'name', 'status', 'timestamp', 'numeric', 'text', or 'unknown')
        """
        if field_name == self.id_field:
            return 'id'
        elif field_name == self.name_field:
            return 'name'
        elif field_name == self.status_field:
            return 'status'
        elif field_name in self.timestamp_fields:
            return 'timestamp'
        elif field_name in self.numeric_fields:
            return 'numeric'
        elif field_name in self.text_fields:
            return 'text'
        else:
            return 'unknown'