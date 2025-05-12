"""
Field mapping utilities for the search system.
"""

from typing import Dict, List, Any, Optional

class FieldMapping:
    """
    Helper class for mapping generic field names to data-specific field names.
    
    This allows the search system to use generic field names (like 'id', 'name', 'status')
    while the actual data source might have different field names.
    """
    
    def __init__(self, 
                 id_field: str = 'id',
                 name_field: str = 'name',
                 status_field: Optional[str] = 'status',
                 timestamp_fields: Optional[List[str]] = None,
                 numeric_fields: Optional[List[str]] = None,
                 text_fields: Optional[List[str]] = None,
                 custom_mappings: Optional[Dict[str, str]] = None):
        """
        Initialize field mapping.
        
        Args:
            id_field: Field name for the primary identifier
            name_field: Field name for the name/title
            status_field: Field name for status information
            timestamp_fields: List of fields containing timestamps
            numeric_fields: List of fields containing numeric values
            text_fields: List of fields containing searchable text
            custom_mappings: Dictionary of generic_name:actual_field_name pairs
        """
        self.id_field = id_field
        self.name_field = name_field
        self.status_field = status_field
        self.timestamp_fields = timestamp_fields or []
        self.numeric_fields = numeric_fields or []
        self.text_fields = text_fields or []
        self.custom_mappings = custom_mappings or {}
        
        # Create reverse mapping for lookups
        self.reverse_mappings = {
            id_field: 'id',
            name_field: 'name'
        }
        if status_field:
            self.reverse_mappings[status_field] = 'status'
            
        if custom_mappings:
            for generic, actual in custom_mappings.items():
                self.reverse_mappings[actual] = generic
    
    def get_actual_field(self, generic_field: str) -> str:
        """
        Get the actual field name for a generic field name.
        
        Args:
            generic_field: Generic field name (e.g., 'id', 'name')
            
        Returns:
            Actual field name in the data source
        """
        if generic_field == 'id':
            return self.id_field
        elif generic_field == 'name':
            return self.name_field
        elif generic_field == 'status' and self.status_field:
            return self.status_field
        elif generic_field in self.custom_mappings:
            return self.custom_mappings[generic_field]
        else:
            # If no mapping exists, assume the field name is the same
            return generic_field
    
    def get_generic_field(self, actual_field: str) -> str:
        """
        Get the generic field name for an actual field name.
        
        Args:
            actual_field: Actual field name in the data source
            
        Returns:
            Generic field name
        """
        return self.reverse_mappings.get(actual_field, actual_field)
    
    def map_filter(self, generic_filter: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a filter with generic field names to actual field names.
        
        Args:
            generic_filter: Filter with generic field names
            
        Returns:
            Filter with actual field names
        """
        return {self.get_actual_field(k): v for k, v in generic_filter.items()}
    
    def reverse_map_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a record with actual field names to generic field names.
        
        Args:
            record: Record with actual field names
            
        Returns:
            Record with generic field names
        """
        return {self.get_generic_field(k): v for k, v in record.items()}