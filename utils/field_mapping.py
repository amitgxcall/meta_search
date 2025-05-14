"""
Field mapping utilities.
"""

from typing import Dict, Optional

class FieldMapping:
    """
    Class for mapping standard field names to source-specific field names.
    """
    
    def __init__(self):
        """Initialize an empty field mapping."""
        self.mappings = {}  # standard_name -> source_name
        
    def add_mapping(self, standard_name: str, source_name: str) -> None:
        """
        Add a mapping from a standard field name to a source-specific field name.
        
        Args:
            standard_name: The standard field name
            source_name: The source-specific field name
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
        Get all mappings.
        
        Returns:
            Dictionary mapping standard names to source-specific names
        """
        return self.mappings