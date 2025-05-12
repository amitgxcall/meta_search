"""
Abstract data provider interface for the search system.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

class DataProvider(ABC):
    """
    Abstract base class for data providers.
    
    Implement this class to provide data from different sources
    (CSV, database, API, etc.) to the search system.
    """
    
    @abstractmethod
    def get_all_fields(self) -> List[str]:
        """
        Get a list of all available fields/columns in the data.
        
        Returns:
            List of field names
        """
        pass
    
    @abstractmethod
    def get_record_count(self) -> int:
        """
        Get the total number of records in the data.
        
        Returns:
            Integer count of records
        """
        pass
    
    @abstractmethod
    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Get all records from the data source.
        
        Returns:
            List of dictionaries representing records
        """
        pass
    
    @abstractmethod
    def get_record_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        """
        Get a specific record by its ID.
        
        Args:
            id_value: Value of the ID field
            
        Returns:
            Dictionary representing the record, or None if not found
        """
        pass
    
    @abstractmethod
    def query_records(self, filters: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Query records based on filters.
        
        Args:
            filters: Dictionary of field:value pairs to filter by
            limit: Maximum number of records to return
            
        Returns:
            List of dictionaries representing matching records
        """
        pass
    
    @abstractmethod
    def get_text_for_vector_search(self, record: Dict[str, Any], field_weights: Dict[str, float]) -> str:
        """
        Convert a record to text for vector search.
        
        Args:
            record: Dictionary representing a record
            field_weights: Dictionary of field:weight pairs
            
        Returns:
            String representation of the record for vector search
        """
        pass
    
    @abstractmethod
    def prepare_for_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a record for output (e.g., formatting dates, handling nulls).
        
        Args:
            record: Dictionary representing a record
            
        Returns:
            Formatted record dictionary
        """
        pass