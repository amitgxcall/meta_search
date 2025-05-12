"""
Unified interface for the job search system.
"""

import os
from typing import Dict, List, Any, Optional, Tuple, Union

from meta_search.providers import get_provider_class, PROVIDER_TYPES

from .utils.field_mapping import FieldMapping
from .search.engine import SearchEngine
from .providers.base import DataProvider
from .providers import get_provider_class, PROVIDER_TYPES

class UnifiedJobSearch:
    """
    Unified interface for the job search system.
    """
    
    def __init__(self, 
                 data_source: str,
                 source_type: Optional[str] = None,
                 field_mapping: Optional[FieldMapping] = None,
                 cache_dir: Optional[str] = None):
        """
        Initialize the unified search interface.
        
        Args:
            data_source: Path to the data source
            source_type: Type of data source (csv, sqlite, etc.)
            field_mapping: Field mapping configuration
            cache_dir: Directory for caching search data
        """
        self.data_source = data_source
        self.provider = self._create_provider(data_source, source_type, field_mapping)
        self.search_engine = SearchEngine(
            data_provider=self.provider,
            cache_dir=cache_dir
        )
    
    def _create_provider(self, 
                        data_source: str, 
                        source_type: Optional[str],
                        field_mapping: Optional[FieldMapping]) -> DataProvider:
        """
        Create appropriate data provider based on source type.
        
        Args:
            data_source: Path to the data source
            source_type: Type of data source
            field_mapping: Field mapping configuration
            
        Returns:
            DataProvider instance
        """
        # Auto-detect source type if not specified
        if source_type is None:
            if data_source.lower().endswith('.csv'):
                source_type = 'csv'
            elif data_source.lower().endswith(('.db', '.sqlite', '.sqlite3')):
                source_type = 'sqlite'
            elif data_source.lower().endswith(('.json')):
                source_type = 'json'
            else:
                raise ValueError(f"Could not auto-detect source type for {data_source}")
        
        # Get provider class
        provider_class = get_provider_class(source_type)
        if not provider_class:
            supported = ", ".join(PROVIDER_TYPES.keys())
            raise ValueError(f"Unsupported source type: {source_type}. Supported types: {supported}")
        
        # Create provider instance
        return provider_class(data_source, field_mapping)
    
    def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for records matching the query.
        
        Args:
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of search result dictionaries
        """
        return self.search_engine.search(query, limit)
    
    def get_record_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        """
        Get a specific record by ID.
        
        Args:
            id_value: ID value to look up
            
        Returns:
            Record dictionary or None if not found
        """
        record = self.provider.get_record_by_id(id_value)
        if record:
            return self.provider.prepare_for_output(record)
        return None
    
    def display_results(self, results: List[Dict[str, Any]], max_width: int = 100) -> None:
        """
        Display search results in a readable format.
        
        Args:
            results: List of search result dictionaries
            max_width: Maximum width for display
        """
        from .search.result_formatter import display_results
        display_results(
            results, 
            max_width=max_width,
            id_field=self.provider.field_mapping.id_field,
            name_field=self.provider.field_mapping.name_field,
            status_field=self.provider.field_mapping.status_field
        )
    
    def format_for_llm(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """
        Format search results for LLM consumption.
        
        Args:
            results: Search results
            query: Original query
            
        Returns:
            Formatted data for LLM
        """
        return self.search_engine.format_for_llm(results, query)
    
    def explain_search(self, query: str) -> Dict[str, Any]:
        """
        Explain how a query would be processed.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with explanation details
        """
        return self.search_engine.explain_search(query)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the data source.
        
        Returns:
            Dictionary of statistics
        """
        total_records = self.provider.get_record_count()
        fields = self.provider.get_all_fields()
        
        # If status field exists, count records by status
        status_counts = {}
        if self.provider.field_mapping.status_field:
            status_field = self.provider.field_mapping.status_field
            all_records = self.provider.get_all_records()
            
            for record in all_records:
                status = record.get(status_field)
                if status:
                    status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "total_records": total_records,
            "available_fields": fields,
            "status_counts": status_counts,
            "data_source": self.data_source,
            "provider_type": type(self.provider).__name__
        }