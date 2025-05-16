"""
Unified interface for the data-agnostic search system.

This module provides a simplified API for using the search system with
various data sources, handling the complexity of provider selection,
field mapping, and search execution.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Union

# Import core components
from utils.field_mapping import FieldMapping
from search.engine import SearchEngine
from providers.base import DataProvider

# Import provider implementations
from providers.csv_provider import CSVProvider

# Try to import other providers
try:
    from providers.sqlite_provider import SQLiteProvider
    from providers.structured_sqlite_provider import StructuredSQLiteProvider
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    from providers.json_provider import JSONProvider
    JSON_AVAILABLE = True
except ImportError:
    JSON_AVAILABLE = False

try:
    from providers.hybrid_provider import HybridProvider
    HYBRID_AVAILABLE = True
except ImportError:
    HYBRID_AVAILABLE = False

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedSearch:
    """
    Unified interface for the data-agnostic search system.
    
    This class provides a simplified API for using the search system,
    handling provider selection, field mapping, and search execution.
    """
    
    def __init__(self, 
                 data_source: str,
                 field_mapping: Optional[FieldMapping] = None,
                 mapping_file: Optional[str] = None,
                 provider_type: Optional[str] = None,
                 auto_detect: bool = True,
                 cache_dir: Optional[str] = None,
                 vector_weight: float = 0.5,
                 table_name: Optional[str] = None):
        """
        Initialize the unified search interface.
        
        Args:
            data_source: Path to the data source
            field_mapping: Field mapping configuration
            mapping_file: Path to field mapping JSON file
            provider_type: Type of provider to use ('csv', 'sqlite', 'json', 'hybrid')
            auto_detect: Whether to auto-detect field mapping if not provided
            cache_dir: Directory for caching search data
            vector_weight: Weight for vector search when using hybrid provider
            table_name: Name of the table to use (for SQLite provider)
        """
        self.data_source = data_source
        
        # Load field mapping
        if field_mapping:
            self.field_mapping = field_mapping
        elif mapping_file:
            self.field_mapping = FieldMapping.from_json(mapping_file)
        elif auto_detect:
            # Auto-detect field mapping based on file extension
            if data_source.lower().endswith('.csv'):
                self.field_mapping = FieldMapping.from_csv_headers(data_source)
            else:
                # Will be detected by provider later
                self.field_mapping = None
        else:
            # Use default mapping
            self.field_mapping = FieldMapping()
        
        # Detect provider type if not specified
        if provider_type is None:
            provider_type = self._detect_provider_type(data_source)
        
        # Create provider
        self.provider = self._create_provider(
            data_source, 
            provider_type,
            self.field_mapping,
            vector_weight,
            table_name
        )
        
        # Get field mapping from provider if not already set
        if self.field_mapping is None and self.provider.field_mapping:
            self.field_mapping = self.provider.field_mapping
        
        # Create search engine
        self.search_engine = SearchEngine(
            data_provider=self.provider,
            cache_dir=cache_dir
        )
    
    def _detect_provider_type(self, data_source: str) -> str:
        """
        Auto-detect provider type based on file extension.
        
        Args:
            data_source: Path to the data source
            
        Returns:
            Provider type string
        """
        # Get file extension
        file_ext = os.path.splitext(data_source)[1].lower()
        
        if file_ext == '.csv':
            return 'csv'
        elif file_ext in ['.db', '.sqlite', '.sqlite3'] and SQLITE_AVAILABLE:
            return 'sqlite'
        elif file_ext == '.json' and JSON_AVAILABLE:
            return 'json'
        elif HYBRID_AVAILABLE:
            # Default to hybrid if available
            return 'hybrid'
        else:
            # Fallback to CSV
            return 'csv'
    
    def _create_provider(self, 
                        data_source: str, 
                        provider_type: str,
                        field_mapping: Optional[FieldMapping],
                        vector_weight: float,
                        table_name: Optional[str]) -> DataProvider:
        """
        Create appropriate data provider based on source type.
        
        Args:
            data_source: Path to the data source
            provider_type: Type of provider to use
            field_mapping: Field mapping configuration
            vector_weight: Weight for vector search
            table_name: Name of the table to use (for SQLite provider)
            
        Returns:
            DataProvider instance
        """
        if provider_type == 'csv':
            return CSVProvider(data_source, field_mapping)
        elif provider_type == 'sqlite' and SQLITE_AVAILABLE:
            provider = SQLiteProvider(data_source, table_name)
            if field_mapping:
                provider.set_field_mapping(field_mapping)
            return provider
        elif provider_type == 'structured-sqlite' and SQLITE_AVAILABLE:
            provider = StructuredSQLiteProvider(data_source, table_name)
            if field_mapping:
                provider.set_field_mapping(field_mapping)
            return provider
        elif provider_type == 'json' and JSON_AVAILABLE:
            provider = JSONProvider(data_source)
            if field_mapping:
                provider.set_field_mapping(field_mapping)
            return provider
        elif provider_type == 'hybrid' and HYBRID_AVAILABLE:
            return HybridProvider(
                data_source=data_source,
                field_mapping=field_mapping,
                vector_weight=vector_weight,
                table_name=table_name
            )
        else:
            logger.warning(f"Provider {provider_type} not available. Falling back to CSV provider.")
            return CSVProvider(data_source, field_mapping)
    
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
    
    def get_record_by_id(self, id_value: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific record by ID.
        
        Args:
            id_value: ID value to look up
            
        Returns:
            Record dictionary or None if not found
        """
        return self.provider.get_by_id(id_value)
    
    def get_all_records(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get all records from the data source.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of record dictionaries
        """
        records = self.provider.get_all_records()
        return records[:limit] if limit else records
    
    def count_records(self, query: Optional[str] = None) -> Union[int, Dict[str, Any]]:
        """
        Count records, optionally filtering by query.
        
        Args:
            query: Search query to filter records (optional)
            
        Returns:
            Record count or dictionary with count details if query provided
        """
        if query:
            # Use counting query handler
            return self.search_engine._handle_counting_query(query)
        else:
            # Get total count
            return self.provider.get_record_count()
    
    def display_results(self, results: List[Dict[str, Any]], max_width: Optional[int] = None) -> None:
        """
        Display search results in a readable format.
        
        Args:
            results: Search results
            max_width: Maximum width for display (auto-detect if None)
        """
        try:
            from search.results.formatter import display_results
            display_results(
                results, 
                max_width=max_width,
                id_field='id',
                name_field='name',
                status_field='status'
            )
        except ImportError:
            # Fallback to simple display
            if not results:
                print("No results found.")
                return
            
            print(f"\nFound {len(results)} results:")
            for i, result in enumerate(results[:10]):
                id_value = result.get('id', 'unknown')
                name_value = result.get('name', 'unknown')
                print(f"{i+1}. {name_value} (ID: {id_value})")
    
    def format_for_llm(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """
        Format search results for LLM consumption.
        
        Args:
            results: Search results
            query: Original query
            
        Returns:
            Dictionary formatted for LLM consumption
        """
        try:
            from search.results.formatter import format_for_llm
            return format_for_llm(
                results, 
                query,
                id_field='id',
                name_field='name',
                status_field='status'
            )
        except ImportError:
            # Fallback to simple formatting
            formatted_results = []
            for result in results:
                # Remove metadata fields
                formatted_results.append({k: v for k, v in result.items() if not k.startswith('_')})
            
            return {
                "query": query,
                "count": len(results),
                "results": formatted_results
            }
    
    def export_results(self, results: List[Dict[str, Any]], format: str = 'json') -> str:
        """
        Export search results to a specific format.
        
        Args:
            results: Search results
            format: Export format ('json' or 'csv')
            
        Returns:
            Formatted string
        """
        if format.lower() == 'json':
            import json
            return json.dumps([{k: v for k, v in r.items() if not k.startswith('_')} for r in results], indent=2)
        elif format.lower() == 'csv':
            import csv
            from io import StringIO
            
            if not results:
                return ""
            
            # Get all unique fields
            all_fields = set()
            for result in results:
                for field in result.keys():
                    if not field.startswith('_'):
                        all_fields.add(field)
            
            # Sort fields
            sorted_fields = sorted(all_fields)
            
            # Create CSV
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=sorted_fields)
            writer.writeheader()
            
            for result in results:
                # Filter out metadata fields
                filtered_result = {k: v for k, v in result.items() if not k.startswith('_')}
                writer.writerow(filtered_result)
            
            return output.getvalue()
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def explain_search(self, query: str) -> Dict[str, Any]:
        """
        Explain how a query would be processed.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with explanation details
        """
        return self.search_engine.explain_search(query)
    
    def get_field_info(self) -> Dict[str, Any]:
        """
        Get information about available fields.
        
        Returns:
            Dictionary with field information
        """
        if not self.field_mapping:
            return {"fields": self.provider.get_all_fields()}
        
        return {
            "id_field": self.field_mapping.id_field,
            "name_field": self.field_mapping.name_field,
            "status_field": self.field_mapping.status_field,
            "timestamp_fields": list(self.field_mapping.timestamp_fields),
            "numeric_fields": list(self.field_mapping.numeric_fields),
            "text_fields": list(self.field_mapping.text_fields),
            "all_fields": self.provider.get_all_fields()
        }