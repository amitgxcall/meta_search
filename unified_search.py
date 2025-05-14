"""
Unified interface for the job search system.
"""

import os
from typing import Dict, List, Any, Optional, Tuple, Union

from .utils.field_mapping import FieldMapping
from .search.engine import SearchEngine
from .providers.base import DataProvider
from .providers.hybrid_provider import HybridProvider
from .providers.csv_provider import CSVProvider
from .providers.sqlite_provider import SQLiteProvider

class UnifiedJobSearch:
    """
    Unified interface for the job search system.
    """
    
    def __init__(self, 
                 data_source: str,
                 source_type: Optional[str] = None,
                 field_mapping: Optional[FieldMapping] = None,
                 cache_dir: Optional[str] = None,
                 use_hybrid: bool = True):
        """
        Initialize the unified search interface.
        
        Args:
            data_source: Path to the data source
            source_type: Type of data source (csv, sqlite, hybrid)
            field_mapping: Field mapping configuration
            cache_dir: Directory for caching search data
            use_hybrid: Whether to use hybrid search (SQLite + vector)
        """
        self.data_source = data_source
        self.field_mapping = field_mapping or self._create_default_field_mapping()
        
        # Detect source type if not specified
        if source_type is None:
            source_type = self._detect_source_type(data_source)
        
        # Force hybrid if requested (and input is CSV)
        if use_hybrid and (source_type == 'csv' or data_source.lower().endswith('.csv')):
            source_type = 'hybrid'
        
        # Create provider
        self.provider = self._create_provider(data_source, source_type, self.field_mapping)
        
        # Create search engine
        self.search_engine = SearchEngine(
            data_provider=self.provider,
            cache_dir=cache_dir
        )
    
    def _detect_source_type(self, data_source: str) -> str:
        """Auto-detect source type based on file extension."""
        if data_source.lower().endswith('.csv'):
            return 'csv'
        elif data_source.lower().endswith(('.db', '.sqlite', '.sqlite3')):
            return 'sqlite'
        elif data_source.lower().endswith('.json'):
            return 'json'
        else:
            raise ValueError(f"Could not auto-detect source type for {data_source}")
    
    def _create_default_field_mapping(self) -> FieldMapping:
        """Create default field mapping with common field names."""
        return FieldMapping(
            id_field='job_id',
            name_field='job_name',
            status_field='status',
            timestamp_fields=['created_at', 'updated_at', 'execution_start_time', 'execution_end_time'],
            numeric_fields=['duration_minutes', 'cpu_usage_percent', 'memory_usage_mb'],
            text_fields=['description', 'error_message', 'tags']
        )
    
    def _create_provider(self, 
                        data_source: str, 
                        source_type: str,
                        field_mapping: FieldMapping) -> DataProvider:
        """
        Create appropriate data provider based on source type.
        
        Args:
            data_source: Path to the data source
            source_type: Type of data source
            field_mapping: Field mapping configuration
            
        Returns:
            DataProvider instance
        """
        if source_type.lower() == 'hybrid':
            return HybridProvider(
                data_source=data_source,
                field_mapping=field_mapping,
                vector_cache_dir=os.path.join(os.getcwd(), 'vector_cache')
            )
        elif source_type.lower() == 'csv':
            return CSVProvider(data_source, field_mapping)
        elif source_type.lower() == 'sqlite':
            return SQLiteProvider(data_source, field_mapping)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
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
        """Get a specific record by ID."""
        record = self.provider.get_record_by_id(id_value)
        if record:
            return self.provider.prepare_for_output(record)
        return None
    
    def display_results(self, results: List[Dict[str, Any]], max_width: int = 100) -> None:
        """Display search results in a readable format."""
        from .search.result_formatter import display_results
        display_results(
            results, 
            max_width=max_width,
            id_field='id',
            name_field='name',
            status_field='status'
        )
    
    def format_for_llm(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """Format search results for LLM consumption."""
        from .search.result_formatter import format_for_llm
        return format_for_llm(
            results, 
            query,
            id_field='id',
            name_field='name',
            status_field='status'
        )
    
    def explain_search(self, query: str) -> Dict[str, Any]:
        """
        Explain how a query would be processed.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with explanation details
        """
        from .query.classification import classify_query
        from .query.filters import extract_filters
        from .query.temporal import extract_temporal_filters
        
        # Extract filters
        structured_filters = extract_filters(query, self.provider.get_all_fields())
        timestamp_fields = getattr(self.provider.field_mapping, 'timestamp_fields', [])
        temporal_filters = extract_temporal_filters(query, timestamp_fields)
        combined_filters = {**structured_filters, **temporal_filters}
        
        # Classify query
        query_type = classify_query(
            query, 
            self.provider.get_all_fields(),
            combined_filters
        )
        
        # Create explanation
        explanation = {
            "query": query,
            "classification": query_type,
            "structured_filters": structured_filters,
            "temporal_filters": temporal_filters,
            "combined_filters": combined_filters,
            "field_weights": self.search_engine.field_weights,
            "provider_type": type(self.provider).__name__,
            "field_mapping": {
                "id_field": self.provider.field_mapping.id_field,
                "name_field": self.provider.field_mapping.name_field,
                "status_field": self.provider.field_mapping.status_field,
                "timestamp_fields": self.provider.field_mapping.timestamp_fields,
                "numeric_fields": self.provider.field_mapping.numeric_fields,
                "text_fields": self.provider.field_mapping.text_fields
            }
        }
        
        return explanation
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the data source."""
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
            "provider_type": type(self.provider).__name__,
            "field_mapping": {
                "id_field": self.provider.field_mapping.id_field,
                "name_field": self.provider.field_mapping.name_field,
                "status_field": self.provider.field_mapping.status_field
            }
        }