"""
Unified interface for the data-agnostic search system.

This module provides a simplified API for using the search system with
various data sources, handling the complexity of provider selection,
field mapping, and search execution.
"""

import os
import logging
import time
import json
from typing import Dict, List, Any, Optional, Union, Tuple
from functools import lru_cache

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
    logging.warning("SQLite support not available. Some functionality will be limited.")

try:
    from providers.json_provider import JSONProvider
    JSON_AVAILABLE = True
except ImportError:
    JSON_AVAILABLE = False
    logging.warning("JSON support not available. Some functionality will be limited.")

try:
    from providers.hybrid_provider import HybridProvider
    HYBRID_AVAILABLE = True
except ImportError:
    HYBRID_AVAILABLE = False
    logging.warning("Hybrid provider not available. Some functionality will be limited.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# File extension to provider type mapping for faster lookup
PROVIDER_TYPE_MAP = {
    '.csv': 'csv',
    '.xlsx': 'csv',  # Handle as CSV for now
    '.xls': 'csv',   # Handle as CSV for now
    '.tsv': 'csv',   # Handle as CSV for now
    '.db': 'sqlite',
    '.sqlite': 'sqlite',
    '.sqlite3': 'sqlite',
    '.json': 'json',
    '.jsonl': 'json'  # Handle as JSON for now
}


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
        start_time = time.time()
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
        
        logger.info(f"Using provider type: {provider_type}")
        
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
        
        # Performance metrics
        self.metrics = {
            'init_time': time.time() - start_time,
            'search_time': 0,
            'total_searches': 0
        }
    
    def _detect_provider_type(self, data_source: str) -> str:
        """
        Auto-detect provider type based on file extension.
        
        Args:
            data_source: Path to the data source
            
        Returns:
            Provider type string
        """
        # Get file extension efficiently
        file_ext = os.path.splitext(data_source)[1].lower()
        
        # Use direct lookup instead of multiple if/elif statements
        if file_ext in PROVIDER_TYPE_MAP:
            return PROVIDER_TYPE_MAP[file_ext]
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
        # Use dictionary-based factory pattern for better performance and maintainability
        provider_factories = {
            'csv': lambda: CSVProvider(data_source, field_mapping),
            'sqlite': lambda: SQLiteProvider(data_source, table_name) if SQLITE_AVAILABLE else None,
            'structured-sqlite': lambda: StructuredSQLiteProvider(data_source, table_name) if SQLITE_AVAILABLE else None,
            'json': lambda: JSONProvider(data_source) if JSON_AVAILABLE else None,
            'hybrid': lambda: HybridProvider(
                data_source=data_source,
                field_mapping=field_mapping,
                vector_weight=vector_weight,
                table_name=table_name
            ) if HYBRID_AVAILABLE else None
        }
        
        # Check for factory existence and get provider
        if provider_type in provider_factories:
            provider = provider_factories[provider_type]()
            
            # Fall back to CSV if factory returned None (provider not available)
            if provider is None:
                logger.warning(f"Provider {provider_type} not available. Falling back to CSV provider.")
                provider = CSVProvider(data_source, field_mapping)
        else:
            logger.warning(f"Unknown provider type: {provider_type}. Falling back to CSV provider.")
            provider = CSVProvider(data_source, field_mapping)
        
        # Set field mapping if needed
        if field_mapping and hasattr(provider, 'set_field_mapping') and provider != field_mapping:
            provider.set_field_mapping(field_mapping)
            
        return provider
    
    def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for records matching the query.
        
        Args:
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of search result dictionaries
        """
        start_time = time.time()
        results = self.search_engine.search(query, limit)
        
        search_time = time.time() - start_time
        self.metrics['search_time'] += search_time
        self.metrics['total_searches'] += 1
        
        logger.info(f"Search for '{query}' completed in {search_time:.4f} seconds, found {len(results) if isinstance(results, list) else 'count'} results")
        
        return results
    
    def get_record_by_id(self, id_value: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific record by ID.
        
        Args:
            id_value: ID value to look up
            
        Returns:
            Record dictionary or None if not found
        """
        start_time = time.time()
        record = self.provider.get_by_id(id_value)
        
        if record:
            logger.info(f"Found record with ID {id_value} in {time.time() - start_time:.4f} seconds")
        else:
            logger.info(f"No record found with ID {id_value}")
            
        return record
    
    def get_all_records(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get all records from the data source.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of record dictionaries
        """
        start_time = time.time()
        records = self.provider.get_all_records()
        
        # Apply limit
        if limit and len(records) > limit:
            records = records[:limit]
            
        logger.info(f"Retrieved {len(records)} records in {time.time() - start_time:.4f} seconds")
        return records
    
    def count_records(self, query: Optional[str] = None) -> Union[int, Dict[str, Any]]:
        """
        Count records, optionally filtering by query.
        
        Args:
            query: Search query to filter records (optional)
            
        Returns:
            Record count or dictionary with count details if query provided
        """
        start_time = time.time()
        
        if query:
            # Use counting query handler
            result = self.search_engine._handle_counting_query(query)
            logger.info(f"Counted records with query '{query}' in {time.time() - start_time:.4f} seconds: {result['count']} matches")
            return result
        else:
            # Get total count
            count = self.provider.get_record_count()
            logger.info(f"Total record count retrieved in {time.time() - start_time:.4f} seconds: {count} records")
            return count
    
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
    
    @lru_cache(maxsize=32)
    def format_for_llm(self, results: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """
        Format search results for LLM consumption.
        Use caching to avoid reformatting the same results repeatedly.
        
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
        start_time = time.time()
        
        if format.lower() == 'json':
            # Use json.dumps directly with list comprehension for better performance
            output = json.dumps(
                [{k: v for k, v in r.items() if not k.startswith('_')} for r in results], 
                indent=2
            )
        elif format.lower() == 'csv':
            import csv
            from io import StringIO
            
            if not results:
                return ""
            
            # Get unique fields efficiently with a set
            all_fields = set()
            for result in results:
                # Use dict keys directly - faster than iterating over items
                all_fields.update(k for k in result.keys() if not k.startswith('_'))
            
            # Sort fields for consistent output
            sorted_fields = sorted(all_fields)
            
            # Create CSV
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=sorted_fields)
            writer.writeheader()
            
            # Use dictionary comprehension for filtering fields
            for result in results:
                writer.writerow({k: v for k, v in result.items() if k in sorted_fields})
            
            output = output.getvalue()
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        logger.info(f"Exported {len(results)} results to {format} in {time.time() - start_time:.4f} seconds")
        return output
    
    def explain_search(self, query: str) -> Dict[str, Any]:
        """
        Explain how a query would be processed.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary with explanation details
        """
        start_time = time.time()
        explanation = self.search_engine.explain_search(query)
        
        logger.info(f"Explained query '{query}' in {time.time() - start_time:.4f} seconds")
        return explanation
    
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
        
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for the search system.
        
        Returns:
            Dictionary with performance metrics
        """
        metrics = {
            **self.metrics,
        }
        
        # Add engine metrics if available
        if hasattr(self.search_engine, 'get_performance_metrics'):
            metrics['engine'] = self.search_engine.get_performance_metrics()
            
        # Add provider metrics if available
        if hasattr(self.provider, 'get_performance_metrics'):
            metrics['provider'] = self.provider.get_performance_metrics()
            
        # Calculate averages
        if self.metrics['total_searches'] > 0:
            metrics['avg_search_time'] = self.metrics['search_time'] / self.metrics['total_searches']
            
        return metrics
    
    def analyze_data_source(self) -> Dict[str, Any]:
        """
        Analyze the data source to get statistics and info.
        
        Returns:
            Dictionary with data source analysis
        """
        start_time = time.time()
        
        result = {
            "data_source": self.data_source,
            "provider_type": self.provider.__class__.__name__,
            "total_records": self.provider.get_record_count()
        }
        
        # Add field information
        if self.field_mapping:
            result["field_mapping"] = {
                "id_field": self.field_mapping.id_field,
                "name_field": self.field_mapping.name_field,
                "status_field": self.field_mapping.status_field,
                "timestamp_fields_count": len(self.field_mapping.timestamp_fields),
                "numeric_fields_count": len(self.field_mapping.numeric_fields),
                "text_fields_count": len(self.field_mapping.text_fields)
            }
        
        # Get sample records if available
        if hasattr(self.provider, 'get_sample_records'):
            samples = self.provider.get_sample_records(2)
            if samples:
                # Use only public fields for the first sample
                result["sample_record"] = {
                    k: v for k, v in samples[0].items() 
                    if not k.startswith('_')
                }
        
        # Get all fields
        result["fields"] = self.provider.get_all_fields()
        
        logger.info(f"Analyzed data source in {time.time() - start_time:.4f} seconds")
        return result