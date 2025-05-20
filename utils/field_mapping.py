"""
Field mapping utilities for normalizing field names across different data sources.

This module provides functionality to map standard field names to source-specific 
field names, allowing the search system to work with different data sources that
may use different naming conventions.

Example:
    # Create field mapping
    mapping = FieldMapping()
    
    # Add mappings for standard fields
    mapping.add_mapping('id', 'item_id')
    mapping.add_mapping('name', 'title')
    
    # Get source field for a standard name
    source_field = mapping.get_source_field('id')  # Returns 'item_id'
    
    # Use in data provider
    provider.set_field_mapping(mapping)
"""

import re
import csv
import json
import os
import logging
import time
from typing import Dict, Optional, List, Set, Any, FrozenSet, Tuple
from datetime import datetime
from functools import lru_cache

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define constants for better maintenance and performance
ID_FIELD_CANDIDATES: FrozenSet[str] = frozenset([
    'id', 'uuid', 'key', 'item_id', 'product_id', 'user_id', 
    'job_id', 'customer_id', 'document_id', 'event_id'
])

NAME_FIELD_CANDIDATES: FrozenSet[str] = frozenset([
    'name', 'title', 'label', 'summary', 'description', 
    'product_name', 'full_name', 'event_name', 'job_name'
])

STATUS_FIELD_CANDIDATES: FrozenSet[str] = frozenset([
    'status', 'state', 'condition', 'type', 'inventory_status', 
    'account_status', 'severity'
])

TIMESTAMP_KEYWORDS: FrozenSet[str] = frozenset([
    'date', 'time', 'created', 'updated', 'timestamp', 
    'execution_start', 'execution_end', 'modified', 'published'
])

NUMERIC_KEYWORDS: FrozenSet[str] = frozenset([
    'count', 'number', 'amount', 'price', 'quantity', 'percent', 
    'ratio', 'duration', 'minutes', 'usage', 'mb', 'cpu'
])

TEXT_KEYWORDS: FrozenSet[str] = frozenset([
    'text', 'description', 'comment', 'note', 'summary', 'detail', 
    'message', 'error', 'name', 'title'
])


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
                 status_field: Optional[str] = None,
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
        
        # Store field type information - use sets for O(1) lookups
        self.id_field = id_field
        self.name_field = name_field
        self.status_field = status_field
        self.timestamp_fields = set(timestamp_fields or [])
        self.numeric_fields = set(numeric_fields or [])
        self.text_fields = set(text_fields or [])
        
        # Reverse mappings cache
        self._reverse_mappings: Dict[str, str] = None
        
    def add_mapping(self, standard_name: str, source_name: str) -> None:
        """
        Add a mapping from a standard field name to a source-specific field name.
        
        Args:
            standard_name: The standard field name (used throughout the system)
            source_name: The source-specific field name (used in the data source)
        """
        self.mappings[standard_name] = source_name
        
        # Invalidate reverse mappings cache
        self._reverse_mappings = None
        
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
        return self.mappings.copy()  # Return a copy to prevent modification
    
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
    
    def _get_reverse_mappings(self) -> Dict[str, str]:
        """
        Get the reverse mapping dictionary (source to standard).
        Uses caching for better performance.
        
        Returns:
            Dictionary mapping source field names to standard field names
        """
        if self._reverse_mappings is None:
            # Build reverse mapping
            self._reverse_mappings = {v: k for k, v in self.mappings.items()}
        
        return self._reverse_mappings
    
    def map_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a record from source-specific field names to standard field names.
        
        Args:
            record: Record with source-specific field names
            
        Returns:
            Record with standard field names
        """
        # Create a new dictionary for the mapped record - more efficient than modifying
        mapped_record = {}
        
        # Get reverse mapping dictionary
        reverse_mapping = self._get_reverse_mappings()
        
        # Map each field - use get() for O(1) lookup
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
        # Create a new dictionary for efficiency
        mapped_record = {}
        
        # Map each field using O(1) lookups
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
        # Use dictionary comprehension for better performance
        return {
            self.mappings.get(field_name, field_name): value 
            for field_name, value in filter_dict.items()
        }

    def get_field_type(self, field_name: str) -> str:
        """
        Get the type of a field.
        
        Args:
            field_name: Name of the field
            
        Returns:
            Field type ('id', 'name', 'status', 'timestamp', 'numeric', 'text', or 'unknown')
        """
        # Use direct comparison and set membership for O(1) lookups
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
    
    def set_primary_fields(self, id_field: str, name_field: str, status_field: Optional[str] = None):
        """
        Set primary field mappings.
        
        Args:
            id_field: Field name for the ID field in the source
            name_field: Field name for the name field in the source
            status_field: Field name for the status field in the source
        """
        self.mappings['id'] = id_field
        self.mappings['name'] = name_field
        self.id_field = id_field
        self.name_field = name_field
        
        if status_field:
            self.mappings['status'] = status_field
            self.status_field = status_field
            
        # Invalidate reverse mappings cache
        self._reverse_mappings = None
    
    @classmethod
    def from_json(cls, json_path: str) -> 'FieldMapping':
        """
        Create field mapping from a JSON configuration file.
        
        Example JSON format:
        {
            "id": "item_id",
            "name": "title",
            "status": "state",
            "timestamp_fields": ["created_at", "updated_at"],
            "numeric_fields": ["price", "quantity"],
            "text_fields": ["description", "tags"]
        }
        
        Args:
            json_path: Path to JSON configuration file
            
        Returns:
            FieldMapping instance
        """
        start_time = time.time()
        
        try:
            with open(json_path, 'r') as f:
                config = json.load(f)
            
            mapping = cls(
                id_field=config.get('id', 'id'),
                name_field=config.get('name', 'name'),
                status_field=config.get('status'),
                timestamp_fields=config.get('timestamp_fields', []),
                numeric_fields=config.get('numeric_fields', []),
                text_fields=config.get('text_fields', [])
            )
            
            # Add any additional mappings
            if 'mappings' in config and isinstance(config['mappings'], dict):
                for standard_name, source_name in config['mappings'].items():
                    if standard_name not in ['id', 'name', 'status']:
                        mapping.add_mapping(standard_name, source_name)
            
            logger.info(f"Loaded field mapping from {json_path} in {time.time() - start_time:.4f} seconds")
            return mapping
        except Exception as e:
            logger.error(f"Error loading field mapping from {json_path}: {e}")
            return cls()  # Return default mapping
    
    @classmethod
    @lru_cache(maxsize=32)
    def from_csv_headers(cls, csv_path: str) -> 'FieldMapping':
        """
        Infer field mapping from CSV headers.
        Uses caching for better performance when called multiple times with the same path.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            FieldMapping instance
        """
        start_time = time.time()
        
        try:
            with open(csv_path, 'r', newline='') as f:
                reader = csv.reader(f)
                headers = next(reader)
            
            # Infer field types from headers
            id_field = cls._find_best_match(headers, ID_FIELD_CANDIDATES)
            name_field = cls._find_best_match(headers, NAME_FIELD_CANDIDATES)
            status_field = cls._find_best_match(headers, STATUS_FIELD_CANDIDATES)
            
            # Pre-allocate lists with estimated size
            timestamp_fields = []
            numeric_fields = []
            text_fields = []
            
            # Efficiently process headers using sets for keyword lookups
            for header in headers:
                header_lower = header.lower()
                
                # Check for timestamp fields
                if any(keyword in header_lower for keyword in TIMESTAMP_KEYWORDS):
                    timestamp_fields.append(header)
                
                # Check for numeric fields
                if any(keyword in header_lower for keyword in NUMERIC_KEYWORDS):
                    numeric_fields.append(header)
                
                # Check for text fields
                if any(keyword in header_lower for keyword in TEXT_KEYWORDS):
                    text_fields.append(header)
            
            mapping = cls(
                id_field=id_field or 'id',
                name_field=name_field or 'name',
                status_field=status_field,
                timestamp_fields=timestamp_fields,
                numeric_fields=numeric_fields,
                text_fields=text_fields
            )
            
            logger.info(f"Inferred field mapping from {csv_path} in {time.time() - start_time:.4f} seconds")
            return mapping
        except Exception as e:
            logger.error(f"Error inferring field mapping from {csv_path}: {e}")
            return cls()  # Return default mapping
    
    @staticmethod
    def _find_best_match(headers: List[str], candidates: FrozenSet[str]) -> Optional[str]:
        """
        Find the best matching header from candidates.
        
        Args:
            headers: List of headers
            candidates: Set of candidate field names
            
        Returns:
            Best matching header or None if no match
        """
        # Convert headers to lowercase once for efficiency
        headers_lower = [h.lower() for h in headers]
        
        # Try exact matches first - use set intersection for O(n) performance
        header_set = set(headers)
        for candidate in candidates:
            if candidate in header_set:
                return candidate
        
        # Try case-insensitive matches
        candidate_lower_set = {c.lower() for c in candidates}
        candidate_indices = [(i, h) for i, h in enumerate(headers_lower) if h in candidate_lower_set]
        if candidate_indices:
            # Return the first match
            return headers[candidate_indices[0][0]]
        
        # Try partial matches using any() for short-circuit evaluation
        for header, header_lower in zip(headers, headers_lower):
            if any(candidate.lower() in header_lower for candidate in candidates):
                return header
        
        return None
    
    def infer_field_types(self, data_sample: Dict[str, Any]) -> None:
        """
        Infer field types from a data sample.
        
        Args:
            data_sample: Sample record to analyze
        """
        # Compile regex patterns once for better performance
        date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        
        for field, value in data_sample.items():
            # Skip None values
            if value is None:
                continue
            
            # Infer type based on value
            if isinstance(value, str):
                # Check if it looks like a timestamp
                if date_pattern.match(value):
                    self.timestamp_fields.add(field)
                else:
                    self.text_fields.add(field)
            elif isinstance(value, (int, float)):
                self.numeric_fields.add(field)
        
        # Ensure primary fields have types
        if self.name_field and self.name_field not in self.text_fields:
            self.text_fields.add(self.name_field)
        
        if self.status_field and self.status_field not in self.text_fields:
            self.text_fields.add(self.status_field)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert field mapping to dictionary.
        
        Returns:
            Dictionary representation of the field mapping
        """
        return {
            'id': self.id_field,
            'name': self.name_field,
            'status': self.status_field,
            'timestamp_fields': list(self.timestamp_fields),
            'numeric_fields': list(self.numeric_fields),
            'text_fields': list(self.text_fields),
            'mappings': self.mappings
        }
    
    def save_to_json(self, output_path: str) -> bool:
        """
        Save field mapping to JSON file.
        
        Args:
            output_path: Path to save the mapping
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            
            logger.info(f"Saved field mapping to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving field mapping to {output_path}: {e}")
            return False