"""
CSV data provider for the search system.
"""

import csv
import os
import datetime
from typing import Dict, List, Any, Optional, Tuple

from ..utils.field_mapping import FieldMapping
from .base import DataProvider

class CSVProvider(DataProvider):
    """
    Data provider for CSV files.
    """
    
    def __init__(self, 
                 file_path: str, 
                 field_mapping: Optional[FieldMapping] = None,
                 date_format: str = "%Y-%m-%d %H:%M:%S"):
        """
        Initialize CSV data provider.
        
        Args:
            file_path: Path to the CSV file
            field_mapping: Field mapping configuration
            date_format: Format string for parsing dates
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
            
        self.file_path = file_path
        self.field_mapping = field_mapping or FieldMapping()
        self.date_format = date_format
        
        # Load all records and determine field types
        self._records, self._fields = self._load_data()
    
    def _load_data(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Load data from the CSV file.
        
        Returns:
            Tuple of (records, field_names)
        """
        records = []
        field_names = []
        
        try:
            with open(self.file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                field_names = reader.fieldnames or []
                
                for row in reader:
                    processed_row = {}
                    
                    for field, value in row.items():
                        # Process field value based on type
                        processed_row[field] = self._process_field_value(field, value)
                    
                    records.append(processed_row)
        
        except Exception as e:
            raise ValueError(f"Error loading CSV file: {str(e)}")
        
        return records, field_names
    
    def _process_field_value(self, field: str, value: str) -> Any:
        """
        Process a field value based on its type.
        
        Args:
            field: Field name
            value: String value from CSV
            
        Returns:
            Processed value with appropriate type
        """
        # Skip empty fields
        if value is None or value.strip() == '':
            return None
        
        # Convert numeric fields
        if field in self.field_mapping.numeric_fields:
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        
        # Convert timestamp fields
        elif field in self.field_mapping.timestamp_fields:
            try:
                return datetime.datetime.strptime(value, self.date_format)
            except ValueError:
                return value
        
        # Keep other fields as strings
        else:
            return value
    
    def get_all_fields(self) -> List[str]:
        """Get a list of all available fields/columns in the data."""
        return self._fields
    
    def get_record_count(self) -> int:
        """Get the total number of records in the data."""
        return len(self._records)
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """Get all records from the data source."""
        return self._records
    
    def get_record_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        """Get a specific record by its ID."""
        id_field = self.field_mapping.id_field
        
        for record in self._records:
            if record.get(id_field) == id_value:
                return record
        
        return None
    
    def query_records(self, filters: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """Query records based on filters."""
        # Map generic field names to actual field names
        actual_filters = self.field_mapping.map_filter(filters)
        
        results = []
        
        for record in self._records:
            if len(results) >= limit:
                break
                
            # Check if record matches all filters
            if self._record_matches_filters(record, actual_filters):
                results.append(record)
        
        return results
    
    def _record_matches_filters(self, record: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """
        Check if a record matches the given filters.
        
        Args:
            record: Record to check
            filters: Filters to match against
            
        Returns:
            True if record matches all filters, False otherwise
        """
        for field, filter_value in filters.items():
            # Skip fields not in record
            if field not in record:
                return False
            
            record_value = record[field]
            
            # Handle missing values
            if record_value is None:
                return False
            
            # Handle different filter types
            if isinstance(filter_value, dict):
                # Operator-based filter (e.g., {"gt": 10})
                for op, op_value in filter_value.items():
                    if not self._apply_operator(op, record_value, op_value):
                        return False
            else:
                # Exact match
                if record_value != filter_value:
                    # Special case: string equality with case insensitivity
                    if (isinstance(record_value, str) and 
                        isinstance(filter_value, str) and 
                        record_value.lower() == filter_value.lower()):
                        continue
                        
                    return False
        
        return True
    
    def _apply_operator(self, op: str, record_value: Any, op_value: Any) -> bool:
        """
        Apply a comparison operator.
        
        Args:
            op: Operator name ('gt', 'lt', etc.)
            record_value: Value from the record
            op_value: Value to compare against
            
        Returns:
            Result of comparison
        """
        if op == "gt":
            return record_value > op_value
        elif op == "gte":
            return record_value >= op_value
        elif op == "lt":
            return record_value < op_value
        elif op == "lte":
            return record_value <= op_value
        elif op == "contains" and isinstance(record_value, str):
            return op_value.lower() in record_value.lower()
        else:
            return False
    
    def get_text_for_vector_search(self, record: Dict[str, Any], field_weights: Dict[str, float]) -> str:
        """Convert a record to text for vector search."""
        text_parts = []
        
        # Add weighted fields
        for field, value in record.items():
            if value is None:
                continue
                
            # Get weight for this field
            weight = field_weights.get(field, field_weights.get('default', 1.0))
            
            # Skip fields with zero weight
            if weight <= 0:
                continue
                
            # Format and add field value
            formatted_value = self._format_field_for_vector(field, value)
            
            # Add multiple times based on weight
            weight_int = int(weight)
            for _ in range(max(1, weight_int)):
                text_parts.append(formatted_value)
        
        return " ".join(text_parts)
    
    def _format_field_for_vector(self, field: str, value: Any) -> str:
        """
        Format a field value for vector search.
        
        Args:
            field: Field name
            value: Field value
            
        Returns:
            Formatted string
        """
        # Format based on type
        if isinstance(value, datetime.datetime):
            formatted = value.strftime(self.date_format)
            days_ago = (datetime.datetime.now() - value).days
            return f"{field}:{formatted} {days_ago} days ago"
        elif isinstance(value, (int, float)):
            return f"{field}:{value}"
        else:
            return f"{field}:{value}"
    
    def prepare_for_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a record for output."""
        output = {}
        
        for field, value in record.items():
            # Format datetime objects
            if isinstance(value, datetime.datetime):
                output[field] = value.strftime(self.date_format)
            # Handle None values
            elif value is None:
                output[field] = ""
            # Pass through other values
            else:
                output[field] = value
        
        # Map field names to generic names
        return self.field_mapping.reverse_map_record(output)