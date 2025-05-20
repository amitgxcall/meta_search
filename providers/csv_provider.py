"""
CSV data provider implementation.

This module provides a data provider for CSV files, handling reading,
searching, and field mapping for CSV data sources.
"""

import os
import csv
import re
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

# Import from base module
from .base import DataProvider
from ..utils.field_mapping import FieldMapping

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pre-compile frequently used regular expressions
FIELD_VALUE_PATTERN = re.compile(r'(\w+)[:=]"([^"]+)"|(\w+)[:=](\S+)')
COMPARISON_GT_PATTERN = re.compile(r'(\w+)\s*>\s*(\d+(?:\.\d+)?)')
COMPARISON_LT_PATTERN = re.compile(r'(\w+)\s*<\s*(\d+(?:\.\d+)?)')
COMPARISON_GTE_PATTERN = re.compile(r'(\w+)\s*>=\s*(\d+(?:\.\d+)?)')
COMPARISON_LTE_PATTERN = re.compile(r'(\w+)\s*<=\s*(\d+(?:\.\d+)?)')


class CSVProvider(DataProvider):
    """
    Data provider that reads from CSV files.
    
    This provider loads CSV data into memory and supports various search
    operations including text search and field-specific filtering.
    """
    
    def __init__(self, source_path: str, field_mapping: Optional[FieldMapping] = None):
        """
        Initialize the CSV provider.
        
        Args:
            source_path: Path to the CSV file
            field_mapping: Field mapping configuration
        """
        super().__init__(source_path)
        self.data = []
        self.headers = []
        self._fields_by_type = {}  # Cache for field type classification
        
        # Connect to data source
        if self.connect():
            # Set field mapping if provided, otherwise infer from data
            if field_mapping:
                self.set_field_mapping(field_mapping)
            else:
                # Try to infer field mapping from CSV headers
                inferred_mapping = FieldMapping.from_csv_headers(source_path)
                
                # Further enhance with data samples if available
                if self.data:
                    inferred_mapping.infer_field_types(self.data[0])
                
                self.set_field_mapping(inferred_mapping)
        
    def connect(self) -> bool:
        """
        Load the CSV file into memory.
        
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(self.source_path):
            logger.error(f"CSV file not found at {self.source_path}")
            return False
        
        try:
            start_time = time.time()
            
            with open(self.source_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                self.headers = reader.fieldnames or []
                
                # Use list comprehension for efficient loading
                self.data = [row for row in reader]
            
            load_time = time.time() - start_time
            logger.info(f"Successfully loaded CSV with {len(self.data)} rows and {len(self.headers)} columns in {load_time:.4f} seconds")
            
            # Pre-classify field types for better performance
            self._classify_fields()
            
            return True
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}", exc_info=True)
            return False
    
    def _classify_fields(self) -> None:
        """
        Classify fields by type for optimized searching.
        This avoids repeatedly checking field types during search.
        """
        if not self.data:
            return
            
        sample = self.data[0]
        self._fields_by_type = {
            'text': set(),
            'numeric': set(),
            'date': set(),
            'boolean': set(),
            'empty': set(),
        }
        
        for field, value in sample.items():
            if not value:
                self._fields_by_type['empty'].add(field)
                continue
                
            # Check if numeric
            try:
                float(value)
                self._fields_by_type['numeric'].add(field)
                continue
            except (ValueError, TypeError):
                pass
                
            # Check if date (simple check)
            if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                self._fields_by_type['date'].add(field)
                continue
                
            # Check if boolean
            if value.lower() in ('true', 'false', 'yes', 'no', '1', '0'):
                self._fields_by_type['boolean'].add(field)
                continue
                
            # Default to text
            self._fields_by_type['text'].add(field)
    
    def _parse_structured_query(self, query: str) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Parse a structured query and filter data accordingly.
        
        Args:
            query: The structured query string (field:value, field>value, etc.)
            
        Returns:
            Tuple of (filtered_data, applied_conditions)
        """
        # Start with all data
        filtered_data = self.data.copy()
        applied_conditions = []
        
        # Process field:value patterns
        for match in FIELD_VALUE_PATTERN.finditer(query):
            field1, value1, field2, value2 = match.groups()
            field = field1 if field1 else field2
            value = value1 if value1 else value2
            
            # Only filter if the field exists
            if field in self.headers:
                # Check if field is numeric (faster comparison)
                if field in self._fields_by_type.get('numeric', set()):
                    try:
                        num_value = float(value)
                        # Filter using list comprehension for better performance
                        filtered_data = [
                            row for row in filtered_data 
                            if field in row and row[field] and float(row[field]) == num_value
                        ]
                        applied_conditions.append(f"{field}={value}")
                    except (ValueError, TypeError):
                        # Fall back to string comparison if number conversion fails
                        filtered_data = [
                            row for row in filtered_data 
                            if field in row and str(row[field]).lower() == value.lower()
                        ]
                        applied_conditions.append(f"{field}={value}")
                else:
                    # Text comparison - case insensitive
                    filtered_data = [
                        row for row in filtered_data 
                        if field in row and str(row[field]).lower() == value.lower()
                    ]
                    applied_conditions.append(f"{field}={value}")
        
        # Process comparison patterns efficiently
        # Greater than
        for match in COMPARISON_GT_PATTERN.finditer(query):
            field, value = match.groups()
            try:
                num_value = float(value)
                filtered_data = [
                    row for row in filtered_data 
                    if field in row and row[field] and float(row[field]) > num_value
                ]
                applied_conditions.append(f"{field}>{value}")
            except (ValueError, TypeError):
                continue
        
        # Less than
        for match in COMPARISON_LT_PATTERN.finditer(query):
            field, value = match.groups()
            try:
                num_value = float(value)
                filtered_data = [
                    row for row in filtered_data 
                    if field in row and row[field] and float(row[field]) < num_value
                ]
                applied_conditions.append(f"{field}<{value}")
            except (ValueError, TypeError):
                continue
        
        # Greater than or equal
        for match in COMPARISON_GTE_PATTERN.finditer(query):
            field, value = match.groups()
            try:
                num_value = float(value)
                filtered_data = [
                    row for row in filtered_data 
                    if field in row and row[field] and float(row[field]) >= num_value
                ]
                applied_conditions.append(f"{field}>={value}")
            except (ValueError, TypeError):
                continue
        
        # Less than or equal
        for match in COMPARISON_LTE_PATTERN.finditer(query):
            field, value = match.groups()
            try:
                num_value = float(value)
                filtered_data = [
                    row for row in filtered_data 
                    if field in row and row[field] and float(row[field]) <= num_value
                ]
                applied_conditions.append(f"{field}<={value}")
            except (ValueError, TypeError):
                continue
        
        return filtered_data, applied_conditions
        
    def search(self, query: str, limit: int = 100, **kwargs) -> List[Dict[str, Any]]:
        """
        Search the CSV data using the appropriate strategy based on query type.
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            **kwargs: Additional search parameters
            
        Returns:
            List of matching items
        """
        start_time = time.time()
        results = []
        
        # Check if query is structured using pre-compiled patterns
        is_structured = bool(
            FIELD_VALUE_PATTERN.search(query) or 
            COMPARISON_GT_PATTERN.search(query) or 
            COMPARISON_LT_PATTERN.search(query) or 
            COMPARISON_GTE_PATTERN.search(query) or 
            COMPARISON_LTE_PATTERN.search(query)
        )
        
        if is_structured:
            logger.info(f"Detected structured query, using CSV structured search")
            filtered_data, applied_conditions = self._parse_structured_query(query)
            
            # Format the results
            for row in filtered_data:
                result = self.map_fields(row.copy())
                result['_score'] = 1.0  # Base score for exact matches
                result['_match_type'] = 'structured'
                result['_conditions'] = applied_conditions
                results.append(result)
            
            logger.info(f"Found {len(results)} results for structured query in {time.time() - start_time:.4f} seconds")
            
            # Return filtered results
            return results[:limit] if limit else results
        
        # Simple text search if not structured
        logger.info(f"Using simple text search for CSV")
        query_lower = query.lower()
        
        # Split into words for word-level matching - use set for faster lookups
        query_words = set(query_lower.split())
        
        # Create prioritized field weights based on field mapping
        field_weights = defaultdict(lambda: 1.0)  # Default weight is 1.0
        
        if self.field_mapping:
            # Give higher weight to key fields
            if self.field_mapping.name_field:
                field_weights[self.field_mapping.name_field] = 3.0
            if self.field_mapping.status_field:
                field_weights[self.field_mapping.status_field] = 2.0
        
        for item in self.data:
            score = 0
            matched_fields = []
            
            for field, value in item.items():
                if not value:
                    continue
                
                value_str = str(value).lower()
                field_weight = field_weights[field]
                
                # Exact match gets highest score
                if query_lower == value_str:
                    score += 10 * field_weight
                    matched_fields.append(field)
                # Partial match gets medium score
                elif query_lower in value_str:
                    score += 5 * field_weight
                    matched_fields.append(field)
                # Word match gets lowest score - use set operations for efficiency
                elif query_words.intersection(set(value_str.split())):
                    score += 1 * field_weight
                    matched_fields.append(field)
            
            if score > 0:
                result = self.map_fields(item.copy())
                result['_score'] = score
                result['_match_type'] = 'text'
                result['_matched_fields'] = matched_fields
                results.append(result)
        
        # Sort by score - define key function once for efficiency
        def get_score(item):
            return item.get('_score', 0)
            
        results.sort(key=get_score, reverse=True)
        
        search_time = time.time() - start_time
        logger.info(f"Found {len(results)} results for text search in {search_time:.4f} seconds")
        
        return results[:limit] if limit else results
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        if self.field_mapping is None:
            logger.warning("Field mapping not set. Cannot determine ID field.")
            return None
        
        id_field = self.field_mapping.id_field
        if not id_field:
            logger.warning("ID field not set in field mapping.")
            return None
        
        # Convert item_id to string for consistent comparison
        item_id_str = str(item_id)
        
        # Linear search but with early return - typically fast enough for small to medium datasets
        for item in self.data:
            if str(item.get(id_field, '')) == item_id_str:
                return self.prepare_for_output(item.copy())
        
        return None
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Get all records from the CSV.
        
        Returns:
            List of all records
        """
        # Use list comprehension for better performance
        return [self.prepare_for_output(item.copy()) for item in self.data]
    
    def get_sample_records(self, count: int = 5) -> List[Dict[str, Any]]:
        """
        Get sample records for field mapping inference.
        
        Args:
            count: Number of samples to return
            
        Returns:
            List of sample records
        """
        # Use slicing for efficiency
        samples = self.data[:min(count, len(self.data))]
        return [item.copy() for item in samples]
    
    def get_text_for_vector_search(self, record: Dict[str, Any], field_weights: Dict[str, float]) -> str:
        """
        Convert a record to text for vector search.
        
        Args:
            record: Record to convert
            field_weights: Dictionary of field weights
            
        Returns:
            Text representation of the record
        """
        # Optimization: pre-allocate list with estimated size
        estimated_size = min(20, len(record))  # Reasonable limit
        text_parts = [None] * estimated_size
        text_count = 0
        
        # Get text fields from field mapping or use cached type classification
        text_fields = set()
        if self.field_mapping and hasattr(self.field_mapping, 'text_fields'):
            text_fields = self.field_mapping.text_fields
        elif '_fields_by_type' in self.__dict__ and 'text' in self._fields_by_type:
            text_fields = self._fields_by_type['text']
        
        # Add weighted fields efficiently
        for field, value in record.items():
            if not value:
                continue
                
            # Skip non-text fields if we have field types
            if text_fields and field not in text_fields:
                continue
                
            # Get weight for this field - default to 1.0
            weight = field_weights.get(field, field_weights.get('default', 1.0))
            
            # Skip fields with zero weight
            if weight <= 0:
                continue
                
            # Format field value once
            value_str = str(value)
            formatted_text = f"{field}: {value_str}"
            
            # Add multiple times based on weight to emphasize important fields
            weight_int = int(weight)
            for _ in range(max(1, weight_int)):
                if text_count < len(text_parts):
                    text_parts[text_count] = formatted_text
                    text_count += 1
                else:
                    # Expand if needed
                    text_parts.append(formatted_text)
                    text_count += 1
        
        # Only join the parts that were actually used
        return " ".join(text_parts[:text_count])
    
    def count_by_field(self, field_name: str) -> Dict[str, int]:
        """
        Count records grouped by a field value.
        
        Args:
            field_name: Field to group by
            
        Returns:
            Dictionary mapping field values to counts
        """
        counts = defaultdict(int)
        
        for item in self.data:
            value = str(item.get(field_name, 'Unknown'))
            counts[value] += 1
        
        return dict(counts)
    
    def get_field_statistics(self, field_name: str) -> Dict[str, Any]:
        """
        Calculate statistics for a numeric field.
        
        Args:
            field_name: Field to analyze
            
        Returns:
            Dictionary with statistics (min, max, avg, etc.)
        """
        # Check if field exists
        if field_name not in self.headers:
            return {"error": f"Field '{field_name}' not found"}
        
        # Try to convert values to numeric
        values = []
        for item in self.data:
            if field_name in item and item[field_name]:
                try:
                    values.append(float(item[field_name]))
                except (ValueError, TypeError):
                    pass
        
        if not values:
            return {"error": f"No numeric values found in field '{field_name}'"}
        
        # Calculate statistics
        values_count = len(values)
        values_sum = sum(values)
        values_min = min(values)
        values_max = max(values)
        values_avg = values_sum / values_count
        
        # Calculate median
        sorted_values = sorted(values)
        mid = values_count // 2
        if values_count % 2 == 0:
            values_median = (sorted_values[mid-1] + sorted_values[mid]) / 2
        else:
            values_median = sorted_values[mid]
        
        return {
            "count": values_count,
            "min": values_min,
            "max": values_max,
            "sum": values_sum,
            "avg": values_avg,
            "median": values_median
        }