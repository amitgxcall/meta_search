"""
CSV data provider implementation.

This module provides a data provider for CSV files, handling reading,
searching, and field mapping for CSV data sources.
"""

import os
import csv
import re
from typing import List, Dict, Any, Optional, Tuple
import logging

# Import from base module
from .base import DataProvider
from ..utils.field_mapping import FieldMapping

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
            with open(self.source_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                self.headers = reader.fieldnames or []
                self.data = list(reader)
            
            logger.info(f"Successfully loaded CSV with {len(self.data)} rows and {len(self.headers)} columns")
            return True
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            return False
    
    def _parse_structured_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Parse a structured query and filter data accordingly.
        
        Args:
            query: The structured query string (field:value, field>value, etc.)
            
        Returns:
            List of matching items
        """
        results = []
        
        # Start with all data
        filtered_data = self.data.copy()
        
        # Split the query into tokens, preserving quoted strings
        parts = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")++', query)
        
        for part in parts:
            if ":" in part:
                # Field:value pattern
                field, value = part.split(":", 1)
                value = value.strip('"')
                
                # Filter data to only include rows where field matches value
                filtered_data = [
                    item for item in filtered_data 
                    if field in item and str(item[field]).lower() == value.lower()
                ]
            
            elif ">" in part and not ">=" in part:
                # Greater than
                field, value = part.split(">", 1)
                value = value.strip('"')
                
                # Try to convert to number if possible
                try:
                    num_value = float(value)
                    # Filter data
                    filtered_data = [
                        item for item in filtered_data 
                        if field in item and item[field] and float(item[field]) > num_value
                    ]
                except (ValueError, TypeError):
                    # Skip this condition if conversion fails
                    continue
            
            elif "<" in part and not "<=" in part:
                # Less than
                field, value = part.split("<", 1)
                value = value.strip('"')
                
                # Try to convert to number if possible
                try:
                    num_value = float(value)
                    # Filter data
                    filtered_data = [
                        item for item in filtered_data 
                        if field in item and item[field] and float(item[field]) < num_value
                    ]
                except (ValueError, TypeError):
                    # Skip this condition if conversion fails
                    continue
            
            elif ">=" in part:
                # Greater than or equal
                field, value = part.split(">=", 1)
                value = value.strip('"')
                
                # Try to convert to number if possible
                try:
                    num_value = float(value)
                    # Filter data
                    filtered_data = [
                        item for item in filtered_data 
                        if field in item and item[field] and float(item[field]) >= num_value
                    ]
                except (ValueError, TypeError):
                    # Skip this condition if conversion fails
                    continue
            
            elif "<=" in part:
                # Less than or equal
                field, value = part.split("<=", 1)
                value = value.strip('"')
                
                # Try to convert to number if possible
                try:
                    num_value = float(value)
                    # Filter data
                    filtered_data = [
                        item for item in filtered_data 
                        if field in item and item[field] and float(item[field]) <= num_value
                    ]
                except (ValueError, TypeError):
                    # Skip this condition if conversion fails
                    continue
        
        # Map fields for all matching items
        for item in filtered_data:
            mapped_item = self.map_fields(item.copy())
            mapped_item['_score'] = 1.0  # Base score for exact matches
            mapped_item['_match_type'] = 'structured'
            results.append(mapped_item)
        
        return results
        
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
        results = []
        
        # Check if query is structured (contains :, >, <, etc.)
        if ":" in query or ">" in query or "<" in query or ">=" in query or "<=" in query:
            logger.info(f"Detected structured query, using CSV structured search")
            results = self._parse_structured_query(query)
            
            # Return filtered results
            return results[:limit] if limit else results
        
        # Simple text search if not structured
        logger.info(f"Using simple text search for CSV")
        query = query.lower()
        text_results = []
        
        for item in self.data:
            # Simple search: check if query is in any field
            score = 0
            for field, value in item.items():
                if not value:
                    continue
                
                value_str = str(value).lower()
                
                # Exact match gets higher score
                if query == value_str:
                    score += 10
                # Partial match gets lower score
                elif query in value_str:
                    score += 5
                # Word match gets even lower score
                elif any(word in value_str for word in query.split()):
                    score += 1
            
            if score > 0:
                result = self.map_fields(item.copy())
                result['_score'] = score
                result['_match_type'] = 'text'
                text_results.append(result)
        
        # Sort by score
        text_results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        return text_results[:limit] if limit else text_results
    
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
        
        for item in self.data:
            if str(item.get(id_field, '')) == str(item_id):
                return self.prepare_for_output(item.copy())
        
        return None
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Get all records from the CSV.
        
        Returns:
            List of all records
        """
        return [self.prepare_for_output(item.copy()) for item in self.data]
    
    def get_sample_records(self, count: int = 5) -> List[Dict[str, Any]]:
        """
        Get sample records for field mapping inference.
        
        Args:
            count: Number of samples to return
            
        Returns:
            List of sample records
        """
        return [item.copy() for item in self.data[:min(count, len(self.data))]]
    
    def get_text_for_vector_search(self, record: Dict[str, Any], field_weights: Dict[str, float]) -> str:
        """
        Convert a record to text for vector search.
        
        Args:
            record: Record to convert
            field_weights: Dictionary of field weights
            
        Returns:
            Text representation of the record
        """
        text_parts = []
        
        # Get text fields from field mapping or use all string fields
        text_fields = self.field_mapping.text_fields if self.field_mapping else []
        
        # If no text fields defined, use all string fields
        if not text_fields:
            for field, value in record.items():
                if isinstance(value, str) and len(value) > 3:
                    text_fields.add(field)
        
        # Add weighted fields
        for field, value in record.items():
            if not value:
                continue
                
            # Skip non-text fields if we have field types
            if text_fields and field not in text_fields:
                continue
                
            # Get weight for this field
            weight = field_weights.get(field, field_weights.get('default', 1.0))
            
            # Skip fields with zero weight
            if weight <= 0:
                continue
                
            # Format field value
            value_str = str(value)
            
            # Add field name and value
            formatted_text = f"{field}: {value_str}"
            
            # Add multiple times based on weight to emphasize important fields
            weight_int = int(weight)
            for _ in range(max(1, weight_int)):
                text_parts.append(formatted_text)
        
        return " ".join(text_parts)