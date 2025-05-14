"""
Parser module for meta_search project.
This module handles parsing of search queries into structured objects.
"""

import re
from typing import Dict, List, Optional, Any, Union
from enum import Enum, auto

class QueryFieldType(Enum):
    """Enum defining the types of query fields."""
    TEXT = auto()
    DATE = auto()
    NUMERIC = auto()
    BOOLEAN = auto()


class QueryField:
    """Class representing a field in a search query."""
    
    def __init__(self, name: str, value: Any, field_type: QueryFieldType):
        """
        Initialize a QueryField.
        
        Args:
            name: The name of the field
            value: The value of the field
            field_type: The type of the field
        """
        self.name = name
        self.value = value
        self.field_type = field_type
    
    def __str__(self) -> str:
        return f"{self.name}:{self.value}"
    
    def __repr__(self) -> str:
        return f"QueryField(name='{self.name}', value='{self.value}', field_type={self.field_type})"


class Query:
    """Class representing a structured search query."""
    
    def __init__(self, raw_query: str, fields: List[QueryField], free_text: str = ""):
        """
        Initialize a Query.
        
        Args:
            raw_query: The original raw query string
            fields: List of structured query fields
            free_text: Unstructured text part of the query
        """
        self.raw_query = raw_query
        self.fields = fields
        self.free_text = free_text
    
    def get_field(self, name: str) -> Optional[QueryField]:
        """
        Get a field by name.
        
        Args:
            name: The field name to look for
            
        Returns:
            The field if found, None otherwise
        """
        for field in self.fields:
            if field.name == name:
                return field
        return None
    
    def get_fields(self, name: str) -> List[QueryField]:
        """
        Get all fields with a given name.
        
        Args:
            name: The field name to look for
            
        Returns:
            A list of matching fields
        """
        return [field for field in self.fields if field.name == name]
    
    def __str__(self) -> str:
        fields_str = " ".join(str(field) for field in self.fields)
        if self.free_text:
            return f"{fields_str} {self.free_text}".strip()
        return fields_str
    
    def __repr__(self) -> str:
        return f"Query(raw_query='{self.raw_query}', fields={self.fields}, free_text='{self.free_text}')"


class QueryParser:
    """Parser for search queries."""
    
    # Regex for field:value patterns
    FIELD_PATTERN = r'(\w+):(["\']([^"\']*)["\']|(\S+))'
    
    def __init__(self, field_types: Dict[str, QueryFieldType] = None):
        """
        Initialize a QueryParser.
        
        Args:
            field_types: Dictionary mapping field names to their types
        """
        self.field_types = field_types or {}
    
    def parse(self, query_string: str) -> Query:
        """
        Parse a query string into a structured Query object.
        
        Args:
            query_string: The raw query string to parse
            
        Returns:
            A Query object representing the parsed query
        """
        # Find all field:value patterns
        field_matches = re.finditer(self.FIELD_PATTERN, query_string)
        
        fields = []
        field_spans = []
        
        # Process each field:value match
        for match in field_matches:
            field_name = match.group(1)
            # Get the value, either quoted or unquoted
            field_value = match.group(3) if match.group(3) is not None else match.group(4)
            
            # Determine field type
            field_type = self.field_types.get(field_name, QueryFieldType.TEXT)
            
            # Convert value based on type
            converted_value = self._convert_value(field_value, field_type)
            
            # Create field and add to list
            field = QueryField(field_name, converted_value, field_type)
            fields.append(field)
            
            # Remember the span of this field in the original string
            field_spans.append(match.span())
        
        # Extract free text (parts not in field:value format)
        free_text = self._extract_free_text(query_string, field_spans)
        
        return Query(query_string, fields, free_text)
    
    def _convert_value(self, value: str, field_type: QueryFieldType) -> Any:
        """
        Convert a string value to the appropriate type.
        
        Args:
            value: The string value to convert
            field_type: The type to convert to
            
        Returns:
            The converted value
        """
        if field_type == QueryFieldType.TEXT:
            return value
        elif field_type == QueryFieldType.NUMERIC:
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        elif field_type == QueryFieldType.BOOLEAN:
            return value.lower() in ('true', 'yes', '1', 't', 'y')
        elif field_type == QueryFieldType.DATE:
            # Simple implementation - in a real project, would use a more robust date parser
            return value
        else:
            return value
    
    def _extract_free_text(self, query_string: str, field_spans: List[tuple]) -> str:
        """
        Extract parts of the query string that are not part of field:value pairs.
        
        Args:
            query_string: The original query string
            field_spans: List of (start, end) positions of field:value pairs
            
        Returns:
            The free text part of the query
        """
        if not field_spans:
            return query_string.strip()
        
        # Sort spans by start position
        field_spans.sort(key=lambda span: span[0])
        
        free_text_parts = []
        last_end = 0
        
        # Extract text between spans
        for start, end in field_spans:
            if start > last_end:
                part = query_string[last_end:start].strip()
                if part:
                    free_text_parts.append(part)
            last_end = end
        
        # Extract text after the last span
        if last_end < len(query_string):
            part = query_string[last_end:].strip()
            if part:
                free_text_parts.append(part)
        
        return " ".join(free_text_parts)