"""
Filters module for meta_search project.
This module provides filter classes for refining search results.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Callable
from .parser import QueryField, QueryFieldType


class Filter(ABC):
    """Abstract base class for all filters."""
    
    def __init__(self, field_name: str):
        """
        Initialize a Filter.
        
        Args:
            field_name: The name of the field to filter on
        """
        self.field_name = field_name
    
    @abstractmethod
    def apply(self, item: Dict[str, Any]) -> bool:
        """
        Apply the filter to an item.
        
        Args:
            item: The item to filter
            
        Returns:
            True if the item passes the filter, False otherwise
        """
        pass
    
    @staticmethod
    def create_from_query_field(field: QueryField) -> 'Filter':
        """
        Create a filter from a QueryField.
        
        Args:
            field: The QueryField to create a filter from
            
        Returns:
            An appropriate Filter instance
        """
        if field.field_type == QueryFieldType.TEXT:
            return TextFilter(field.name, field.value)
        elif field.field_type == QueryFieldType.NUMERIC:
            # Parse operators for numeric filters
            value = field.value
            op = "="
            if isinstance(value, str):
                for prefix in [">", "<", ">=", "<=", "="]:
                    if value.startswith(prefix):
                        op = prefix
                        value = value[len(prefix):].strip()
                        try:
                            value = float(value) if '.' in value else int(value)
                        except ValueError:
                            pass
                        break
            return NumericFilter(field.name, value, op)
        elif field.field_type == QueryFieldType.DATE:
            # Similar operator parsing for dates could be implemented
            return DateFilter(field.name, field.value)
        elif field.field_type == QueryFieldType.BOOLEAN:
            return BooleanFilter(field.name, field.value)
        else:
            # Default to text filter
            return TextFilter(field.name, field.value)


class TextFilter(Filter):
    """Filter for text fields."""
    
    def __init__(self, field_name: str, value: str, case_sensitive: bool = False):
        """
        Initialize a TextFilter.
        
        Args:
            field_name: The name of the field to filter on
            value: The value to match
            case_sensitive: Whether to do case-sensitive matching
        """
        super().__init__(field_name)
        self.value = value
        self.case_sensitive = case_sensitive
    
    def apply(self, item: Dict[str, Any]) -> bool:
        """
        Apply the filter to an item.
        
        Args:
            item: The item to filter
            
        Returns:
            True if the item passes the filter, False otherwise
        """
        if self.field_name not in item:
            return False
        
        field_value = item[self.field_name]
        if field_value is None:
            return False
        
        # Convert both to strings for comparison
        field_str = str(field_value)
        value_str = str(self.value)
        
        if not self.case_sensitive:
            field_str = field_str.lower()
            value_str = value_str.lower()
        
        return value_str in field_str


class NumericFilter(Filter):
    """Filter for numeric fields."""
    
    OPERATORS = {
        "=": lambda a, b: a == b,
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "!=": lambda a, b: a != b,
    }
    
    def __init__(self, field_name: str, value: Union[int, float], operator: str = "="):
        """
        Initialize a NumericFilter.
        
        Args:
            field_name: The name of the field to filter on
            value: The value to compare with
            operator: The comparison operator to use
        """
        super().__init__(field_name)
        self.value = value
        self.operator = operator
        
        if operator not in self.OPERATORS:
            raise ValueError(f"Invalid operator: {operator}")
        
        self.comparator = self.OPERATORS[operator]
    
    def apply(self, item: Dict[str, Any]) -> bool:
        """
        Apply the filter to an item.
        
        Args:
            item: The item to filter
            
        Returns:
            True if the item passes the filter, False otherwise
        """
        if self.field_name not in item:
            return False
        
        field_value = item[self.field_name]
        if field_value is None:
            return False
        
        # Try to convert to numeric if it's not
        if not isinstance(field_value, (int, float)):
            try:
                field_value = float(field_value) if isinstance(self.value, float) else int(field_value)
            except (ValueError, TypeError):
                return False
        
        return self.comparator(field_value, self.value)


class DateFilter(Filter):
    """Filter for date fields."""
    
    OPERATORS = {
        "=": lambda a, b: a == b,
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "!=": lambda a, b: a != b,
    }
    
    def __init__(self, field_name: str, value: Union[str, datetime], operator: str = "="):
        """
        Initialize a DateFilter.
        
        Args:
            field_name: The name of the field to filter on
            value: The date value to compare with
            operator: The comparison operator to use
        """
        super().__init__(field_name)
        
        # Convert value to datetime if it's a string
        if isinstance(value, str):
            try:
                # Try several common date formats
                formats = [
                    "%Y-%m-%d",
                    "%Y/%m/%d",
                    "%d-%m-%Y",
                    "%d/%m/%Y",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y/%m/%d %H:%M:%S",
                ]
                
                for fmt in formats:
                    try:
                        self.value = datetime.strptime(value, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    # If no format matched, store as-is
                    self.value = value
            except Exception:
                self.value = value
        else:
            self.value = value
        
        self.operator = operator
        
        if operator not in self.OPERATORS:
            raise ValueError(f"Invalid operator: {operator}")
        
        self.comparator = self.OPERATORS[operator]
    
    def apply(self, item: Dict[str, Any]) -> bool:
        """
        Apply the filter to an item.
        
        Args:
            item: The item to filter
            
        Returns:
            True if the item passes the filter, False otherwise
        """
        if self.field_name not in item:
            return False
        
        field_value = item[self.field_name]
        if field_value is None:
            return False
        
        # Convert field value to datetime if needed
        if not isinstance(field_value, datetime):
            if isinstance(field_value, str):
                try:
                    # Try several common date formats
                    formats = [
                        "%Y-%m-%d",
                        "%Y/%m/%d",
                        "%d-%m-%Y",
                        "%d/%m/%Y",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y/%m/%d %H:%M:%S",
                    ]
                    
                    for fmt in formats:
                        try:
                            field_value = datetime.strptime(field_value, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        # If no format matched, return False
                        return False
                except Exception:
                    return False
            else:
                return False
        
        # If value is not a datetime (couldn't be parsed), do string comparison
        if not isinstance(self.value, datetime):
            return str(self.value) == str(field_value)
        
        return self.comparator(field_value, self.value)


class BooleanFilter(Filter):
    """Filter for boolean fields."""
    
    def __init__(self, field_name: str, value: bool):
        """
        Initialize a BooleanFilter.
        
        Args:
            field_name: The name of the field to filter on
            value: The boolean value to match
        """
        super().__init__(field_name)
        self.value = value if isinstance(value, bool) else (value.lower() in ('true', 'yes', '1', 't', 'y'))
    
    def apply(self, item: Dict[str, Any]) -> bool:
        """
        Apply the filter to an item.
        
        Args:
            item: The item to filter
            
        Returns:
            True if the item passes the filter, False otherwise
        """
        if self.field_name not in item:
            return False
        
        field_value = item[self.field_name]
        if field_value is None:
            return False
        
        # Convert to boolean if needed
        if not isinstance(field_value, bool):
            if isinstance(field_value, str):
                field_value = field_value.lower() in ('true', 'yes', '1', 't', 'y')
            else:
                field_value = bool(field_value)
        
        return field_value == self.value


class FilterGroup:
    """A group of filters with a boolean operator."""
    
    def __init__(self, filters: List[Union[Filter, 'FilterGroup']], operator: str = "AND"):
        """
        Initialize a FilterGroup.
        
        Args:
            filters: List of filters or filter groups
            operator: Boolean operator to apply ("AND" or "OR")
        """
        self.filters = filters
        self.operator = operator.upper()
        
        if self.operator not in ("AND", "OR"):
            raise ValueError(f"Invalid operator: {operator}")
    
    def apply(self, item: Dict[str, Any]) -> bool:
        """
        Apply the filter group to an item.
        
        Args:
            item: The item to filter
            
        Returns:
            True if the item passes the filter group, False otherwise
        """
        if not self.filters:
            return True
        
        if self.operator == "AND":
            return all(f.apply(item) for f in self.filters)
        else:  # OR
            return any(f.apply(item) for f in self.filters)