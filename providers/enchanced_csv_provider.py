"""
Enhanced CSV provider with smarter query handling.
"""

import os
import csv
from typing import List, Dict, Any, Optional, Tuple

# Import directly from local directories
import sys
from providers.base import DataProvider

class EnhancedCSVProvider(DataProvider):
    """
    Enhanced CSV provider with smarter query handling.
    """
    
    def __init__(self, source_path: str):
        """
        Initialize the enhanced CSV provider.
        
        Args:
            source_path: Path to the CSV file
        """
        super().__init__(source_path)
        self.data = []
        self.headers = []
        self.connect()
        
    def connect(self) -> bool:
        """
        Load the CSV file into memory.
        
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(self.source_path):
            print(f"Error: CSV file not found at {self.source_path}")
            return False
        
        try:
            with open(self.source_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                self.headers = reader.fieldnames or []
                self.data = list(reader)
            return True
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            return False
    
    def search(self, query: str) -> List[Dict[str, Any]]:
        """
        Search the CSV data with enhanced query handling.
        
        Args:
            query: The search query
            
        Returns:
            List of matching items
        """
        # Try to identify structured query patterns
        try:
            from search.query_patterns import QueryPatternMatcher
            
            # Get the ID field name
            id_field = None
            if self.field_mapping:
                for standard_name, source_name in self.field_mapping.get_mappings().items():
                    if standard_name == 'id':
                        id_field = source_name
                        break
            
            if id_field is None and 'id' in self.headers:
                id_field = 'id'
            elif id_field is None:
                # Try to find an ID-like field
                for header in self.headers:
                    if header.lower().endswith('id'):
                        id_field = header
                        break
            
            # Create the matcher with the appropriate ID field
            matcher = QueryPatternMatcher(id_field)
            pattern_match = matcher.match(query)
            
            if pattern_match:
                # Handle structured queries
                if pattern_match["type"] == "id_query":
                    # Direct ID lookup
                    id_value = pattern_match["id"]
                    results = []
                    
                    for item in self.data:
                        # Check for exact ID match
                        if id_field and str(item.get(id_field, '')) == id_value:
                            result = self.map_fields(item.copy())
                            result['_score'] = 100  # Give a high score for exact ID match
                            results.append(result)
                    
                    if results:
                        return results
                
                elif pattern_match["type"] == "field_value_query":
                    # Field-value lookup
                    field = pattern_match["field"]
                    value = pattern_match["value"]
                    
                    # Map standard field name to source field if needed
                    source_field = None
                    if self.field_mapping:
                        source_field = self.field_mapping.get_source_field(field)
                    
                    if not source_field:
                        # Try direct match or case-insensitive match
                        for header in self.headers:
                            if header.lower() == field.lower():
                                source_field = header
                                break
                    
                    if source_field:
                        results = []
                        for item in self.data:
                            if str(item.get(source_field, '')).lower() == value.lower():
                                result = self.map_fields(item.copy())
                                result['_score'] = 100  # High score for exact field match
                                results.append(result)
                        
                        if results:
                            return results
        except ImportError:
            print("Note: Advanced query pattern matching not available.")
        
        # Fall back to the standard search if no structured pattern matched
        # or if the structured search didn't find any results
        return self._standard_search(query)
    
    def _standard_search(self, query: str) -> List[Dict[str, Any]]:
        """
        Perform a standard search across all fields.
        
        Args:
            query: The search query
            
        Returns:
            List of matching items
        """
        query = query.lower()
        results = []
        
        # Split query into terms for better matching
        terms = query.split()
        
        for item in self.data:
            # Simple search: check if query terms are in any field
            score = 0
            term_matches = {}  # Track which terms matched
            
            for field, value in item.items():
                if not value:
                    continue
                
                value_str = str(value).lower()
                
                # Check each term
                for term in terms:
                    # Skip terms we've already matched in this field
                    if term in term_matches and field in term_matches[term]:
                        continue
                    
                    # Exact match gets higher score
                    if term == value_str:
                        score += 10
                        term_matches.setdefault(term, []).append(field)
                    # Partial match gets lower score
                    elif term in value_str:
                        score += 5
                        term_matches.setdefault(term, []).append(field)
                    # Word boundary match gets medium score
                    elif f" {term} " in f" {value_str} ":
                        score += 7
                        term_matches.setdefault(term, []).append(field)
            
            # Bonus for matching more terms
            matched_terms = len(term_matches)
            if matched_terms > 0:
                # Higher score if we match more of the query terms
                score += matched_terms * 3
                
                result = self.map_fields(item.copy())
                result['_score'] = score
                result['_matched_terms'] = list(term_matches.keys())
                results.append(result)
        
        # Sort by score
        results.sort(key=lambda x: x['_score'], reverse=True)
        return results
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        if self.field_mapping is None:
            print("Error: Field mapping not set. Cannot determine ID field.")
            return None
        
        id_field = self.field_mapping.get_source_field('id')
        if not id_field:
            print("Error: ID field not mapped in field mapping.")
            return None
        
        for item in self.data:
            if str(item.get(id_field, '')) == str(item_id):
                return self.map_fields(item.copy())
        
        return None
    
    def get_all_items(self) -> List[Dict[str, Any]]:
        """
        Get all items from the CSV.
        
        Returns:
            List of all items with fields mapped
        """
        return [self.map_fields(item.copy()) for item in self.data]