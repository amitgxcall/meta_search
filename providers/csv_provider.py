"""
CSV data provider implementation.
"""

import os
import csv
from typing import List, Dict, Any, Optional

# Direct import instead of relative import
import sys
from providers.base import DataProvider

class CSVProvider(DataProvider):
    """
    Data provider that reads from CSV files.
    """
    
    def __init__(self, source_path: str):
        """
        Initialize the CSV provider.
        
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
        Search the CSV data.
        
        Args:
            query: The search query
            
        Returns:
            List of matching items
        """
        query = query.lower()
        results = []
        
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