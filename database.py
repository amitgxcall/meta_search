"""
SQLite data provider implementation.
"""

import os
import sqlite3
from typing import List, Dict, Any, Optional

import sys
from providers.base import DataProvider

class SQLiteProvider(DataProvider):
    """
    Data provider that reads from SQLite databases.
    """
    
    def __init__(self, source_path: str, table_name: str = None):
        """
        Initialize the SQLite provider.
        
        Args:
            source_path: Path to the SQLite database file
            table_name: Name of the table to use (if None, will try to detect)
        """
        super().__init__(source_path)
        self.conn = None
        self.table_name = table_name
        self.columns = []
        
    def connect(self) -> bool:
        """
        Connect to the SQLite database.
        
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(self.source_path):
            print(f"Error: SQLite database not found at {self.source_path}")
            return False
        
        try:
            self.conn = sqlite3.connect(self.source_path)
            self.conn.row_factory = sqlite3.Row
            
            # If table name not provided, try to detect
            if self.table_name is None:
                cursor = self.conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                if not tables:
                    print("Error: No tables found in database.")
                    return False
                
                # Use first table
                self.table_name = tables[0][0]
            
            # Get column names
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({self.table_name});")
            self.columns = [row[1] for row in cursor.fetchall()]
            
            return True
        except Exception as e:
            print(f"Error connecting to SQLite database: {e}")
            return False
    
    def search(self, query: str) -> List[Dict[str, Any]]:
        """
        Search the SQLite database.
        
        Args:
            query: The search query
            
        Returns:
            List of matching items
        """
        if self.conn is None and not self.connect():
            return []
        
        results = []
        
        try:
            cursor = self.conn.cursor()
            
            # Build a query that searches across all text columns
            search_conditions = []
            params = []
            
            for col in self.columns:
                search_conditions.append(f"{col} LIKE ?")
                params.append(f"%{query}%")
            
            sql_query = f"""
                SELECT * FROM {self.table_name} 
                WHERE {' OR '.join(search_conditions)}
            """
            
            cursor.execute(sql_query, params)
            rows = cursor.fetchall()
            
            for row in rows:
                item = {col: row[col] for col in self.columns}
                mapped_item = self.map_fields(item)
                
                # Simple score based on how many columns match
                score = sum(1 for col in self.columns if query.lower() in str(row[col]).lower())
                mapped_item['_score'] = score
                
                results.append(mapped_item)
            
            # Sort by score
            results.sort(key=lambda x: x['_score'], reverse=True)
            
            return results
        except Exception as e:
            print(f"Error searching SQLite database: {e}")
            return []
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        if self.conn is None and not self.connect():
            return None
            
        if self.field_mapping is None:
            print("Error: Field mapping not set. Cannot determine ID field.")
            return None
        
        id_field = self.field_mapping.get_source_field('id')
        if not id_field:
            print("Error: ID field not mapped in field mapping.")
            return None
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM {self.table_name} WHERE {id_field} = ?", (item_id,))
            row = cursor.fetchone()
            
            if row:
                item = {col: row[col] for col in self.columns}
                return self.map_fields(item)
            
            return None
        except Exception as e:
            print(f"Error getting item by ID from SQLite database: {e}")
            return None

    def get_all_items(self) -> List[Dict[str, Any]]:
        """
        Get all items from the database.
        
        Returns:
            List of all items
        """
        if self.conn is None and not self.connect():
            return []
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM {self.table_name}")
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                item = {col: row[col] for col in self.columns}
                results.append(self.map_fields(item))
            
            return results
        except Exception as e:
            print(f"Error getting all items from SQLite database: {e}")
            return []