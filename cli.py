#!/usr/bin/env python3
"""
Standalone CLI for the meta_search system.

This script has no package dependencies and can be run directly.
It contains all the necessary functionality to search data sources.

Usage:
    python standalone_cli.py --data-source data/job_details.csv --id-field job_id --name-field job_name --query "failed jobs"
"""

import os
import sys
import argparse
import logging
import re
import json
import csv
import sqlite3
from typing import List, Dict, Any, Optional, Tuple, Union
import hashlib
import pickle
import numpy as np
import datetime


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#################################################
# Field Mapping Implementation
#################################################

class FieldMapping:
    """
    Class for mapping standard field names to source-specific field names.
    """
    
    def __init__(self):
        """Initialize an empty field mapping."""
        self.mappings = {}  # standard_name -> source_name
        
    def add_mapping(self, standard_name: str, source_name: str) -> None:
        """
        Add a mapping from a standard field name to a source-specific field name.
        
        Args:
            standard_name: The standard field name
            source_name: The source-specific field name
        """
        self.mappings[standard_name] = source_name
        
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
        Get all mappings.
        
        Returns:
            Dictionary mapping standard names to source-specific names
        """
        return self.mappings

#################################################
# DataProvider Base Class
#################################################

class DataProvider:
    """
    Abstract base class for all data providers.
    """
    
    def __init__(self, source_path: str):
        """
        Initialize the data provider.
        
        Args:
            source_path: Path to the data source
        """
        self.source_path = source_path
        self.field_mapping = None
        
    def set_field_mapping(self, field_mapping: FieldMapping) -> None:
        """
        Set the field mapping for this provider.
        
        Args:
            field_mapping: FieldMapping object that maps standard field names to source-specific names
        """
        self.field_mapping = field_mapping
        
    def connect(self) -> bool:
        """
        Connect to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        # Override in child classes
        return False
    
    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Search the data source.
        
        Args:
            query: The search query
            **kwargs: Additional search parameters
            
        Returns:
            List of search results
        """
        # Override in child classes
        return []
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        # Override in child classes
        return None
    
    def map_fields(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map fields from source-specific names to standard names.
        
        Args:
            item: The item with source-specific field names
            
        Returns:
            The item with standard field names
        """
        if self.field_mapping is None:
            return item
        
        mapped_item = {}
        
        # Copy unmapped fields as-is
        for key, value in item.items():
            mapped_item[key] = value
        
        # Apply field mappings
        for standard_name, source_name in self.field_mapping.get_mappings().items():
            if source_name in item:
                mapped_item[standard_name] = item[source_name]
                
        return mapped_item

#################################################
# CSV Provider Implementation
#################################################

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
        self.id_field = None
        self.name_field = None
        self.vector_engine = None
        self.vector_index_built = False
        self.vector_index_path = source_path + '.vector'
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
            print(f"Successfully loaded CSV with {len(self.data)} rows and {len(self.headers)} columns")
            
            # Initialize vector search engine
            self.vector_engine = VectorSearchEngine()
            
            # Try to load existing vector index
            if os.path.exists(self.vector_index_path):
                self.vector_index_built = self.vector_engine.load_index(self.vector_index_path)
                if self.vector_index_built:
                    print(f"Loaded vector index from {self.vector_index_path}")
            
            return True
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            return False
    
    def build_vector_index(self) -> bool:
        """
        Build vector index for the CSV data.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.data:
            print("No CSV data to build vector index from")
            return False
        
        print("Building vector index for CSV data...")
        
        # Initialize vector engine if not already done
        if not self.vector_engine:
            self.vector_engine = VectorSearchEngine()
        
        # Determine text fields for embedding
        text_fields = []
        for field in self.headers:
            # Check if field contains text values (sample first 5 rows)
            sample_size = min(5, len(self.data))
            for i in range(sample_size):
                if i < len(self.data) and field in self.data[i]:
                    value = self.data[i][field]
                    if isinstance(value, str) and len(value) > 5:
                        text_fields.append(field)
                        break
        
        if not text_fields:
            print("No suitable text fields found for vector indexing")
            # Use all fields as fallback
            text_fields = self.headers
        
        print(f"Using fields for vectors: {', '.join(text_fields)}")
        
        # Build vectors for each item
        count = 0
        for item in self.data:
            # Generate ID or use existing ID
            if self.id_field and self.id_field in item:
                item_id = str(item[self.id_field])
            else:
                item_id = str(count)
            
            # Combine text fields
            text_values = []
            for field in text_fields:
                if field in item and item[field]:
                    text_values.append(str(item[field]))
            
            text_content = " ".join(text_values)
            
            # Skip if no text
            if not text_content:
                continue
            
            # Generate embedding
            embedding = VectorSearchEngine.get_mock_embedding(text_content)
            
            # Add to vector index
            self.vector_engine.add_item(item_id, item, embedding)
            count += 1
        
        print(f"Added {count} items to vector index")
        
        # Save vector index
        if self.vector_engine.save_index(self.vector_index_path):
            self.vector_index_built = True
            print(f"Vector index saved to {self.vector_index_path}")
            return True
        else:
            print("Failed to save vector index")
            return False
    
    def _parse_structured_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Parse a structured query and filter data accordingly.
        
        Args:
            query: The structured query string
            
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
    
    def search_vector(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Perform vector similarity search.
        
        Args:
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of search results with similarity scores
        """
        # Build vector index if not already built
        if not self.vector_index_built:
            print("Vector index not found, building now...")
            if not self.build_vector_index():
                print("Could not build vector index")
                return []
        
        # Generate embedding for query
        query_embedding = VectorSearchEngine.get_mock_embedding(query)
        
        # Perform similarity search
        vector_results = self.vector_engine.search(query_embedding, limit)
        
        # Format results
        results = []
        for item_id, similarity, item_data in vector_results:
            result = self.map_fields(item_data.copy())
            result['_score'] = similarity
            result['_match_type'] = 'vector'
            results.append(result)
        
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
        
        # Check if query is structured
        if ":" in query or ">" in query or "<" in query or ">=" in query or "<=" in query:
            print(f"Detected structured query, using CSV structured search")
            results = self._parse_structured_query(query)
            
            # If no results from structured search, try vector search
            if not results:
                print("No structured matches found, trying vector similarity search...")
                results = self.search_vector(query, limit)
                
            return results[:limit] if limit else results
        
        # Try simple text search first
        print(f"Using simple text search for CSV")
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
        
        # If text search found results, return them
        if text_results:
            return text_results[:limit] if limit else text_results
        
        # Fall back to vector similarity search if no text matches
        print("No text matches found, trying vector similarity search...")
        vector_results = self.search_vector(query, limit)
        
        # Combine results (in this case, just vector results since text search found nothing)
        results = vector_results
        
        # Apply limit
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

#################################################
# SQLite Provider Implementation
#################################################

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
            # Just log the error but don't print it repeatedly
            if not hasattr(self, '_already_logged_missing_file'):
                print(f"Error: SQLite database not found at {self.source_path}")
                self._already_logged_missing_file = True
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
    
    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
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
            results.sort(key=lambda x: x.get('_score', 0), reverse=True)
            
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

#################################################
# Structured SQLite Provider Implementation
#################################################

class StructuredSQLiteProvider(SQLiteProvider):
    """
    SQLite provider with multi-strategy search capabilities for structured queries.
    """
    
    def __init__(self, source_path: str, table_name: str = None, id_field: str = "id", name_field: str = "name"):
        """
        Initialize the structured SQLite provider.
        
        Args:
            source_path: Path to the SQLite database file
            table_name: Name of the table to use (if None, will try to detect)
            id_field: Name of the ID field
            name_field: Name of the name/title field
        """
        super().__init__(source_path, table_name)
        self.id_field = id_field
        self.name_field = name_field
        self.text_fields = None  # Will be set after connection
        self.column_types = {}
        
        # Connect and get column types
        if self.connect():
            self._get_column_types()
            self._set_text_fields()
    
    def _get_column_types(self) -> None:
        """
        Get and cache column types from the database.
        """
        if not self.conn:
            return
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns = cursor.fetchall()
            
            for column in columns:
                # Column structure: (cid, name, type, notnull, dflt_value, pk)
                self.column_types[column[1]] = column[2].upper()
        except Exception as e:
            print(f"Error getting column types: {e}")
    
    def _set_text_fields(self) -> None:
        """
        Identify text fields for searching.
        """
        self.text_fields = []
        
        for col, col_type in self.column_types.items():
            if col_type in ('TEXT', 'VARCHAR', 'CHAR', 'CLOB'):
                self.text_fields.append(col)
        
        # If no text fields found, use all columns
        if not self.text_fields:
            self.text_fields = self.columns
        
        # Always include name field if it exists
        if self.name_field not in self.text_fields and self.name_field in self.columns:
            self.text_fields.append(self.name_field)
        
        print(f"Using text fields for search: {', '.join(self.text_fields)}")
    
    def _is_numeric_column(self, column_name: str) -> bool:
        """
        Check if a column is numeric.
        
        Args:
            column_name: Column name to check
            
        Returns:
            True if the column is numeric, False otherwise
        """
        column_type = self.column_types.get(column_name, 'TEXT')
        return column_type in ('INTEGER', 'REAL', 'NUMERIC', 'FLOAT', 'DOUBLE')
    
    def _parse_structured_query(self, query: str) -> Tuple[List[str], List[Any]]:
        """
        Parse a structured query into SQL conditions and parameters.
        
        Args:
            query: The structured query string
            
        Returns:
            Tuple of (conditions, parameters)
        """
        conditions = []
        params = []
        
        # Split the query into tokens, preserving quoted strings
        parts = re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")++', query)
        
        for part in parts:
            if ":" in part:
                # Field:value pattern
                field, value = part.split(":", 1)
                value = value.strip('"')
                
                # Handle numeric values differently
                if self._is_numeric_column(field) and value.replace('.', '', 1).isdigit():
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        pass  # Keep as string if conversion fails
                
                conditions.append(f"{field} = ?")
                params.append(value)
            
            elif ">" in part and not ">=" in part:
                # Greater than
                field, value = part.split(">", 1)
                value = value.strip('"')
                
                if self._is_numeric_column(field):
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        continue  # Skip if not a valid number
                    
                    conditions.append(f"{field} > ?")
                    params.append(value)
            
            elif "<" in part and not "<=" in part:
                # Less than
                field, value = part.split("<", 1)
                value = value.strip('"')
                
                if self._is_numeric_column(field):
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        continue  # Skip if not a valid number
                    
                    conditions.append(f"{field} < ?")
                    params.append(value)
            
            elif ">=" in part:
                # Greater than or equal
                field, value = part.split(">=", 1)
                value = value.strip('"')
                
                if self._is_numeric_column(field):
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        continue  # Skip if not a valid number
                    
                    conditions.append(f"{field} >= ?")
                    params.append(value)
            
            elif "<=" in part:
                # Less than or equal
                field, value = part.split("<=", 1)
                value = value.strip('"')
                
                if self._is_numeric_column(field):
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        continue  # Skip if not a valid number
                    
                    conditions.append(f"{field} <= ?")
                    params.append(value)
        
        return conditions, params
    
    def search_structured(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search using structured query syntax (field:value, field>value, etc.).
        
        Args:
            query: The structured query string
            limit: Maximum number of results to return
            
        Returns:
            List of matching results
        """
        if self.conn is None and not self.connect():
            return []
        
        results = []
        
        try:
            # Parse the structured query
            conditions, params = self._parse_structured_query(query)
            
            if not conditions:
                return []  # No valid conditions found
            
            # Build and execute the SQL query
            where_clause = " AND ".join(conditions)
            sql = f"SELECT * FROM {self.table_name} WHERE {where_clause} LIMIT {limit}"
            
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            # Process results
            for row in rows:
                item = {col: row[col] for col in self.columns}
                result = self.map_fields(item)
                result['_score'] = 1.0  # Base score for structured matches
                result['_match_type'] = 'structured'
                results.append(result)
            
            return results
            
        except Exception as e:
            print(f"Error in structured search: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def search_exact_phrase(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search for exact phrases (2+ consecutive words).
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of matching results
        """
        if self.conn is None and not self.connect():
            return []
        
        # Need at least 2 words for exact phrase matching
        words = query.strip().split()
        if len(words) < 2:
            return []
        
        results = []
        
        try:
            # Build phrase conditions
            conditions = []
            params = []
            
            phrase = query.strip()
            
            for field in self.text_fields:
                conditions.append(f"{field} LIKE ?")
                params.append(f"%{phrase}%")
            
            if not conditions:
                return []
            
            # Build and execute the SQL query
            where_clause = " OR ".join(conditions)
            sql = f"SELECT * FROM {self.table_name} WHERE {where_clause} LIMIT {limit}"
            
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            # Process results
            for row in rows:
                item = {col: row[col] for col in self.columns}
                result = self.map_fields(item)
                result['_score'] = 0.9  # Slightly lower than structured
                result['_match_type'] = 'exact_phrase'
                results.append(result)
            
            return results
            
        except Exception as e:
            print(f"Error in exact phrase search: {e}")
            return []
    
    def search_contains_words(self, query: str, min_words: int = 2, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search for records containing at least min_words words from the query.
        
        Args:
            query: The search query
            min_words: Minimum number of words that must match
            limit: Maximum number of results to return
            
        Returns:
            List of matching results
        """
        if self.conn is None and not self.connect():
            return []
        
        # Filter out very short words
        words = [w for w in query.strip().split() if len(w) > 2]
        if len(words) < min_words:
            return []
        
        results = []
        
        try:
            # Build word conditions for each text field
            field_conditions = []
            params = []
            
            for field in self.text_fields:
                word_conditions = []
                field_params = []
                
                for word in words:
                    word_conditions.append(f"{field} LIKE ?")
                    field_params.append(f"%{word}%")
                
                if word_conditions:
                    field_conditions.append("(" + " OR ".join(word_conditions) + ")")
                    params.extend(field_params)
            
            if not field_conditions:
                return []
            
            # For SQLite, we need to count matches in application code
            # First, get all potential matches
            where_clause = " OR ".join(field_conditions)
            sql = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
            
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            # Filter and score based on word match count
            for row in rows:
                item = {col: row[col] for col in self.columns}
                
                # Count how many of the words are found in any text field
                match_count = 0
                for field in self.text_fields:
                    field_value = str(row[field]).lower()
                    for word in words:
                        if word.lower() in field_value:
                            match_count += 1
                
                # Only include if meets minimum word count
                if match_count >= min_words:
                    result = self.map_fields(item)
                    # Score based on percentage of words matched
                    result['_score'] = 0.5 + 0.3 * (match_count / len(words))
                    result['_match_type'] = 'contains'
                    result['_match_count'] = match_count
                    results.append(result)
            
            # Sort by match count and limit results
            results.sort(key=lambda x: x.get('_match_count', 0), reverse=True)
            return results[:limit]
            
        except Exception as e:
            print(f"Error in contains word search: {e}")
            return []
    
    def search_basic(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fallback to basic search when other strategies fail.
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of matching results
        """
        if self.conn is None and not self.connect():
            return []
        
        results = []
        
        try:
            # Simple LIKE search on the name field
            if self.name_field in self.columns:
                sql = f"SELECT * FROM {self.table_name} WHERE {self.name_field} LIKE ? LIMIT {limit}"
                
                cursor = self.conn.cursor()
                cursor.execute(sql, (f"%{query}%",))
                rows = cursor.fetchall()
                
                for row in rows:
                    item = {col: row[col] for col in self.columns}
                    result = self.map_fields(item)
                    result['_score'] = 0.4  # Lowest priority match
                    result['_match_type'] = 'basic'
                    results.append(result)
            
            return results
            
        except Exception as e:
            print(f"Error in basic search: {e}")
            return []
    
    def detect_query_type(self, query: str) -> str:
        """
        Detect the type of query to determine search strategy.
        
        Args:
            query: The search query
            
        Returns:
            Query type string
        """
        # Check for structured query patterns (field:value, field>value, etc.)
        if ":" in query or ">" in query or "<" in query or ">=" in query or "<=" in query:
            return "structured"
        
        # Check for exact phrase (multiple words)
        if len(query.strip().split()) > 1:
            return "exact_phrase"
        
        # Single word query
        return "basic"
    
    def search(self, query: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Multi-strategy search with fallbacks.
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of matching results
        """
        # Detect query type and use appropriate strategy
        query_type = self.detect_query_type(query)
        print(f"Detected query type: {query_type}")
        
        # Try structured query first if appropriate
        if query_type == "structured":
            results = self.search_structured(query, limit)
            if results:
                return results
        
        # Try exact phrase matching next
        if query_type in ["structured", "exact_phrase"]:
            results = self.search_exact_phrase(query, limit)
            if results:
                return results
        
        # Try contains matching (minimum 2 words)
        if len(query.strip().split()) >= 2:
            results = self.search_contains_words(query, 2, limit)
            if results:
                return results
        
        # Fall back to basic search
        return self.search_basic(query, limit)

#################################################
# Vector Search Implementation
#################################################

class VectorSearchEngine:
    """
    Vector-based search implementation.
    """
    
    def __init__(self, embedding_dim: int = 768):
        """
        Initialize the vector search engine.
        
        Args:
            embedding_dim: Dimension of the embedding vectors
        """
        self.embedding_dim = embedding_dim
        self.index = {}  # id -> embedding
        self.id_to_data = {}  # id -> original data
    
    def add_item(self, item_id: str, item_data: Dict[str, Any], embedding: List[float]) -> None:
        """
        Add an item to the vector index.
        
        Args:
            item_id: Unique identifier for the item
            item_data: Original item data
            embedding: Vector embedding for the item
        """
        # Convert to numpy array for efficient operations
        embedding_array = np.array(embedding, dtype=np.float32)
        
        # Normalize the vector for cosine similarity
        norm = np.linalg.norm(embedding_array)
        if norm > 0:
            embedding_array = embedding_array / norm
        
        self.index[item_id] = embedding_array
        self.id_to_data[item_id] = item_data
    
    def search(self, query_embedding: List[float], limit: int = 10) -> List[Tuple[str, float, Dict[str, Any]]]:
        """
        Search for similar items.
        
        Args:
            query_embedding: Vector embedding for the query
            limit: Maximum number of results to return
            
        Returns:
            List of tuples (item_id, similarity_score, item_data)
        """
        if not self.index:
            return []
        
        # Convert to numpy array and normalize
        query_array = np.array(query_embedding, dtype=np.float32)
        norm = np.linalg.norm(query_array)
        if norm > 0:
            query_array = query_array / norm
        
        # Calculate similarities
        results = []
        for item_id, item_embedding in self.index.items():
            # Cosine similarity is just the dot product of normalized vectors
            similarity = float(np.dot(query_array, item_embedding))
            results.append((item_id, similarity, self.id_to_data[item_id]))
        
        # Sort by similarity (descending)
        results.sort(key=lambda x: x[1], reverse=True)
        
        return results[:limit]
    
    def save_index(self, file_path: str) -> bool:
        """
        Save the vector index to disk.
        
        Args:
            file_path: Path to save the index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            data = {
                "embedding_dim": self.embedding_dim,
                "index": {k: v.tolist() for k, v in self.index.items()},
                "id_to_data": self.id_to_data
            }
            
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            
            return True
        except Exception as e:
            print(f"Error saving index: {e}")
            return False
    
    def load_index(self, file_path: str) -> bool:
        """
        Load the vector index from disk.
        
        Args:
            file_path: Path to load the index from
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(file_path):
            return False
        
        try:
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            
            self.embedding_dim = data["embedding_dim"]
            self.index = {k: np.array(v, dtype=np.float32) for k, v in data["index"].items()}
            self.id_to_data = data["id_to_data"]
            
            return True
        except Exception as e:
            print(f"Error loading index: {e}")
            return False

    @staticmethod
    def get_mock_embedding(text: str, dim: int = 768) -> List[float]:
        """
        Generate a mock embedding for text.
        In a real implementation, you would use a proper embedding model.
        
        Args:
            text: Text to generate an embedding for
            dim: Dimension of the embedding
            
        Returns:
            Vector embedding
        """
        # This is just a simple deterministic mock implementation
        # In a real system, you'd use a proper embedding model
        
        # Get a deterministic hash of the text
        hash_obj = hashlib.md5(text.encode())
        hash_bytes = hash_obj.digest()
        
        # Use the hash to seed a random number generator
        rng = np.random.RandomState(int.from_bytes(hash_bytes[:4], byteorder='little'))
        
        # Generate a random vector
        embedding = rng.randn(dim).astype(np.float32)
        
        # Normalize to unit length
        embedding = embedding / np.linalg.norm(embedding)
        
        return embedding.tolist()

#################################################
# Hybrid Provider Implementation
#################################################

class HybridProvider(DataProvider):
    """
    Hybrid data provider that combines structured data with vector search.
    """
    
    def __init__(self, source_path: str, vector_index_path: Optional[str] = None, table_name: Optional[str] = None):
        """
        Initialize the hybrid provider.
        
        Args:
            source_path: Path to the data source file
            vector_index_path: Path to the vector index file (if None, uses source_path + '.vector')
            table_name: Name of the table to use (for SQLite provider)
        """
        super().__init__(source_path)
        
        # Set vector index path
        if vector_index_path is None:
            self.vector_index_path = source_path + '.vector'
        else:
            self.vector_index_path = vector_index_path
        
        # Determine provider type based on file extension
        self.file_ext = os.path.splitext(source_path)[1].lower()
        
        # Initialize appropriate provider
        if self.file_ext == '.csv':
            print(f"Using CSV provider for {source_path}")
            self.data_provider = CSVProvider(source_path)
        elif self.file_ext in ['.db', '.sqlite', '.sqlite3']:
            print(f"Using SQLite provider for {source_path}")
            self.data_provider = StructuredSQLiteProvider(source_path, table_name)  # Use structured provider
        else:
            print(f"Unknown file type: {self.file_ext}. Defaulting to CSV provider.")
            self.data_provider = CSVProvider(source_path)
        
        # Initialize vector search
        self.vector_search = VectorSearchEngine()
        
        # Keep track of whether the vector index is built
        self.vector_index_built = False
        
        # Connect to data sources
        self.connect()
    
    def connect(self) -> bool:
        """
        Connect to the data sources.
        
        Returns:
            True if successful, False otherwise
        """
        # Connect to data provider
        if not self.data_provider.connect():
            return False
        
        # Try to load existing vector index
        if os.path.exists(self.vector_index_path):
            self.vector_index_built = self.vector_search.load_index(self.vector_index_path)
        
        return True
    
    def set_field_mapping(self, field_mapping: FieldMapping) -> None:
        """
        Set the field mapping for this provider.
        
        Args:
            field_mapping: FieldMapping object
        """
        super().set_field_mapping(field_mapping)
        self.data_provider.set_field_mapping(field_mapping)
    
    def build_vector_index(self) -> bool:
        """
        Build the vector index from scratch.
        
        Returns:
            True if successful, False otherwise
        """
        print("Building vector index...")
        
        # Get all items from data provider
        items = self._get_all_items_from_provider()
        
        if not items:
            print("No items found in data source.")
            return False
        
        # Infer text fields from first item
        text_fields = self._infer_text_fields(items[0])
        
        # Get ID field
        id_field = self._get_id_field()
        
        # Build vector index
        for item in items:
            if id_field not in item:
                print(f"Warning: Item missing ID field '{id_field}': {item}")
                continue
            
            item_id = str(item[id_field])
            
            # Combine text fields for embedding
            text = self._combine_text_fields(item, text_fields)
            
            # Skip items with no text
            if not text:
                continue
            
            # Generate embedding
            embedding = VectorSearchEngine.get_mock_embedding(text)
            
            # Add to vector index
            self.vector_search.add_item(item_id, item, embedding)
        
        print(f"Added {len(self.vector_search.index)} items to vector index")
        
        # Save vector index
        if self.vector_search.save_index(self.vector_index_path):
            self.vector_index_built = True
            print(f"Vector index built with {len(self.vector_search.index)} items and saved to {self.vector_index_path}")
            return True
        else:
            print("Failed to save vector index.")
            return False
    
    def _get_all_items_from_provider(self) -> List[Dict[str, Any]]:
        """
        Get all items from the data provider.
        
        Returns:
            List of all items
        """
        if hasattr(self.data_provider, 'get_all_items'):
            return self.data_provider.get_all_items()
        
        # Fallback to empty list
        return []
    
    def _infer_text_fields(self, item: Dict[str, Any]) -> List[str]:
        """
        Infer which fields are text fields based on the first item.
        
        Args:
            item: First item from the data source
            
        Returns:
            List of text field names
        """
        text_fields = []
        for key, value in item.items():
            if isinstance(value, str) and len(value) > 5:
                text_fields.append(key)
        
        print(f"Using text fields for embeddings: {', '.join(text_fields)}")
        return text_fields
    
    def _get_id_field(self) -> str:
        """
        Get the ID field name.
        
        Returns:
            ID field name
        """
        id_field = 'id'
        if self.field_mapping is not None:
            # Look for mapped 'id' field
            for standard_name, source_name in self.field_mapping.get_mappings().items():
                if standard_name == 'id':
                    id_field = source_name
                    break
        
        return id_field
    
    def _combine_text_fields(self, item: Dict[str, Any], text_fields: List[str]) -> str:
        """
        Combine multiple text fields into a single text for embedding.
        
        Args:
            item: Item from the data source
            text_fields: List of text field names
            
        Returns:
            Combined text
        """
        text_values = []
        for field in text_fields:
            if field in item and item[field]:
                text_values.append(str(item[field]))
        
        return " ".join(text_values)
    
    def detect_query_type(self, query: str) -> str:
        """
        Auto-detect the type of query to determine which search strategy to use.
        
        Args:
            query: The search query
            
        Returns:
            Query type ("structured", "vector", "hybrid")
        """
        # Check for structured query patterns (field:value, field>value, etc.)
        if ":" in query or ">" in query or "<" in query or ">=" in query or "<=" in query:
            return "structured"
        
        # For now, use hybrid search for all other queries
        return "hybrid"
    
    def search(self, query: str, hybrid_weight: float = 0.5, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search using both structured data and vector search.
        
        Args:
            query: The search query
            hybrid_weight: Weight for combining results (0 = structured only, 1 = vector only)
            limit: Maximum number of results to return
            
        Returns:
            List of search results
        """
        # Auto-detect query type
        query_type = self.detect_query_type(query)
        print(f"Detected query type: {query_type}")
        
        # For structured queries, use structured search only
        if query_type == "structured":
            return self.data_provider.search(query, limit=limit)
        
        # Build vector index if not already built
        if not self.vector_index_built:
            if not self.build_vector_index():
                # If we couldn't build the vector index, just use the data provider
                return self.data_provider.search(query, limit=limit)
        
        # Get data provider results
        structured_results = self.data_provider.search(query, limit=limit)
        
        # Get vector search results
        query_embedding = VectorSearchEngine.get_mock_embedding(query)
        vector_results = self.vector_search.search(query_embedding, limit=limit)
        
        # Convert vector results to same format as structured results
        vector_results_dict = [
            {**item_data, "_score": similarity, "_result_type": "vector"} 
            for item_id, similarity, item_data in vector_results
        ]
        
        # Mark structured results
        for item in structured_results:
            item["_result_type"] = "structured"
        
        # If one of the methods returns no results, just use the other
        if not structured_results:
            return vector_results_dict
        if not vector_results_dict:
            return structured_results
        
        # Combine results
        return self._combine_results(structured_results, vector_results_dict, hybrid_weight, limit)
    
    def _combine_results(self, 
                        structured_results: List[Dict[str, Any]], 
                        vector_results: List[Dict[str, Any]], 
                        hybrid_weight: float,
                        limit: int) -> List[Dict[str, Any]]:
        """
        Combine results from structured data and vector search.
        
        Args:
            structured_results: Results from structured data search
            vector_results: Results from vector search
            hybrid_weight: Weight for combining results (0 = structured only, 1 = vector only)
            limit: Maximum number of results to return
            
        Returns:
            Combined results
        """
        # Create a map of item IDs to items
        all_items = {}
        
        # Add structured results
        id_field = self._get_id_field()
        
        for item in structured_results:
            if id_field in item:
                item_id = str(item[id_field])
                all_items[item_id] = {
                    **item,
                    "_structured_score": item.get("_score", 0),
                    "_vector_score": 0,
                    "_combined_score": 0
                }
        
        # Add vector results
        for item in vector_results:
            if id_field in item:
                item_id = str(item[id_field])
                if item_id in all_items:
                    # Update existing item
                    all_items[item_id]["_vector_score"] = item.get("_score", 0)
                else:
                    # Add new item
                    all_items[item_id] = {
                        **item,
                        "_structured_score": 0,
                        "_vector_score": item.get("_score", 0),
                        "_combined_score": 0
                    }
        
        # Normalize scores
        max_structured_score = max((item["_structured_score"] for item in all_items.values()), default=1)
        max_vector_score = max((item["_vector_score"] for item in all_items.values()), default=1)
        
        # Compute combined scores
        for item_id, item in all_items.items():
            normalized_structured_score = item["_structured_score"] / max_structured_score if max_structured_score > 0 else 0
            normalized_vector_score = item["_vector_score"] / max_vector_score if max_vector_score > 0 else 0
            
            # Weighted combination
            item["_combined_score"] = (
                (1 - hybrid_weight) * normalized_structured_score + 
                hybrid_weight * normalized_vector_score
            )
            
            # Use combined score as the main score
            item["_score"] = item["_combined_score"]
        
        # Convert to list and sort by combined score
        results = list(all_items.values())
        results.sort(key=lambda x: x["_combined_score"], reverse=True)
        
        # Limit results
        return results[:limit]
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by its ID.
        
        Args:
            item_id: The ID of the item to get
            
        Returns:
            The item if found, None otherwise
        """
        return self.data_provider.get_by_id(item_id)

#################################################
# Search Engine Implementation
#################################################

class SearchEngine:
    """
    Main search engine that coordinates searching across providers.
    """
    
    def __init__(self):
        """Initialize the search engine."""
        self.providers = []
        
    def register_provider(self, provider: DataProvider) -> None:
        """
        Register a data provider with the search engine.
        
        Args:
            provider: The data provider to register
        """
        self.providers.append(provider)
    
    def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search across all registered providers.
        
        Args:
            query: The search query
            limit: Maximum number of results to return
            
        Returns:
            List of search results
        """
        results = []
        
        for provider in self.providers:
            provider_results = provider.search(query)
            results.extend(provider_results)
            
        # Sort results by relevance (if available)
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        # Limit the number of results
        return results[:limit]

#################################################
# Command Line Interface
#################################################

def detect_query_type(query_str, data_source):
    """
    Detect the query type and return the appropriate provider based on both
    the query syntax and the file extension.
    
    Args:
        query_str: The search query
        data_source: Path to the data source file
    
    Returns:
        Provider name for the query type
    """
    # Get file extension
    file_ext = os.path.splitext(data_source)[1].lower()
    
    # Check for structured query patterns
    is_structured = ":" in query_str or ">" in query_str or "<" in query_str or ">=" in query_str or "<=" in query_str
    
    # Choose provider based on file type and query type
    if file_ext == '.csv':
        return "csv"  # Always use CSV provider for CSV files
    elif file_ext in ['.db', '.sqlite', '.sqlite3']:
        if is_structured:
            return "structured-sqlite"
        else:
            return "sqlite"
    else:
        # For unknown file types, default to a reasonable choice
        if is_structured:
            return "structured-sqlite"
        else:
            return "hybrid"

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Meta Search CLI')
    parser.add_argument('--data-source', required=True,
                        help='Path to the data source file')
    parser.add_argument('--id-field', required=True,
                        help='Field to use as ID')
    parser.add_argument('--name-field', required=True,
                        help='Field to use as name')
    parser.add_argument('--query', required=True,
                        help='Search query')
    parser.add_argument('--provider', 
                        choices=['csv', 'sqlite', 'structured-sqlite', 'json', 'hybrid'],
                        help='Provider type to use (auto-detected if not specified)')
    parser.add_argument('--vector-weight', type=float, default=0.5,
                        help='Weight for vector search when using hybrid provider (0-1)')
    parser.add_argument('--vector-index', 
                        help='Path to vector index file (for hybrid provider)')
    parser.add_argument('--build-index', action='store_true',
                        help='Force rebuild of vector index (for hybrid provider)')
    parser.add_argument('--table-name',
                        help='Table name for SQLite provider')
    parser.add_argument('--max-results', type=int, default=10,
                        help='Maximum number of results to return (default: 10)')
    return parser.parse_args()

def main():
    """Main entry point for the CLI."""
    try:
        args = parse_args()
        
        print(f"Searching for '{args.query}' in {args.data_source}")
        print(f"Using {args.id_field} as ID field and {args.name_field} as name field")
        
        # Check if data source file exists
        if not os.path.exists(args.data_source):
            print(f"ERROR: The file '{args.data_source}' doesn't exist!")
            print("\nPossible solutions:")
            print("1. Check if you're in the correct directory")
            print("2. Provide the full path to the file")
            print("3. Create the file if it doesn't exist yet")
            
            # Try to find similar files
            current_dir = os.getcwd()
            file_ext = os.path.splitext(args.data_source)[1].lower()
            parent_dir = os.path.dirname(args.data_source)
            search_dir = parent_dir if parent_dir else current_dir
            
            print(f"\nLooking for similar files in {search_dir}...")
            
            similar_files = []
            for root, dirs, files in os.walk(search_dir, topdown=True, followlinks=False):
                # Limit depth to current or parent directory
                if root != search_dir:
                    continue
                    
                for file in files:
                    # Look for files with same extension
                    if file_ext and file.endswith(file_ext):
                        similar_files.append(os.path.join(root, file))
                    # Or for database/CSV files
                    elif file.endswith(('.db', '.sqlite', '.sqlite3', '.csv')):
                        similar_files.append(os.path.join(root, file))
            
            if similar_files:
                print("\nFound these similar files you could use instead:")
                for file in similar_files:
                    print(f"  {os.path.relpath(file, current_dir)}")
                print("\nTry running the command with one of these files:")
                print(f"python3 cli.py --data-source FILE_PATH --id-field {args.id_field} --name-field {args.name_field} --query \"{args.query}\"")
            else:
                print("\nNo similar files found in the directory.")
                
            sys.exit(1)
        
        # Auto-detect provider based on file extension and query type
        provider_name = args.provider
        if not provider_name:
            provider_name = detect_query_type(args.query, args.data_source)
            print(f"Auto-detected provider: {provider_name}")
        
        # Initialize the search engine
        engine = SearchEngine()
        
        # Create field mapping
        field_mapping = FieldMapping()
        field_mapping.add_mapping('id', args.id_field)
        field_mapping.add_mapping('name', args.name_field)
        
        # Set up the appropriate provider
        if provider_name == 'csv':
            provider = CSVProvider(args.data_source)
            provider.id_field = args.id_field
            provider.name_field = args.name_field
            print(f"Using CSV provider with structured query support")
        elif provider_name == 'sqlite':
            provider = SQLiteProvider(args.data_source, args.table_name)
        elif provider_name == 'structured-sqlite':
            provider = StructuredSQLiteProvider(args.data_source, args.table_name)
            provider.id_field = args.id_field
            provider.name_field = args.name_field
            print(f"Using structured SQLite provider with multi-strategy search")
        elif provider_name == 'hybrid':
            provider = HybridProvider(args.data_source, args.vector_index, args.table_name)
            print(f"Using hybrid provider with vector weight: {args.vector_weight}")
        else:
            print(f"Unknown provider type: {provider_name}")
            sys.exit(1)
        
        # Set field mapping
        provider.set_field_mapping(field_mapping)
        
        # For hybrid providers, rebuild index if requested
        if provider_name == 'hybrid' and args.build_index:
            print("Rebuilding vector index...")
            provider.build_vector_index()
        
        # Register the provider with the engine
        engine.register_provider(provider)
        
        # Regular search
        try:
            if provider_name == 'hybrid':
                # Use standard hybrid weight
                results = provider.search(args.query, args.vector_weight, args.max_results)
            else:
                results = provider.search(args.query, args.max_results)
        except TypeError as e:
            # Fallback for providers with different parameter requirements
            print(f"Warning: {e}")
            print("Trying alternative search method...")
            results = provider.search(args.query)
            if results and args.max_results:
                results = results[:args.max_results]
        # Display results
        if not results:
            print("No results found.")
        else:
            print(f"\nFound {len(results)} results:")
            
            result_index = 1
            for result in results:
                # Check if this is a separator item
                if result.get("_separator", False):
                    print(f"\n--- {result.get('_message', 'Vector search results below:')} ---\n")
                    continue
                
                print(f"\nResult {result_index}:")
                result_index += 1
                
                # Format scores for better readability if they exist
                if "_score" in result:
                    result["_score"] = f"{result['_score']:.4f}"
                if "_structured_score" in result:
                    result["_structured_score"] = f"{result['_structured_score']:.4f}"
                if "_vector_score" in result:
                    result["_vector_score"] = f"{result['_vector_score']:.4f}"
                if "_combined_score" in result:
                    result["_combined_score"] = f"{result['_combined_score']:.4f}"
                
                # Print all fields
                for key, value in result.items():
                    # Skip internal fields starting with underscore
                    if not key.startswith("_"):
                        print(f"  {key}: {value}")
        
    except ImportError as e:
        print(f"Import error: {e}")
        print("This could be due to missing module files. Make sure all required files exist.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()