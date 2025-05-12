"""
SQLite data provider for the search system.
"""

import os
import sqlite3
import json
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import re

from ..data_provider import DataProvider, FieldMapping

class SQLiteDataProvider(DataProvider):
    """
    Data provider implementation for SQLite databases.
    
    This provider connects to a SQLite database and implements the DataProvider interface.
    """
    
    def __init__(self, db_path: str, table_name: str = 'jobs', 
                 field_mapping: Optional[FieldMapping] = None,
                 initialize_from_csv: Optional[str] = None):
        """
        Initialize with a SQLite database.
        
        Args:
            db_path: Path to the SQLite database file
            table_name: Name of the table to use
            field_mapping: Field mapping for generic field names
            initialize_from_csv: Path to a CSV file to initialize the database (if not exists)
        """
        self.db_path = db_path
        self.table_name = table_name
        
        # Initialize database if needed
        if initialize_from_csv and not os.path.exists(db_path):
            self._initialize_from_csv(initialize_from_csv)
        
        # Connect to the database
        self._test_connection()
        
        # Get field information
        self.fields = self._get_table_fields()
        
        # Create default field mapping if none provided
        if field_mapping is None:
            # Detect common field names
            id_field = self._detect_id_field()
            name_field = self._detect_name_field()
            status_field = self._detect_status_field()
            timestamp_fields = self._detect_timestamp_fields()
            
            self.field_mapping = FieldMapping(
                id_field=id_field,
                name_field=name_field,
                status_field=status_field,
                timestamp_fields=timestamp_fields
            )
        else:
            self.field_mapping = field_mapping
    
    def _test_connection(self):
        """Test the database connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}'")
            if not cursor.fetchone():
                raise ValueError(f"Table '{self.table_name}' not found in database")
            conn.close()
        except sqlite3.Error as e:
            raise ConnectionError(f"Could not connect to SQLite database: {str(e)}")
    
    def _initialize_from_csv(self, csv_path: str):
        """Initialize database from a CSV file"""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Create parent directory if needed
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        
        # Load CSV and create database
        df = pd.read_csv(csv_path)
        
        # Convert timestamps to strings for SQLite compatibility
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)
        
        # Create connection and save to database
        conn = sqlite3.connect(self.db_path)
        df.to_sql(self.table_name, conn, index=False, if_exists='replace')
        
        # Create indices for common fields
        cursor = conn.cursor()
        for col in ['job_id', 'id', 'job_name', 'name', 'status']:
            if col in df.columns:
                try:
                    cursor.execute(f"CREATE INDEX idx_{col} ON {self.table_name} ({col})")
                except sqlite3.Error:
                    pass  # Ignore if index already exists
        
        conn.commit()
        conn.close()
        
        print(f"Initialized SQLite database at {self.db_path} from {csv_path}")
    
    def _get_table_fields(self) -> List[str]:
        """Get fields from the table schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.table_name})")
        fields = [row[1] for row in cursor.fetchall()]
        conn.close()
        return fields
    
    def _detect_id_field(self) -> str:
        """Auto-detect ID field"""
        for candidate in ['id', 'job_id', 'task_id', 'record_id', 'ID']:
            if candidate in self.fields:
                return candidate
        
        # If no standard ID field is found, use the first column
        return self.fields[0]
    
    def _detect_name_field(self) -> str:
        """Auto-detect name field"""
        for candidate in ['name', 'job_name', 'task_name', 'title', 'NAME']:
            if candidate in self.fields:
                return candidate
        
        # If no standard name field is found, use the second column or first if only one
        if len(self.fields) > 1:
            return self.fields[1]
        return self.fields[0]
    
    def _detect_status_field(self) -> Optional[str]:
        """Auto-detect status field"""
        for candidate in ['status', 'job_status', 'task_status', 'state', 'STATUS']:
            if candidate in self.fields:
                return candidate
        return None
    
    def _detect_timestamp_fields(self) -> List[str]:
        """Auto-detect timestamp fields"""
        timestamp_fields = []
        timestamp_keywords = ['time', 'date', 'timestamp', 'start', 'end', 'created', 'updated']
        
        for field in self.fields:
            field_lower = field.lower()
            if any(keyword in field_lower for keyword in timestamp_keywords):
                timestamp_fields.append(field)
        
        return timestamp_fields
    
    def get_all_fields(self) -> List[str]:
        """Get all available fields"""
        return self.fields
    
    def get_record_count(self) -> int:
        """Get total number of records"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """Get all records"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name}")
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return records
    
    def get_record_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        """Get record by ID"""
        id_field = self.field_mapping.id_field
        if id_field not in self.fields:
            return None
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE {id_field} = ?", (id_value,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return dict(row)
        return None
    
    def query_records(self, filters: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """Query records with filters"""
        if not filters:
            # If no filters, return all records up to limit
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {self.table_name} LIMIT {limit}")
            records = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return records
        
        # Build SQL query with filters
        conditions = []
        params = []
        
        for field, value in filters.items():
            # Skip fields that don't exist in the database
            if field not in self.fields and not field.endswith('_partial') and field not in ['is_latest', 'is_latest_run', 'days_ago']:
                continue
            
            # Handle special fields
            if field == 'is_latest' and value is True:
                if 'is_latest' in self.fields:
                    conditions.append("is_latest = 1")
                continue
                
            if field == 'is_latest_run' and value is True:
                if 'is_latest_run' in self.fields:
                    conditions.append("is_latest_run = 1")
                continue
            
            # Handle partial name matching
            if field.endswith('_partial') and isinstance(value, str):
                base_field = field.replace('_partial', '')
                if base_field in self.fields:
                    conditions.append(f"LOWER({base_field}) LIKE ?")
                    params.append(f"%{value.lower()}%")
                continue
            
            # Handle days_ago comparison
            if field == 'days_ago' and isinstance(value, str) and value.startswith(('<', '>', '<=', '>=')):
                if 'days_ago' in self.fields:
                    match = re.match(r'([><]=?)(.+)', value)
                    if match:
                        operator, days = match.groups()
                        try:
                            days = float(days)
                            conditions.append(f"days_ago {operator} ?")
                            params.append(days)
                        except ValueError:
                            pass
                continue
            
            # Handle numeric comparison
            if isinstance(value, str) and value.startswith(('<', '>', '<=', '>=')):
                match = re.match(r'([><]=?)(.+)', value)
                if match:
                    operator, val = match.groups()
                    try:
                        numeric_val = float(val)
                        conditions.append(f"{field} {operator} ?")
                        params.append(numeric_val)
                    except ValueError:
                        pass
                continue
            
            # Default: exact equality
            conditions.append(f"{field} = ?")
            params.append(value)
        
        # If no valid conditions, return empty list
        if not conditions:
            return []
        
        # Execute query
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = f"SELECT * FROM {self.table_name} WHERE {' AND '.join(conditions)} LIMIT {limit}"
        cursor.execute(query, params)
        
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return records
    
    def get_text_for_vector_search(self, record: Dict[str, Any], field_weights: Dict[str, float]) -> str:
        """Convert record to text for vector search"""
        weighted_parts = []
        
        for field, value in record.items():
            if value is not None and field != 'text_for_search':
                # Get weight for this field
                weight = field_weights.get(field, field_weights.get('default', 1.0))
                
                # Add field:value pairs based on weight
                weighted_parts.extend([f"{field}:{value}"] * int(weight))
                
                # For high-weight fields, also add without field prefix
                if weight >= 2.0 and isinstance(value, str):
                    weighted_parts.extend([value] * int(weight - 1))
                
                # Special handling for name field
                if field == self.field_mapping.name_field and isinstance(value, str):
                    words = value.split('_')
                    for word in words:
                        if len(word) > 3:  # Only add meaningful words
                            weighted_parts.extend([word] * int(weight))
                
                # Add temporal indicators
                if field == 'is_latest' and value == 1:
                    weighted_parts.extend(['latest', 'recent', 'new', 'current'] * 2)
                
                if field == 'is_latest_run' and value == 1:
                    weighted_parts.extend(['latest run', 'most recent', 'latest execution'] * 2)
                
                if field == 'days_ago' and value is not None:
                    try:
                        days = float(value)
                        if days < 1:
                            weighted_parts.extend(['today', 'current', 'latest'] * 2)
                        elif days < 2:
                            weighted_parts.extend(['yesterday', 'recent', 'latest'] * 2)
                        elif days < 7:
                            weighted_parts.extend(['this week', 'recent'] * 2)
                        elif days < 30:
                            weighted_parts.extend(['this month'] * 2)
                    except (ValueError, TypeError):
                        pass
                
                # Add status indicators
                if field == self.field_mapping.status_field:
                    status = str(value).lower()
                    if status == 'failed':
                        weighted_parts.extend(['error', 'failure', 'problem', 'failed', 'unsuccessful'] * 2)
                    elif status == 'completed':
                        weighted_parts.extend(['success', 'completed', 'done', 'finished'] * 2)
                    elif status == 'running':
                        weighted_parts.extend(['active', 'ongoing', 'in progress', 'running'] * 2)
        
        return " ".join(weighted_parts)
    
    def prepare_for_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare record for output"""
        output = {}
        
        for field, value in record.items():
            # Handle None values
            if value is None:
                output[field] = None
                continue
            
            # Handle booleans stored as integers
            if field in ['is_latest', 'is_latest_run'] and isinstance(value, int):
                output[field] = bool(value)
                continue
            
            # Handle timestamps from strings
            if field in self.field_mapping.timestamp_fields and isinstance(value, str):
                # Keep string format for timestamps in SQLite
                output[field] = value
                continue
            
            # Pass through all other values
            output[field] = value
        
        return output