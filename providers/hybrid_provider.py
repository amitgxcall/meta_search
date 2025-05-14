"""
Hybrid provider that combines SQLite and vector search.
"""

import os
import sqlite3
import json
import datetime
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import hashlib

from ..providers.base import DataProvider
from ..query.classification import classify_query
from ..utils.field_mapping import FieldMapping

class HybridProvider(DataProvider):
    """
    Data provider that uses SQLite for structured queries and
    vector search for semantic queries.
    """
    
    def __init__(self, 
                 data_source: str, 
                 field_mapping: Optional[FieldMapping] = None,
                 sqlite_db_path: Optional[str] = None,
                 vector_cache_dir: str = '.vector_cache',
                 table_name: str = 'jobs'):
        """
        Initialize hybrid provider.
        
        Args:
            data_source: Path to the primary data source (CSV)
            field_mapping: Field mapping configuration
            sqlite_db_path: Path to SQLite database (created if not exists)
            vector_cache_dir: Directory for vector cache
            table_name: SQLite table name
        """
        self.data_source = data_source
        self.field_mapping = field_mapping or FieldMapping()
        
        # Initialize SQLite database
        self.sqlite_db_path = sqlite_db_path or self._get_default_db_path(data_source)
        self.table_name = table_name
        self._ensure_sqlite_db()
        
        # Vector search components will be initialized on demand
        self.vector_cache_dir = vector_cache_dir
        self.vector_search_initialized = False
        self._vector_texts = None
    
    def _get_default_db_path(self, data_source: str) -> str:
        """Generate default SQLite path based on data source."""
        if data_source.lower().endswith('.csv'):
            return data_source.rsplit('.', 1)[0] + '.db'
        return os.path.splitext(data_source)[0] + '.db'
    
    def _ensure_sqlite_db(self) -> None:
        """Ensure SQLite database exists, creating it from CSV if needed."""
        if os.path.exists(self.sqlite_db_path):
            # Check if the table exists
            conn = sqlite3.connect(self.sqlite_db_path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}'")
            table_exists = cursor.fetchone() is not None
            conn.close()
            
            if table_exists:
                return
        
        # Database or table doesn't exist, create from data source
        if self.data_source.lower().endswith('.csv'):
            self._initialize_from_csv()
        else:
            raise ValueError(f"Cannot initialize SQLite from data source: {self.data_source}")
    
    def _initialize_from_csv(self) -> None:
        """Initialize SQLite database from CSV file."""
        print(f"Creating SQLite database from {self.data_source}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(self.sqlite_db_path)), exist_ok=True)
        
        # Read CSV into pandas
        df = pd.read_csv(self.data_source)
        
        # Convert to SQLite
        conn = sqlite3.connect(self.sqlite_db_path)
        df.to_sql(self.table_name, conn, if_exists='replace', index=False)
        
        # Create indexes for common fields
        cursor = conn.cursor()
        
        # Create index for ID field
        id_field = self.field_mapping.id_field
        if id_field in df.columns:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{id_field} ON {self.table_name} ({id_field})")
        
        # Create index for name field
        name_field = self.field_mapping.name_field
        if name_field in df.columns:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{name_field} ON {self.table_name} ({name_field})")
        
        # Create index for status field if it exists
        status_field = self.field_mapping.status_field
        if status_field and status_field in df.columns:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{status_field} ON {self.table_name} ({status_field})")
        
        # Create indexes for timestamp fields
        for ts_field in self.field_mapping.timestamp_fields:
            if ts_field in df.columns:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{ts_field} ON {self.table_name} ({ts_field})")
        
        conn.commit()
        conn.close()
        print(f"SQLite database created at {self.sqlite_db_path}")
    
    def _initialize_vector_search(self) -> None:
        """Initialize vector search components."""
        if self.vector_search_initialized:
            return
            
        from ..search.vector_search import VectorSearchEngine
        
        # Create vector cache directory
        os.makedirs(self.vector_cache_dir, exist_ok=True)
        
        # Initialize vector search engine
        self.vector_engine = VectorSearchEngine(self.vector_cache_dir)
        
        # Get all records and convert to texts for vector search
        all_records = self.get_all_records()
        texts = []
        
        # Create text representations for vector search
        for record in all_records:
            text = self.get_text_for_vector_search(record, {
                self.field_mapping.name_field: 5.0,  # Name field has highest weight
                self.field_mapping.id_field: 1.0,
                'default': 1.0
            })
            texts.append(text)
        
        # Store original texts
        self._vector_texts = texts
        
        # Initialize vector search with texts
        data_hash = hashlib.md5(self.data_source.encode()).hexdigest()
        self.vector_engine.initialize(data_hash, texts)
        
        self.vector_search_initialized = True
    
    def get_all_fields(self) -> List[str]:
        """Get all available fields in the data."""
        conn = sqlite3.connect(self.sqlite_db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.table_name})")
        fields = [row[1] for row in cursor.fetchall()]
        conn.close()
        return fields
    
    def get_record_count(self) -> int:
        """Get total number of records in the data."""
        conn = sqlite3.connect(self.sqlite_db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """Get all records from the data source."""
        conn = sqlite3.connect(self.sqlite_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name}")
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return records
    
    def get_record_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        """Get a specific record by ID."""
        id_field = self.field_mapping.id_field
        
        conn = sqlite3.connect(self.sqlite_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE {id_field} = ?", (id_value,))
        row = cursor.fetchone()
        conn.close()
        
        return dict(row) if row else None
    
    def query_records(self, filters: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Query records based on filters.
        This uses SQLite for efficient structured filtering.
        """
        if not filters:
            # Return all records up to limit
            conn = sqlite3.connect(self.sqlite_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {self.table_name} LIMIT {limit}")
            records = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return records
        
        # Build SQL query with filters
        where_clauses = []
        params = []
        
        for field, value in filters.items():
            # Handle temporal queries with special field names
            if field == "recent" and value is True:
                # Find the most recent timestamp field
                if self.field_mapping.timestamp_fields:
                    ts_field = self.field_mapping.timestamp_fields[0]
                    where_clauses.append(f"{ts_field} = (SELECT MAX({ts_field}) FROM {self.table_name})")
                continue
            
            # Handle relative time queries
            if field == "days_ago":
                if self.field_mapping.timestamp_fields:
                    ts_field = self.field_mapping.timestamp_fields[0]
                    # SQLite date calculations
                    where_clauses.append(f"DATE({ts_field}) >= DATE('now', '-{int(value)} days')")
                continue
            
            # Normal field filtering
            if isinstance(value, dict):
                # Operator-based filters
                for op, op_value in value.items():
                    if op == "gt":
                        where_clauses.append(f"{field} > ?")
                        params.append(op_value)
                    elif op == "gte":
                        where_clauses.append(f"{field} >= ?")
                        params.append(op_value)
                    elif op == "lt":
                        where_clauses.append(f"{field} < ?")
                        params.append(op_value)
                    elif op == "lte":
                        where_clauses.append(f"{field} <= ?")
                        params.append(op_value)
                    elif op == "contains":
                        where_clauses.append(f"{field} LIKE ?")
                        params.append(f"%{op_value}%")
            else:
                # Exact match
                where_clauses.append(f"{field} = ?")
                params.append(value)
        
        # Build full query
        query = f"SELECT * FROM {self.table_name}"
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        query += f" LIMIT {limit}"
        
        # Execute the query
        conn = sqlite3.connect(self.sqlite_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, params)
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return records
    
    def get_text_for_vector_search(self, record: Dict[str, Any], field_weights: Dict[str, float]) -> str:
        """Convert a record to text for vector search."""
        text_parts = []
        
        # Prioritize name field
        name_field = self.field_mapping.name_field
        if name_field in record and record[name_field]:
            job_name = str(record[name_field])
            
            # Add with special prefix for exact matching
            text_parts.append(f"job_name:{job_name}")
            
            # Add multiple repetitions for higher weight
            weight = field_weights.get(name_field, 5.0)
            for _ in range(int(weight)):
                text_parts.append(job_name)
            
            # Add individual words from the job name
            for word in job_name.split('_'):
                if word:
                    text_parts.extend([word] * 3)
        
        # Add other fields with lower weights
        for field, value in record.items():
            if field != name_field and value is not None:
                weight = field_weights.get(field, field_weights.get('default', 1.0))
                if weight > 0:
                    text_parts.append(f"{field}:{value}")
        
        return " ".join(text_parts)
    
    def prepare_for_output(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a record for output."""
        output = {}
        
        for field, value in record.items():
            # Format datetime objects if needed
            if isinstance(value, datetime.datetime):
                output[field] = value.strftime("%Y-%m-%d %H:%M:%S")
            # Handle None values
            elif value is None:
                output[field] = ""
            # Pass through other values
            else:
                output[field] = value
        
        # Map field names to generic names
        return self.field_mapping.reverse_map_record(output)
    
    def execute_vector_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Execute vector similarity search.
        
        Args:
            query: Query string
            limit: Maximum number of results
            
        Returns:
            List of records matching the query
        """
        # Initialize vector search if needed
        if not self.vector_search_initialized:
            self._initialize_vector_search()
        
        # Perform vector search
        indices, scores = self.vector_engine.search(query, limit)
        
        # Get the actual records
        results = []
        all_records = self.get_all_records()
        
        for i, (idx, score) in enumerate(zip(indices, scores)):
            if idx < len(all_records):
                record = all_records[idx]
                results.append({
                    'job_details': self.prepare_for_output(record),
                    'score': score,
                    'match_type': 'vector',
                    'rank': i + 1
                })
        
        return results
    
    def search_hybrid(self, query: str, query_type: str, 
                     structured_filters: Dict[str, Any], 
                     limit: int = 10) -> List[Dict[str, Any]]:
        """
        Execute a hybrid search combining structured and vector approaches.
        
        Args:
            query: Original query string
            query_type: Type of query ('structured', 'vector', 'hybrid')
            structured_filters: Extracted structured filters
            limit: Maximum number of results
            
        Returns:
            List of search results
        """
        results = []
        
        # For structured queries, use SQLite
        if query_type in ['structured', 'hybrid'] and structured_filters:
            structured_results = self.query_records(structured_filters, limit)
            
            # Format results
            for record in structured_results:
                results.append({
                    'job_details': self.prepare_for_output(record),
                    'score': 1.0,  # Perfect score for exact matches
                    'match_type': 'structured',
                    'matched_filters': structured_filters
                })
        
        # For vector or hybrid queries, or if structured search returned no results
        if query_type in ['vector', 'hybrid'] or (query_type == 'structured' and not results):
            # Get IDs to exclude (already found in structured results)
            id_field = self.field_mapping.id_field
            exclude_ids = set()
            
            for result in results:
                job_details = result['job_details']
                if 'id' in job_details:
                    exclude_ids.add(job_details['id'])
                elif id_field in job_details:
                    exclude_ids.add(job_details[id_field])
            
            # Calculate how many more results we need
            vector_limit = max(0, limit - len(results))
            
            if vector_limit > 0:
                # Execute vector search
                vector_results = self.execute_vector_search(query, vector_limit)
                
                # Add results that aren't already included
                for result in vector_results:
                    job_details = result['job_details']
                    result_id = None
                    
                    if 'id' in job_details:
                        result_id = job_details['id']
                    elif id_field in job_details:
                        result_id = job_details[id_field]
                    
                    if result_id is not None and result_id not in exclude_ids:
                        results.append(result)
                        exclude_ids.add(result_id)
        
        # Sort results (structured matches first, then by score)
        results.sort(key=lambda x: (
            0 if x['match_type'] == 'structured' else 1,
            -x['score']
        ))
        
        return results[:limit]