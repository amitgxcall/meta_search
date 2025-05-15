"""
Test utilities for meta_search.

This module provides utilities for testing the meta_search system,
including fixtures, mock data generators, and test helpers.

Example:
    # Use a mock provider in a test
    provider = create_mock_provider([
        {'id': '1', 'name': 'Test 1', 'status': 'success'},
        {'id': '2', 'name': 'Test 2', 'status': 'failed'}
    ])
    
    # Test search functionality
    results = provider.search("Test")
    assert len(results) == 2
"""

import os
import random
import tempfile
import datetime
import json
import csv
import sqlite3
from typing import List, Dict, Any, Optional, Tuple, Callable

from ..providers.base import DataProvider
from ..utils.field_mapping import FieldMapping
from ..search.vector_search import VectorSearchEngine


class MockDataProvider(DataProvider):
    """
    Mock data provider for testing.
    
    This provider uses an in-memory data structure for testing without
    requiring actual data files.
    
    Attributes:
        records: List of mock records
        field_mapping: Field mapping for the provider
    """
    
    def __init__(self, records: List[Dict[str, Any]], field_mapping: Optional[FieldMapping] = None):
        """
        Initialize the mock provider.
        
        Args:
            records: List of mock records
            field_mapping: Field mapping for the provider
        """
        super().__init__("mock://data")
        self.records = records
        self.field_mapping = field_mapping or FieldMapping()
        self.is_connected = True
    
    def connect(self) -> bool:
        """
        Connect to the mock data source.
        
        Returns:
            Always returns True
        """
        self.is_connected = True
        return True
    
    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Search the mock data.
        
        Args:
            query: Search query
            **kwargs: Additional parameters
            
        Returns:
            List of matching records
        """
        query_lower = query.lower()
        results = []
        
        for record in self.records:
            score = 0
            
            # Simple search implementation that checks if query is in any field
            for field, value in record.items():
                if isinstance(value, str) and query_lower in value.lower():
                    score += 1
            
            if score > 0:
                result = record.copy()
                result['_score'] = score
                results.append(result)
        
        # Sort by score
        results.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        return results
    
    def get_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a record by its ID.
        
        Args:
            item_id: ID of the record to get
            
        Returns:
            The record if found, None otherwise
        """
        id_field = self.field_mapping.get_source_field('id') or 'id'
        
        for record in self.records:
            if str(record.get(id_field)) == str(item_id):
                return record.copy()
        
        return None
    
    def get_all_fields(self) -> List[str]:
        """
        Get all available fields.
        
        Returns:
            List of field names
        """
        if not self.records:
            return []
        
        # Get unique fields from all records
        all_fields = set()
        for record in self.records:
            all_fields.update(record.keys())
        
        return list(all_fields)
    
    def get_record_count(self) -> int:
        """
        Get the total number of records.
        
        Returns:
            Number of records
        """
        return len(self.records)


def create_mock_provider(records: List[Dict[str, Any]], 
                        field_mapping: Optional[FieldMapping] = None) -> MockDataProvider:
    """
    Create a mock data provider with the given records.
    
    Args:
        records: List of mock records
        field_mapping: Field mapping for the provider
        
    Returns:
        MockDataProvider instance
    """
    return MockDataProvider(records, field_mapping)


def create_temp_csv(records: List[Dict[str, Any]], 
                  fieldnames: Optional[List[str]] = None) -> str:
    """
    Create a temporary CSV file with the given records.
    
    Args:
        records: List of records to write
        fieldnames: List of field names (if None, inferred from records)
        
    Returns:
        Path to the temporary CSV file
    """
    if not records:
        raise ValueError("No records provided")
    
    # Determine fieldnames if not provided
    if fieldnames is None:
        fieldnames = list(records[0].keys())
    
    # Create temporary file
    fd, path = tempfile.mkstemp(suffix='.csv')
    
    try:
        with os.fdopen(fd, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                # Filter record to only include fields in fieldnames
                filtered_record = {k: v for k, v in record.items() if k in fieldnames}
                writer.writerow(filtered_record)
    except Exception:
        os.unlink(path)
        raise
    
    return path


def create_temp_sqlite(records: List[Dict[str, Any]], 
                     table_name: str = 'items',
                     fieldnames: Optional[List[str]] = None) -> str:
    """
    Create a temporary SQLite database with the given records.
    
    Args:
        records: List of records to write
        table_name: Name of the table to create
        fieldnames: List of field names (if None, inferred from records)
        
    Returns:
        Path to the temporary SQLite database
    """
    if not records:
        raise ValueError("No records provided")
    
    # Determine fieldnames if not provided
    if fieldnames is None:
        fieldnames = list(records[0].keys())
    
    # Create temporary file
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    
    try:
        # Connect to database
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        
        # Create table
        fields_sql = ', '.join(f'"{field}" TEXT' for field in fieldnames)
        cursor.execute(f'CREATE TABLE "{table_name}" ({fields_sql})')
        
        # Insert records
        for record in records:
            # Filter record to only include fields in fieldnames
            filtered_record = {k: record.get(k, '') for k in fieldnames}
            
            # Build SQL
            placeholders = ', '.join(['?'] * len(fieldnames))
            values = [str(filtered_record.get(field, '')) for field in fieldnames]
            
            # Execute
            cursor.execute(
                f'INSERT INTO "{table_name}" ({", ".join(f""""{f}"""" for f in fieldnames)}) VALUES ({placeholders})',
                values
            )
        
        # Commit and close
        conn.commit()
        conn.close()
    except Exception:
        os.unlink(path)
        raise
    
    return path


def create_temp_json(records: List[Dict[str, Any]], 
                   root_key: Optional[str] = None) -> str:
    """
    Create a temporary JSON file with the given records.
    
    Args:
        records: List of records to write
        root_key: Root key for the JSON object (if None, records are top-level array)
        
    Returns:
        Path to the temporary JSON file
    """
    if not records:
        raise ValueError("No records provided")
    
    # Create temporary file
    fd, path = tempfile.mkstemp(suffix='.json')
    
    try:
        with os.fdopen(fd, 'w') as f:
            if root_key:
                json.dump({root_key: records}, f, indent=2)
            else:
                json.dump(records, f, indent=2)
    except Exception:
        os.unlink(path)
        raise
    
    return path


def generate_mock_job_data(count: int = 10) -> List[Dict[str, Any]]:
    """
    Generate mock job data for testing.
    
    Args:
        count: Number of records to generate
        
    Returns:
        List of mock job records
    """
    statuses = ['completed', 'failed', 'cancelled', 'running', 'queued', 'scheduled']
    priorities = ['critical', 'high', 'medium', 'low']
    job_types = ['backup', 'sync', 'report', 'cleanup', 'analysis', 'audit', 'maintenance']
    
    records = []
    
    for i in range(1, count + 1):
        job_type = random.choice(job_types)
        
        record = {
            'job_id': str(i),
            'job_name': f"{job_type}_{i:03d}",
            'status': random.choice(statuses),
            'priority': random.choice(priorities),
            'description': f"{job_type.capitalize()} job {i}",
            'created_at': (datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))).isoformat(),
            'duration_minutes': random.randint(1, 120),
            'owner': f"user{random.randint(1, 5)}"
        }
        
        # Add error message for failed jobs
        if record['status'] == 'failed':
            record['error_message'] = random.choice([
                "Connection timeout",
                "Resource not found",
                "Permission denied",
                "Out of memory",
                "Invalid input data"
            ])
        
        records.append(record)
    
    return records


def generate_mock_vector_index(records: List[Dict[str, Any]], 
                             text_fields: List[str],
                             output_path: str) -> str:
    """
    Generate a mock vector index for testing.
    
    Args:
        records: List of records to index
        text_fields: List of text fields to use for indexing
        output_path: Path to save the index
        
    Returns:
        Path to the vector index file
    """
    # Create vector search engine
    engine = VectorSearchEngine()
    
    # Add items to index
    for record in records:
        # Get ID
        item_id = record.get('job_id', record.get('id', str(id(record))))
        
        # Combine text fields
        text = " ".join(str(record.get(field, '')) for field in text_fields if field in record)
        
        # Generate embedding
        embedding = VectorSearchEngine.get_mock_embedding(text)
        
        # Add to index
        engine.add_item(item_id, record, embedding)
    
    # Save index
    if engine.save_index(output_path):
        return output_path
    else:
        raise RuntimeError("Failed to save vector index")


def cleanup_temp_files(*paths: str) -> None:
    """
    Clean up temporary files.
    
    Args:
        *paths: Paths of files to delete
    """
    for path in paths:
        try:
            if os.path.exists(path):
                os.unlink(path)
        except Exception as e:
            print(f"Error cleaning up {path}: {e}")