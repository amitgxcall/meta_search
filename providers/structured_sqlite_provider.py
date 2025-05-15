"""
Structured SQLite query provider implementation.

This module provides a specialized SQLite provider that excels at handling
structured queries with field:value syntax, comparisons, and complex conditions.
It directly translates structured query syntax into optimized SQL.

Example:
    # Create provider
    provider = StructuredSQLiteProvider('jobs.db', 'jobs')
    
    # Search with structured syntax
    results = provider.search("status:failed priority:high duration_minutes>30")
"""

import os
import sqlite3
import re
from typing import List, Dict, Any, Optional, Tuple, Union
import logging

from .sqlite_provider import SQLiteProvider

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StructuredSQLiteProvider(SQLiteProvider):
    """
    SQLite provider optimized for structured queries.
    
    This provider extends the base SQLiteProvider with enhanced capabilities
    for structured query parsing and SQL generation. It directly translates
    structured query syntax (field:value) into optimized SQL queries.
    
    Attributes:
        query_parser: Optional custom query parser
        use_fts: Whether to use SQLite FTS for text search
        default_operator: Default operator for combining conditions ('AND' or 'OR')
    """
    
    def __init__(self, 
                source_path: str, 
                table_name: Optional[str] = None,
                use_fts: bool = False,
                default_operator: str = 'AND'):
        """
        Initialize the structured SQLite provider.
        
        Args:
            source_path: Path to the SQLite database file
            table_name: Name of the table to use (if None, will try to detect)
            use_fts: Whether to use SQLite FTS for text search
            default_operator: Default operator for combining conditions ('AND' or 'OR')
        """
        super().__init__(source_path, table_name)
        self.use_fts = use_fts
        self.default_operator = default_operator.upper()
        
        # Column types cache
        self.column_types = {}
        
        # Connect and get column types
        if self.connect():
            self._get_column_types()
    
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
            logger.warning(f"Error getting column types: {e}")
    
    def _get_column_type(self, column_name: str) -> str:
        """
        Get the SQLite type for a column.
        
        Args:
            column_name: Column name to lookup
            
        Returns:
            SQLite type (TEXT, INTEGER, REAL, etc.) or TEXT as default
        """
        return self.column_types.get(column_name, 'TEXT')
    
    def _is_numeric_column(self, column_name: str) -> bool:
        """
        Check if a column is numeric.
        
        Args:
            column_name: Column name to check
            
        Returns:
            True if the column is numeric, False otherwise
        """
        column_type = self._get_column_type(column_name)
        return column_type in ('INTEGER', 'REAL', 'NUMERIC')
    
    def _is_text_column(self, column_name: str) -> bool:
        """
        Check if a column is text.
        
        Args:
            column_name: Column name to check
            
        Returns:
            True if the column is text, False otherwise
        """
        column_type = self._get_column_type(column_name)
        return column_type in ('TEXT', 'VARCHAR', 'CHAR', 'CLOB')
    
    def _parse_structured_query(self, query: str) -> Tuple[List[str], List[Any], List[str]]:
        """
        Parse a structured query into SQL clauses and parameters.
        
        Args:
            query: Structured query string
            
        Returns:
            Tuple of (where_clauses, params, keywords)
        """
        where_clauses = []
        params = []
        keywords = []
        
        # Regular expressions for different query patterns
        field_value_pattern = r'(\w+)[:=]"([^"]+)"|(\w+)[:=](\S+)'
        comparison_pattern = r'(\w+)\s*(<=|>=|<|>|=|!=)\s*(\d+(?:\.\d+)?)'
        keyword_pattern = r'\b(\w+)\b'
        
        # Extract field:value patterns
        for match in re.finditer(field_value_pattern, query):
            field1, value1, field2, value2 = match.groups()
            field = field1 if field1 else field2
            value = value1 if value1 else value2
            
            # Remove the matched part from the query for keyword extraction
            query = query.replace(match.group(0), ' ')
            
            if self._is_numeric_column(field) and value.replace('.', '', 1).isdigit():
                # Handle numeric values
                try:
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                    
                    where_clauses.append(f"{field} = ?")
                    params.append(value)
                except ValueError:
                    # If conversion fails, treat as text
                    where_clauses.append(f"{field} = ?")
                    params.append(value)
            else:
                # Handle text values
                if self.use_fts and self._is_text_column(field):
                    # Use FTS match for text
                    where_clauses.append(f"{field} MATCH ?")
                    params.append(value)
                else:
                    # Use standard equality for text
                    where_clauses.append(f"{field} = ?")
                    params.append(value)
        
        # Extract comparison operators
        for match in re.finditer(comparison_pattern, query):
            field, operator, value = match.groups()
            
            # Remove the matched part from the query for keyword extraction
            query = query.replace(match.group(0), ' ')
            
            # Try to convert value to appropriate type
            try:
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)
            except ValueError:
                continue  # Skip if conversion fails
            
            where_clauses.append(f"{field} {operator} ?")
            params.append(value)
        
        # Extract remaining keywords for full-text search
        if query.strip():
            # Clean up the query by removing operators and extra spaces
            cleaned_query = re.sub(r'\b(AND|OR|NOT)\b', ' ', query, flags=re.IGNORECASE)
            cleaned_query = re.sub(r'\s+', ' ', cleaned_query).strip()
            
            # Extract remaining keywords
            for word in cleaned_query.split():
                if len(word) > 2:  # Skip very short words
                    keywords.append(word)
        
        return where_clauses, params, keywords
    
    def search(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Search using structured query syntax.
        
        This method parses structured query syntax and translates it
        directly into optimized SQL queries.
        
        Args:
            query: The search query with structured syntax
            **kwargs: Additional search parameters
            
        Returns:
            List of search results
        """
        if self.conn is None and not self.connect():
            return []
        
        results = []
        
        try:
            # Parse the structured query
            where_clauses, params, keywords = self._parse_structured_query(query)
            
            # If we have keywords, add them as full-text search or LIKE conditions
            if keywords:
                text_clauses = []
                
                for keyword in keywords:
                    if self.use_fts:
                        # If using FTS, add a MATCH clause for each keyword
                        text_columns = [col for col in self.columns if self._is_text_column(col)]
                        if text_columns:
                            fts_clause = " OR ".join([f"{col} MATCH ?" for col in text_columns])
                            text_clauses.append(f"({fts_clause})")
                            params.extend([keyword] * len(text_columns))
                    else:
                        # Otherwise, use LIKE for each text column
                        like_clauses = []
                        for col in self.columns:
                            if self._is_text_column(col):
                                like_clauses.append(f"{col} LIKE ?")
                                params.append(f"%{keyword}%")
                        
                        if like_clauses:
                            text_clauses.append(f"({' OR '.join(like_clauses)})")
                
                if text_clauses:
                    # Combine all text search clauses with OR
                    combined_text_clause = f"({' OR '.join(text_clauses)})"
                    where_clauses.append(combined_text_clause)
            
            # Build the complete WHERE clause
            if where_clauses:
                # Combine with the specified operator (default is AND)
                where_clause = f" {self.default_operator} ".join(where_clauses)
                sql_query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
            else:
                # No conditions, return all results
                sql_query = f"SELECT * FROM {self.table_name}"
            
            # Execute the query
            cursor = self.conn.cursor()
            cursor.execute(sql_query, params)
            rows = cursor.fetchall()
            
            # Process the results
            for row in rows:
                item = {col: row[col] for col in self.columns}
                mapped_item = self.map_fields(item)
                
                # Compute a relevance score
                score = self._compute_relevance_score(row, query, keywords)
                mapped_item['_score'] = score
                mapped_item['_query_type'] = 'structured'
                
                results.append(mapped_item)
            
            # Sort by score
            results.sort(key=lambda x: x.get('_score', 0), reverse=True)
            
        except Exception as e:
            logger.error(f"Error executing structured search: {e}", exc_info=True)
        
        return results
    
    def _compute_relevance_score(self, row: sqlite3.Row, query: str, keywords: List[str]) -> float:
        """
        Compute a relevance score for a result row.
        
        Args:
            row: Result row from SQLite
            query: Original query string
            keywords: Extracted keywords
            
        Returns:
            Relevance score
        """
        score = 1.0  # Base score for matches
        
        # Boost score based on exact keyword matches
        for keyword in keywords:
            for col in self.columns:
                value = str(row[col]).lower()
                if keyword.lower() in value:
                    # Exact matches get higher score
                    if keyword.lower() == value:
                        score += 2.0
                    else:
                        score += 0.5
        
        # Boost score for matches in important fields
        important_fields = ['name', 'title', 'id', 'description']
        for field in important_fields:
            if field in self.columns and query.lower() in str(row[field]).lower():
                score += 1.0
        
        return score
    
    def get_schema(self) -> Dict[str, str]:
        """
        Get the database schema for the current table.
        
        Returns:
            Dictionary mapping column names to their types
        """
        return self.column_types.copy()
    
    def count(self, query: str = "") -> int:
        """
        Count results for the given query.
        
        Args:
            query: Structured query string (empty for all records)
            
        Returns:
            Count of matching records
        """
        if self.conn is None and not self.connect():
            return 0
        
        try:
            # Parse the structured query
            where_clauses, params, keywords = self._parse_structured_query(query)
            
            # Build the SQL query
            if where_clauses:
                # Combine with the specified operator (default is AND)
                where_clause = f" {self.default_operator} ".join(where_clauses)
                sql_query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {where_clause}"
            else:
                # No conditions, count all records
                sql_query = f"SELECT COUNT(*) FROM {self.table_name}"
            
            # Execute the query
            cursor = self.conn.cursor()
            cursor.execute(sql_query, params)
            count = cursor.fetchone()[0]
            
            return count
            
        except Exception as e:
            logger.error(f"Error executing count query: {e}", exc_info=True)
            return 0
    
    def explain_query(self, query: str) -> Dict[str, Any]:
        """
        Explain how a structured query will be executed.
        
        Args:
            query: Structured query string
            
        Returns:
            Dictionary with query explanation
        """
        if self.conn is None and not self.connect():
            return {"error": "Not connected to database"}
        
        try:
            # Parse the structured query
            where_clauses, params, keywords = self._parse_structured_query(query)
            
            # Build the complete WHERE clause
            if where_clauses:
                # Combine with the specified operator (default is AND)
                where_clause = f" {self.default_operator} ".join(where_clauses)
                sql_query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
            else:
                # No conditions, return all results
                sql_query = f"SELECT * FROM {self.table_name}"
            
            # Get EXPLAIN output
            cursor = self.conn.cursor()
            cursor.execute(f"EXPLAIN QUERY PLAN {sql_query}", params)
            explain_rows = cursor.fetchall()
            
            explanation = {
                "original_query": query,
                "sql_query": sql_query,
                "params": params,
                "where_clauses": where_clauses,
                "keywords": keywords,
                "explain_plan": [dict(row) for row in explain_rows],
                "column_types": self.column_types
            }
            
            return explanation
            
        except Exception as e:
            logger.error(f"Error explaining query: {e}", exc_info=True)
            return {"error": str(e), "query": query}