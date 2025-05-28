import sqlite3
import pandas as pd
import os
from datetime import datetime

# Configuration - Hardcode your CSV file path here
CSV_FILE_PATH = "logs.csv"  # Change this to your CSV file path

class LogQueryEngine:
    def __init__(self, csv_file_path=None):
        """Initialize the query engine with a CSV file path."""
        self.conn = sqlite3.connect(':memory:')  # In-memory database
        self.cursor = self.conn.cursor()
        self.csv_file_path = csv_file_path or CSV_FILE_PATH
        self.table_name = 'logs'
        self.data_loaded = False
        
    def _infer_column_type(self, col_name, dtype, series):
        """Infer the appropriate SQL type for a column based on name patterns and data type."""
        col_name_lower = col_name.lower()
        
        # Handle count columns - force to INTEGER
        if col_name_lower.endswith('count') or col_name_lower.startswith('count'):
            return 'INTEGER'
        
        # Handle datetime columns - columns ending with _dt
        if col_name_lower.endswith('_dt'):
            return 'TIMESTAMP'
        
        # Handle other datetime-like column names
        datetime_indicators = ['date', 'time', 'timestamp', 'created', 'updated', 'modified']
        if any(indicator in col_name_lower for indicator in datetime_indicators):
            # Check if the data looks like datetime
            if not series.empty and series.notna().any():
                sample_value = str(series.dropna().iloc[0])
                # Simple check for common datetime patterns
                if any(char in sample_value for char in ['-', '/', ':', 'T']) and len(sample_value) > 8:
                    return 'TIMESTAMP'
        
        # Standard type inference
        if 'int' in str(dtype):
            return 'INTEGER'
        elif 'float' in str(dtype):
            return 'REAL'
        elif 'datetime' in str(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
        
    def _process_datetime_column(self, series, col_name):
        """Process a datetime column to ensure proper format."""
        try:
            # Try to convert to datetime if it's not already
            if series.dtype != 'datetime64[ns]':
                # Common datetime formats to try
                datetime_formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%SZ',
                    '%Y-%m-%d',
                    '%m/%d/%Y %H:%M:%S',
                    '%m/%d/%Y',
                    '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y'
                ]
                
                # Try pandas to_datetime first (it's quite flexible)
                try:
                    return pd.to_datetime(series, errors='coerce')
                except:
                    # If that fails, try specific formats
                    for fmt in datetime_formats:
                        try:
                            return pd.to_datetime(series, format=fmt, errors='coerce')
                        except:
                            continue
            
            return series
            
        except Exception as e:
            print(f"Warning: Could not process datetime column '{col_name}': {e}")
            return series
    
    def _process_count_column(self, series, col_name):
        """Process a count column to ensure it's integer."""
        try:
            # Convert to numeric, coercing errors to NaN
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            # Convert to integer, filling NaN with 0 (or you could use a different default)
            return numeric_series.fillna(0).astype('Int64')  # Nullable integer type
            
        except Exception as e:
            print(f"Warning: Could not process count column '{col_name}': {e}")
            return series
        
    def load_csv_data(self):
        """Load data from CSV into SQLite database."""
        if not os.path.exists(self.csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {self.csv_file_path}")
            
        # Read CSV with pandas
        df = pd.read_csv(self.csv_file_path)
        
        # Trim whitespace from all string/object columns
        for column in df.columns:
            if df[column].dtype == 'object':  # String columns
                df[column] = df[column].astype(str).str.strip()
                # Convert 'nan' strings back to actual NaN values
                df[column] = df[column].replace('nan', pd.NA)
        
        # Process special column types
        processed_columns = []
        for column in df.columns:
            col_name_lower = column.lower()
            
            # Process count columns
            if col_name_lower.endswith('count') or col_name_lower.startswith('count'):
                df[column] = self._process_count_column(df[column], column)
                processed_columns.append(f"{column} (processed as count/integer)")
            
            # Process datetime columns
            elif col_name_lower.endswith('_dt') or any(indicator in col_name_lower for indicator in ['date', 'time', 'timestamp']):
                original_dtype = df[column].dtype
                df[column] = self._process_datetime_column(df[column], column)
                if df[column].dtype != original_dtype:
                    processed_columns.append(f"{column} (processed as datetime)")
        
        # Create table with appropriate columns
        columns = []
        for col_name in df.columns:
            sql_type = self._infer_column_type(col_name, df[col_name].dtype, df[col_name])
            columns.append(f'"{col_name}" {sql_type}')
            
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({', '.join(columns)})"
        self.cursor.execute(create_table_sql)
        
        # Insert data into the table
        df.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
        
        print(f"Loaded {len(df)} rows from {self.csv_file_path}")
        print("All text values have been trimmed of leading/trailing whitespace")
        
        if processed_columns:
            print("Special column processing applied:")
            for col_info in processed_columns:
                print(f"  - {col_info}")
        
        self.data_loaded = True
        
    def get_schema(self):
        """Get the schema of the logs table."""
        if not self.data_loaded:
            self.load_csv_data()
            
        self.cursor.execute(f"PRAGMA table_info({self.table_name})")
        columns = self.cursor.fetchall()
        schema_info = []
        for col in columns:
            # Format: (id, name, type, notnull, default_value, pk)
            col_name = col[1]
            col_type = col[2]
            
            # Add special indicators for our processed columns
            if col_name.lower().endswith('count') or col_name.lower().startswith('count'):
                col_type += " [COUNT]"
            elif col_name.lower().endswith('_dt'):
                col_type += " [DATETIME]"
            
            schema_info.append(f"{col_name} ({col_type})")
        return schema_info
    
    def execute_query(self, query):
        """Execute a SQL query against the log data."""
        if not self.data_loaded:
            self.load_csv_data()
            
        try:
            result = pd.read_sql_query(query, self.conn)
            return result
        except Exception as e:
            return f"Error executing query: {e}"
    
    def run_sql(self, sql_query):
        """
        Execute SQL query and return results in a structured format.
        
        Args:
            sql_query (str): The SQL query to execute
            
        Returns:
            dict: Dictionary containing:
                - 'success': Boolean indicating if query was successful
                - 'data': DataFrame with results (if successful) or None
                - 'error': Error message (if unsuccessful) or None
                - 'row_count': Number of rows returned (if successful)
                - 'columns': List of column names (if successful)
        """
        if not self.data_loaded:
            self.load_csv_data()
            
        try:
            # Execute the query
            result_df = pd.read_sql_query(sql_query, self.conn)
            
            return {
                'success': True,
                'data': result_df,
                'error': None,
                'row_count': len(result_df),
                'columns': list(result_df.columns) if not result_df.empty else []
            }
            
        except Exception as e:
            return {
                'success': False,
                'data': None,
                'error': str(e),
                'row_count': 0,
                'columns': []
            }
    
    def run_sql_simple(self, sql_query):
        """
        Execute SQL query and return just the DataFrame (simpler version).
        
        Args:
            sql_query (str): The SQL query to execute
            
        Returns:
            pandas.DataFrame or None: Query results or None if error occurred
        """
        if not self.data_loaded:
            self.load_csv_data()
            
        try:
            return pd.read_sql_query(sql_query, self.conn)
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


def show_about(query_engine):
    """Display information about the application and the loaded data."""
    print("\n===== Log Query Engine =====")
    print("A simple SQLite-based tool for querying log data from CSV files.")
    print("All text values are automatically trimmed of leading/trailing whitespace.")
    print("Special column processing:")
    print("  - Columns ending with 'count' or starting with 'count' → INTEGER")
    print("  - Columns ending with '_dt' → TIMESTAMP (datetime)")
    
    print("\n-- Available Commands --")
    print("  about             : Show this information")
    print("  schema            : Show the schema of the loaded data")
    print("  exit/quit         : Exit the application")
    print("  Any valid SQL     : Execute SQL query against the log data")
    
    print("\n-- Sample Queries --")
    print("  SELECT * FROM logs LIMIT 5")
    print("  SELECT COUNT(*) FROM logs")
    print("  SELECT DISTINCT column_name FROM logs")
    print("  SELECT * FROM logs WHERE some_count > 0")
    print("  SELECT * FROM logs WHERE some_dt > '2024-01-01'")
    
    print("\n-- Currently Loaded Data --")
    print(f"  File: {query_engine.csv_file_path}")
    
    # Get row count
    result = query_engine.execute_query("SELECT COUNT(*) FROM logs")
    if isinstance(result, pd.DataFrame):
        row_count = result.iloc[0, 0]
        print(f"  Rows: {row_count}")
    
    # Get schema info
    print("\n-- Schema --")
    schema = query_engine.get_schema()
    for column in schema:
        print(f"  {column}")


def main():
    try:
        # Initialize the query engine (uses hardcoded CSV_FILE_PATH)
        query_engine = LogQueryEngine()
        
        print(f"Using CSV file: {query_engine.csv_file_path}")
        print("Log Query Engine initialized. Enter SQL queries or type 'about' for help.")
        
        while True:
            # Get input from user
            user_input = input("\nEnter command or SQL query: ").strip()
            
            # Check for special commands
            if user_input.lower() in ('exit', 'quit'):
                break
            elif user_input.lower() == 'about':
                show_about(query_engine)
            elif user_input.lower() == 'schema':
                schema = query_engine.get_schema()
                print("\nTable Schema:")
                for column in schema:
                    print(f"  {column}")
            else:
                # Execute as SQL query
                result = query_engine.execute_query(user_input)
                
                # Display the result
                if isinstance(result, pd.DataFrame):
                    if len(result) > 0:
                        print(f"\nQuery returned {len(result)} rows:")
                        print(result)
                    else:
                        print("Query returned no results.")
                else:
                    print(result)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up
        if 'query_engine' in locals():
            query_engine.close()
            print("Query engine closed.")


if __name__ == "__main__":
    main()