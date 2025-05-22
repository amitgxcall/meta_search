import sqlite3
import pandas as pd
import os

class LogQueryEngine:
    def __init__(self, csv_file_path):
        """Initialize the query engine with a CSV file path."""
        self.conn = sqlite3.connect(':memory:')  # In-memory database
        self.cursor = self.conn.cursor()
        self.csv_file_path = csv_file_path
        self.table_name = 'logs'
        
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
        
        # Create table with appropriate columns
        columns = []
        for col_name, dtype in zip(df.columns, df.dtypes):
            if 'int' in str(dtype):
                sql_type = 'INTEGER'
            elif 'float' in str(dtype):
                sql_type = 'REAL'
            elif 'datetime' in str(dtype):
                sql_type = 'TIMESTAMP'
            else:
                sql_type = 'TEXT'
                
            columns.append(f'"{col_name}" {sql_type}')
            
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({', '.join(columns)})"
        self.cursor.execute(create_table_sql)
        
        # Insert data into the table
        df.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
        print(f"Loaded {len(df)} rows from {self.csv_file_path}")
        print("All text values have been trimmed of leading/trailing whitespace")
        
    def get_schema(self):
        """Get the schema of the logs table."""
        self.cursor.execute(f"PRAGMA table_info({self.table_name})")
        columns = self.cursor.fetchall()
        schema_info = []
        for col in columns:
            # Format: (id, name, type, notnull, default_value, pk)
            schema_info.append(f"{col[1]} ({col[2]})")
        return schema_info
    
    def execute_query(self, query):
        """Execute a SQL query against the log data."""
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
    
    print("\n-- Available Commands --")
    print("  about             : Show this information")
    print("  schema            : Show the schema of the loaded data")
    print("  exit/quit         : Exit the application")
    print("  Any valid SQL     : Execute SQL query against the log data")
    
    print("\n-- Sample Queries --")
    print("  SELECT * FROM logs LIMIT 5")
    print("  SELECT COUNT(*) FROM logs")
    print("  SELECT DISTINCT column_name FROM logs")
    
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
    # Path to your CSV file
    csv_file_path = input("Enter path to your log CSV file: ").strip()
    
    try:
        # Initialize the query engine and load data
        query_engine = LogQueryEngine(csv_file_path)
        query_engine.load_csv_data()
        
        print("\nLog Query Engine initialized. Enter SQL queries or type 'about' for help.")
        
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