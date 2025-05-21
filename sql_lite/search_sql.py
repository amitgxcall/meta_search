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
            
        # Read CSV with pandas to automatically detect types
        df = pd.read_csv(self.csv_file_path)
        
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
    
    def execute_query(self, query):
        """Execute a SQL query against the log data."""
        try:
            result = pd.read_sql_query(query, self.conn)
            return result
        except Exception as e:
            return f"Error executing query: {e}"
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


def main():
    # Path to your CSV file
    csv_file_path = input("Enter path to your log CSV file: ").strip()
    
    try:
        # Initialize the query engine and load data
        query_engine = LogQueryEngine(csv_file_path)
        query_engine.load_csv_data()
        
        print("\nLog Query Engine initialized. Enter SQL queries or 'exit' to quit.")
        print("Example query: SELECT * FROM logs LIMIT 5")
        
        while True:
            # Get SQL query from user
            query = input("\nEnter SQL query: ").strip()
            
            if query.lower() in ('exit', 'quit'):
                break
                
            # Execute the query
            result = query_engine.execute_query(query)
            
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