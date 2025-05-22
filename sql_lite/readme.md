# Log Query Engine - Usage Guide

## Overview
The Log Query Engine allows you to load CSV log data into SQLite and execute SQL queries against it. The engine automatically trims whitespace from text fields and provides both interactive and programmatic interfaces.

## Setup

### 1. Configure CSV File Path
Edit the `CSV_FILE_PATH` variable at the top of `log_query_engine.py`:

```python
CSV_FILE_PATH = "path/to/your/logs.csv"  # Change this to your CSV file path
```

### 2. Required Dependencies
```bash
pip install pandas sqlite3
```

## Interactive Usage

Run the script directly for interactive mode:
```bash
python log_query_engine.py
```

Available commands:
- `about` - Show application information
- `schema` - Display table schema
- `exit` or `quit` - Exit the application
- Any valid SQL query - Execute against the log data

## Programmatic Usage

### Import and Basic Usage
```python
from log_query_engine import LogQueryEngine

# Initialize engine (uses hardcoded CSV_FILE_PATH)
engine = LogQueryEngine()

# Execute a simple query and get DataFrame
df = engine.run_sql_simple("SELECT * FROM logs LIMIT 10")
if df is not None:
    print(df)

# Clean up
engine.close()
```

### Using Custom CSV File
```python
# Use a different CSV file
engine = LogQueryEngine("path/to/different/file.csv")

# Execute query
result = engine.run_sql("SELECT COUNT(*) FROM logs")
if result['success']:
    print(f"Total rows: {result['data'].iloc[0, 0]}")

engine.close()
```

### Detailed Query Results
```python
from log_query_engine import LogQueryEngine

engine = LogQueryEngine()

# Execute query with detailed results
query = "SELECT level, COUNT(*) as count FROM logs GROUP BY level"
result = engine.run_sql(query)

if result['success']:
    print(f"Query successful!")
    print(f"Rows returned: {result['row_count']}")
    print(f"Columns: {result['columns']}")
    print("Data:")
    print(result['data'])
else:
    print(f"Query failed: {result['error']}")

engine.close()
```

### Simple Query Results
```python
from log_query_engine import LogQueryEngine

engine = LogQueryEngine()

# Simple method - returns DataFrame or None
df = engine.run_sql_simple("SELECT * FROM logs WHERE level = 'ERROR'")

if df is not None:
    print(f"Found {len(df)} error records")
    print(df[['timestamp', 'message']])  # Display specific columns
else:
    print("Query failed or returned no results")

engine.close()
```

## Example Usage Script

Create a file called `query_example.py`:

```python
from log_query_engine import LogQueryEngine

def analyze_logs():
    # Initialize the engine
    engine = LogQueryEngine()
    
    try:
        # Get total record count
        result = engine.run_sql("SELECT COUNT(*) as total FROM logs")
        if result['success']:
            total_records = result['data'].iloc[0, 0]
            print(f"Total log records: {total_records}")
        
        # Get error distribution
        error_query = """
        SELECT level, COUNT(*) as count 
        FROM logs 
        WHERE level IN ('ERROR', 'WARNING', 'CRITICAL')
        GROUP BY level 
        ORDER BY count DESC
        """
        
        errors = engine.run_sql_simple(error_query)
        if errors is not None and not errors.empty:
            print("\nError Distribution:")
            print(errors)
        
        # Get recent errors
        recent_errors = engine.run_sql_simple("""
        SELECT timestamp, level, message 
        FROM logs 
        WHERE level = 'ERROR' 
        ORDER BY timestamp DESC 
        LIMIT 5
        """)
        
        if recent_errors is not None and not recent_errors.empty:
            print("\nRecent Errors:")
            print(recent_errors)
    
    finally:
        engine.close()

if __name__ == "__main__":
    analyze_logs()
```

## Method Reference

### LogQueryEngine Methods

#### `__init__(csv_file_path=None)`
- Initialize the engine
- If `csv_file_path` is None, uses the hardcoded `CSV_FILE_PATH`

#### `run_sql(sql_query)`
Returns detailed results as a dictionary:
```python
{
    'success': bool,      # True if query succeeded
    'data': DataFrame,    # Query results (or None if failed)
    'error': str,         # Error message (or None if succeeded)
    'row_count': int,     # Number of rows returned
    'columns': list       # List of column names
}
```

#### `run_sql_simple(sql_query)`
Returns pandas DataFrame directly, or None if query fails.

#### `get_schema()`
Returns list of column definitions as strings (e.g., "column_name (TEXT)")

#### `close()`
Closes the database connection. Always call this when done.

## SQL Query Examples

```sql
-- Basic queries
SELECT * FROM logs LIMIT 10;
SELECT COUNT(*) FROM logs;

-- Filtering
SELECT * FROM logs WHERE level = 'ERROR';
SELECT * FROM logs WHERE timestamp > '2024-01-01';

-- Aggregation
SELECT level, COUNT(*) FROM logs GROUP BY level;
SELECT DATE(timestamp) as date, COUNT(*) FROM logs GROUP BY DATE(timestamp);

-- Sorting
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 20;

-- Complex queries
SELECT 
    level,
    COUNT(*) as count,
    MIN(timestamp) as first_occurrence,
    MAX(timestamp) as last_occurrence
FROM logs 
WHERE level IN ('ERROR', 'CRITICAL')
GROUP BY level;
```

## Notes

- The table name is always `logs`
- Text values are automatically trimmed of whitespace
- Data is loaded into an in-memory SQLite database
- The engine automatically loads data on first query if not already loaded
- Always call `close()` to free resources