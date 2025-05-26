# Memory-efficient streaming
for row in execute_query_streaming("SELECT * FROM large_table"):
    process_row(row)  # Process one row at a time

# Standard query with limits
result = execute_query("SELECT * FROM users LIMIT 1000")
print(f"Found {result.row_count} rows in {result.execution_time:.2f}s")

# Process large CSV in chunks
for chunk in read_csv_streaming("large_file.csv", chunk_size=500):
    process_dataframe_chunk(chunk)