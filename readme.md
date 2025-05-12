# Data-Agnostic Job Search System

A flexible, extensible system for searching job data from various sources using a hybrid of structured queries and vector similarity search.

## Features

- **Data Source Agnostic**: Works with various data sources (CSV, SQLite, etc.)
- **Flexible Field Mapping**: Adapts to different field naming conventions
- **Hybrid Search**: Combines structured queries with vector similarity search
- **Natural Language Understanding**: Parses natural language queries
- **Caching**: Optimizes performance with vector caching
- **LLM Integration**: Built-in formatting for LLM consumption

## Architecture

The system is built with a modular, extensible architecture:

- **Data Providers**: Abstract interface for different data sources
  - CSV Provider: Loads data from CSV files
  - SQLite Provider: Loads data from SQLite databases
  - (Extensible for other data sources)

- **Search Engine**: Coordinates search across data providers
  - Query Classification: Determines query type
  - Structured Search: Exact filtering
  - Vector Search: Semantic similarity
  - Result Merging: Combines results from both approaches

- **Unified Search Interface**: Provides a simple, consistent API

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Command-line Interface

```bash
# Basic usage with auto-detection of source type
python search_cli.py --data-source job_details.csv

# Specifying source type
python search_cli.py --data-source jobs.db --source-type sqlite

# One-time query
python search_cli.py --data-source job_details.csv --query "latest failed jobs"

# Format for LLM
python search_cli.py --data-source job_details.csv --query "latest failed jobs" --llm

# Custom field mapping
python search_cli.py --data-source tasks.csv --custom-fields --id-field task_id --name-field task_name --status-field state
```

### Python API

```python
from job_search import UnifiedJobSearch, FieldMapping

# Basic usage with auto-detection
search = UnifiedJobSearch("job_details.csv")

# Custom field mapping
field_mapping = FieldMapping(
    id_field="task_id",
    name_field="task_name",
    status_field="state"
)
search = UnifiedJobSearch("tasks.csv", field_mapping=field_mapping)

# Search
results = search.search("latest failed jobs")

# Display results
search.display_results(results)

# Format for LLM
llm_data = search.format_for_llm(results, "latest failed jobs")
```

## Example Queries

The system supports various query types:

- **Natural Language**: "find failed jobs", "show jobs started by admin"
- **Temporal**: "latest jobs", "jobs from last 7 days"
- **Field-specific**: "job_name:database_backup", "status:failed" 
- **Numeric**: "duration_minutes>30", "cpu_usage_percent<80"
- **Combined**: "latest database jobs that failed", "high priority jobs with errors"

## Extending the System

### Adding a New Data Provider

1. Subclass `DataProvider` from `job_search.data_provider`
2. Implement all required methods
3. Register in `job_search.providers.__init__.py`

```python
from job_search.data_provider import DataProvider

class MyCustomProvider(DataProvider):
    def __init__(self, data_source, field_mapping=None):
        # Initialize provider
        ...
    
    def get_all_fields(self):
        # Return list of available fields
        ...
    
    # Implement other required methods
    ...
```

## License

MIT

job_search/
│
├── __init__.py
├── providers/
│   ├── __init__.py
│   ├── base.py              # Abstract base provider
│   ├── csv_provider.py      # CSV implementation
│   ├── sqlite_provider.py   # SQLite implementation
│   └── json_provider.py     # JSON implementation
│
├── search/
│   ├── __init__.py
│   ├── engine.py            # Main search engine
│   ├── query_classifier.py  # Query type classification
│   ├── query_patterns.py    # Pattern matching for queries
│   ├── vector_search.py     # Vector similarity search
│   └── result_formatter.py  # Result formatting utilities
│
├── utils/
│   ├── __init__.py
│   ├── field_mapping.py     # Field mapping functionality
│   ├── cache.py             # Caching utilities
│   └── text_processing.py   # Text processing helpers
│
├── cli.py                   # Command-line interface
└── unified_search.py        # Unified search API# meta_search
