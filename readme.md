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
from meta_search import UnifiedJobSearch, FieldMapping

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

1. Subclass `DataProvider` from `meta_search.data_provider`
2. Implement all required methods
3. Register in `meta_search.providers.__init__.py`

```python
from meta_search.data_provider import DataProvider

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

meta_search/
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


Meta Search System: Search Query Prioritization and Results Calculation
The Meta Search system is a versatile search framework designed to work with multiple data sources (CSV, SQLite, JSON) using a hybrid approach that combines structured querying and vector-based semantic search. The system intelligently prioritizes different search strategies based on query type and calculates relevance scores using sophisticated algorithms.
Search Query Prioritization
The system analyzes incoming queries and prioritizes them as follows:

Counting Queries: Queries that ask "how many" or include counting keywords like "count", "total", "number of", etc. are detected and processed specially to return count information rather than full records.
Structured Queries: Queries with explicit field:value patterns (e.g., "status:failed"), comparison operators (>, <, >=, <=), or specific keywords that map to filters are prioritized for exact matching.
Semantic Queries: Natural language queries that don't contain structured patterns are processed using semantic similarity.
Hybrid Queries: Queries that contain both structured elements and semantic keywords are processed using a combination of approaches.

The system also has special handling for ID-based queries, detecting patterns like "id 123" or "job id 456" to perform direct record lookups instead of search.
Results Calculation
The system calculates search result relevance in different ways depending on the search strategy:
Structured Search (SQLite)
For structured queries, the system:

Translates field:value patterns and comparison operators directly into SQL conditions
Performs exact matching for field values
Uses appropriate comparison operators for numeric fields
Calculates relevance scores based on the number and quality of condition matches
Boosts scores for matches in important fields (name, title, id, description)

Text Search (CSV)
For simple text search in CSV files, the system:

Checks if the query appears in any field
Assigns higher scores (10 points) for exact matches
Assigns medium scores (5 points) for partial matches
Assigns lower scores (1 point) for individual word matches
Sorts results by total score

Vector Search
The vector search implementation:

Normalizes query vectors and document vectors
Uses cosine similarity between vectors (dot product of normalized vectors)
Returns results sorted by similarity score
Leverages FAISS for efficient vector search when available, falling back to numpy for simpler cases

Hybrid Search
The hybrid provider combines results from both structured and vector searches:

Executes both search strategies separately
Normalizes scores from each approach (dividing by the maximum score)
Combines normalized scores using a configurable weight parameter:
combined_score = (1-weight)*structured_score + weight*vector_score
Ranks results by combined score

This approach allows balancing between exact matching (structured search) and semantic similarity (vector search) based on the query's characteristics and user preferences.
Advanced Features
The system includes several advanced features that enhance search effectiveness:

Query Classification: Automatically determines the best search strategy based on query patterns
Filter Extraction: Parses structured filters and comparison operators from queries
Temporal Query Handling: Processes time-based queries like "jobs from last 7 days"
Field Weighting: Assigns different weights to fields based on importance (name fields get higher weight than description fields, etc.)
Result Formatting: Formats search results for different outputs including console display, LLM consumption, and exports to JSON/CSV

This architecture provides a flexible and powerful search system that can handle both precise structured queries and fuzzy natural language searches across different data sources.