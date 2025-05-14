Hybrid SQLite-Vector Search System Architecture
You're right - a hybrid system that automatically decides between SQLite (for exact/contains queries) and vector search (for semantic similarity) would be much more efficient and robust. Let me outline a modular architecture to implement this approach.
System Overview


meta_search/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── config.py                # Configuration management
│   └── exceptions.py            # Custom exceptions
│
├── providers/
│   ├── __init__.py              # Provider registry
│   ├── base.py                  # Abstract provider class
│   ├── csv_provider.py          # CSV implementation
│   ├── sqlite_provider.py       # SQLite implementation
│   └── hybrid_provider.py       # NEW: Combines SQLite + vector search
│
├── query/
│   ├── __init__.py
│   ├── parser.py                # Query parsing logic
│   ├── temporal.py              # Temporal query handling
│   ├── classification.py        # Query type classification
│   └── filters.py               # Filter extraction
│
├── search/
│   ├── __init__.py
│   ├── engine.py                # Main search coordination
│   ├── structured.py            # Structured search methods
│   ├── vector_search.py         # Vector search methods
│   └── result_formatter.py      # Result formatting
│
├── utils/
│   ├── __init__.py
│   ├── field_mapping.py         # Field mapping utilities
│   ├── cache.py                 # Caching mechanisms
│   └── text_processing.py       # Text processing utilities
│
├── cli.py                       # Command line interface
└── unified_search.py            # Main API