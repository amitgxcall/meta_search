Hybrid SQLite + Vector Search Architecture
Let me outline a comprehensive hybrid solution that intelligently selects between SQLite for exact/contains queries and FAISS for similarity search.
Core Architecture

┌─────────────┐     ┌───────────────────┐     ┌───────────────┐
│ User Query  │────▶│ Query Classifier  │────▶│ Query Parser  │
└─────────────┘     └───────────────────┘     └───────┬───────┘
                                                     │
                                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Search Coordinator                       │
└───────────┬─────────────────────────────────────┬───────────────┘
            │                                     │
            ▼                                     ▼
┌───────────────────┐                   ┌───────────────────┐
│  SQLite Engine    │                   │   Vector Engine   │
│  (Exact Matches)  │                   │  (Similarity)     │
└────────┬──────────┘                   └─────────┬─────────┘
         │                                        │
         └────────────────┬─────────────────────┐│
                          ▼                     ▼
                ┌───────────────────────────────────┐
                │        Results Combiner           │
                └───────────────────┬───────────────┘
                                    │
                                    ▼
                          ┌───────────────────┐
                          │  Formatted Output │
                          └───────────────────┘