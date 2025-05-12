#!/usr/bin/env python3
"""
Test script for the job search system.
"""

from meta_search import UnifiedJobSearch

def main():
    """Run basic tests for the search system."""
    # Initialize with sample data
    print("Initializing search with sample data...")
    search = UnifiedJobSearch("data/job_details.csv")
    
    # Test 1: Simple search
    print("\n===== Test 1: Simple Search =====")
    query = "failed jobs"
    print(f"Query: '{query}'")
    results = search.search(query)
    print(f"Found {len(results)} results")
    search.display_results(results)
    
    # Test 2: Field-specific search
    print("\n===== Test 2: Field-Specific Search =====")
    query = "status:failed priority:high"
    print(f"Query: '{query}'")
    results = search.search(query)
    print(f"Found {len(results)} results")
    search.display_results(results)
    
    # Test 3: Temporal search
    print("\n===== Test 3: Temporal Search =====")
    query = "jobs from last 7 days"
    print(f"Query: '{query}'")
    results = search.search(query)
    print(f"Found {len(results)} results")
    search.display_results(results)
    
    # Test 4: Numeric comparison
    print("\n===== Test 4: Numeric Comparison =====")
    query = "duration_minutes>30"
    print(f"Query: '{query}'")
    results = search.search(query)
    print(f"Found {len(results)} results")
    search.display_results(results)
    
    # Test 5: LLM formatting
    print("\n===== Test 5: LLM Formatting =====")
    query = "database jobs"
    print(f"Query: '{query}'")
    results = search.search(query)
    llm_format = search.format_for_llm(results, query)
    
    # Print formatted output
    import json
    print(json.dumps(llm_format, indent=2))
    
    # Test 6: Explain search
    print("\n===== Test 6: Explain Search =====")
    query = "recent failed database jobs"
    explanation = search.explain_search(query)
    print(json.dumps(explanation, indent=2))
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    main()