#!/usr/bin/env python3
"""
Test script for the job search system.
"""

import os
import sys
from meta_search.unified_search import UnifiedJobSearch

def main():
    """Run basic tests for the search system."""
    # Print diagnostics about current working directory
    print(f"Current working directory: {os.getcwd()}")
    
    # Try different possible locations
    possible_paths = [
        # Relative to current working directory
        "data/job_details.csv",
        "data/sample_job_details.csv",
        "./data/job_details.csv",
        "./data/sample_job_details.csv",
        
        # Relative to the script directory
        os.path.join(os.path.dirname(__file__), "data", "job_details.csv"),
        os.path.join(os.path.dirname(__file__), "data", "sample_job_details.csv"),
        
        # Relative to project root
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "job_details.csv"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sample_job_details.csv"),
        
        # Relative to meta_search directory
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "meta_search", "data", "job_details.csv"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "meta_search", "data", "sample_job_details.csv"),
    ]
    
    # Find the first valid path
    valid_path = None
    print("\nChecking possible file paths:")
    for path in possible_paths:
        exists = os.path.exists(path)
        print(f"  {path}: {'EXISTS' if exists else 'not found'}")
        if exists and not valid_path:
            valid_path = path
    
    if not valid_path:
        print("\nERROR: Could not find job details CSV file in any expected location.")
        print("\nCurrent directory contents:")
        for item in os.listdir():
            print(f"  {item}")
        
        data_dir = os.path.join(os.getcwd(), 'data')
        if os.path.exists(data_dir):
            print("\nFiles in data directory:")
            for item in os.listdir(data_dir):
                print(f"  {item}")
        else:
            print("\nNo 'data' directory found in current working directory.")
            
        meta_search_dir = os.path.join(os.getcwd(), 'meta_search')
        if os.path.exists(os.path.join(meta_search_dir, 'data')):
            print("\nFiles in meta_search/data directory:")
            for item in os.listdir(os.path.join(meta_search_dir, 'data')):
                print(f"  {item}")
        
        print("\nConsider creating the data file at one of these locations, or update the path in the code.")
        sys.exit(1)
    
    print(f"\nUsing data file: {valid_path}")
    
    # Initialize with found data
    print("Initializing search with sample data...")
    search = UnifiedJobSearch(valid_path)
    
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