#!/usr/bin/env python3
"""
Test script for the data-agnostic search system.

This script demonstrates the functionality of the refactored data-agnostic 
search system by:
1. Creating a simple test dataset
2. Testing different search features
3. Verifying field mapping functionality
4. Testing exports and formatting

Usage:
    python test_data_agnostic.py
"""

import os
import sys
import csv
import json
import tempfile
import time
from typing import List, Dict, Any

# Helper function to find the project root
def find_project_root():
    """Find the project root directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while True:
        if os.path.exists(os.path.join(current_dir, "unified_search.py")):
            return current_dir
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            # Reached root directory without finding project
            return os.path.dirname(os.path.abspath(__file__))
        current_dir = parent_dir

# Add project root to path
project_root = find_project_root()
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now import our modules
try:
    from unified_search import UnifiedSearch
    from utils.field_mapping import FieldMapping
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running from the project root or have the package installed.")
    sys.exit(1)

# Create sample data for testing
def create_sample_products_csv():
    """Create a sample products CSV for testing."""
    fd, path = tempfile.mkstemp(suffix='.csv')
    
    try:
        with os.fdopen(fd, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow([
                "product_id", "product_name", "category", "price", 
                "stock", "status", "description", "created_at"
            ])
            
            # Write data
            writer.writerow([
                "P001", "Organic Apples", "Produce", "4.99", 
                "250", "in_stock", "Fresh organic apples from local farms", "2025-01-15"
            ])
            writer.writerow([
                "P002", "Premium Coffee", "Beverages", "12.99", 
                "100", "in_stock", "Premium organic fair-trade coffee", "2025-01-20"
            ])
            writer.writerow([
                "P003", "Whole Wheat Bread", "Bakery", "3.49", 
                "50", "in_stock", "Freshly baked whole wheat bread", "2025-01-22"
            ])
            writer.writerow([
                "P004", "Organic Milk", "Dairy", "4.29", 
                "75", "in_stock", "Organic whole milk from grass-fed cows", "2025-01-18"
            ])
            writer.writerow([
                "P005", "Chocolate Cookies", "Bakery", "5.99", 
                "0", "out_of_stock", "Delicious chocolate chip cookies", "2025-01-10"
            ])
            writer.writerow([
                "P006", "Natural Honey", "Pantry", "8.99", 
                "30", "in_stock", "Locally sourced pure natural honey", "2025-01-25"
            ])
            writer.writerow([
                "P007", "Organic Chicken", "Meat", "9.99", 
                "15", "low_stock", "Free-range organic chicken", "2025-01-21"
            ])
            writer.writerow([
                "P008", "Premium Olive Oil", "Pantry", "15.99", 
                "40", "in_stock", "Extra virgin olive oil from Italy", "2025-01-12"
            ])
            writer.writerow([
                "P009", "Sourdough Bread", "Bakery", "4.99", 
                "25", "in_stock", "Traditional sourdough bread baked daily", "2025-01-24"
            ])
            writer.writerow([
                "P010", "Dark Chocolate", "Snacks", "3.49", 
                "60", "in_stock", "Premium dark chocolate with 70% cocoa", "2025-01-19"
            ])
        
        print(f"Created sample products CSV at {path}")
        return path
    except Exception as e:
        print(f"Error creating sample CSV: {e}")
        os.unlink(path)
        sys.exit(1)

def create_sample_mapping_file():
    """Create a sample mapping file for testing."""
    fd, path = tempfile.mkstemp(suffix='.json')
    
    mapping = {
        "id": "product_id",
        "name": "product_name",
        "status": "status",
        "timestamp_fields": ["created_at"],
        "numeric_fields": ["price", "stock"],
        "text_fields": ["description", "category"]
    }
    
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(mapping, f, indent=2)
        
        print(f"Created sample mapping file at {path}")
        return path
    except Exception as e:
        print(f"Error creating mapping file: {e}")
        os.unlink(path)
        sys.exit(1)

def run_tests(data_path, mapping_path):
    """Run tests on the data-agnostic search system."""
    print("\n=== Running Tests ===\n")
    
    # Test 1: Basic search with auto-detection
    print("Test 1: Basic search with auto-detection")
    search = UnifiedSearch(data_path)
    results = search.search("organic")
    print(f"Found {len(results)} results for 'organic'")
    search.display_results(results)
    
    # Test 2: Search with explicit field mapping
    print("\nTest 2: Search with explicit field mapping")
    field_mapping = FieldMapping(
        id_field="product_id",
        name_field="product_name",
        status_field="status",
        numeric_fields=["price", "stock"],
        text_fields=["description", "category"]
    )
    search = UnifiedSearch(data_path, field_mapping=field_mapping)
    results = search.search("bakery")
    print(f"Found {len(results)} results for 'bakery'")
    search.display_results(results)
    
    # Test 3: Search with mapping file
    print("\nTest 3: Search with mapping file")
    search = UnifiedSearch(data_path, mapping_file=mapping_path)
    results = search.search("price>10")
    print(f"Found {len(results)} results for 'price>10'")
    search.display_results(results)
    
    # Test 4: Structured search
    print("\nTest 4: Structured search")
    results = search.search("category:Bakery")
    print(f"Found {len(results)} results for 'category:Bakery'")
    search.display_results(results)
    
    # Test 5: Getting by ID
    print("\nTest 5: Getting by ID")
    item = search.get_record_by_id("P001")
    if item:
        print("Found item with ID P001:")
        for key, value in item.items():
            print(f"  {key}: {value}")
    else:
        print("Failed to find item with ID P001")
    
    # Test 6: Counting query
    print("\nTest 6: Counting query")
    count_result = search.count_records("status:in_stock")
    print(f"Count result: {count_result}")
    
    # Test 7: Export to JSON
    print("\nTest 7: Export to JSON")
    results = search.search("in_stock")
    json_output = search.export_results(results, format="json")
    print(f"JSON output sample: {json_output[:200]}...")
    
    # Test 8: Export to CSV
    print("\nTest 8: Export to CSV")
    csv_output = search.export_results(results, format="csv")
    print(f"CSV output sample: {csv_output[:200]}...")
    
    # Test 9: Format for LLM
    print("\nTest 9: Format for LLM")
    llm_format = search.format_for_llm(results, "in_stock")
    print(f"Suggested response: {llm_format['suggested_response']}")
    
    # Test 10: Explain search
    print("\nTest 10: Explain search")
    explanation = search.explain_search("organic products in category:Produce")
    print("Search explanation:")
    for key, value in explanation.items():
        print(f"  {key}: {value}")
    
    print("\nAll tests completed!")

def cleanup(file_paths):
    """Clean up temporary files."""
    for path in file_paths:
        try:
            os.unlink(path)
            print(f"Cleaned up {path}")
        except Exception as e:
            print(f"Error cleaning up {path}: {e}")

def main():
    """Main entry point for test script."""
    print("=== Data-Agnostic Search System Test ===")
    
    # Create test files
    data_path = create_sample_products_csv()
    mapping_path = create_sample_mapping_file()
    
    try:
        # Run tests
        run_tests(data_path, mapping_path)
    finally:
        # Clean up
        cleanup([data_path, mapping_path])

if __name__ == "__main__":
    main()