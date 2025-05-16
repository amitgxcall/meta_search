#!/usr/bin/env python3
"""
Data-agnostic vector search index inspection utility.

This script loads a vector search index from a specified file path,
displays statistics about the index, and shows information about records.
It works with any data structure, not just job-specific data.

Usage:
    # Basic usage
    python test_vector_index.py --index-path your_vector_dir
    
    # With field mapping from JSON
    python test_vector_index.py --index-path your_vector_dir --mapping-file field_mapping.json
    
    # With field mapping from CSV headers
    python test_vector_index.py --index-path your_vector_dir --csv-file data.csv

Arguments:
    --index-path: Path to the vector index file or directory
    --mapping-file: JSON file with field mappings (optional)
    --csv-file: CSV file to extract headers for mapping (optional)
    --id-field: Field to use as ID (default: "id")
    --name-field: Field to use as name/title (default: "name")
    --limit: Maximum number of records to display (default: 10)
    --verbose: Show more detailed information about each record
"""

import os
import sys
import argparse
import pickle
import numpy as np
import json
import csv
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("VectorIndexTest")

# Try to import FAISS
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    logger.warning("FAISS not available. Some functionality may be limited.")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Vector index inspection tool')
    parser.add_argument(
        '--index-path', '-i', required=True,
        help='Path to the vector index file or directory containing index.pkl and index.faiss'
    )
    parser.add_argument(
        '--mapping-file', '-m',
        help='JSON file with field mappings'
    )
    parser.add_argument(
        '--csv-file', '-c',
        help='CSV file to extract headers for mapping'
    )
    parser.add_argument(
        '--id-field',
        default='id',
        help='Field to use as ID (default: "id")'
    )
    parser.add_argument(
        '--name-field',
        default='name',
        help='Field to use as name/title (default: "name")'
    )
    parser.add_argument(
        '--limit', '-l', type=int, default=10,
        help='Maximum number of records to display (default: 10)'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Show more detailed information about each record'
    )
    parser.add_argument(
        '--generate-mapping', '-g',
        help='Generate field mapping from data and save to specified file'
    )
    parser.add_argument(
        '--search', '-s',
        help='Search the vector index with a text query'
    )
    
    return parser.parse_args()

def load_vector_index(file_path):
    """
    Load a vector index from a file.
    
    Args:
        file_path: Path to the vector index file or directory
        
    Returns:
        Loaded vector index data or None if loading fails
    """
    # Check if path exists
    if not os.path.exists(file_path):
        logger.error(f"Vector index file not found: {file_path}")
        return None
    
    # Handle directory path with index.pkl and index.faiss
    if os.path.isdir(file_path):
        pkl_path = os.path.join(file_path, "index.pkl")
        faiss_path = os.path.join(file_path, "index.faiss")
        
        if os.path.exists(pkl_path) and os.path.exists(faiss_path):
            logger.info(f"Found index.pkl and index.faiss in directory: {file_path}")
            try:
                # Load the pickle file
                with open(pkl_path, 'rb') as f:
                    data = pickle.load(f)
                
                # Add the faiss path
                data["faiss_path"] = faiss_path
                data["use_faiss"] = True
                
                logger.info(f"Vector index loaded from {pkl_path}")
                return data
            except Exception as e:
                logger.error(f"Error loading vector index from {pkl_path}: {e}")
                return None
    
    # Handle direct file path
    try:
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        
        logger.info(f"Vector index loaded from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error loading vector index: {e}")
        return None

def load_field_mapping(mapping_file=None, csv_file=None, id_field='id', name_field='name'):
    """
    Load field mapping from a JSON file or CSV headers.
    
    Args:
        mapping_file: Path to JSON mapping file
        csv_file: Path to CSV file for header extraction
        id_field: Default ID field
        name_field: Default name field
        
    Returns:
        Field mapping dictionary
    """
    # Default mapping
    field_mapping = {
        'id': id_field,
        'name': name_field
    }
    
    # Try to load from JSON mapping file
    if mapping_file and os.path.exists(mapping_file):
        try:
            with open(mapping_file, 'r') as f:
                mapping_data = json.load(f)
            
            # Update mapping with data from file
            if isinstance(mapping_data, dict):
                for standard_name, source_name in mapping_data.items():
                    field_mapping[standard_name] = source_name
                
                logger.info(f"Loaded field mapping from {mapping_file}")
            else:
                logger.warning(f"Invalid mapping format in {mapping_file}")
        except Exception as e:
            logger.error(f"Error loading mapping file: {e}")
    
    # Try to load from CSV headers
    elif csv_file and os.path.exists(csv_file):
        try:
            with open(csv_file, 'r', newline='') as f:
                reader = csv.reader(f)
                headers = next(reader)  # Read the first row as headers
            
            # Create mapping from headers
            for header in headers:
                # Create a standardized name (lowercase, no spaces)
                std_name = header.lower().replace(' ', '_')
                field_mapping[std_name] = header
            
            logger.info(f"Created field mapping from CSV headers in {csv_file}")
        except Exception as e:
            logger.error(f"Error reading CSV headers: {e}")
    
    return field_mapping

def generate_mappings_from_records(data, output_file=None):
    """
    Generate field mappings by analyzing existing records.
    
    Args:
        data: Loaded vector index data
        output_file: Path to save mapping file (optional)
        
    Returns:
        Generated field mapping dictionary
    """
    id_to_data = data.get("id_to_data", {})
    
    if not id_to_data:
        print("No records found to generate mappings.")
        return {}
    
    # Get first record to analyze fields
    sample_record = next(iter(id_to_data.values()))
    
    # Create mapping
    mapping = {}
    
    # Try to find id field
    id_candidates = ["id", "item_id", "job_id", "document_id", "record_id", "uuid", "_id"]
    for field in id_candidates:
        if field in sample_record:
            mapping["id"] = field
            break
    
    # Try to find name field
    name_candidates = ["name", "title", "job_name", "document_name", "item_name", "heading"]
    for field in name_candidates:
        if field in sample_record:
            mapping["name"] = field
            break
    
    # Try to find other common fields
    field_types = {
        "status": ["status", "state", "condition", "job_status"],
        "category": ["category", "type", "class", "group"],
        "date": ["date", "created_at", "timestamp", "execution_start_time", "created_date"],
        "description": ["description", "content", "text", "details", "summary"]
    }
    
    for mapping_name, candidates in field_types.items():
        for field in candidates:
            if field in sample_record:
                mapping[mapping_name] = field
                break
    
    # Save mapping to file if requested
    if output_file:
        try:
            with open(output_file, 'w') as f:
                json.dump(mapping, f, indent=2)
            print(f"Field mapping saved to {output_file}")
        except Exception as e:
            print(f"Error saving mapping to {output_file}: {e}")
    
    return mapping

def format_created_date(timestamp):
    """Format a timestamp as a human-readable date string."""
    if not timestamp or timestamp == "unknown":
        return "unknown"
    
    try:
        # Convert timestamp to datetime
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return str(timestamp)

def display_index_info(data):
    """
    Display general information about the vector index.
    
    Args:
        data: Loaded vector index data
    """
    # Extract basic information
    embedding_dim = data.get("embedding_dim", "unknown")
    use_faiss = data.get("use_faiss", False)
    version = data.get("version", "unknown")
    created = format_created_date(data.get("created", "unknown"))
    
    # Count items
    id_to_data = data.get("id_to_data", {})
    item_count = len(id_to_data)
    
    # Print information
    print("\n=== Vector Index Information ===")
    print(f"Embedding Dimension: {embedding_dim}")
    print(f"Using FAISS: {use_faiss}")
    print(f"Version: {version}")
    print(f"Created: {created}")
    print(f"Total Items: {item_count}")
    
    # Check FAISS-specific information
    if use_faiss and "faiss_path" in data:
        print(f"FAISS Index Path: {data['faiss_path']}")
        id_list = data.get("id_list", [])
        print(f"ID List Length: {len(id_list)}")
        
        # Try to load FAISS index if available
        if FAISS_AVAILABLE and os.path.exists(data["faiss_path"]):
            try:
                faiss_index = faiss.read_index(data["faiss_path"])
                print(f"FAISS Index Size: {faiss_index.ntotal}")
                print(f"FAISS Index Type: {type(faiss_index).__name__}")
            except Exception as e:
                print(f"Error reading FAISS index: {e}")
    else:
        # Information about numpy index
        index = data.get("index", {})
        print(f"Numpy Index Size: {len(index)}")

def display_records(data, field_mapping, limit=10, verbose=False):
    """
    Display information about records in the vector index.
    
    Args:
        data: Loaded vector index data
        field_mapping: Field mapping dictionary
        limit: Maximum number of records to display
        verbose: Whether to show detailed information
    """
    id_to_data = data.get("id_to_data", {})
    
    if not id_to_data:
        print("\nNo records found in the index.")
        return
    
    # Get a list of item IDs
    item_ids = list(id_to_data.keys())
    
    # Limit the number of records to display
    display_ids = item_ids[:limit]
    
    print(f"\n=== First {len(display_ids)} Records (out of {len(item_ids)}) ===")
    
    # Get field names for display
    id_field = field_mapping.get('id', 'id')
    name_field = field_mapping.get('name', 'name')
    
    # Try to detect common fields for display
    sample_record = id_to_data[item_ids[0]] if item_ids else {}
    common_fields = []
    
    # Add ID and name fields first
    if id_field in sample_record:
        common_fields.append(id_field)
    if name_field in sample_record and name_field != id_field:
        common_fields.append(name_field)
    
    # Look for other common fields that might be useful
    for field in sample_record.keys():
        if field not in common_fields and field != id_field and field != name_field:
            if any(keyword in field.lower() for keyword in ['status', 'type', 'category', 'date', 'tag']):
                common_fields.append(field)
            
            # Limit to 5 fields for display
            if len(common_fields) >= 5:
                break
    
    for i, item_id in enumerate(display_ids):
        record = id_to_data[item_id]
        
        # Display record number and ID
        display_id = record.get(id_field, item_id)
        print(f"\n[Record {i+1}] ID: {display_id}")
        
        if verbose:
            # Display all fields for verbose output
            for field, value in record.items():
                if isinstance(value, (dict, list)):
                    # Format complex objects
                    try:
                        formatted_value = json.dumps(value, indent=2)
                        # Truncate long values
                        if len(formatted_value) > 200:
                            formatted_value = formatted_value[:197] + "..."
                        print(f"  {field}: {formatted_value}")
                    except:
                        print(f"  {field}: {type(value).__name__} (complex object)")
                else:
                    # Format simple values
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:97] + "..."
                    print(f"  {field}: {value_str}")
        else:
            # Display a summary with common fields
            for field in common_fields:
                if field in record:
                    value = record[field]
                    value_str = str(value)
                    if len(value_str) > 60:
                        value_str = value_str[:57] + "..."
                    print(f"  {field}: {value_str}")
            
            # Show field count
            print(f"  Fields: {len(record)}")

def search_index(data, query, field_mapping, limit=10):
    """
    Search the vector index with a text query.
    
    Args:
        data: Loaded vector index data
        query: Text query
        field_mapping: Field mapping dictionary
        limit: Maximum number of results to return
    """
    print(f"\n=== Searching for: '{query}' ===")
    
    # Check if we can perform a search
    if "index" not in data and (not FAISS_AVAILABLE or "faiss_path" not in data):
        print("Cannot perform search: index data not available or FAISS not installed.")
        return
    
    # Get field names for display
    id_field = field_mapping.get('id', 'id')
    name_field = field_mapping.get('name', 'name')
    
    # Try to import necessary modules for search
    try:
        from search.vector_search import VectorSearchEngine
        
        # Create a temporary engine
        engine = VectorSearchEngine(
            embedding_dim=data.get("embedding_dim", 768),
            use_faiss=data.get("use_faiss", False) and FAISS_AVAILABLE
        )
        
        # Load the engine with our data
        if engine.use_faiss:
            engine.id_to_data = data["id_to_data"]
            engine.id_list = data["id_list"]
            engine.faiss_index = faiss.read_index(data["faiss_path"])
        else:
            engine.id_to_data = data["id_to_data"]
            engine.index = {k: np.array(v, dtype=np.float32) for k, v in data["index"].items()}
        
        # Generate query embedding
        query_embedding = VectorSearchEngine.get_mock_embedding(query, engine.embedding_dim)
        
        # Search
        results = engine.search(query_embedding, limit)
        
        # Display results
        if not results:
            print("No results found.")
        else:
            print(f"Found {len(results)} results:")
            
            # Try to detect common fields for display
            sample_record = results[0][2] if results else {}
            common_fields = []
            
            # Add name field first
            if name_field in sample_record:
                common_fields.append(name_field)
            
            # Look for other common fields that might be useful
            for field in sample_record.keys():
                if field not in common_fields and field != id_field and field != name_field:
                    if any(keyword in field.lower() for keyword in ['status', 'type', 'category', 'date', 'tag']):
                        common_fields.append(field)
                    
                    # Limit to 4 fields for display
                    if len(common_fields) >= 4:
                        break
            
            for i, (item_id, score, item_data) in enumerate(results):
                # Get display ID from the record if available
                display_id = item_data.get(id_field, item_id)
                print(f"\n[Result {i+1}] Score: {score:.4f}, ID: {display_id}")
                
                # Display common fields
                for field in common_fields:
                    if field in item_data:
                        value = item_data[field]
                        value_str = str(value)
                        if len(value_str) > 60:
                            value_str = value_str[:57] + "..."
                        print(f"  {field}: {value_str}")
                
                # Add a text sample from first text field for context
                for field in item_data.keys():
                    value = item_data[field]
                    if isinstance(value, str) and len(value) > 20 and field not in common_fields:
                        # Found a text field
                        value_str = str(value)
                        if len(value_str) > 60:
                            value_str = value_str[:57] + "..."
                        print(f"  {field}: {value_str}")
                        break
    
    except ImportError:
        print("Cannot perform search: VectorSearchEngine module not available.")
    except Exception as e:
        print(f"Error performing search: {e}")

def main():
    """Main entry point."""
    args = parse_args()
    
    # Load the vector index
    print(f"Loading vector index from {args.index_path}...")
    data = load_vector_index(args.index_path)
    
    if data is None:
        print("Failed to load vector index. Exiting.")
        sys.exit(1)
    
    # Ensure critical fields exist
    if "id_to_data" not in data:
        print("WARNING: Missing 'id_to_data' in index. This may not be a valid vector index.")
    
    # Load field mapping
    field_mapping = load_field_mapping(
        mapping_file=args.mapping_file,
        csv_file=args.csv_file,
        id_field=args.id_field,
        name_field=args.name_field
    )
    
    # Generate field mapping if requested
    if args.generate_mapping:
        print(f"Generating field mapping from data...")
        generated_mapping = generate_mappings_from_records(data, args.generate_mapping)
        
        print("\n=== Generated Field Mapping ===")
        for standard_name, source_name in generated_mapping.items():
            print(f"  {standard_name} -> {source_name}")
        
        # Use generated mapping if no other mapping provided
        if not args.mapping_file and not args.csv_file:
            field_mapping = generated_mapping
    
    # Show field mapping
    print("\n=== Field Mapping ===")
    for standard_name, source_name in field_mapping.items():
        print(f"  {standard_name} -> {source_name}")
    
    # Display information about the index
    display_index_info(data)
    
    # Display records
    display_records(data, field_mapping, limit=args.limit, verbose=args.verbose)
    
    # Search if requested
    if args.search:
        search_index(data, args.search, field_mapping, limit=args.limit)

if __name__ == "__main__":
    main()