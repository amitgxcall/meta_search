#!/usr/bin/env python3
"""
Simple hybrid search CLI that works correctly with your data.
"""

import argparse
import sys
import os
import re
import csv
from collections import defaultdict

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Meta Search CLI')
    parser.add_argument('--data-source', required=True,
                        help='Path to the data source file')
    parser.add_argument('--id-field', required=True,
                        help='Field to use as ID')
    parser.add_argument('--name-field', required=True,
                        help='Field to use as name')
    parser.add_argument('--query', required=True,
                        help='Search query')
    return parser.parse_args()

def load_csv_data(file_path):
    """Load data from a CSV file."""
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return []

def extract_id_from_query(query):
    """Extract an ID from a query string."""
    # Look for common ID patterns
    id_pattern = re.compile(r".*id\s*[:=]?\s*(\w+[-]?\w*)", re.IGNORECASE)
    match = id_pattern.search(query)
    if match:
        return match.group(1)
    return None

def find_exact_id_match(data, id_value, id_field):
    """Find an exact match for the given ID."""
    for item in data:
        if str(item.get(id_field, '')).lower() == id_value.lower():
            return item
    return None

def text_search(data, query, id_field, name_field):
    """
    Perform text search with phrase prioritization.
    """
    query = query.lower()
    query_terms = query.lower().split()
    results = []
    
    # Define score levels
    PHRASE_MATCH = 100    # Exact phrase match
    ADJACENT_MATCH = 50   # Adjacent words match
    WORD_MATCH = 10       # Individual words match
    
    for item in data:
        score = 0
        matched_fields = defaultdict(list)
        all_text = ""
        
        # Combine text fields for whole-document matching
        for field, value in item.items():
            if value and isinstance(value, str):
                value_lower = value.lower()
                all_text += " " + value_lower
                
                # Check for exact phrase match (highest priority)
                if query in value_lower:
                    score += PHRASE_MATCH
                    matched_fields[field].append(f"PHRASE: '{query}'")
                
                # Check for adjacent terms (medium priority)
                if len(query_terms) >= 2:
                    for i in range(len(query_terms) - 1):
                        two_words = f"{query_terms[i]} {query_terms[i+1]}"
                        if two_words in value_lower:
                            score += ADJACENT_MATCH
                            matched_fields[field].append(f"ADJACENT: '{two_words}'")
                
                # Check for individual terms (lowest priority)
                for term in query_terms:
                    if term in value_lower:
                        score += WORD_MATCH
                        matched_fields[field].append(f"WORD: '{term}'")
        
        if score > 0:
            # Determine best match type
            match_type = "TEXT MATCH"
            if any("PHRASE" in match for matches in matched_fields.values() for match in matches):
                match_type = "EXACT PHRASE MATCH"
            elif any("ADJACENT" in match for matches in matched_fields.values() for match in matches):
                match_type = "CONSECUTIVE WORDS MATCH"
            
            # Add to results
            results.append({
                'item': item,
                'score': score,
                'match_type': match_type,
                'matched_fields': dict(matched_fields)
            })
    
    # Sort by score (descending)
    results.sort(key=lambda x: x['score'], reverse=True)
    return results

def vector_search(data, query, id_field, name_field):
    """
    Simulate vector search using keyword expansion for semantic matching.
    This is a simple demonstration of how vector search would work.
    """
    # Define semantic expansions for common terms
    semantic_map = {
        "security": ["protection", "firewall", "defense", "guard", "shield", "safeguard"],
        "threat": ["risk", "danger", "hazard", "vulnerability", "attack", "exploit"],
        "detection": ["identify", "discover", "locate", "find", "recognize", "sense", "spot"],
        "user": ["account", "profile", "identity", "member", "person", "individual"],
        "data": ["information", "records", "files", "documents", "content", "storage"],
        "sync": ["synchronize", "update", "refresh", "replicate", "mirror", "coordinate"],
        "process": ["task", "job", "workflow", "operation", "activity", "procedure"],
        "monitor": ["watch", "observe", "track", "check", "survey", "oversee"],
        "system": ["platform", "infrastructure", "framework", "environment", "network"],
        "backup": ["archive", "copy", "duplicate", "replicate", "preserve", "save"]
    }
    
    # Expand query with semantic terms
    query_terms = query.lower().split()
    expanded_terms = set(query_terms)
    semantic_matches = {}
    
    # Add semantically related terms
    for term in query_terms:
        if term in semantic_map:
            for related in semantic_map[term]:
                expanded_terms.add(related)
                semantic_matches[related] = term
    
    results = []
    
    for item in data:
        # Extract text for matching
        all_text = " ".join([str(v).lower() for k, v in item.items() if v and isinstance(v, str)])
        
        # Count semantic matches
        direct_matches = set()
        semantic_hits = set()
        
        for term in expanded_terms:
            if term in all_text:
                if term in query_terms:
                    direct_matches.add(term)
                else:
                    semantic_hits.add((term, semantic_matches.get(term, "related")))
        
        # Calculate score based on matches
        score = 0
        
        # Direct matches are worth more
        score += len(direct_matches) * 20
        
        # Semantic matches are worth less
        score += len(semantic_hits) * 10
        
        # Bonus for having both direct and semantic matches
        if direct_matches and semantic_hits:
            score += 15
        
        if score > 0:
            results.append({
                'item': item,
                'score': score,
                'match_type': "VECTOR SIMILARITY",
                'direct_matches': direct_matches,
                'semantic_matches': semantic_hits
            })
    
    # Sort by score (descending)
    results.sort(key=lambda x: x['score'], reverse=True)
    return results

def combine_results(text_results, vector_results, id_field, vector_weight=0.7):
    """
    Combine text and vector results with proper weighting.
    """
    # Create a dictionary to hold combined results
    combined = {}
    text_weight = 1.0 - vector_weight
    
    # Add text results
    for result in text_results:
        item = result['item']
        item_id = item[id_field]
        
        # Normalize score to 0-1 range (assuming max score is ~500)
        normalized_score = min(result['score'] / 300.0, 1.0)
        
        combined[item_id] = {
            'item': item,
            'text_score': normalized_score,
            'vector_score': 0,
            'combined_score': normalized_score * text_weight,
            'text_details': result,
            'vector_details': None,
            'match_types': [result['match_type']]
        }
    
    # Add vector results
    for result in vector_results:
        item = result['item']
        item_id = item[id_field]
        
        # Normalize score to 0-1 range (assuming max score is ~200)
        normalized_score = min(result['score'] / 100.0, 1.0)
        
        if item_id in combined:
            # Update existing entry
            entry = combined[item_id]
            entry['vector_score'] = normalized_score
            entry['combined_score'] += normalized_score * vector_weight
            entry['vector_details'] = result
            if result['match_type'] not in entry['match_types']:
                entry['match_types'].append(result['match_type'])
        else:
            # Create new entry
            combined[item_id] = {
                'item': item,
                'text_score': 0,
                'vector_score': normalized_score,
                'combined_score': normalized_score * vector_weight,
                'text_details': None,
                'vector_details': result,
                'match_types': [result['match_type']]
            }
    
    # Convert to list and sort by combined score
    results = list(combined.values())
    results.sort(key=lambda x: x['combined_score'], reverse=True)
    
    return results[:10]  # Limit to top 10 results

def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    try:
        print(f"🔎 Hybrid Search: '{args.query}' in {args.data_source}")
        print(f"Using {args.id_field} as ID field and {args.name_field} as name field")
        
        # Load the CSV data
        data = load_csv_data(args.data_source)
        if not data:
            print("No data found or error loading file.")
            sys.exit(1)
        
        # Handle ID queries directly
        id_value = extract_id_from_query(args.query)
        if id_value:
            print(f"Detected ID search for: '{id_value}'")
            print(f"Search method: DIRECT ID LOOKUP")
            
            # Look for exact ID match
            exact_match = find_exact_id_match(data, id_value, args.id_field)
            
            if exact_match:
                print(f"\n=== DIRECT ID MATCH ===")
                print(f"Found exact match for ID '{id_value}':")
                for key, value in exact_match.items():
                    print(f"  {key}: {value}")
                
                print("\nDo you want to continue with a hybrid search? (y/n)")
                response = input().strip().lower()
                if response != 'y':
                    sys.exit(0)
        
        # Perform text search
        text_results = text_search(data, args.query, args.id_field, args.name_field)
        print(f"Text search found {len(text_results)} results")
        
        # Perform vector search
        vector_results = vector_search(data, args.query, args.id_field, args.name_field)
        print(f"Vector search found {len(vector_results)} results")
        
        # Combine results
        combined_results = combine_results(text_results, vector_results, args.id_field, 0.7)
        
        # Display results
        if not combined_results:
            print("No results found.")
        else:
            print(f"\nFound {len(combined_results)} results:")
            
            for idx, result in enumerate(combined_results, 1):
                item = result['item']
                
                # Create appropriate icon based on match types
                match_types = result['match_types']
                
                if "EXACT PHRASE MATCH" in match_types:
                    icon = "⭐⭐⭐"
                elif "CONSECUTIVE WORDS MATCH" in match_types:
                    icon = "⭐⭐"
                elif "TEXT MATCH" in match_types and "VECTOR SIMILARITY" in match_types:
                    icon = "⭐🔍"
                elif "TEXT MATCH" in match_types:
                    icon = "⭐"
                else:
                    icon = "🔍"
                
                print(f"\n{icon} Result {idx}: {item.get(args.name_field, '')}")
                
                # Show scores
                if result['text_score'] > 0 and result['vector_score'] > 0:
                    print(f"  Combined score: {result['combined_score']:.2f}")
                    print(f"  Text score: {result['text_score']:.2f}")
                    print(f"  Vector score: {result['vector_score']:.2f}")
                elif result['text_score'] > 0:
                    print(f"  Text score: {result['text_score']:.2f}")
                else:
                    print(f"  Vector score: {result['vector_score']:.2f}")
                
                # Show match details
                text_details = result.get('text_details')
                if text_details:
                    print(f"  Match type: {text_details['match_type']}")
                    
                    # Show fields where matches occurred
                    matched_fields = text_details.get('matched_fields', {})
                    if matched_fields:
                        for field, matches in matched_fields.items():
                            matches_str = ", ".join(matches)
                            print(f"  - Matched in {field}: {matches_str}")
                
                # Show vector match details
                vector_details = result.get('vector_details')
                if vector_details:
                    if "VECTOR SIMILARITY" not in match_types:
                        print(f"  Match type: VECTOR SIMILARITY")
                    
                    # Show direct and semantic matches
                    direct_matches = vector_details.get('direct_matches', set())
                    if direct_matches:
                        print(f"  Direct matches: {', '.join(direct_matches)}")
                    
                    semantic_matches = vector_details.get('semantic_matches', set())
                    if semantic_matches:
                        semantic_str = ", ".join([f"{term} → {orig}" for term, orig in semantic_matches])
                        print(f"  Semantic expansions: {semantic_str}")
                
                # Print important details
                important_fields = [args.id_field, args.name_field, 'description', 'status', 'environment', 'category']
                print("  Record details:")
                for key, value in item.items():
                    if key in important_fields:
                        print(f"    {key}: {value}")
                
                # For first result, ask if they want to see all fields
                if idx == 1:
                    print("\n  See all fields? (y/n)")
                    show_all = input().strip().lower() == 'y'
                    if show_all:
                        for key, value in item.items():
                            if key not in important_fields:
                                print(f"    {key}: {value}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()