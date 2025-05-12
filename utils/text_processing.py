"""
Text processing utilities for the search system.
"""

import re
from typing import List, Dict, Any, Optional, Set
import string

class TextProcessor:
    """
    Text processing utilities for search and indexing.
    """
    
    def __init__(self, 
                stop_words: Optional[Set[str]] = None,
                min_word_length: int = 2,
                remove_punctuation: bool = True):
        """
        Initialize text processor.
        
        Args:
            stop_words: Set of stop words to remove
            min_word_length: Minimum word length to keep
            remove_punctuation: Whether to remove punctuation
        """
        self.min_word_length = min_word_length
        self.remove_punctuation = remove_punctuation
        
        # Default English stop words
        default_stop_words = {
            'a', 'an', 'the', 'and', 'or', 'but', 'if', 'then', 'else', 'when',
            'at', 'from', 'by', 'for', 'with', 'about', 'to', 'in', 'on', 'of',
            'is', 'are', 'was', 'were', 'be', 'been', 'being',
            'this', 'that', 'these', 'those',
            'i', 'you', 'he', 'she', 'it', 'we', 'they',
            'my', 'your', 'his', 'her', 'its', 'our', 'their'
        }
        
        # Combine with provided stop words
        self.stop_words = default_stop_words if stop_words is None else stop_words.union(default_stop_words)
        
        # Punctuation translation table
        if remove_punctuation:
            self.translator = str.maketrans('', '', string.punctuation)
        else:
            self.translator = None
    
    def normalize(self, text: str) -> str:
        """
        Normalize text for search.
        
        Args:
            text: Input text
            
        Returns:
            Normalized text
        """
        # Convert to lowercase
        text = text.lower()
        
        # Remove punctuation
        if self.remove_punctuation:
            text = text.translate(self.translator)
        
        return text
    
    def tokenize(self, text: str) -> List[str]:
        """
        Split text into tokens.
        
        Args:
            text: Input text
            
        Returns:
            List of tokens
        """
        # Normalize first
        text = self.normalize(text)
        
        # Split into words
        tokens = re.findall(r'\b\w+\b', text)
        
        # Filter short words and stop words
        return [
            token for token in tokens
            if len(token) >= self.min_word_length and token not in self.stop_words
        ]
    
    def extract_keywords(self, text: str, top_n: int = 10) -> List[str]:
        """
        Extract important keywords from text.
        
        Args:
            text: Input text
            top_n: Maximum number of keywords to extract
            
        Returns:
            List of keywords
        """
        # Tokenize
        tokens = self.tokenize(text)
        
        # Count frequency
        freq = {}
        for token in tokens:
            freq[token] = freq.get(token, 0) + 1
        
        # Sort by frequency (descending)
        sorted_tokens = sorted(freq.items(), key=lambda x: x[1], reverse=True)
        
        # Return top N
        return [token for token, count in sorted_tokens[:top_n]]
    
    def extract_phrases(self, text: str, max_length: int = 3) -> List[str]:
        """
        Extract phrases from text.
        
        Args:
            text: Input text
            max_length: Maximum phrase length in words
            
        Returns:
            List of phrases
        """
        # Normalize
        text = self.normalize(text)
        
        # Split into words
        words = re.findall(r'\b\w+\b', text)
        
        # Extract phrases
        phrases = []
        for i in range(len(words)):
            for j in range(1, min(max_length + 1, len(words) - i + 1)):
                phrase = ' '.join(words[i:i+j])
                phrases.append(phrase)
        
        return phrases
    
    def similarity(self, text1: str, text2: str) -> float:
        """
        Calculate simple similarity between two texts.
        
        Args:
            text1: First text
            text2: Second text
            
        Returns:
            Similarity score (0.0 to 1.0)
        """
        # Tokenize
        tokens1 = set(self.tokenize(text1))
        tokens2 = set(self.tokenize(text2))
        
        # Calculate Jaccard similarity
        intersection = tokens1.intersection(tokens2)
        union = tokens1.union(tokens2)
        
        if not union:
            return 0.0
            
        return len(intersection) / len(union)
    
    def highlight_matches(self, text: str, query: str, 
                        before: str = '<b>', after: str = '</b>') -> str:
        """
        Highlight query matches in text.
        
        Args:
            text: Text to highlight in
            query: Query to highlight
            before: String to insert before matches
            after: String to insert after matches
            
        Returns:
            Text with highlights
        """
        # Normalize and tokenize query
        query_tokens = self.tokenize(query)
        
        # If no valid tokens, return original text
        if not query_tokens:
            return text
        
        # Create regex pattern for each token
        patterns = []
        for token in query_tokens:
            # Escape special characters
            escaped = re.escape(token)
            # Create case-insensitive pattern
            patterns.append(rf'\b({escaped})\b')
        
        # Combine patterns
        combined_pattern = '|'.join(patterns)
        
        # Apply highlighting
        return re.sub(
            combined_pattern,
            f'{before}\\1{after}',
            text,
            flags=re.IGNORECASE
        )