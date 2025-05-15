"""
Error handling utilities for meta_search.

This module provides a consistent approach to error handling throughout
the meta_search system, including custom exceptions, error logging,
and utility functions for error handling.

Example:
    try:
        # Some risky operation
        result = provider.search(query)
    except DataSourceError as e:
        logger.error(f"Data source error: {e}")
        # Handle the error
"""

import logging
from typing import Any, Optional, Tuple, Dict, List, Callable

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetaSearchError(Exception):
    """Base exception for all meta_search errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            details: Additional error details
        """
        self.message = message
        self.details = details or {}
        super().__init__(message)


class ConfigurationError(MetaSearchError):
    """Exception raised for configuration errors."""
    pass


class DataSourceError(MetaSearchError):
    """Exception raised for data source errors."""
    pass


class ProviderError(MetaSearchError):
    """Exception raised for provider errors."""
    pass


class SearchError(MetaSearchError):
    """Exception raised for search errors."""
    pass


class ValidationError(MetaSearchError):
    """Exception raised for validation errors."""
    pass


def safe_execute(func: Callable, 
                *args, 
                default_return: Any = None, 
                error_message: str = "Error executing function", 
                reraise: bool = False, 
                log_error: bool = True, 
                **kwargs) -> Any:
    """
    Safely execute a function and handle exceptions.
    
    Args:
        func: Function to execute
        *args: Function arguments
        default_return: Default return value if function fails
        error_message: Message to log if function fails
        reraise: Whether to reraise the exception
        log_error: Whether to log the error
        **kwargs: Function keyword arguments
        
    Returns:
        Function result or default return value
        
    Raises:
        Exception: If reraise is True and an exception occurs
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_error:
            logger.error(f"{error_message}: {str(e)}")
        
        if reraise:
            raise
        
        return default_return


def validate_data_source(source_path: str) -> Tuple[bool, Optional[str]]:
    """
    Validate a data source path.
    
    Args:
        source_path: Path to the data source
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    import os
    
    if not os.path.exists(source_path):
        return False, f"Data source not found: {source_path}"
    
    # Check file extension
    _, ext = os.path.splitext(source_path)
    
    if ext.lower() not in ['.csv', '.json', '.db', '.sqlite', '.sqlite3']:
        return False, f"Unsupported file type: {ext}"
    
    return True, None


def validate_query(query: str) -> Tuple[bool, Optional[str]]:
    """
    Validate a search query.
    
    Args:
        query: Search query
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not query or not query.strip():
        return False, "Query cannot be empty"
    
    if len(query) > 1000:
        return False, "Query is too long (maximum 1000 characters)"
    
    return True, None


class ErrorHandler:
    """
    Central error handler for the meta_search system.
    
    This class provides methods for handling different types of errors
    consistently throughout the system.
    """
    
    def __init__(self, log_errors: bool = True):
        """
        Initialize the error handler.
        
        Args:
            log_errors: Whether to log errors
        """
        self.log_errors = log_errors
    
    def handle_data_source_error(self, 
                               source_path: str, 
                               error: Exception, 
                               reraise: bool = False) -> Optional[Dict[str, Any]]:
        """
        Handle a data source error.
        
        Args:
            source_path: Path to the data source
            error: The exception that occurred
            reraise: Whether to reraise the exception
            
        Returns:
            Error information dictionary or None
            
        Raises:
            DataSourceError: If reraise is True
        """
        error_info = {
            'type': 'data_source_error',
            'source_path': source_path,
            'error': str(error),
            'error_type': type(error).__name__
        }
        
        if self.log_errors:
            logger.error(f"Data source error for {source_path}: {str(error)}")
        
        if reraise:
            raise DataSourceError(
                f"Error accessing data source {source_path}: {str(error)}",
                details=error_info
            )
        
        return error_info
    
    def handle_search_error(self, 
                          query: str, 
                          error: Exception, 
                          reraise: bool = False) -> Optional[Dict[str, Any]]:
        """
        Handle a search error.
        
        Args:
            query: Search query
            error: The exception that occurred
            reraise: Whether to reraise the exception
            
        Returns:
            Error information dictionary or None
            
        Raises:
            SearchError: If reraise is True
        """
        error_info = {
            'type': 'search_error',
            'query': query,
            'error': str(error),
            'error_type': type(error).__name__
        }
        
        if self.log_errors:
            logger.error(f"Search error for query '{query}': {str(error)}")
        
        if reraise:
            raise SearchError(
                f"Error executing search for '{query}': {str(error)}",
                details=error_info
            )
        
        return error_info
    
    def handle_provider_error(self, 
                            provider_type: str, 
                            operation: str, 
                            error: Exception, 
                            reraise: bool = False) -> Optional[Dict[str, Any]]:
        """
        Handle a provider error.
        
        Args:
            provider_type: Type of provider
            operation: Operation that caused the error
            error: The exception that occurred
            reraise: Whether to reraise the exception
            
        Returns:
            Error information dictionary or None
            
        Raises:
            ProviderError: If reraise is True
        """
        error_info = {
            'type': 'provider_error',
            'provider_type': provider_type,
            'operation': operation,
            'error': str(error),
            'error_type': type(error).__name__
        }
        
        if self.log_errors:
            logger.error(f"Provider error ({provider_type}.{operation}): {str(error)}")
        
        if reraise:
            raise ProviderError(
                f"Error in provider {provider_type} during {operation}: {str(error)}",
                details=error_info
            )
        
        return error_info