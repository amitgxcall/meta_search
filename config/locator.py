"""
Configuration file handling for meta_search.

This module provides functionality for locating, loading, and managing
configuration files for the meta_search system.

Example:
    # Find configuration file
    config_path = find_config_file()
    
    # Load configuration
    config = load_config(config_path)
"""

import os
import sys
import logging
import json
from typing import Optional, Dict, Any, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_default_config_locations() -> List[str]:
    """
    Get default configuration file locations in order of priority.
    
    Returns:
        List of default configuration file paths
    """
    # Start with empty list
    locations = []
    
    # 1. Current working directory
    locations.append(os.path.join(os.getcwd(), 'meta_search.config.json'))
    locations.append(os.path.join(os.getcwd(), 'config.json'))
    
    # 2. User configuration directory
    user_config_dir = os.path.expanduser("~/.config/meta_search")
    locations.append(os.path.join(user_config_dir, 'config.json'))
    
    # 3. XDG configuration directory (Linux/Unix)
    xdg_config_home = os.environ.get('XDG_CONFIG_HOME', '')
    if xdg_config_home:
        locations.append(os.path.join(xdg_config_home, 'meta_search', 'config.json'))
    
    # 4. Application directory
    app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    locations.append(os.path.join(app_dir, 'config', 'config.json'))
    
    # 5. System-wide configuration (Linux/Unix)
    if sys.platform.startswith('linux') or sys.platform.startswith('darwin'):
        locations.append('/etc/meta_search/config.json')
    
    # 6. Windows AppData directory
    if sys.platform.startswith('win'):
        appdata = os.environ.get('APPDATA', '')
        if appdata:
            locations.append(os.path.join(appdata, 'meta_search', 'config.json'))
    
    return locations


def find_config_file(specified_path: Optional[str] = None) -> Optional[str]:
    """
    Find the configuration file to use.
    
    Args:
        specified_path: User-specified configuration file path
        
    Returns:
        Path to the configuration file if found, None otherwise
    """
    # 1. Use specified path if provided
    if specified_path:
        if os.path.exists(specified_path):
            logger.info(f"Using specified configuration file: {specified_path}")
            return specified_path
        else:
            logger.warning(f"Specified configuration file not found: {specified_path}")
    
    # 2. Check environment variable
    env_config = os.environ.get('META_SEARCH_CONFIG', '')
    if env_config and os.path.exists(env_config):
        logger.info(f"Using configuration file from environment variable: {env_config}")
        return env_config
    
    # 3. Check default locations
    for location in get_default_config_locations():
        if os.path.exists(location):
            logger.info(f"Using configuration file from default location: {location}")
            return location
    
    logger.info("No configuration file found, using default configuration")
    return None


def load_config_file(config_path: str) -> Dict[str, Any]:
    """
    Load a configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If the file is not found
        json.JSONDecodeError: If the file is not valid JSON
    """
    with open(config_path, 'r') as f:
        return json.load(f)


def create_default_config(output_path: str) -> bool:
    """
    Create a default configuration file.
    
    Args:
        output_path: Path to write the configuration file
        
    Returns:
        True if successful, False otherwise
    """
    # Create default configuration
    default_config = {
        "provider_settings": {
            "default_provider": "hybrid",
            "csv": {
                "delimiter": ",",
                "quotechar": "\"",
                "encoding": "utf-8"
            },
            "sqlite": {
                "timeout": 5.0,
                "detect_types": 0,
                "isolation_level": None
            },
            "hybrid": {
                "vector_weight": 0.5,
                "text_fields": None,
                "sequential": False
            }
        },
        "search_settings": {
            "max_results": 10,
            "min_score": 0.0,
            "sort_by": "_score",
            "sort_order": "desc",
            "exclude_fields": ["_internal", "_metadata"]
        },
        "field_weights": {
            "name": 2.0,
            "description": 1.5,
            "status": 1.0,
            "error_message": 1.0,
            "default": 0.5
        },
        "cache_settings": {
            "enabled": True,
            "directory": "cache",
            "ttl": 86400,
            "max_size": 104857600
        }
    }
    
    # Create parent directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    
    # Write configuration file
    try:
        with open(output_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        logger.info(f"Created default configuration file: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating default configuration file: {e}")
        return False


def get_config_directories() -> Dict[str, str]:
    """
    Get all possible configuration directories.
    
    Returns:
        Dictionary of location names to directory paths
    """
    directories = {}
    
    # Current working directory
    directories['current_directory'] = os.getcwd()
    
    # User home directory
    directories['user_home'] = os.path.expanduser('~')
    
    # User config directory
    directories['user_config'] = os.path.expanduser('~/.config/meta_search')
    
    # XDG config directory
    xdg_config_home = os.environ.get('XDG_CONFIG_HOME', '')
    if xdg_config_home:
        directories['xdg_config'] = os.path.join(xdg_config_home, 'meta_search')
    
    # Application directory
    app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    directories['app_directory'] = os.path.join(app_dir, 'config')
    
    # System-wide configuration directory (Linux/Unix)
    if sys.platform.startswith('linux') or sys.platform.startswith('darwin'):
        directories['system_config'] = '/etc/meta_search'
    
    # Windows AppData directory
    if sys.platform.startswith('win'):
        appdata = os.environ.get('APPDATA', '')
        if appdata:
            directories['appdata'] = os.path.join(appdata, 'meta_search')
    
    return directories