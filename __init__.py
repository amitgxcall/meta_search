"""
Job Search System - A flexible, extensible system for searching job data.
"""

from .unified_search import UnifiedJobSearch
from .utils.field_mapping import FieldMapping
from .providers.base import DataProvider

__version__ = '0.1.0'