import sqlite3
import threading
import pandas as pd
import queue
import time
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Generator, Iterator
from contextlib import contextmanager
import weakref
from dataclasses import dataclass
import logging

# Configuration variables - define at the top
DATABASE_PATH = "database.db"
CSV_FILE_PATH = "data.csv"
MAX_THREADS = 4  # Reduced for better memory management
CONNECTION_TIMEOUT = 30
MAX_RETRIES = 3
FETCH_SIZE = 1000  # Batch size for memory-efficient fetching
MAX_RESULT_SIZE = 10000  # Maximum rows to keep in memory
ENABLE_QUERY_CACHE = True
CACHE_SIZE = 100

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class QueryResult:
    """Lightweight query result container"""
    data: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    execution_time: float
    
    def __post_init__(self):
        # Use __slots__ equivalent for memory efficiency
        self.__dict__ = {k: v for k, v in self.__dict__.items()}

class MemoryEfficientConnectionPool:
    """Memory-optimized connection pool with automatic cleanup"""
    
    def __init__(self, db_path: str, max_connections: int = MAX_THREADS):
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = queue.Queue(maxsize=max_connections)
        self._active_connections = weakref.WeakSet()
        self._lock = threading.RLock()
        self._closed = False
        
        # Initialize with minimal connections
        self._initialize_minimal_pool()
    
    def _initialize_minimal_pool(self):
        """Initialize with fewer connections, create on demand"""
        initial_connections = min(2, self.max_connections)
        for _ in range(initial_connections):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create optimized read-only connection"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=CONNECTION_TIMEOUT,
                isolation_level=None  # Autocommit mode for read-only
            )
            
            # Optimize for read-only operations
            conn.execute("PRAGMA query_only = 1")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
            conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
            conn.execute("PRAGMA synchronous = OFF")
            
            conn.row_factory = sqlite3.Row
            self._active_connections.add(conn)
            return conn
            
        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            return None
    
    @contextmanager
    def get_connection(self):
        """Context manager for safe connection handling"""
        conn = None
        try:
            # Try to get from pool first
            try:
                conn = self._pool.get_nowait()
            except queue.Empty:
                # Create new connection if pool empty and under limit
                if len(self._active_connections) < self.max_connections:
                    conn = self._create_connection()
                else:
                    # Wait for connection from pool
                    conn = self._pool.get(timeout=5)
            
            if not conn:
                raise Exception("Unable to obtain database connection")
                
            yield conn
            
        except Exception as e:
            if conn:
                try:
                    conn.close()
                except:
                    pass
            raise e
        finally:
            if conn and not self._closed:
                try:
                    # Return to pool if still valid
                    self._pool.put_nowait(conn)
                except queue.Full:
                    # Close if pool is full
                    conn.close()
    
    def close_all(self):
        """Close all connections and cleanup"""
        self._closed = True
        
        # Close pooled connections
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except (queue.Empty, Exception):
                break
        
        # Close any remaining active connections
        for conn in list(self._active_connections):
            try:
                conn.close()
            except:
                pass

class QueryCache:
    """Simple LRU cache for query results"""
    
    def __init__(self, max_size: int = CACHE_SIZE):
        self.max_size = max_size
        self.cache = {}
        self.access_order = []
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[QueryResult]:
        with self._lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.access_order.remove(key)
                self.access_order.append(key)
                return self.cache[key]
            return None
    
    def put(self, key: str, value: QueryResult):
        with self._lock:
            if len(self.cache) >= self.max_size:
                # Remove least recently used
                oldest = self.access_order.pop(0)
                del self.cache[oldest]
            
            self.cache[key] = value
            self.access_order.append(key)
    
    def clear(self):
        with self._lock:
            self.cache.clear()
            self.access_order.clear()

# Global instances
_connection_pool = MemoryEfficientConnectionPool(DATABASE_PATH, MAX_THREADS)
_query_cache = QueryCache() if ENABLE_QUERY_CACHE else None

def execute_query_streaming(query: str, params: tuple = None, 
                          fetch_size: int = FETCH_SIZE) -> Generator[Dict[str, Any], None, None]:
    """
    Execute query with streaming results for memory efficiency
    
    Args:
        query (str): SQL SELECT query
        params (tuple, optional): Query parameters
        fetch_size (int): Number of rows to fetch at a time
    
    Yields:
        Dict[str, Any]: Individual row as dictionary
    """
    if not query.strip().upper().startswith('SELECT'):
        raise ValueError("Only SELECT queries are allowed")
    
    with _connection_pool.get_connection() as conn:
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break
                
                for row in rows:
                    yield dict(row)
                    
                # Yield control to allow garbage collection
                if fetch_size > 100:
                    gc.collect()
                    
        finally:
            cursor.close()

def execute_query(query: str, params: tuple = None, 
                 use_cache: bool = ENABLE_QUERY_CACHE,
                 max_rows: int = MAX_RESULT_SIZE) -> QueryResult:
    """
    Execute a SELECT query with memory optimization and caching
    
    Args:
        query (str): SQL SELECT query
        params (tuple, optional): Query parameters
        use_cache (bool): Whether to use query cache
        max_rows (int): Maximum rows to return
    
    Returns:
        QueryResult: Query results with metadata
    """
    if not query.strip().upper().startswith('SELECT'):
        raise ValueError("Only SELECT queries are allowed")
    
    # Check cache first
    cache_key = f"{query}:{str(params)}" if use_cache and _query_cache else None
    if cache_key:
        cached_result = _query_cache.get(cache_key)
        if cached_result:
            logger.debug(f"Cache hit for query: {query[:50]}...")
            return cached_result
    
    start_time = time.time()
    retries = 0
    
    while retries < MAX_RETRIES:
        try:
            with _connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                
                try:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    # Get column names
                    columns = [description[0] for description in cursor.description]
                    
                    # Fetch data in batches to manage memory
                    data = []
                    total_rows = 0
                    
                    while len(data) < max_rows:
                        batch = cursor.fetchmany(FETCH_SIZE)
                        if not batch:
                            break
                        
                        batch_data = [dict(zip(columns, row)) for row in batch]
                        data.extend(batch_data)
                        total_rows += len(batch)
                        
                        # Memory management
                        if len(data) % (FETCH_SIZE * 5) == 0:
                            gc.collect()
                    
                    execution_time = time.time() - start_time
                    
                    result = QueryResult(
                        data=data,
                        columns=columns,
                        row_count=len(data),
                        execution_time=execution_time
                    )
                    
                    # Cache result if enabled and reasonable size
                    if cache_key and len(data) < 1000:
                        _query_cache.put(cache_key, result)
                    
                    return result
                    
                finally:
                    cursor.close()
                    
        except sqlite3.OperationalError as e:
            retries += 1
            if retries >= MAX_RETRIES:
                raise Exception(f"Query failed after {MAX_RETRIES} retries: {str(e)}")
            
            time.sleep(0.1 * retries)
            logger.warning(f"Retry {retries} for query: {query[:50]}...")
            
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")

def execute_queries_parallel(queries: List[str], 
                           max_workers: int = MAX_THREADS,
                           max_rows_per_query: int = MAX_RESULT_SIZE) -> List[QueryResult]:
    """
    Execute multiple SELECT queries in parallel with memory optimization
    
    Args:
        queries (List[str]): List of SELECT queries
        max_workers (int): Maximum worker threads
        max_rows_per_query (int): Max rows per individual query
    
    Returns:
        List[QueryResult]: Results from all queries
    """
    # Validate all queries are SELECT statements
    for query in queries:
        if not query.strip().upper().startswith('SELECT'):
            raise ValueError(f"Only SELECT queries allowed: {query[:50]}...")
    
    results = [None] * len(queries)  # Preserve order
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit queries with index for ordering
        future_to_index = {
            executor.submit(execute_query, query, None, True, max_rows_per_query): i
            for i, query in enumerate(queries)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                result = future.result()
                results[index] = result
                logger.info(f"Query {index + 1} completed: {result.row_count} rows in {result.execution_time:.2f}s")
            except Exception as e:
                logger.error(f"Query {index + 1} failed: {str(e)}")
                results[index] = QueryResult([], [], 0, 0.0)
            
            # Periodic garbage collection
            gc.collect()
    
    return results

def read_csv_streaming(file_path: str = CSV_FILE_PATH, 
                      chunk_size: int = FETCH_SIZE) -> Iterator[pd.DataFrame]:
    """
    Read CSV file in chunks for memory efficiency
    
    Args:
        file_path (str): Path to CSV file
        chunk_size (int): Rows per chunk
    
    Yields:
        pd.DataFrame: Chunks of CSV data
    """
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            yield chunk
            gc.collect()  # Clean up after each chunk
            
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        return
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        return

def read_csv_file(file_path: str = CSV_FILE_PATH, 
                 max_rows: int = MAX_RESULT_SIZE) -> pd.DataFrame:
    """
    Read CSV file with memory limits
    
    Args:
        file_path (str): Path to CSV file
        max_rows (int): Maximum rows to load
    
    Returns:
        pd.DataFrame: CSV data (limited rows)
    """
    try:
        df = pd.read_csv(file_path, nrows=max_rows)
        logger.info(f"Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
        return df
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        return pd.DataFrame()

def get_table_info(table_name: str) -> QueryResult:
    """Get table schema information"""
    query = f"PRAGMA table_info({table_name})"
    return execute_query(query)

def get_database_tables() -> QueryResult:
    """Get list of all tables in database"""
    query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    return execute_query(query)

def analyze_table(table_name: str) -> Dict[str, QueryResult]:
    """Analyze table with basic statistics"""
    queries = {
        'row_count': f"SELECT COUNT(*) as count FROM {table_name}",
        'sample_data': f"SELECT * FROM {table_name} LIMIT 5"
    }
    
    results = {}
    for key, query in queries.items():
        try:
            results[key] = execute_query(query)
        except Exception as e:
            logger.error(f"Failed to analyze {table_name}.{key}: {e}")
            results[key] = QueryResult([], [], 0, 0.0)
    
    return results

def cleanup_resources():
    """Clean up all resources and connections"""
    global _connection_pool, _query_cache
    
    if _connection_pool:
        _connection_pool.close_all()
    
    if _query_cache:
        _query_cache.clear()
    
    # Force garbage collection
    gc.collect()
    logger.info("Resources cleaned up")

# Example usage
def example_usage():
    """Example demonstrating memory-efficient usage"""
    
    try:
        # Get database info
        logger.info("Getting database tables...")
        tables_result = get_database_tables()
        logger.info(f"Found {tables_result.row_count} tables")
        
        # Read CSV in chunks (memory efficient)
        logger.info("Reading CSV in chunks...")
        total_rows = 0
        for chunk in read_csv_streaming():
            total_rows += len(chunk)
            logger.info(f"Processed chunk: {len(chunk)} rows")
            if total_rows >= MAX_RESULT_SIZE:
                break
        
        # Example parallel queries
        sample_queries = [
            "SELECT COUNT(*) as count FROM sqlite_master",
            "SELECT type, COUNT(*) as count FROM sqlite_master GROUP BY type",
            "SELECT name FROM sqlite_master WHERE type='table' LIMIT 5"
        ]
        
        logger.info("Executing parallel queries...")
        results = execute_queries_parallel(sample_queries)
        
        for i, result in enumerate(results):
            logger.info(f"Query {i+1}: {result.row_count} rows, {result.execution_time:.3f}s")
    
    except Exception as e:
        logger.error(f"Example execution failed: {e}")

if __name__ == "__main__":
    try:
        example_usage()
    finally:
        cleanup_resources()