import os
import json
import time
import logging
import hashlib
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

class CacheManager:
    """
    Manages caching of API responses to reduce API calls and handle rate limiting.
    
    This class provides methods to save, load, and manage cached API responses.
    """
    
    def __init__(self, cache_dir: str = "cache", max_age_days: int = 7):
        """
        Initialize the cache manager.
        
        Args:
            cache_dir: Directory to store cache files
            max_age_days: Maximum age of cache files in days before they're considered stale
        """
        self.cache_dir = cache_dir
        self.max_age_days = max_age_days
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
            logging.info(f"Created cache directory: {cache_dir}")
    
    def get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Generate a unique cache key for an API request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            Cache key string
        """
        # Create a string representation of the request
        # Sort the params to ensure consistent key generation
        sorted_params = {}
        
        # Convert all values to strings to ensure consistent serialization
        for key in sorted(params.keys()):
            if params[key] is None:
                sorted_params[key] = "null"
            elif isinstance(params[key], bool):
                sorted_params[key] = "true" if params[key] else "false"
            else:
                sorted_params[key] = str(params[key])
        
        # Create a deterministic JSON string
        param_str = json.dumps(sorted_params, sort_keys=True)
        key_str = f"{endpoint}:{param_str}"
        
        # Create a hash of the string for the filename
        hash_value = hashlib.md5(key_str.encode()).hexdigest()
        
        # Log the key generation for debugging
        logging.debug(f"Cache key for {endpoint}: {hash_value} (params: {param_str})")
        
        return hash_value
    
    def get_cache_path(self, cache_key: str) -> str:
        """
        Get the file path for a cache key.
        
        Args:
            cache_key: Cache key
            
        Returns:
            File path
        """
        return os.path.join(self.cache_dir, f"{cache_key}.json")
    
    def save_to_cache(self, endpoint: str, params: Dict[str, Any], data: Any) -> None:
        """
        Save API response data to cache.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            data: Response data to cache
        """
        cache_key = self.get_cache_key(endpoint, params)
        cache_path = self.get_cache_path(cache_key)
        
        cache_data = {
            "endpoint": endpoint,
            "params": params,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2)
            logging.debug(f"Saved data to cache: {cache_path}")
        except Exception as e:
            logging.error(f"Error saving to cache: {str(e)}")
    
    def load_from_cache(self, endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
        """
        Load API response data from cache if available and not expired.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            Cached data or None if not available or expired
        """
        cache_key = self.get_cache_key(endpoint, params)
        cache_path = self.get_cache_path(cache_key)
        
        # Add detailed logging
        logging.info(f"Attempting to load from cache: {endpoint} (key: {cache_key})")
        
        # Check if cache file exists
        if not os.path.exists(cache_path):
            logging.info(f"Cache miss: {endpoint} - File does not exist: {cache_path}")
            return None
        
        try:
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)
            
            # Check if cache is expired
            timestamp = datetime.fromisoformat(cache_data["timestamp"])
            age = datetime.now() - timestamp
            
            if age > timedelta(days=self.max_age_days):
                logging.info(f"Cache expired: {endpoint} (age: {age.days} days, {age.seconds // 3600} hours) - max age: {self.max_age_days} days")
                return None
            
            logging.info(f"Cache hit: {endpoint} (age: {age.days} days, {age.seconds // 3600} hours)")
            return cache_data["data"]
            
        except Exception as e:
            logging.error(f"Error loading from cache: {str(e)}")
            return None
    
    def clear_cache(self, older_than_days: Optional[int] = None) -> int:
        """
        Clear cache files.
        
        Args:
            older_than_days: Only clear files older than this many days (None for all)
            
        Returns:
            Number of files cleared
        """
        count = 0
        now = datetime.now()
        
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith('.json'):
                continue
                
            file_path = os.path.join(self.cache_dir, filename)
            
            # If older_than_days is specified, check file age
            if older_than_days is not None:
                try:
                    with open(file_path, 'r') as f:
                        cache_data = json.load(f)
                    
                    timestamp = datetime.fromisoformat(cache_data["timestamp"])
                    age = now - timestamp
                    
                    if age <= timedelta(days=older_than_days):
                        continue
                except Exception:
                    # If we can't read the file, assume it's corrupt and delete it
                    pass
            
            try:
                os.remove(file_path)
                count += 1
            except Exception as e:
                logging.error(f"Error deleting cache file {file_path}: {str(e)}")
        
        logging.info(f"Cleared {count} cache files")
        return count
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache.
        
        Returns:
            Dictionary with cache statistics
        """
        total_files = 0
        total_size = 0
        oldest_timestamp = None
        newest_timestamp = None
        
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith('.json'):
                continue
                
            file_path = os.path.join(self.cache_dir, filename)
            total_files += 1
            total_size += os.path.getsize(file_path)
            
            try:
                with open(file_path, 'r') as f:
                    cache_data = json.load(f)
                
                timestamp = datetime.fromisoformat(cache_data["timestamp"])
                
                if oldest_timestamp is None or timestamp < oldest_timestamp:
                    oldest_timestamp = timestamp
                
                if newest_timestamp is None or timestamp > newest_timestamp:
                    newest_timestamp = timestamp
            except Exception:
                # Skip files we can't read
                pass
        
        return {
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "oldest_timestamp": oldest_timestamp.isoformat() if oldest_timestamp else None,
            "newest_timestamp": newest_timestamp.isoformat() if newest_timestamp else None
        } 