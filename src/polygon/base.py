"""
Base Polygon client with authentication, rate limiting, and caching support.
"""

import configparser
import os
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Dict, Any
from pathlib import Path

from polygon import RESTClient
from loguru import logger
from diskcache import Cache

# Configure logger
logger.add("logs/polygon_{time}.log", rotation="1 day", retention="7 days", level="INFO")


class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, max_calls_per_minute: int):
        self.max_calls_per_minute = max_calls_per_minute
        self.calls = []
        
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if self.max_calls_per_minute == -1:  # Unlimited
                return func(*args, **kwargs)
                
            now = time.time()
            # Remove calls older than 1 minute
            self.calls = [call_time for call_time in self.calls if now - call_time < 60]
            
            if len(self.calls) >= self.max_calls_per_minute:
                sleep_time = 60 - (now - self.calls[0])
                if sleep_time > 0:
                    logger.warning(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                    self.calls = []
                    
            self.calls.append(time.time())
            return func(*args, **kwargs)
        return wrapper


class PolygonBase:
    """Base class for Polygon API interactions with caching and rate limiting."""
    
    def __init__(self, config_path: str = "config/config.ini"):
        """
        Initialize the Polygon client.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self.client = RESTClient(self.config['polygon']['api_key'])
        
        # Set up rate limiter
        rpm = int(self.config['rate_limits']['polygon_rpm'])
        self.rate_limiter = RateLimiter(rpm if rpm > 0 else -1)
        
        # Set up cache
        cache_dir = Path(self.config['paths']['cache_dir'])
        cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache = Cache(str(cache_dir))
        
        # Data directory
        self.data_dir = Path(self.config['paths']['data_dir'])
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized Polygon client with {rpm} requests/minute limit")
        
    def _load_config(self, config_path: str) -> configparser.ConfigParser:
        """Load configuration from file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Config file not found at {config_path}. "
                "Please copy config/config.ini.example to config/config.ini and add your API key."
            )
            
        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Validate required sections
        required_sections = ['polygon', 'paths', 'rate_limits', 'cache']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")
                
        # Validate API key
        if not config['polygon'].get('api_key') or config['polygon']['api_key'] == 'YOUR_POLYGON_API_KEY_HERE':
            raise ValueError("Please set your Polygon API key in config/config.ini")
            
        return config
        
    def cache_key(self, prefix: str, **kwargs) -> str:
        """Generate a cache key from prefix and parameters."""
        params = "_".join(f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None)
        return f"{prefix}_{params}"
        
    def get_cache_ttl(self, data_type: str) -> int:
        """Get cache TTL for a data type from config."""
        ttl_key = f"{data_type}_data_ttl"
        return int(self.config['cache'].get(ttl_key, 3600))
        
    def with_cache(self, data_type: str):
        """Decorator to add caching to API calls."""
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                # Generate cache key
                cache_key = self.cache_key(func.__name__, *args, **kwargs)
                
                # Check cache
                cached_data = self.cache.get(cache_key)
                if cached_data is not None:
                    logger.debug(f"Cache hit for {cache_key}")
                    return cached_data
                    
                # Call the actual function
                logger.debug(f"Cache miss for {cache_key}")
                result = func(self, *args, **kwargs)
                
                # Store in cache
                ttl = self.get_cache_ttl(data_type)
                self.cache.set(cache_key, result, expire=ttl)
                
                return result
            return wrapper
        return decorator
        
    def clear_cache(self, prefix: Optional[str] = None):
        """Clear cache entries. If prefix is provided, only clear matching keys."""
        if prefix:
            # Clear specific prefix
            keys_to_delete = [key for key in self.cache if key.startswith(prefix)]
            for key in keys_to_delete:
                del self.cache[key]
            logger.info(f"Cleared {len(keys_to_delete)} cache entries with prefix '{prefix}'")
        else:
            # Clear all
            self.cache.clear()
            logger.info("Cleared all cache entries")
            
    def handle_pagination(self, api_iterator, limit: Optional[int] = None) -> list:
        """
        Handle pagination for Polygon API responses.
        
        Args:
            api_iterator: Iterator from Polygon API
            limit: Maximum number of items to fetch
            
        Returns:
            List of all items from paginated response
        """
        items = []
        count = 0
        
        try:
            for item in api_iterator:
                items.append(item)
                count += 1
                
                if limit and count >= limit:
                    break
                    
        except Exception as e:
            logger.error(f"Error during pagination: {e}")
            raise
            
        logger.debug(f"Fetched {len(items)} items")
        return items