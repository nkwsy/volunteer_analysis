import os
import requests
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
import json

from utils.cache_manager import CacheManager

class GalaxyDigitalAPI:
    """
    Client for interacting with the Galaxy Digital API.
    
    This class provides methods to fetch volunteer data, opportunities, 
    and hours logged from the Galaxy Digital platform.
    """
    
    def __init__(self, api_key: Optional[str] = None, email: Optional[str] = None, 
                 password: Optional[str] = None, base_url: Optional[str] = None,
                 debug: bool = False, skip_login: bool = False, use_cache: bool = True,
                 cache_dir: str = "cache", cache_max_age_days: int = 7, test_mode: bool = False,
                 test_limit: int = 10):
        """
        Initialize the Galaxy Digital API client.
        
        Args:
            api_key: API key for Galaxy Digital. If not provided, will be read from environment.
            email: Email for Galaxy Digital login. If not provided, will be read from environment.
            password: Password for Galaxy Digital login. If not provided, will be read from environment.
            base_url: Base URL for the API. If not provided, will be read from environment.
            debug: Whether to enable debug logging
            skip_login: Whether to skip the initial login attempt (useful for testing)
            use_cache: Whether to use caching for API responses
            cache_dir: Directory to store cache files
            cache_max_age_days: Maximum age of cache files in days before they're considered stale
            test_mode: Whether to run in test mode (limits the number of users queried)
            test_limit: Maximum number of users to query in test mode
        """
        self.api_key = api_key or os.getenv("GALAXY_API_KEY")
        self.email = email or os.getenv("GALAXY_EMAIL")
        self.password = password or os.getenv("GALAXY_PASSWORD")
        self.base_url = base_url or os.getenv("GALAXY_BASE_URL", "https://api.galaxydigital.com/api")
        self.debug = debug or os.getenv("DEBUG", "False").lower() == "true"
        self.use_cache = use_cache
        self.test_mode = test_mode
        self.test_limit = test_limit
        
        if self.test_mode:
            logging.info(f"Running in TEST MODE - limiting to {self.test_limit} users")
        
        # Initialize cache manager if caching is enabled
        if self.use_cache:
            self.cache_manager = CacheManager(cache_dir=cache_dir, max_age_days=cache_max_age_days)
            logging.info(f"API caching enabled (max age: {cache_max_age_days} days)")
        else:
            self.cache_manager = None
            logging.info("API caching disabled")
        
        if self.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.debug("Debug mode enabled for Galaxy Digital API")
        
        if not self.api_key or not self.email or not self.password:
            logging.warning("Galaxy Digital API key, email, or password not provided. API calls will fail.")
            logging.debug(f"API Key present: {bool(self.api_key)}, Email present: {bool(self.email)}, Password present: {bool(self.password)}")
        
        self.session = requests.Session()
        self.token = None
        self.login_response = None
        
        # Authenticate on initialization unless skipped
        if not skip_login:
            try:
                self.login()
            except Exception as e:
                logging.error(f"Initial authentication failed: {str(e)}")
                logging.warning("Continuing without authentication. Some API calls may fail.")
    
    def login(self, max_retries: int = 3, retry_delay: int = 2) -> Optional[str]:
        """
        Authenticate with the Galaxy Digital API.
        
        Args:
            max_retries: Maximum number of login attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            Authentication token or None if authentication failed
        """
        login_url = f"{self.base_url}/users/login"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        data = {
            'key': self.api_key,
            'user_email': self.email,
            'user_password': self.password,
        }
        
        if self.debug:
            # Mask password in debug output
            debug_data = data.copy()
            if 'user_password' in debug_data:
                debug_data['user_password'] = '********'
            logging.debug(f"Login request to {login_url} with data: {json.dumps(debug_data)}")
        
        for attempt in range(max_retries):
            try:
                response = requests.post(login_url, headers=headers, json=data)
                
                if self.debug:
                    logging.debug(f"Login response status: {response.status_code}")
                    if response.status_code != 200:
                        logging.debug(f"Response content: {response.text[:500]}...")
                
                # Handle different response status codes
                if response.status_code == 200:
                    resp_data = response.json()
                    self.login_response = resp_data.get('data', {})
                    self.token = self.login_response.get('token')
                    
                    if not self.token:
                        logging.error("Authentication succeeded but no token was returned")
                        return None
                    
                    # Update session headers with token
                    self.session.headers.update({
                        'Accept': 'application/json',
                        'Authorization': f"Bearer {self.token}"
                    })
                    
                    logging.info("Successfully authenticated with Galaxy Digital API")
                    return self.token
                elif response.status_code == 401:
                    logging.error("Authentication failed: Invalid credentials")
                    return None
                elif response.status_code == 500:
                    if attempt < max_retries - 1:
                        logging.warning(f"Server error during login (attempt {attempt+1}/{max_retries}). Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logging.error("Server error during login. Max retries exceeded.")
                        response.raise_for_status()
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"Error authenticating with Galaxy Digital API: {str(e)}")
                if hasattr(e, 'response') and e.response:
                    logging.error(f"Response: {e.response.text[:500]}...")
                
                if attempt < max_retries - 1:
                    logging.warning(f"Retrying login in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    logging.error("Max retries exceeded for login")
                    raise
        
        return None
    
    def _make_request(self, endpoint: str, method: str = "GET", params: Optional[Dict] = None, 
                     data: Optional[Dict] = None, handle_404: bool = False, use_cache: Optional[bool] = None) -> Dict:
        """
        Make a request to the Galaxy Digital API.
        
        Args:
            endpoint: API endpoint to call
            method: HTTP method (GET, POST, etc.)
            params: Query parameters
            data: Request body for POST/PUT requests
            handle_404: If True, return empty data on 404 instead of raising an exception
            use_cache: Whether to use cache for this request (overrides instance setting)
            
        Returns:
            Response data as dictionary
        """
        url = f"{self.base_url}/{endpoint}"
        
        if params is None:
            params = {}
            
        # Add default parameters for pagination
        if method == "GET" and 'per_page' not in params:
            params['per_page'] = 150
        
        # Determine if we should use cache for this request
        should_use_cache = self.use_cache if use_cache is None else use_cache
        
        # Add detailed logging
        logging.info(f"Making request to {endpoint} (method: {method}, use_cache: {should_use_cache})")
        
        # Only use cache for GET requests
        if should_use_cache and method == "GET" and self.cache_manager:
            logging.info(f"Checking cache for {endpoint}")
            # Try to get from cache first
            cached_data = self.cache_manager.load_from_cache(endpoint, params)
            if cached_data is not None:
                logging.info(f"Using cached data for {endpoint}")
                return {"data": cached_data}
            else:
                logging.info(f"No valid cache found for {endpoint}, making API request")
        else:
            if not should_use_cache:
                logging.info(f"Cache disabled for this request to {endpoint}")
            elif method != "GET":
                logging.info(f"Cache not used for non-GET request to {endpoint}")
            elif not self.cache_manager:
                logging.info(f"Cache manager not available for request to {endpoint}")
        
        # If not in cache or cache disabled, make the actual request
        try:
            # Add a small delay to avoid rate limiting
            time.sleep(0.1)
            
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data
            )
            
            # Handle 404 errors specially if requested
            if handle_404 and response.status_code == 404:
                if self.debug:
                    logging.debug(f"404 Not Found for {url} - returning empty data")
                return {"data": []}
            
            # Handle rate limiting (429 Too Many Requests)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 5))
                logging.warning(f"Rate limited by API. Waiting {retry_after} seconds before retrying...")
                time.sleep(retry_after)
                return self._make_request(endpoint, method, params, data, handle_404, use_cache)
                
            response.raise_for_status()
            result = response.json()
            
            # Save successful GET responses to cache
            if should_use_cache and method == "GET" and self.cache_manager:
                self.cache_manager.save_to_cache(endpoint, params, result.get('data', []))
            
            return result
        except requests.exceptions.RequestException as e:
            logging.error(f"Error making request to Galaxy Digital API: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logging.error(f"Response: {e.response.text}")
            
            # If unauthorized, try to re-authenticate and retry
            if hasattr(e, 'response') and e.response and e.response.status_code == 401:
                logging.info("Token expired, attempting to re-authenticate")
                self.login()
                return self._make_request(endpoint, method, params, data, handle_404, use_cache)
            
            # If handle_404 is True and we got a 404, return empty data
            if handle_404 and hasattr(e, 'response') and e.response and e.response.status_code == 404:
                if self.debug:
                    logging.debug(f"404 Not Found for {url} - returning empty data")
                return {"data": []}
                
            raise
    
    def get_all_data(self, endpoint: str, params: Optional[Dict] = None, use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get all data from a paginated endpoint.
        
        Args:
            endpoint: API endpoint to call
            params: Additional query parameters
            use_cache: Whether to use cache for this request
            
        Returns:
            List of all data from all pages
        """
        # Create a consistent cache key for both saving and loading
        if params is None:
            params = {}
        
        # Make a copy of params to avoid modifying the original
        working_params = params.copy()
        
        # Add standard parameters
        working_params['per_page'] = 150
        working_params['show_inactive'] = 'No'
        
        # Check if we have a complete cached response first
        should_use_cache = self.use_cache if use_cache is None else use_cache
        
        # Log the cache status for this request
        logging.info(f"get_all_data for {endpoint}: use_cache={should_use_cache}, cache_manager={self.cache_manager is not None}")
        
        if should_use_cache and self.cache_manager:
            # Create a special cache key for the complete dataset
            cache_params = working_params.copy()
            cache_params['complete_dataset'] = True
            
            # Try to load from cache
            logging.info(f"Attempting to load complete dataset for {endpoint} from cache")
            cached_data = self.cache_manager.load_from_cache(endpoint, cache_params)
            if cached_data is not None:
                logging.info(f"Using complete cached dataset for {endpoint} ({len(cached_data)} records)")
                
                # If in test mode, limit the number of records returned
                if self.test_mode and endpoint == "users":
                    limited_data = cached_data[:self.test_limit]
                    logging.info(f"TEST MODE: Limiting {len(cached_data)} records to {len(limited_data)} records")
                    return limited_data
                
                return cached_data
            else:
                logging.info(f"No complete cached dataset found for {endpoint}")
                
                # Try to find any cache file for this endpoint to diagnose issues
                cache_dir = self.cache_manager.cache_dir
                if os.path.exists(cache_dir):
                    cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.json')]
                    logging.info(f"Found {len(cache_files)} cache files in {cache_dir}")
                    
                    # Check if there are any cache files for this endpoint
                    endpoint_cache_key = self.cache_manager.get_cache_key(endpoint, {})
                    endpoint_cache_files = [f for f in cache_files if endpoint_cache_key in f]
                    if endpoint_cache_files:
                        logging.info(f"Found {len(endpoint_cache_files)} cache files for {endpoint}: {', '.join(endpoint_cache_files)}")
                    else:
                        logging.info(f"No cache files found for {endpoint}")
        
        all_data = []
        records = 0
        page_return = 150
        
        # If in test mode and endpoint is "users", limit to one page
        max_pages = 1 if self.test_mode and endpoint == "users" else float('inf')
        current_page = 0
        
        while True:
            current_page += 1
            
            # If in test mode and we've reached the max pages, break
            if self.test_mode and endpoint == "users" and current_page > max_pages:
                logging.info(f"TEST MODE: Stopping after {max_pages} page(s) with {records} records")
                break
                
            if records != 0 and all_data:
                working_params['since_id'] = all_data[-1]['id']
                
            response = self._make_request(endpoint, params=working_params, use_cache=use_cache)
            data = response.get('data', [])
            all_data.extend(data)
            
            records += len(data)
            page_return = len(data)
            
            # If in test mode and we have enough records, break
            if self.test_mode and endpoint == "users" and records >= self.test_limit:
                logging.info(f"TEST MODE: Reached limit of {self.test_limit} records, stopping pagination")
                # Trim to exact limit
                all_data = all_data[:self.test_limit]
                break
                
            if page_return != 150:
                logging.debug(f"Finished pagination: {records} total records retrieved")
                
                # Save the complete dataset to cache
                if should_use_cache and self.cache_manager:
                    # Use the same cache key format as when loading
                    cache_params = working_params.copy()
                    cache_params['complete_dataset'] = True
                    
                    # Remove the since_id parameter which is specific to pagination
                    if 'since_id' in cache_params:
                        del cache_params['since_id']
                    
                    logging.info(f"Saving complete dataset to cache for {endpoint} ({len(all_data)} records)")
                    self.cache_manager.save_to_cache(endpoint, cache_params, all_data)
                    
                return all_data
        
        # If we exited the loop due to test mode, save what we have to cache
        if self.test_mode and endpoint == "users" and should_use_cache and self.cache_manager:
            cache_params = working_params.copy()
            cache_params['complete_dataset'] = True
            cache_params['test_mode'] = True
            
            if 'since_id' in cache_params:
                del cache_params['since_id']
            
            logging.info(f"TEST MODE: Saving limited dataset to cache for {endpoint} ({len(all_data)} records)")
            self.cache_manager.save_to_cache(endpoint, cache_params, all_data)
            
        return all_data
    
    def get_volunteers(self, params: Optional[Dict] = None, use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get list of volunteers.
        
        Args:
            params: Additional query parameters
            use_cache: Whether to use cache for this request
            
        Returns:
            List of volunteer data
        """
        # If use_cache is not specified, use the instance setting
        if use_cache is None:
            use_cache = self.use_cache
            
        logging.info(f"Getting volunteers with use_cache={use_cache}" + 
                    (f", TEST MODE (limit={self.test_limit})" if self.test_mode else ""))
        
        # Check if we have a complete cached dataset first
        if use_cache and self.cache_manager:
            # Create a special cache key for the complete dataset
            cache_params = {'complete_dataset': True, 'per_page': 150, 'show_inactive': 'No'}
            
            if self.test_mode:
                cache_params['test_mode'] = True
                
            # Try to load from cache
            cached_data = self.cache_manager.load_from_cache("users", cache_params)
            if cached_data is not None:
                if self.test_mode and len(cached_data) > self.test_limit:
                    limited_data = cached_data[:self.test_limit]
                    logging.info(f"TEST MODE: Using cached volunteer data (limited from {len(cached_data)} to {len(limited_data)} records)")
                    return limited_data
                else:
                    logging.info(f"Using cached volunteer data ({len(cached_data)} records)")
                    return cached_data
        
        # If not in cache or cache disabled, get from API
        return self.get_all_data("users", params=params, use_cache=use_cache)
    
    def get_volunteer(self, volunteer_id: str, use_cache: Optional[bool] = None) -> Dict:
        """
        Get details for a specific volunteer.
        
        Args:
            volunteer_id: ID of the volunteer
            use_cache: Whether to use cache for this request
            
        Returns:
            Volunteer data
        """
        response = self._make_request(f"users/{volunteer_id}", use_cache=use_cache)
        volunteer_data = response.get('data', {})
        
        if self.debug and volunteer_data:
            logging.debug(f"Retrieved detailed data for volunteer {volunteer_id}")
            logging.debug(f"Volunteer data keys: {list(volunteer_data.keys())}")
            
        return volunteer_data
    
    def get_volunteer_hours(self, volunteer_id: str, start_date: Optional[str] = None, 
                           end_date: Optional[str] = None, use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get hours logged by a volunteer.
        
        Args:
            volunteer_id: ID of the volunteer
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            use_cache: Whether to use cache for this request
            
        Returns:
            List of hour entries
        """
        params = {'per_page': 150}  # Ensure we get as many records as possible
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        
        # Use handle_404=True to return empty list for volunteers with no hours
        response = self._make_request(f"users/{volunteer_id}/hours", params=params, handle_404=True, use_cache=use_cache)
        hours_data = response.get('data', [])
        
        if self.debug:
            if hours_data:
                logging.debug(f"Retrieved {len(hours_data)} hours for volunteer {volunteer_id}")
                # Log the structure of the first hour entry to help with debugging
                if len(hours_data) > 0:
                    logging.debug(f"Sample hour entry keys: {list(hours_data[0].keys())}")
            else:
                logging.debug(f"No hours found for volunteer {volunteer_id}")
                
        return hours_data
    
    def get_opportunities(self, params: Optional[Dict] = None, use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get list of volunteer opportunities.
        
        Args:
            params: Additional query parameters
            use_cache: Whether to use cache for this request
            
        Returns:
            List of opportunity data
        """
        return self.get_all_data("needs", params=params, use_cache=use_cache)
    
    def get_opportunity(self, opportunity_id: str, use_cache: Optional[bool] = None) -> Dict:
        """
        Get details for a specific opportunity.
        
        Args:
            opportunity_id: ID of the opportunity
            use_cache: Whether to use cache for this request
            
        Returns:
            Opportunity data
        """
        response = self._make_request(f"needs/{opportunity_id}", use_cache=use_cache)
        return response.get('data', {})
    
    def get_opportunity_volunteers(self, opportunity_id: str, use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get volunteers who participated in an opportunity.
        
        Args:
            opportunity_id: ID of the opportunity
            use_cache: Whether to use cache for this request
            
        Returns:
            List of volunteers
        """
        response = self._make_request(f"needs/{opportunity_id}/responses", use_cache=use_cache)
        return response.get('data', [])
    
    def get_hours_summary(self, start_date: Optional[str] = None, 
                         end_date: Optional[str] = None, use_cache: Optional[bool] = None) -> Dict:
        """
        Get summary of hours logged across all volunteers.
        
        Args:
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            use_cache: Whether to use cache for this request
            
        Returns:
            Summary data
        """
        params = {}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
            
        response = self._make_request("hours/summary", params=params, use_cache=use_cache)
        return response.get('data', {})
    
    def get_all_hours(self, start_date: Optional[str] = None, 
                     end_date: Optional[str] = None, use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get all hours data in one API call.
        
        Args:
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            use_cache: Whether to use cache for this request
            
        Returns:
            List of all hours data
        """
        params = {'per_page': 150}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
            
        logging.info(f"Getting all hours data with params: {params}")
        
        # Use the get_all_data method to handle pagination
        return self.get_all_data("hours", params=params, use_cache=use_cache)
    
    def get_volunteer_addresses(self, use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get addresses for all volunteers.
        
        Args:
            use_cache: Whether to use cache for this request
            
        Returns:
            List of volunteer addresses
        """
        volunteers = self.get_volunteers(use_cache=use_cache)
        addresses = []
        
        for volunteer in volunteers:
            if 'address' in volunteer and volunteer['address']:
                addresses.append({
                    'name': f"{volunteer.get('first_name', '')} {volunteer.get('last_name', '')}",
                    'email': volunteer.get('email', ''),
                    'address': volunteer['address'],
                    'volunteer_id': volunteer.get('id')
                })
        
        return addresses
        
    def clear_cache(self, older_than_days: Optional[int] = None) -> int:
        """
        Clear the API cache.
        
        Args:
            older_than_days: Only clear files older than this many days (None for all)
            
        Returns:
            Number of files cleared
        """
        if not self.cache_manager:
            logging.warning("Cache manager not initialized. Cannot clear cache.")
            return 0
            
        return self.cache_manager.clear_cache(older_than_days=older_than_days)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache.
        
        Returns:
            Dictionary with cache statistics or empty dict if caching is disabled
        """
        if not self.cache_manager:
            logging.warning("Cache manager not initialized. Cannot get cache stats.")
            return {}
            
        return self.cache_manager.get_cache_stats()
    
    def get_detailed_volunteers(self, volunteer_ids: List[str], use_cache: Optional[bool] = None) -> List[Dict]:
        """
        Get detailed information for a list of volunteers.
        
        Args:
            volunteer_ids: List of volunteer IDs
            use_cache: Whether to use cache for this request
            
        Returns:
            List of detailed volunteer data
        """
        detailed_volunteers = []
        
        # Check if we have a complete cached dataset first
        if self.use_cache if use_cache is None else use_cache:
            if self.cache_manager:
                # Try to load from cache first
                cache_key = "detailed_volunteers"
                cache_params = {'ids': ','.join(sorted(volunteer_ids))}
                cached_data = self.cache_manager.load_from_cache(cache_key, cache_params)
                if cached_data is not None:
                    logging.info(f"Using cached detailed volunteer data for {len(cached_data)} volunteers")
                    return cached_data
        
        # If we have a lot of IDs, process them in batches to avoid rate limiting
        batch_size = 100
        total_batches = (len(volunteer_ids) + batch_size - 1) // batch_size
        
        logging.info(f"Getting detailed data for {len(volunteer_ids)} volunteers in {total_batches} batches")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(volunteer_ids))
            batch_ids = volunteer_ids[start_idx:end_idx]
            
            logging.info(f"Processing batch {batch_num + 1}/{total_batches} with {len(batch_ids)} volunteers")
            
            # Add a small delay between batches to avoid rate limiting
            if batch_num > 0:
                time.sleep(1)
                
            for volunteer_id in batch_ids:
                try:
                    volunteer_data = self.get_volunteer(volunteer_id, use_cache=use_cache)
                    if volunteer_data:
                        detailed_volunteers.append(volunteer_data)
                except Exception as e:
                    logging.warning(f"Error getting detailed data for volunteer {volunteer_id}: {str(e)}")
            
            # Log progress
            logging.info(f"Completed batch {batch_num + 1}/{total_batches}, retrieved {len(detailed_volunteers)} volunteers so far")
        
        # Save to cache if enabled
        if self.use_cache if use_cache is None else use_cache:
            if self.cache_manager and detailed_volunteers:
                cache_key = "detailed_volunteers"
                cache_params = {'ids': ','.join(sorted(volunteer_ids))}
                self.cache_manager.save_to_cache(cache_key, cache_params, detailed_volunteers)
                logging.info(f"Saved detailed data for {len(detailed_volunteers)} volunteers to cache")
                
        logging.info(f"Retrieved detailed data for {len(detailed_volunteers)} out of {len(volunteer_ids)} volunteers")
        return detailed_volunteers 