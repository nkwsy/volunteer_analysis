import re
import time
import logging
import requests
from typing import Dict, List, Tuple, Optional
import os
from dotenv import load_dotenv
import random

# Load environment variables
load_dotenv()

def sanitize_address(address: str) -> str:
    """
    Clean up address string for geocoding.
    
    Args:
        address: Address string to sanitize
        
    Returns:
        Sanitized address string
    """
    # Remove unit/apartment numbers as they often cause geocoding failures
    replacements = [
        (r'\bUnit\s+\w+', ''),  # Remove "Unit XXX"
        (r'\bApt\.?\s+\w+', ''), # Remove "Apt XXX" or "Apt. XXX"
        (r'\b[A-Z]$', ''),      # Remove single letter unit numbers at end
        (r'\.', ''),            # Remove periods
        (r'\s+', ' '),          # Normalize whitespace
    ]
    
    result = address
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    
    return result.strip()

def is_zip_code_only(address: str) -> bool:
    """
    Check if the address is only a zip code.
    
    Args:
        address: Address string to check
        
    Returns:
        True if the address is only a zip code, False otherwise
    """
    # Remove all whitespace and check if the result is a 5-digit number
    clean = re.sub(r'\s+', '', address)
    return bool(re.match(r'^\d{5}(-\d{4})?$', clean))

def geocode_zip_code(zip_code: str, api_key: Optional[str] = None) -> Optional[Tuple[float, float]]:
    """
    Geocode a zip code to get coordinates, with a small random offset to distribute points.
    
    Args:
        zip_code: Zip code to geocode
        api_key: Google Maps API key (optional, will use GOOGLE_MAPS_API_KEY env var if not provided)
        
    Returns:
        Tuple of (latitude, longitude) or None if geocoding failed
    """
    # Get API key from environment if not provided
    if not api_key:
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        
    if not api_key:
        logging.error("No Google Maps API key provided. Set GOOGLE_MAPS_API_KEY in your .env file.")
        return None
        
    try:
        # Clean the zip code
        clean_zip = re.sub(r'\s+', '', zip_code)
        if '-' in clean_zip:
            clean_zip = clean_zip.split('-')[0]  # Use only the first part of ZIP+4
        
        # Build the Google Geocoding API URL
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": clean_zip,
            "key": api_key
        }
        
        # Make the request
        response = requests.get(base_url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Check if Google returned results
            if data["status"] == "OK" and len(data["results"]) > 0:
                # Get the first result
                location = data["results"][0]["geometry"]["location"]
                
                # Add a small random offset (approximately within 500m)
                # 0.005 degrees is roughly 500 meters
                lat_offset = random.uniform(-0.003, 0.003)
                lng_offset = random.uniform(-0.003, 0.003)
                
                return (location["lat"] + lat_offset, location["lng"] + lng_offset)
            else:
                logging.warning(f"Google Geocoding API returned no results for zip code: {zip_code}. Status: {data['status']}")
                return None
        else:
            logging.error(f"Google Geocoding API request failed with status code {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logging.error(f"Error geocoding zip code {zip_code}: {str(e)}")
        return None

def geocode_address(address: str, api_key: Optional[str] = None) -> Optional[Tuple[float, float]]:
    """
    Geocode an address to get coordinates using Google's Geocoding API.
    
    Args:
        address: Address string to geocode
        api_key: Google Maps API key (optional, will use GOOGLE_MAPS_API_KEY env var if not provided)
        
    Returns:
        Tuple of (latitude, longitude) or None if geocoding failed
    """
    # Get API key from environment if not provided
    if not api_key:
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        
    if not api_key:
        logging.error("No Google Maps API key provided. Set GOOGLE_MAPS_API_KEY in your .env file.")
        return None
    
    # Check if this is a zip code only
    if is_zip_code_only(address):
        logging.info(f"Geocoding zip code: {address}")
        return geocode_zip_code(address, api_key)
        
    try:
        # Sanitize the address
        clean_address = sanitize_address(address)
        
        # Build the Google Geocoding API URL
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": clean_address,
            "key": api_key
        }
        
        # Make the request
        response = requests.get(base_url, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Check if Google returned results
            if data["status"] == "OK" and len(data["results"]) > 0:
                # Get the first result
                location = data["results"][0]["geometry"]["location"]
                return (location["lat"], location["lng"])
            else:
                logging.warning(f"Google Geocoding API returned no results for address: {address}. Status: {data['status']}")
                return None
        else:
            logging.error(f"Google Geocoding API request failed with status code {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logging.error(f"Error geocoding address {address}: {str(e)}")
        return None

def batch_geocode(addresses: List[Dict], api_key: Optional[str] = None, exclude_zip_only: bool = False) -> List[Dict]:
    """
    Geocode a batch of addresses using Google's Geocoding API.
    
    Args:
        addresses: List of dictionaries with address information
        api_key: Google Maps API key (optional, will use GOOGLE_MAPS_API_KEY env var if not provided)
        exclude_zip_only: Whether to exclude addresses that are only zip codes
        
    Returns:
        List of dictionaries with added coordinates
    """
    # Get API key from environment if not provided
    if not api_key:
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        
    if not api_key:
        logging.error("No Google Maps API key provided. Set GOOGLE_MAPS_API_KEY in your .env file.")
        return addresses
    
    geocoded = []
    
    for addr in addresses:
        try:
            # Check if address field exists
            if 'address' not in addr or not addr['address']:
                logging.warning(f"No address found for entry: {addr}")
                continue
            
            # Check if this is a zip code only address
            is_zip_only = is_zip_code_only(addr['address'])
            
            # If we should exclude zip code only addresses and this is one, skip it
            if exclude_zip_only and is_zip_only:
                logging.info(f"Skipping zip code only address: {addr['address']}")
                continue
                
            # Geocode the address
            result = geocode_address(addr['address'], api_key)
            
            if result:
                lat, lng = result
                addr_copy = addr.copy()
                addr_copy['latitude'] = lat
                addr_copy['longitude'] = lng
                addr_copy['is_zip_only'] = is_zip_only
                geocoded.append(addr_copy)
                # Be nice to the geocoding service - Google allows 50 requests per second
                # but we'll be conservative
                time.sleep(0.1)
            else:
                logging.warning(f"Could not geocode address: {addr['address']}")
        except Exception as e:
            logging.error(f"Error processing address {addr.get('address', 'unknown')}: {str(e)}")
            continue
    
    return geocoded