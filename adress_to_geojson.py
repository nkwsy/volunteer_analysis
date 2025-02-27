import csv
import json
import time
import re
from typing import Dict, List
from pathlib import Path
from utils.osmnx_load import get_ox
ox = get_ox()
def read_addresses(csv_path: str) -> List[Dict]:
    """Read addresses from CSV file."""
    addresses = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            # Combine address components
            full_address = f"{row[3]}, {row[4]}, {row[5]} {row[6]}"
            addresses.append({
                'name': f"{row[0]} {row[1]}",
                'email': row[2],
                'address': full_address
            })
    return addresses

def sanitize_address(address: str) -> str:
    """Clean up address string for geocoding."""
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

def geocode_addresses(addresses: List[Dict]) -> List[Dict]:
    """Geocode addresses to get coordinates using OSMnx."""
    geocoded = []
    
    for addr in addresses:
        try:
            # Sanitize the address before geocoding
            clean_address = sanitize_address(addr['address'])
            # Use OSMnx's geocoder (which uses Nominatim)
            result = ox.geocode(clean_address)
            if result:
                lat, lng = result
                geocoded.append({
                    **addr,
                    'coordinates': [lng, lat]  # GeoJSON uses [longitude, latitude]
                })
                # Be nice to the geocoding service
                time.sleep(1)
            else:
                print(f"Could not geocode address: {addr['address']}")
        except Exception as e:
            print(f"Error geocoding address {addr['address']}: {str(e)}")
            continue
    
    return geocoded

def create_geojson(geocoded_addresses: List[Dict], output_path: str):
    """Create GeoJSON file from geocoded addresses."""
    features = []
    for addr in geocoded_addresses:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": addr['coordinates']
            },
            "properties": {
                "name": addr['name'],
                "email": addr['email'],
                "address": addr['address']
            }
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(output_path, 'w') as f:
        json.dump(geojson, f, indent=2)

def main():
    input_csv = "addresses.csv"
    output_geojson = "addresses.geojson"
    
    print("Reading addresses...")
    addresses = read_addresses(input_csv)
    
    print("Geocoding addresses...")
    geocoded = geocode_addresses(addresses)
    
    print("Creating GeoJSON file...")
    create_geojson(geocoded, output_geojson)
    
    print(f"Done! Created {output_geojson} with {len(geocoded)} points")

if __name__ == "__main__":
    main() 