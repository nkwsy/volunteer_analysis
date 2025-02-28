import os
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import time

from api.galaxy_digital import GalaxyDigitalAPI
from models.volunteer import Volunteer, VolunteerHours
from models.opportunity import Opportunity, OpportunityParticipation


class DataService:
    """
    Service for handling volunteer and opportunity data.
    
    This class provides methods to fetch, process, and analyze volunteer and opportunity data.
    """
    
    def __init__(self, api_client: Optional[GalaxyDigitalAPI] = None):
        """
        Initialize the data service.
        
        Args:
            api_client: Galaxy Digital API client. If not provided, a new one will be created.
        """
        self.api_client = api_client
        self.api_available = self.api_client is not None
        self.volunteers: List[Volunteer] = []
        self.opportunities: List[Opportunity] = []
        self.volunteer_df: Optional[pd.DataFrame] = None
        self.opportunity_df: Optional[pd.DataFrame] = None
        self.hours_df: Optional[pd.DataFrame] = None
        
        if not self.api_available:
            logging.warning("No API client provided. Only local data operations will be available.")
    
    def load_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, debug_mode: bool = False):
        """
        Load data from the Galaxy Digital API.
        
        Args:
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            debug_mode: Whether to enable additional debug logging
        """
        if not self.api_available:
            logging.warning("Cannot load data from API: No API client available")
            return
            
        try:
            # Check if we have a valid cache file first
            cache_available = False
            if hasattr(self.api_client, 'cache_manager') and self.api_client.cache_manager:
                cache_stats = self.api_client.get_cache_stats()
                if cache_stats and cache_stats.get('total_files', 0) > 0:
                    logging.info(f"Found {cache_stats.get('total_files', 0)} cache files ({cache_stats.get('total_size_mb', 0):.2f} MB)")
                    cache_available = True
                else:
                    logging.info("No cache files found or cache is empty")
            
            # Load volunteers
            logging.info("Loading volunteer data from API or cache...")
            volunteer_data = self.api_client.get_volunteers()
            
            if not volunteer_data:
                logging.warning("No volunteer data returned from API or cache")
                return
                
            logging.info(f"Successfully loaded {len(volunteer_data)} volunteers from API or cache")
            
            # Check if we need to get detailed volunteer information
            # This is a sample of the first volunteer to check available fields
            if volunteer_data and len(volunteer_data) > 0:
                sample_volunteer = volunteer_data[0]
                required_fields = ['first_name', 'last_name', 'email', 'address', 'city', 'state', 'zip_code']
                missing_fields = [field for field in required_fields if field not in sample_volunteer]
                
                if missing_fields:
                    logging.info(f"Volunteer data is missing fields: {missing_fields}. Will fetch detailed information.")
                    
                    # Get a subset of volunteers for detailed info to avoid rate limiting
                    # In test mode, get all detailed info; in production, limit to 100
                    max_detailed = len(volunteer_data) if self.api_client.test_mode else min(100, len(volunteer_data))
                    volunteer_ids = [v['id'] for v in volunteer_data[:max_detailed]]
                    
                    detailed_volunteers = self.api_client.get_detailed_volunteers(volunteer_ids)
                    
                    # Create a lookup dictionary for detailed volunteer data
                    detailed_lookup = {v['id']: v for v in detailed_volunteers}
                    
                    # Update volunteer data with detailed information where available
                    for i, volunteer in enumerate(volunteer_data):
                        if volunteer['id'] in detailed_lookup:
                            volunteer_data[i] = detailed_lookup[volunteer['id']]
                            
                    logging.info(f"Updated {len(detailed_lookup)} volunteers with detailed information")
            
            # Load all hours data at once instead of per volunteer
            logging.info("Loading all hours data from API or cache...")
            try:
                all_hours_data = self.api_client.get_all_hours(start_date=start_date, end_date=end_date)
                logging.info(f"Successfully loaded {len(all_hours_data)} hour records from API or cache")
                
                # Create a dictionary to map volunteer IDs to their hours
                volunteer_hours_map = {}
                for hour_data in all_hours_data:
                    volunteer_id = hour_data.get('user_id')
                    if volunteer_id:
                        if volunteer_id not in volunteer_hours_map:
                            volunteer_hours_map[volunteer_id] = []
                        volunteer_hours_map[volunteer_id].append(hour_data)
                
                logging.info(f"Organized hours data for {len(volunteer_hours_map)} volunteers")
            except Exception as hours_error:
                logging.error(f"Error loading all hours data: {str(hours_error)}")
                logging.warning("Continuing without hours data")
                volunteer_hours_map = {}
            
            self.volunteers = []
            
            # Track errors for reporting
            error_count = 0
            total_volunteers = len(volunteer_data)
            
            logging.info(f"Processing {total_volunteers} volunteers...")
            
            # Save progress periodically
            save_interval = 100  # Save after every 100 volunteers
            last_save = 0
            
            for i, v_data in enumerate(volunteer_data):
                try:
                    # Log progress every 100 volunteers
                    if i > 0 and i % 100 == 0:
                        logging.info(f"Processed {i}/{total_volunteers} volunteers...")
                    
                    # Get hours for this volunteer from the pre-loaded map
                    volunteer_id = v_data['id']
                    hours_data = volunteer_hours_map.get(volunteer_id, [])
                    
                    # Convert hours data to VolunteerHours objects
                    hours = []
                    for h_data in hours_data:
                        # Extract opportunity ID from the need object if available
                        opportunity_id = ''
                        if 'need' in h_data and h_data['need'] is not None:
                            opportunity_id = h_data['need'].get('id', '')
                        
                        # Extract hour data
                        hour_date = None
                        if 'hour_date_start' in h_data and h_data['hour_date_start']:
                            try:
                                # Format from API is typically "2022-08-09 00:00:00"
                                hour_date = datetime.strptime(h_data['hour_date_start'], "%Y-%m-%d %H:%M:%S")
                            except ValueError:
                                try:
                                    # Try alternative format with timezone
                                    hour_date = datetime.fromisoformat(h_data['hour_date_start'].replace('Z', '+00:00'))
                                except ValueError:
                                    logging.warning(f"Could not parse date: {h_data['hour_date_start']}")
                                    hour_date = datetime.now()
                        else:
                            hour_date = datetime.now()
                        
                        # Extract hours value
                        hour_value = 0.0
                        if 'hour_hours' in h_data and h_data['hour_hours']:
                            try:
                                # Handle different formats of hour_hours from the API
                                hour_str = str(h_data['hour_hours']).strip()
                                # Remove any non-numeric characters except decimal point
                                hour_str = ''.join(c for c in hour_str if c.isdigit() or c == '.')
                                if hour_str:
                                    hour_value = float(hour_str)
                                else:
                                    logging.warning(f"Empty hours value after cleaning: {h_data['hour_hours']}")
                            except (ValueError, TypeError) as e:
                                logging.warning(f"Invalid hours value: {h_data['hour_hours']} - Error: {str(e)}")
                        
                        # Log the hour value for debugging
                        if debug_mode and i < 5:  # Only log for the first few volunteers
                            logging.debug(f"Hour value: {hour_value} from raw value: {h_data.get('hour_hours', 'N/A')}")
                        
                        hours.append(VolunteerHours(
                            id=h_data.get('id', ''),
                            volunteer_id=volunteer_id,
                            opportunity_id=opportunity_id,
                            hours=hour_value,
                            date=hour_date,
                            notes=h_data.get('hour_description', ''),
                            status=h_data.get('hour_status', 'approved')
                        ))
                    
                    # Create Volunteer object
                    volunteer = Volunteer(
                        id=v_data['id'],
                        first_name=v_data.get('user_fname', v_data.get('first_name', '')),
                        last_name=v_data.get('user_lname', v_data.get('last_name', '')),
                        email=v_data.get('user_email', v_data.get('email')),
                        phone=v_data.get('user_phone', v_data.get('phone')),
                        address=v_data.get('user_address', v_data.get('address')),
                        city=v_data.get('user_city', v_data.get('city')),
                        state=v_data.get('user_state', v_data.get('state')),
                        zip_code=v_data.get('user_postal', v_data.get('zip_code')),
                        join_date=datetime.fromisoformat(v_data.get('created_at', '').replace('Z', '+00:00')) 
                                  if 'created_at' in v_data else None,
                        status=v_data.get('user_status', v_data.get('status', 'active')),
                        hours=hours
                    )
                    
                    # Log volunteer data for debugging
                    logging.debug(f"Created volunteer: {volunteer.id} - {volunteer.full_name} with {len(hours)} hours")
                    
                    # Log address information for debugging
                    if volunteer.full_address:
                        logging.debug(f"Volunteer {volunteer.id} address: {volunteer.full_address}")
                    else:
                        logging.debug(f"Volunteer {volunteer.id} has no address information")
                        # Log the raw data to help diagnose issues
                        address_fields = {
                            'user_address': v_data.get('user_address'),
                            'address': v_data.get('address'),
                            'user_city': v_data.get('user_city'),
                            'city': v_data.get('city'),
                            'user_state': v_data.get('user_state'),
                            'state': v_data.get('state'),
                            'user_postal': v_data.get('user_postal'),
                            'zip_code': v_data.get('zip_code')
                        }
                        logging.debug(f"Address fields in raw data: {address_fields}")
                    
                    self.volunteers.append(volunteer)
                    
                    # Save progress periodically
                    if (i + 1) % save_interval == 0 and i > last_save:
                        logging.info(f"Saving progress after processing {i+1} volunteers...")
                        try:
                            # Create dataframes with current data
                            self._create_dataframes()
                            last_save = i
                        except Exception as save_error:
                            logging.error(f"Error saving progress: {str(save_error)}")
                            
                except Exception as e:
                    # Log the error but continue processing other volunteers
                    error_count += 1
                    logging.warning(f"Error processing volunteer {v_data.get('id')}: {str(e)}")
                    continue
            
            if error_count > 0:
                logging.warning(f"Encountered errors with {error_count} out of {total_volunteers} volunteers")
            
            # Load opportunities
            try:
                logging.info("Loading opportunity data from API or cache...")
                opportunity_data = self.api_client.get_opportunities()
                self.opportunities = []
                
                if opportunity_data:
                    logging.info(f"Successfully loaded {len(opportunity_data)} opportunities from API or cache")
                    
                    # Process all opportunities at once
                    for o_data in opportunity_data:
                        try:
                            opportunity = Opportunity(
                                id=o_data['id'],
                                title=o_data.get('title', ''),
                                description=o_data.get('description', ''),
                                address=o_data.get('address', ''),
                                city=o_data.get('city', ''),
                                state=o_data.get('state', ''),
                                zip_code=o_data.get('zip_code', ''),
                                start_date=datetime.fromisoformat(o_data.get('start_date', '').replace('Z', '+00:00')) 
                                          if 'start_date' in o_data and o_data['start_date'] else None,
                                end_date=datetime.fromisoformat(o_data.get('end_date', '').replace('Z', '+00:00')) 
                                        if 'end_date' in o_data and o_data['end_date'] else None,
                                status=o_data.get('status', 'active')
                            )
                            self.opportunities.append(opportunity)
                        except Exception as opp_error:
                            logging.warning(f"Error processing opportunity {o_data.get('id')}: {str(opp_error)}")
                            continue
                else:
                    logging.warning("No opportunity data returned from API or cache")
                
                logging.info(f"Processed {len(self.opportunities)} opportunities")
            except Exception as opp_load_error:
                logging.error(f"Error loading opportunities: {str(opp_load_error)}")
                logging.info("Continuing with volunteer data only")
            
            # Create DataFrames
            self._create_dataframes()
            
            logging.info(f"Successfully loaded {len(self.volunteers)} volunteers and {len(self.opportunities)} opportunities")
            
        except Exception as e:
            logging.error(f"Error loading data: {str(e)}", exc_info=True)
            
            # Try to save what we have so far
            if self.volunteers:
                logging.info(f"Attempting to save partial data ({len(self.volunteers)} volunteers)...")
                try:
                    self._create_dataframes()
                    logging.info("Partial data saved successfully")
                except Exception as save_error:
                    logging.error(f"Error saving partial data: {str(save_error)}")
            
            raise
    
    def load_from_geojson(self, file_path: str):
        """
        Load volunteer data from a GeoJSON file.
        
        Args:
            file_path: Path to the GeoJSON file
        """
        try:
            logging.info(f"Loading data from GeoJSON file: {file_path}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                logging.error(f"GeoJSON file not found: {file_path}")
                raise FileNotFoundError(f"GeoJSON file not found: {file_path}")
            
            # Read GeoJSON file
            with open(file_path, 'r') as f:
                geojson = json.load(f)
            
            # Validate GeoJSON
            if 'type' not in geojson or geojson['type'] != 'FeatureCollection':
                logging.error(f"Invalid GeoJSON format: missing 'type' or not a FeatureCollection")
                raise ValueError("Invalid GeoJSON format: not a FeatureCollection")
            
            if 'features' not in geojson or not isinstance(geojson['features'], list):
                logging.error(f"Invalid GeoJSON format: missing 'features' array")
                raise ValueError("Invalid GeoJSON format: missing features array")
            
            # Clear existing data
            self.volunteers = []
            
            # Process features
            zip_code_only_count = 0
            for feature in geojson['features']:
                # Skip placeholder features
                if 'properties' in feature and feature['properties'].get('is_placeholder', False):
                    logging.debug("Skipping placeholder feature")
                    continue
                
                # Extract properties
                properties = feature.get('properties', {})
                
                # Extract geometry
                geometry = feature.get('geometry', None)
                lat, lng = None, None
                
                if geometry and geometry['type'] == 'Point':
                    coords = geometry.get('coordinates', [])
                    if len(coords) >= 2:
                        lng, lat = coords[0], coords[1]
                
                # Create volunteer object
                volunteer = Volunteer(
                    id=properties.get('id', ''),
                    first_name=properties.get('name', '').split(' ')[0] if ' ' in properties.get('name', '') else properties.get('name', ''),
                    last_name=' '.join(properties.get('name', '').split(' ')[1:]) if ' ' in properties.get('name', '') else '',
                    email=properties.get('email', ''),
                    phone=properties.get('phone', ''),
                    address=properties.get('address', ''),
                    city=properties.get('city', ''),
                    state=properties.get('state', ''),
                    zip_code=properties.get('zip_code', ''),
                    join_date=datetime.fromisoformat(properties.get('join_date', datetime.now().isoformat())) 
                              if 'join_date' in properties else datetime.now(),
                    status='active',
                    hours=[],
                    latitude=lat,
                    longitude=lng
                )
                
                # Add is_zip_only attribute if it exists in properties
                if 'is_zip_only' in properties:
                    setattr(volunteer, 'is_zip_only', properties['is_zip_only'])
                    if properties['is_zip_only']:
                        zip_code_only_count += 1
                else:
                    # Determine if this is a zip code only address
                    is_zip_only = bool(volunteer.zip_code and not volunteer.address and not volunteer.city and not volunteer.state)
                    setattr(volunteer, 'is_zip_only', is_zip_only)
                    if is_zip_only:
                        zip_code_only_count += 1
                
                # Add hours if available
                if 'hours' in properties:
                    hours_data = properties['hours']
                    if isinstance(hours_data, list):
                        for h_data in hours_data:
                            volunteer.hours.append(VolunteerHours(
                                id=h_data.get('id', ''),
                                volunteer_id=volunteer.id,
                                opportunity_id=h_data.get('opportunity_id', ''),
                                hours=float(h_data.get('hours', 0)),
                                date=datetime.fromisoformat(h_data.get('date', datetime.now().isoformat()))
                                      if 'date' in h_data else datetime.now(),
                                notes=h_data.get('notes', ''),
                                status='approved'
                            ))
                    elif isinstance(hours_data, (int, float)):
                        # If hours is just a number, create a single hours entry
                        volunteer.hours.append(VolunteerHours(
                            id='1',
                            volunteer_id=volunteer.id,
                            opportunity_id='',
                            hours=float(hours_data),
                            date=datetime.now(),
                            notes='',
                            status='approved'
                        ))
                
                self.volunteers.append(volunteer)
            
            # Create DataFrames
            self._create_dataframes()
            
            logging.info(f"Loaded {len(self.volunteers)} volunteers from GeoJSON")
            
        except Exception as e:
            logging.error(f"Error loading data from GeoJSON: {str(e)}", exc_info=True)
            raise
    
    def _create_dataframes(self):
        """Create pandas DataFrames from the loaded data."""
        # Create volunteer DataFrame
        volunteer_data = []
        for volunteer in self.volunteers:
            total_hours = sum(h.hours for h in volunteer.hours)
            engagement_score = self._calculate_engagement_score(volunteer)
            
            volunteer_data.append({
                'id': volunteer.id,
                'name': f"{volunteer.first_name} {volunteer.last_name}",
                'first_name': volunteer.first_name,
                'last_name': volunteer.last_name,
                'email': volunteer.email,
                'phone': volunteer.phone,
                'address': volunteer.address,
                'city': volunteer.city,
                'state': volunteer.state,
                'zip_code': volunteer.zip_code,
                'join_date': volunteer.join_date,
                'status': volunteer.status,
                'total_hours': total_hours,
                'engagement_score': engagement_score,
                'latitude': getattr(volunteer, 'latitude', None),
                'longitude': getattr(volunteer, 'longitude', None),
                'is_zip_only': getattr(volunteer, 'is_zip_only', False)
            })
        
        if not volunteer_data:
            logging.warning("No volunteer data available to create DataFrame")
            self.volunteer_df = pd.DataFrame()
            self.hours_df = pd.DataFrame()
            self.opportunity_df = pd.DataFrame()
            return
            
        self.volunteer_df = pd.DataFrame(volunteer_data)
        
        # Save volunteer data to GeoJSON after creating DataFrame
        # This ensures we save progress even if later steps fail
        try:
            self.save_volunteer_geojson()
            logging.info(f"Saved {len(self.volunteers)} volunteers to GeoJSON for future use")
        except Exception as e:
            logging.error(f"Error saving volunteer data to GeoJSON: {str(e)}")
        
        # Create hours DataFrame
        hours_data = []
        for volunteer in self.volunteers:
            for hour in volunteer.hours:
                hours_data.append({
                    'id': hour.id,
                    'volunteer_id': volunteer.id,
                    'volunteer_name': f"{volunteer.first_name} {volunteer.last_name}",
                    'opportunity_id': hour.opportunity_id,
                    'opportunity_title': next((o.title for o in self.opportunities 
                                             if o.id == hour.opportunity_id), ''),
                    'hours': hour.hours,
                    'date': hour.date,
                    'notes': hour.notes,
                    'status': hour.status
                })
        
        if hours_data:
            self.hours_df = pd.DataFrame(hours_data)
        else:
            logging.warning("No hours data available to create DataFrame")
            self.hours_df = pd.DataFrame()
        
        # Create opportunity DataFrame
        opportunity_data = []
        for opportunity in self.opportunities:
            opportunity_data.append({
                'id': opportunity.id,
                'title': opportunity.title,
                'description': opportunity.description,
                'address': opportunity.address,
                'city': opportunity.city,
                'state': opportunity.state,
                'zip_code': opportunity.zip_code,
                'start_date': opportunity.start_date,
                'end_date': opportunity.end_date,
                'status': opportunity.status,
                'latitude': getattr(opportunity, 'latitude', None),
                'longitude': getattr(opportunity, 'longitude', None)
            })
        
        if opportunity_data:
            self.opportunity_df = pd.DataFrame(opportunity_data)
        else:
            logging.warning("No opportunity data available to create DataFrame")
            self.opportunity_df = pd.DataFrame()
    
    def _calculate_engagement_score(self, volunteer: Volunteer) -> float:
        """
        Calculate an engagement score for a volunteer.
        
        The score is based on:
        - Total hours
        - Frequency of volunteering
        - Recency of volunteering
        
        Args:
            volunteer: Volunteer object
            
        Returns:
            Engagement score (0-100)
        """
        # If no hours, return 0
        if not volunteer.hours:
            return 0
        
        # Calculate total hours (max 50 points)
        total_hours = sum(h.hours for h in volunteer.hours)
        hours_score = min(total_hours / 100 * 50, 50)
        
        # Calculate frequency (max 25 points)
        # Count unique dates
        unique_dates = len(set(h.date.date() for h in volunteer.hours))
        frequency_score = min(unique_dates / 10 * 25, 25)
        
        # Calculate recency (max 25 points)
        # Get most recent volunteer date
        if volunteer.hours:
            most_recent = max(h.date for h in volunteer.hours)
            days_since = (datetime.now() - most_recent).days
            recency_score = max(0, 25 - (days_since / 30 * 5))  # Lose 5 points per month
        else:
            recency_score = 0
        
        # Calculate total score
        total_score = hours_score + frequency_score + recency_score
        
        return total_score
    
    def get_volunteer_geojson(self) -> Dict:
        """
        Create GeoJSON from volunteer data.
        
        Returns:
            GeoJSON dictionary
        """
        features = []
        volunteers_with_coords = 0
        volunteers_without_coords = 0
        zip_code_only_count = 0
        
        for volunteer in self.volunteers:
            # Get a proper name or use a placeholder
            full_name = volunteer.full_name.strip()
            if not full_name or full_name == ' ':
                full_name = "Volunteer " + volunteer.id
                
            # Log address information for debugging
            logging.debug(f"Processing volunteer {volunteer.id} for GeoJSON with address: {volunteer.full_address}")
            
            # Determine if this is a zip code only address
            is_zip_only = bool(volunteer.zip_code and not volunteer.address and not volunteer.city and not volunteer.state)
            if is_zip_only:
                zip_code_only_count += 1
                logging.debug(f"Volunteer {volunteer.id} has zip code only address: {volunteer.zip_code}")
            
            # Skip volunteers without address
            if not volunteer.full_address:
                logging.debug(f"Volunteer {volunteer.id} has no full address, using minimal data")
                # Instead of skipping, include with minimal data
                feature = {
                    "type": "Feature",
                    "geometry": None,
                    "properties": {
                        "id": volunteer.id,
                        "name": full_name,
                        "email": volunteer.email,
                        "address": "No address provided",
                        "city": volunteer.city if volunteer.city else "",
                        "state": volunteer.state if volunteer.state else "",
                        "zip_code": volunteer.zip_code if volunteer.zip_code else "",
                        "total_hours": volunteer.total_hours,
                        "engagement_score": self._calculate_engagement_score(volunteer),
                        "needs_geocoding": True,
                        "is_zip_only": is_zip_only
                    }
                }
                features.append(feature)
                volunteers_without_coords += 1
                continue
                
            # Use coordinates if available
            lat = getattr(volunteer, 'latitude', None)
            lng = getattr(volunteer, 'longitude', None)
            
            # Skip if no coordinates (can be geocoded later)
            if lat is None or lng is None or (lat == 0 and lng == 0):
                volunteers_without_coords += 1
                # Still include in GeoJSON but with null geometry
                # This allows us to save the data and geocode it later
                feature = {
                    "type": "Feature",
                    "geometry": None,
                    "properties": {
                        "id": volunteer.id,
                        "name": full_name,
                        "email": volunteer.email,
                        "address": volunteer.full_address,
                        "city": volunteer.city if volunteer.city else "",
                        "state": volunteer.state if volunteer.state else "",
                        "zip_code": volunteer.zip_code if volunteer.zip_code else "",
                        "total_hours": volunteer.total_hours,
                        "engagement_score": self._calculate_engagement_score(volunteer),
                        "needs_geocoding": True,
                        "is_zip_only": is_zip_only
                    }
                }
            else:
                volunteers_with_coords += 1
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lng, lat]
                    },
                    "properties": {
                        "id": volunteer.id,
                        "name": full_name,
                        "email": volunteer.email,
                        "address": volunteer.full_address,
                        "city": volunteer.city if volunteer.city else "",
                        "state": volunteer.state if volunteer.state else "",
                        "zip_code": volunteer.zip_code if volunteer.zip_code else "",
                        "total_hours": volunteer.total_hours,
                        "engagement_score": self._calculate_engagement_score(volunteer),
                        "needs_geocoding": False,
                        "is_zip_only": is_zip_only
                    }
                }
            
            features.append(feature)
        
        if volunteers_without_coords > 0:
            logging.info(f"{volunteers_without_coords} volunteers need geocoding. Use the geocoding feature to add coordinates.")
        
        if volunteers_with_coords > 0:
            logging.info(f"{volunteers_with_coords} volunteers have coordinates and will be displayed on the map.")
            
        if zip_code_only_count > 0:
            logging.info(f"{zip_code_only_count} volunteers have zip code-only addresses. Use the 'Exclude Zip Code-Only Addresses' option to filter them out.")
        
        # Check if we have any features with valid geometry
        valid_geometry_features = [f for f in features if f.get('geometry') is not None]
        
        # Add a default feature if no features were created or if no features have valid geometry
        if not features or not valid_geometry_features:
            logging.warning(f"No volunteer features with valid geometry found. Adding a placeholder feature with valid geometry.")
            
            # Add a placeholder feature with valid geometry
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-87.6244, 41.9067]  # Default Chicago coordinates
                },
                "properties": {
                    "id": "placeholder",
                    "name": "No volunteers found",
                    "email": "",
                    "address": "Chicago, IL",
                    "total_hours": 0,
                    "engagement_score": 0,
                    "needs_geocoding": False,
                    "is_placeholder": True
                }
            })
            logging.info("Added placeholder feature with valid geometry to ensure valid GeoJSON")
        
        # Create the GeoJSON object
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        # Log summary of the GeoJSON being created
        logging.info(f"Created GeoJSON with {len(features)} features ({volunteers_with_coords} with coordinates, {volunteers_without_coords} without)")
        
        return geojson
    
    def save_volunteer_geojson(self, file_path: str = "addresses.geojson"):
        """
        Save volunteer data as GeoJSON.
        
        Args:
            file_path: Path to save the GeoJSON file
        """
        geojson = self.get_volunteer_geojson()
        
        # Debug logging to check what's being saved
        feature_count = len(geojson.get('features', []))
        logging.info(f"Saving GeoJSON with {feature_count} features to {file_path}")
        
        # Check if we have any features with valid geometry
        valid_geometries = sum(1 for feature in geojson.get('features', []) 
                              if feature.get('geometry') is not None)
        logging.info(f"GeoJSON contains {valid_geometries} features with valid geometry")
        
        try:
            # Ensure the file is properly written
            with open(file_path, 'w') as f:
                json.dump(geojson, f, indent=2)
                f.flush()  # Ensure data is written to disk
                os.fsync(f.fileno())  # Force write to physical storage
                
            # Verify the file was written correctly
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                logging.info(f"GeoJSON file saved successfully: {file_path} ({file_size} bytes)")
                
                # Double-check file content
                try:
                    with open(file_path, 'r') as f:
                        content = f.read(100)  # Read first 100 chars to verify
                    logging.info(f"File content verification - first 100 chars: {content[:100]}")
                except Exception as read_error:
                    logging.error(f"Error verifying file content: {str(read_error)}")
            else:
                logging.error(f"Failed to save GeoJSON file: {file_path} does not exist after save attempt")
        except Exception as e:
            logging.error(f"Error saving GeoJSON file: {str(e)}", exc_info=True)
    
    def get_opportunity_geojson(self) -> Dict:
        """
        Create GeoJSON from opportunity data.
        
        Returns:
            GeoJSON dictionary
        """
        features = []
        
        for opportunity in self.opportunities:
            # Skip opportunities without address
            if not opportunity.full_address:
                continue
                
            # Geocode address (in a real implementation, you would use a geocoding service)
            # For now, we'll assume we have coordinates
            lat, lng = 0, 0  # Placeholder
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
                },
                "properties": {
                    "title": opportunity.title,
                    "description": opportunity.description,
                    "address": opportunity.full_address,
                    "start_date": opportunity.start_date.isoformat() if opportunity.start_date else None,
                    "end_date": opportunity.end_date.isoformat() if opportunity.end_date else None,
                    "category": opportunity.category,
                    "organization": opportunity.organization
                }
            }
            
            features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    def get_volunteer_hours_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for volunteer hours.
        
        Returns:
            Dictionary with summary statistics
        """
        # Check if hours_df exists and is not empty
        if self.hours_df is None or len(self.hours_df) == 0:
            logging.warning("No hours data available for summary")
            return {
                'total_hours': 0,
                'average_hours_per_volunteer': 0,
                'total_volunteers': len(self.volunteers) if self.volunteers else 0,
                'total_opportunities': 0,
                'hours_by_month': {},
                'hours_by_opportunity': {},
                'top_volunteers': {},
                'top_opportunities': {}
            }
        
        # Check if required columns exist
        required_columns = ['hours', 'volunteer_id', 'opportunity_id', 'date']
        missing_columns = [col for col in required_columns if col not in self.hours_df.columns]
        
        if missing_columns:
            logging.warning(f"Missing required columns in hours_df: {missing_columns}")
            # Calculate what we can with available columns
            summary = {}
            
            if 'hours' in self.hours_df.columns:
                summary['total_hours'] = self.hours_df['hours'].sum()
            else:
                summary['total_hours'] = 0
                
            if 'volunteer_id' in self.hours_df.columns:
                summary['total_volunteers'] = self.hours_df['volunteer_id'].nunique()
                if 'hours' in self.hours_df.columns:
                    summary['average_hours_per_volunteer'] = self.hours_df.groupby('volunteer_id')['hours'].sum().mean()
                else:
                    summary['average_hours_per_volunteer'] = 0
            else:
                summary['total_volunteers'] = len(self.volunteers) if self.volunteers else 0
                summary['average_hours_per_volunteer'] = 0
                
            if 'opportunity_id' in self.hours_df.columns:
                summary['total_opportunities'] = self.hours_df['opportunity_id'].nunique()
            else:
                summary['total_opportunities'] = 0
                
            # These require specific columns, so we'll return empty if they're missing
            summary['hours_by_month'] = {}
            summary['hours_by_opportunity'] = {}
            summary['top_volunteers'] = {}
            summary['top_opportunities'] = {}
            
            return summary
            
        # If all required columns exist, calculate full summary
        try:
            summary = {
                'total_hours': self.hours_df['hours'].sum(),
                'average_hours_per_volunteer': self.hours_df.groupby('volunteer_id')['hours'].sum().mean(),
                'total_volunteers': self.hours_df['volunteer_id'].nunique(),
                'total_opportunities': self.hours_df['opportunity_id'].nunique()
            }
            
            # Hours by month (requires date column)
            try:
                summary['hours_by_month'] = self.hours_df.set_index('date').groupby(pd.Grouper(freq='M'))['hours'].sum().to_dict()
            except Exception as e:
                logging.warning(f"Error calculating hours by month: {str(e)}")
                summary['hours_by_month'] = {}
            
            # These require opportunity_title and volunteer_name columns which might not exist
            # in the sample data or might have different names
            if 'opportunity_title' in self.hours_df.columns:
                try:
                    summary['hours_by_opportunity'] = self.hours_df.groupby('opportunity_title')['hours'].sum().to_dict()
                    summary['top_opportunities'] = self.hours_df.groupby('opportunity_title')['hours'].sum().nlargest(10).to_dict()
                except Exception as e:
                    logging.warning(f"Error calculating opportunity statistics: {str(e)}")
                    summary['hours_by_opportunity'] = {}
                    summary['top_opportunities'] = {}
            else:
                summary['hours_by_opportunity'] = {}
                summary['top_opportunities'] = {}
                
            if 'volunteer_name' in self.hours_df.columns:
                try:
                    summary['top_volunteers'] = self.hours_df.groupby('volunteer_name')['hours'].sum().nlargest(10).to_dict()
                except Exception as e:
                    logging.warning(f"Error calculating top volunteers: {str(e)}")
                    summary['top_volunteers'] = {}
            else:
                summary['top_volunteers'] = {}
                
            return summary
            
        except Exception as e:
            logging.error(f"Error generating volunteer hours summary: {str(e)}", exc_info=True)
            return {
                'total_hours': 0,
                'average_hours_per_volunteer': 0,
                'total_volunteers': len(self.volunteers) if self.volunteers else 0,
                'total_opportunities': 0,
                'hours_by_month': {},
                'hours_by_opportunity': {},
                'top_volunteers': {},
                'top_opportunities': {}
            }
    
    def get_volunteer_engagement_metrics(self) -> Dict[str, Any]:
        """
        Get engagement metrics for volunteers.
        
        Returns:
            Dictionary with engagement metrics
        """
        if self.volunteer_df is None or len(self.volunteer_df) == 0:
            logging.warning("No volunteer data available for engagement metrics")
            return {
                'average_engagement_score': 0,
                'high_engagement_count': 0,
                'medium_engagement_count': 0,
                'low_engagement_count': 0,
                'long_term_volunteer_count': 0,
                'long_term_percentage': 0
            }
            
        try:
            # Check if engagement_score column exists
            if 'engagement_score' not in self.volunteer_df.columns:
                logging.warning("No engagement_score column in volunteer_df")
                return {
                    'average_engagement_score': 0,
                    'high_engagement_count': 0,
                    'medium_engagement_count': 0,
                    'low_engagement_count': 0,
                    'long_term_volunteer_count': 0,
                    'long_term_percentage': 0
                }
                
            metrics = {
                'average_engagement_score': self.volunteer_df['engagement_score'].mean(),
                'high_engagement_count': len(self.volunteer_df[self.volunteer_df['engagement_score'] >= 70]),
                'medium_engagement_count': len(self.volunteer_df[(self.volunteer_df['engagement_score'] >= 40) & 
                                                              (self.volunteer_df['engagement_score'] < 70)]),
                'low_engagement_count': len(self.volunteer_df[self.volunteer_df['engagement_score'] < 40])
            }
            
            # Check if is_long_term column exists
            if 'is_long_term' in self.volunteer_df.columns:
                metrics['long_term_volunteer_count'] = len(self.volunteer_df[self.volunteer_df['is_long_term']])
                metrics['long_term_percentage'] = len(self.volunteer_df[self.volunteer_df['is_long_term']]) / len(self.volunteer_df) * 100 if len(self.volunteer_df) > 0 else 0
            else:
                metrics['long_term_volunteer_count'] = 0
                metrics['long_term_percentage'] = 0
                
            return metrics
            
        except Exception as e:
            logging.error(f"Error generating volunteer engagement metrics: {str(e)}", exc_info=True)
            return {
                'average_engagement_score': 0,
                'high_engagement_count': 0,
                'medium_engagement_count': 0,
                'low_engagement_count': 0,
                'long_term_volunteer_count': 0,
                'long_term_percentage': 0
            }
    
    def get_opportunity_participation_metrics(self) -> Dict[str, Any]:
        """
        Get participation metrics for opportunities.
        
        Returns:
            Dictionary with participation metrics
        """
        if self.hours_df is None or len(self.hours_df) == 0 or self.opportunity_df is None:
            logging.warning("No hours or opportunity data available for participation metrics")
            return {
                'average_volunteers_per_opportunity': 0,
                'average_hours_per_opportunity': 0,
                'most_popular_opportunities': [],
                'highest_hour_opportunities': []
            }
            
        try:
            # Check if required columns exist
            required_columns = ['opportunity_id', 'volunteer_id', 'hours']
            missing_columns = [col for col in required_columns if col not in self.hours_df.columns]
            
            if missing_columns:
                logging.warning(f"Missing required columns in hours_df for participation metrics: {missing_columns}")
                return {
                    'average_volunteers_per_opportunity': 0,
                    'average_hours_per_opportunity': 0,
                    'most_popular_opportunities': [],
                    'highest_hour_opportunities': []
                }
                
            # Calculate participation for each opportunity
            participation_data = []
            
            for opportunity_id in self.hours_df['opportunity_id'].unique():
                if not opportunity_id:  # Skip empty opportunity IDs
                    continue
                    
                opportunity_hours = self.hours_df[self.hours_df['opportunity_id'] == opportunity_id]
                volunteers = opportunity_hours['volunteer_id'].unique()
                total_hours = opportunity_hours['hours'].sum()
                avg_hours = total_hours / len(volunteers) if len(volunteers) > 0 else 0
                
                # Get opportunity title if available
                if 'opportunity_title' in opportunity_hours.columns and len(opportunity_hours) > 0:
                    opportunity_title = opportunity_hours['opportunity_title'].iloc[0]
                else:
                    opportunity_title = f"Opportunity {opportunity_id}"
                
                participation_data.append({
                    'opportunity_id': opportunity_id,
                    'opportunity_title': opportunity_title,
                    'volunteer_count': len(volunteers),
                    'total_hours': total_hours,
                    'average_hours_per_volunteer': avg_hours
                })
            
            # If no participation data, return empty metrics
            if not participation_data:
                return {
                    'average_volunteers_per_opportunity': 0,
                    'average_hours_per_opportunity': 0,
                    'most_popular_opportunities': [],
                    'highest_hour_opportunities': []
                }
                
            participation_df = pd.DataFrame(participation_data)
            
            metrics = {
                'average_volunteers_per_opportunity': participation_df['volunteer_count'].mean(),
                'average_hours_per_opportunity': participation_df['total_hours'].mean(),
                'most_popular_opportunities': participation_df.nlargest(5, 'volunteer_count')[['opportunity_title', 'volunteer_count']].to_dict('records'),
                'highest_hour_opportunities': participation_df.nlargest(5, 'total_hours')[['opportunity_title', 'total_hours']].to_dict('records')
            }
            
            return metrics
            
        except Exception as e:
            logging.error(f"Error generating opportunity participation metrics: {str(e)}", exc_info=True)
            return {
                'average_volunteers_per_opportunity': 0,
                'average_hours_per_opportunity': 0,
                'most_popular_opportunities': [],
                'highest_hour_opportunities': []
            }
    
    def fix_hour_values(self):
        """
        Fix any incorrect hour values in the existing volunteer data.
        This can be called after loading data from a GeoJSON file to ensure
        hour values are correctly formatted.
        
        Returns:
            int: Number of hour values fixed
        """
        if not self.volunteers:
            logging.warning("No volunteer data available to fix hour values")
            return 0
            
        fixed_count = 0
        for volunteer in self.volunteers:
            for hour in volunteer.hours:
                if isinstance(hour.hours, str):
                    try:
                        # Clean the hour value
                        hour_str = hour.hours.strip()
                        # Remove any non-numeric characters except decimal point
                        hour_str = ''.join(c for c in hour_str if c.isdigit() or c == '.')
                        if hour_str:
                            old_value = hour.hours
                            hour.hours = float(hour_str)
                            fixed_count += 1
                            logging.info(f"Fixed hour value: {old_value} -> {hour.hours}")
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Could not fix invalid hour value: {hour.hours} - Error: {str(e)}")
                        # Set to 0 if we can't parse it
                        hour.hours = 0.0
                        fixed_count += 1
        
        # Recreate dataframes with the fixed hour values
        if fixed_count > 0:
            self._create_dataframes()
            logging.info(f"Fixed {fixed_count} hour values and recreated dataframes")
            
        return fixed_count 