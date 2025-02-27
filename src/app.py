import os
import streamlit as st
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import folium

# Load environment variables
load_dotenv()

# Import components
from api.galaxy_digital import GalaxyDigitalAPI
from utils.data_service import DataService
from utils.geocoding import batch_geocode
from components.map_component import create_map, display_map, add_reference_marker
from components.chart_component import (
    create_hours_histogram, create_hours_by_month_chart, create_top_volunteers_chart,
    create_top_opportunities_chart, create_engagement_scatter_plot,
    create_engagement_distribution_chart, create_hours_cumulative_chart,
    create_opportunity_participation_chart
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Set page config
st.set_page_config(page_title="Volunteer Analysis Dashboard", layout="wide")


def main():
    """Main application function."""
    st.title("Volunteer Analysis Dashboard")
    
    # Debug mode toggle in sidebar
    with st.sidebar:
        col1, col2 = st.columns(2)
        with col1:
            debug_mode = st.checkbox("Debug Mode", value=False)
            if debug_mode:
                logging.getLogger().setLevel(logging.DEBUG)
                st.info("Debug mode enabled. Check the console for detailed logs.")
        with col2:
            test_mode = st.checkbox("Test Mode", value=False)
            if test_mode:
                test_limit = st.slider("Test User Limit", min_value=5, max_value=50, value=10, 
                                      help="Maximum number of users to query in test mode")
                st.info(f"Test mode enabled. Limiting to {test_limit} users.")
            else:
                test_limit = 10  # Default value, won't be used unless test_mode is True
    
    # Initialize session state
    if 'data_service' not in st.session_state:
        try:
            # Initialize API client with debug mode and skip_login=True to handle login separately
            api_client = GalaxyDigitalAPI(debug=debug_mode, skip_login=True, 
                                         test_mode=test_mode, test_limit=test_limit)
            
            # Attempt to login and show appropriate messages
            if api_client.login():
                st.success("Successfully connected to Galaxy Digital API")
            else:
                st.error("Failed to authenticate with Galaxy Digital API. Check your credentials in .env file.")
                st.info("You can still use the application with local data files.")
            
            st.session_state.data_service = DataService(api_client=api_client)
            
            # Automatically load addresses.geojson if it exists
            if os.path.exists("addresses.geojson"):
                try:
                    logging.info("Automatically loading data from addresses.geojson")
                    st.session_state.data_service.load_from_geojson("addresses.geojson")
                    st.success(f"Successfully loaded {len(st.session_state.data_service.volunteers)} volunteers from addresses.geojson")
                except Exception as e:
                    logging.error(f"Error auto-loading addresses.geojson: {str(e)}")
                    st.warning("Could not automatically load addresses.geojson. You can try loading it manually.")
        except Exception as e:
            st.error(f"Error initializing API client: {str(e)}")
            logging.error(f"API client initialization error: {str(e)}", exc_info=True)
            st.info("Continuing with limited functionality. You can still use local data files.")
            # Create data service without API client
            st.session_state.data_service = DataService(api_client=None)
    
    if 'ref_lat' not in st.session_state:
        st.session_state.ref_lat = 41.9067
    
    if 'ref_lng' not in st.session_state:
        st.session_state.ref_lng = -87.6244
    
    # Sidebar for data loading and filtering
    with st.sidebar:
        st.header("Data Source")
        
        data_source = st.radio(
            "Select Data Source",
            ["Galaxy Digital API", "Local GeoJSON File"]
        )
        
        if data_source == "Galaxy Digital API":
            # Use environment variables for API credentials instead of text inputs
            api_key = os.getenv("GALAXY_API_KEY", "")
            email = os.getenv("GALAXY_EMAIL", "")
            password = os.getenv("GALAXY_PASSWORD", "")
            base_url = os.getenv("GALAXY_BASE_URL", "https://api.galaxydigital.com/api")
            
            # Display credential status
            if api_key and email and password:
                st.success("API credentials loaded from environment variables")
            else:
                st.warning("API credentials not found in environment variables. Please set them in your .env file.")
                st.info("Required variables: GALAXY_API_KEY, GALAXY_EMAIL, GALAXY_PASSWORD")
            
            # Cache settings
            st.subheader("Cache Settings")
            use_cache = st.checkbox("Use API Cache", value=True, 
                                   help="Cache API responses to reduce API calls and avoid rate limiting")
            
            cache_max_age = st.slider("Cache Max Age (days)", 1, 30, 7,
                                     help="Maximum age of cached data before it's considered stale")
            
            # Cache management
            if st.session_state.data_service and st.session_state.data_service.api_available:
                api_client = st.session_state.data_service.api_client
                
                if hasattr(api_client, 'cache_manager') and api_client.cache_manager:
                    # Get cache stats
                    cache_stats = api_client.get_cache_stats()
                    
                    if cache_stats:
                        st.write("Cache Statistics:")
                        st.write(f"- Files: {cache_stats.get('total_files', 0)}")
                        st.write(f"- Size: {cache_stats.get('total_size_mb', 0):.2f} MB")
                        
                        if cache_stats.get('oldest_timestamp'):
                            oldest = datetime.fromisoformat(cache_stats['oldest_timestamp'])
                            st.write(f"- Oldest: {oldest.strftime('%Y-%m-%d')}")
                        
                        if cache_stats.get('newest_timestamp'):
                            newest = datetime.fromisoformat(cache_stats['newest_timestamp'])
                            st.write(f"- Newest: {newest.strftime('%Y-%m-%d')}")
                    
                    # Cache clearing options
                    if st.button("Clear Expired Cache"):
                        with st.spinner("Clearing expired cache..."):
                            cleared = api_client.clear_cache(older_than_days=cache_max_age)
                            st.success(f"Cleared {cleared} expired cache files")
                    
                    if st.button("Clear All Cache"):
                        with st.spinner("Clearing all cache..."):
                            cleared = api_client.clear_cache()
                            st.success(f"Cleared {cleared} cache files")
            
            # Date range for data
            st.subheader("Date Range")
            col1, col2 = st.columns(2)
            
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=datetime.now() - timedelta(days=90)
                )
            
            with col2:
                end_date = st.date_input(
                    "End Date",
                    value=datetime.now()
                )
            
            # Load data button
            if st.button("Load Data from API"):
                with st.spinner("Loading data from Galaxy Digital API..."):
                    try:
                        # Create API client with provided credentials
                        api_client = GalaxyDigitalAPI(
                            api_key=api_key,
                            email=email,
                            password=password,
                            base_url=base_url,
                            debug=debug_mode,
                            use_cache=use_cache,
                            cache_max_age_days=cache_max_age,
                            test_mode=test_mode,
                            test_limit=test_limit
                        )
                        
                        # Attempt to login
                        if not api_client.login():
                            st.error("Failed to authenticate with Galaxy Digital API. Please check your credentials.")
                            st.stop()
                        
                        # Create data service with API client
                        st.session_state.data_service = DataService(api_client=api_client)
                        
                        # Load data with progress indicator
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        status_text.text("Loading volunteer data...")
                        
                        try:
                            # Load data
                            st.session_state.data_service.load_data(
                                start_date=start_date.strftime("%Y-%m-%d"),
                                end_date=end_date.strftime("%Y-%m-%d")
                            )
                            
                            progress_bar.progress(100)
                            status_text.empty()
                            
                            # Check if any data was loaded
                            if (len(st.session_state.data_service.volunteers) == 0 and 
                                len(st.session_state.data_service.opportunities) == 0):
                                st.warning("No data was found for the selected date range. Try expanding your date range.")
                            else:
                                st.success(f"Successfully loaded {len(st.session_state.data_service.volunteers)} volunteers and {len(st.session_state.data_service.opportunities)} opportunities.")
                                
                                # Show cache usage if available
                                if api_client.use_cache and api_client.cache_manager:
                                    cache_stats = api_client.get_cache_stats()
                                    if cache_stats and cache_stats.get('total_files', 0) > 0:
                                        st.info(f"Using {cache_stats.get('total_files', 0)} cached API responses ({cache_stats.get('total_size_mb', 0):.2f} MB)")
                        except Exception as e:
                            progress_bar.progress(100)
                            status_text.empty()
                            st.error(f"Error loading data: {str(e)}")
                            
                            # Check if we have partial data that we can still use
                            if hasattr(st.session_state.data_service, 'volunteers') and len(st.session_state.data_service.volunteers) > 0:
                                st.warning(f"Partial data was loaded: {len(st.session_state.data_service.volunteers)} volunteers. Some functionality may be limited.")
                                
                                # Try to create dataframes from partial data
                                try:
                                    st.session_state.data_service._create_dataframes()
                                except Exception as df_error:
                                    logging.error(f"Error creating dataframes from partial data: {str(df_error)}")
                    except Exception as e:
                        st.error(f"Error initializing API client: {str(e)}")
                        logging.error(f"API client initialization error: {str(e)}", exc_info=True)
        
        else:  # Local GeoJSON File
            uploaded_file = st.file_uploader(
                "Upload GeoJSON file",
                type=['geojson', 'json']
            )
            
            if uploaded_file is not None:
                # Save the uploaded file temporarily
                with open("temp_upload.geojson", "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Load data from file
                with st.spinner("Loading data from GeoJSON file..."):
                    try:
                        st.session_state.data_service.load_from_geojson("temp_upload.geojson")
                        st.success("Data loaded successfully!")
                    except Exception as e:
                        st.error(f"Error loading data: {str(e)}")
                        logging.error(f"Error loading GeoJSON file: {str(e)}", exc_info=True)
                        st.info("If you're seeing a 'module fiona has no attribute path' error, this is likely due to a compatibility issue with the GeoPandas library. The application has been updated to handle this error, but you may need to restart the application.")
            
            # Option to use default file
            if st.button("Use Default GeoJSON"):
                with st.spinner("Loading data from default GeoJSON file..."):
                    try:
                        # Check if the file exists first
                        if os.path.exists("addresses.geojson"):
                            st.session_state.data_service.load_from_geojson("addresses.geojson")
                            st.success("Data loaded successfully!")
                        else:
                            st.error("Default GeoJSON file 'addresses.geojson' not found.")
                            st.info("Please upload a GeoJSON file instead.")
                    except Exception as e:
                        st.error(f"Error loading data: {str(e)}")
                        logging.error(f"Error loading default GeoJSON file: {str(e)}", exc_info=True)
                        st.info("If you're seeing a 'module fiona has no attribute path' error, this is likely due to a compatibility issue with the GeoPandas library. The application has been updated to handle this error, but you may need to restart the application.")
        
        # Google Maps API key from environment
        st.header("Geocoding Settings")
        google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY", "")
        
        if google_maps_api_key:
            st.success("Google Maps API key loaded from environment variables")
        else:
            st.warning("Google Maps API key not found in environment variables. Please set GOOGLE_MAPS_API_KEY in your .env file.")
            st.info("Required for geocoding addresses. Get a key from https://developers.google.com/maps/documentation/geocoding/get-api-key")
        
        # Geocode button (only show if we have volunteer data)
        if hasattr(st.session_state, 'data_service') and hasattr(st.session_state.data_service, 'volunteers') and len(st.session_state.data_service.volunteers) > 0:
            exclude_zip_only_geocoding = st.checkbox("Skip Zip Code-Only Addresses When Geocoding", value=False,
                                                   help="Don't geocode addresses that only have zip codes")
            
            if st.button("Geocode Volunteer Addresses"):
                if not google_maps_api_key:
                    st.error("Google Maps API key is required for geocoding. Please enter a valid API key.")
                else:
                    with st.spinner("Geocoding volunteer addresses..."):
                        try:
                            # Prepare addresses for geocoding
                            addresses_to_geocode = []
                            skipped_zip_only = 0
                            
                            for volunteer in st.session_state.data_service.volunteers:
                                # Skip volunteers that already have coordinates
                                if hasattr(volunteer, 'latitude') and hasattr(volunteer, 'longitude') and volunteer.latitude and volunteer.longitude:
                                    continue
                                    
                                # Check if this is a zip code only address
                                is_zip_only = bool(volunteer.zip_code and not volunteer.address and not volunteer.city and not volunteer.state)
                                
                                # Skip zip code only addresses if requested
                                if exclude_zip_only_geocoding and is_zip_only:
                                    skipped_zip_only += 1
                                    continue
                                
                                if volunteer.address:
                                    full_address = f"{volunteer.address}, {volunteer.city}, {volunteer.state} {volunteer.zip_code}"
                                    addresses_to_geocode.append({
                                        'id': volunteer.id,
                                        'address': full_address
                                    })
                                elif volunteer.zip_code and not exclude_zip_only_geocoding:
                                    # If we only have a zip code, use that
                                    addresses_to_geocode.append({
                                        'id': volunteer.id,
                                        'address': volunteer.zip_code
                                    })
                            
                            # Show progress
                            progress_text = st.empty()
                            progress_text.text(f"Geocoding {len(addresses_to_geocode)} addresses...")
                            
                            if skipped_zip_only > 0:
                                st.info(f"Skipped {skipped_zip_only} zip code-only addresses as requested")
                            
                            # Geocode addresses
                            from utils.geocoding import batch_geocode
                            geocoded_addresses = batch_geocode(addresses_to_geocode, api_key=google_maps_api_key)
                            
                            # Update volunteer objects with geocoded coordinates
                            geocoded_count = 0
                            for geocoded in geocoded_addresses:
                                for volunteer in st.session_state.data_service.volunteers:
                                    if volunteer.id == geocoded['id']:
                                        volunteer.latitude = geocoded['latitude']
                                        volunteer.longitude = geocoded['longitude']
                                        if 'is_zip_only' in geocoded:
                                            setattr(volunteer, 'is_zip_only', geocoded['is_zip_only'])
                                        geocoded_count += 1
                                        break
                            
                            # Recreate dataframes with the updated coordinates
                            st.session_state.data_service._create_dataframes()
                            
                            # Save the updated data to GeoJSON
                            st.session_state.data_service.save_volunteer_geojson()
                            
                            progress_text.empty()
                            st.success(f"Successfully geocoded {geocoded_count} out of {len(addresses_to_geocode)} addresses.")
                            
                            # If we have a map view, suggest refreshing
                            st.info("Please refresh the map view to see the updated coordinates.")
                            
                        except Exception as e:
                            st.error(f"Error geocoding addresses: {str(e)}")
                            logging.error(f"Geocoding error: {str(e)}", exc_info=True)
        
        # Map options
        if hasattr(st.session_state.data_service, 'volunteer_df') and st.session_state.data_service.volunteer_df is not None:
            st.header("Map Options")
            
            show_markers = st.checkbox("Show Markers", value=False)
            show_dots = st.checkbox("Show Dots", value=True)
            marker_size = st.slider("Dot Size", 1, 10, 3, disabled=not show_dots)
            show_heatmap = st.checkbox("Show Heatmap", value=True)
            heatmap_radius = st.slider("Heatmap Radius", 5, 30, 15, disabled=not show_heatmap)
            exclude_zip_only = st.checkbox("Exclude Zip Code-Only Addresses", value=False, 
                                          help="Filter out addresses that only have zip codes without street information")
            
            # Color by option
            color_options = ["None", "Total Hours", "Engagement Score"]
            color_by = st.selectbox("Color By", color_options)
            
            # Map color_by selection to DataFrame column
            color_by_column = None
            if color_by == "Total Hours":
                color_by_column = "total_hours"
            elif color_by == "Engagement Score":
                color_by_column = "engagement_score"
    
    # Main content area
    if hasattr(st.session_state.data_service, 'volunteer_df') and st.session_state.data_service.volunteer_df is not None:
        # Create tabs for different visualizations
        tab1, tab2, tab3, tab4 = st.tabs([
            "Map View", 
            "Volunteer Analysis", 
            "Opportunity Analysis",
            "Engagement Analysis"
        ])
        
        with tab1:
            st.header("Volunteer Locations")
            
            # Check if we have any volunteers with coordinates
            has_coordinates = False
            if hasattr(st.session_state.data_service, 'volunteer_df') and st.session_state.data_service.volunteer_df is not None:
                if 'latitude' in st.session_state.data_service.volunteer_df.columns and 'longitude' in st.session_state.data_service.volunteer_df.columns:
                    # Check if we have any non-null coordinates
                    valid_coords = st.session_state.data_service.volunteer_df.dropna(subset=['latitude', 'longitude'])
                    has_coordinates = len(valid_coords) > 0
            
            if not has_coordinates:
                st.warning("No volunteers with coordinates found. Please use the geocoding feature in the sidebar to add coordinates.")
                
                # Show a placeholder map
                m = folium.Map(location=[st.session_state.ref_lat, st.session_state.ref_lng], zoom_start=10)
                
                # Add reference point marker
                m = add_reference_marker(
                    m,
                    st.session_state.ref_lat,
                    st.session_state.ref_lng
                )
                
                # Display the map
                map_data = display_map(m, height=600)
                
                # Update reference point if map was clicked
                if map_data["last_clicked"] is not None:
                    st.session_state.ref_lat = map_data["last_clicked"]["lat"]
                    st.session_state.ref_lng = map_data["last_clicked"]["lng"]
                    st.success(f"Reference point updated to: {st.session_state.ref_lat:.6f}, {st.session_state.ref_lng:.6f}")
                    st.rerun()
            else:
                # Create map with current reference point
                try:
                    m = create_map(
                        st.session_state.data_service.volunteer_df,
                        center=[st.session_state.ref_lat, st.session_state.ref_lng],
                        heatmap=show_heatmap,
                        radius=heatmap_radius,
                        show_markers=show_markers,
                        show_dots=show_dots,
                        marker_size=marker_size,
                        color_by=color_by_column,
                        exclude_zip_only=exclude_zip_only
                    )
                    
                    # Add reference point marker
                    m = add_reference_marker(
                        m,
                        st.session_state.ref_lat,
                        st.session_state.ref_lng
                    )
                    
                    # Display the map
                    map_data = display_map(m, height=600)
                    
                    # Update reference point if map was clicked
                    if map_data["last_clicked"] is not None:
                        st.session_state.ref_lat = map_data["last_clicked"]["lat"]
                        st.session_state.ref_lng = map_data["last_clicked"]["lng"]
                        st.success(f"Reference point updated to: {st.session_state.ref_lat:.6f}, {st.session_state.ref_lng:.6f}")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error creating map: {str(e)}")
                    logging.error(f"Map creation error: {str(e)}", exc_info=True)
                    st.info("Try using the geocoding feature in the sidebar to add coordinates to volunteers.")
                    
                    # Show a placeholder map
                    m = folium.Map(location=[st.session_state.ref_lat, st.session_state.ref_lng], zoom_start=10)
                    display_map(m, height=600)
        
        with tab2:
            st.header("Volunteer Analysis")
            
            # Summary metrics
            if hasattr(st.session_state.data_service, 'hours_df') and st.session_state.data_service.hours_df is not None:
                try:
                    hours_summary = st.session_state.data_service.get_volunteer_hours_summary()
                    
                    # Display summary metrics in columns
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Volunteers", hours_summary.get('total_volunteers', 0))
                    
                    with col2:
                        st.metric("Total Hours", f"{hours_summary.get('total_hours', 0):.1f}")
                    
                    with col3:
                        st.metric("Avg Hours/Volunteer", f"{hours_summary.get('average_hours_per_volunteer', 0):.1f}")
                    
                    with col4:
                        st.metric("Total Opportunities", hours_summary.get('total_opportunities', 0))
                    
                    # Hours distribution
                    st.subheader("Hours Distribution")
                    if 'total_hours' in st.session_state.data_service.volunteer_df.columns:
                        hours_hist = create_hours_histogram(
                            st.session_state.data_service.volunteer_df,
                            hours_column='total_hours',
                            title="Volunteer Hours Distribution"
                        )
                        st.pyplot(hours_hist)
                    else:
                        st.info("No hours data available for distribution chart.")
                    
                    # Hours by month
                    st.subheader("Hours by Month")
                    if 'hours_by_month' in hours_summary and hours_summary['hours_by_month']:
                        hours_month_chart = create_hours_by_month_chart(
                            hours_summary['hours_by_month'],
                            title="Volunteer Hours by Month"
                        )
                        st.pyplot(hours_month_chart)
                    else:
                        st.info("No monthly data available.")
                    
                    # Top volunteers
                    st.subheader("Top Volunteers")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'top_volunteers' in hours_summary and hours_summary['top_volunteers']:
                            top_vol_chart = create_top_volunteers_chart(
                                hours_summary['top_volunteers'],
                                title="Top Volunteers by Hours"
                            )
                            st.pyplot(top_vol_chart)
                        else:
                            st.info("No volunteer data available for top volunteers chart.")
                    
                    with col2:
                        # Cumulative hours chart
                        if 'total_hours' in st.session_state.data_service.volunteer_df.columns:
                            hours_cum_chart = create_hours_cumulative_chart(
                                st.session_state.data_service.volunteer_df,
                                hours_column='total_hours',
                                title="Cumulative Volunteer Hours"
                            )
                            st.plotly_chart(hours_cum_chart, use_container_width=True)
                        else:
                            st.info("No hours data available for cumulative chart.")
                except Exception as e:
                    st.error(f"Error displaying volunteer analysis: {str(e)}")
                    logging.error(f"Error in volunteer analysis tab: {str(e)}", exc_info=True)
                    st.info("Some charts could not be displayed due to missing or incompatible data.")
            else:
                st.info("No hours data available. Please load data from Galaxy Digital API.")
        
        with tab3:
            st.header("Opportunity Analysis")
            
            if hasattr(st.session_state.data_service, 'hours_df') and st.session_state.data_service.hours_df is not None:
                try:
                    hours_summary = st.session_state.data_service.get_volunteer_hours_summary()
                    participation_metrics = st.session_state.data_service.get_opportunity_participation_metrics()
                    
                    # Display summary metrics in columns
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric(
                            "Avg Volunteers per Opportunity", 
                            f"{participation_metrics.get('average_volunteers_per_opportunity', 0):.1f}"
                        )
                    
                    with col2:
                        st.metric(
                            "Avg Hours per Opportunity", 
                            f"{participation_metrics.get('average_hours_per_opportunity', 0):.1f}"
                        )
                    
                    # Top opportunities
                    st.subheader("Top Opportunities")
                    if 'top_opportunities' in hours_summary and hours_summary['top_opportunities']:
                        top_opp_chart = create_top_opportunities_chart(
                            hours_summary['top_opportunities'],
                            title="Top Opportunities by Hours"
                        )
                        st.pyplot(top_opp_chart)
                    else:
                        st.info("No opportunity data available for top opportunities chart.")
                    
                    # Opportunity participation
                    st.subheader("Opportunity Participation Analysis")
                    
                    # Create DataFrame for participation chart
                    if 'most_popular_opportunities' in participation_metrics and participation_metrics['most_popular_opportunities']:
                        # Check if we have the necessary data for the participation chart
                        if hasattr(st.session_state.data_service, 'hours_df') and 'opportunity_id' in st.session_state.data_service.hours_df.columns:
                            try:
                                # Extract participation data
                                participation_data = []
                                
                                for opportunity_id in st.session_state.data_service.hours_df['opportunity_id'].unique():
                                    if not opportunity_id:  # Skip empty opportunity IDs
                                        continue
                                        
                                    opportunity_hours = st.session_state.data_service.hours_df[
                                        st.session_state.data_service.hours_df['opportunity_id'] == opportunity_id
                                    ]
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
                                
                                if participation_data:
                                    participation_df = pd.DataFrame(participation_data)
                                    
                                    # Create participation chart
                                    participation_chart = create_opportunity_participation_chart(
                                        participation_df,
                                        title="Opportunity Participation Analysis"
                                    )
                                    st.plotly_chart(participation_chart, use_container_width=True)
                                else:
                                    st.info("No participation data available for chart.")
                            except Exception as e:
                                st.error(f"Error creating participation chart: {str(e)}")
                                logging.error(f"Error in participation chart: {str(e)}", exc_info=True)
                                st.info("Could not display participation chart due to data issues.")
                        else:
                            st.info("Missing required data for participation chart.")
                    else:
                        st.info("No participation data available.")
                except Exception as e:
                    st.error(f"Error displaying opportunity analysis: {str(e)}")
                    logging.error(f"Error in opportunity analysis tab: {str(e)}", exc_info=True)
                    st.info("Some charts could not be displayed due to missing or incompatible data.")
            else:
                st.info("No opportunity data available. Please load data from Galaxy Digital API.")
        
        with tab4:
            st.header("Engagement Analysis")
            
            if hasattr(st.session_state.data_service, 'volunteer_df') and st.session_state.data_service.volunteer_df is not None:
                try:
                    engagement_metrics = st.session_state.data_service.get_volunteer_engagement_metrics()
                    
                    # Display summary metrics in columns
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "Average Engagement Score", 
                            f"{engagement_metrics.get('average_engagement_score', 0):.1f}"
                        )
                    
                    with col2:
                        st.metric(
                            "High Engagement Volunteers", 
                            engagement_metrics.get('high_engagement_count', 0)
                        )
                    
                    with col3:
                        st.metric(
                            "Long-term Volunteers", 
                            f"{engagement_metrics.get('long_term_volunteer_count', 0)} " +
                            f"({engagement_metrics.get('long_term_percentage', 0):.1f}%)"
                        )
                    
                    # Engagement distribution
                    st.subheader("Engagement Distribution")
                    
                    # Check if engagement_score column exists
                    if 'engagement_score' in st.session_state.data_service.volunteer_df.columns:
                        # Create engagement distribution chart
                        engagement_dist_chart = create_engagement_distribution_chart(
                            st.session_state.data_service.volunteer_df,
                            engagement_column='engagement_score',
                            title="Volunteer Engagement Distribution"
                        )
                        st.plotly_chart(engagement_dist_chart, use_container_width=True)
                        
                        # Engagement scatter plot
                        st.subheader("Engagement Analysis")
                        
                        # Check if total_hours column exists
                        if 'total_hours' in st.session_state.data_service.volunteer_df.columns:
                            # Create engagement scatter plot
                            engagement_scatter = create_engagement_scatter_plot(
                                st.session_state.data_service.volunteer_df,
                                x_column='total_hours',
                                y_column='engagement_score',
                                title="Volunteer Engagement vs. Hours"
                            )
                            st.plotly_chart(engagement_scatter, use_container_width=True)
                        else:
                            st.info("Missing hours data for engagement scatter plot.")
                    else:
                        st.info("No engagement score data available for charts.")
                except Exception as e:
                    st.error(f"Error displaying engagement analysis: {str(e)}")
                    logging.error(f"Error in engagement analysis tab: {str(e)}", exc_info=True)
                    st.info("Some charts could not be displayed due to missing or incompatible data.")
            else:
                st.info("No engagement data available. Please load data from Galaxy Digital API.")
    
    else:
        # Display welcome message if no data is loaded
        st.info(
            "Welcome to the Volunteer Analysis Dashboard! "
            "Please load data using the options in the sidebar."
        )
        
        # Display sample data option
        if st.button("Load Sample Data"):
            with st.spinner("Loading sample data..."):
                try:
                    st.session_state.data_service.load_from_geojson("addresses.geojson")
                    st.success("Sample data loaded successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error loading sample data: {str(e)}")


if __name__ == "__main__":
    main() 