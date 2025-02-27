import streamlit as st
import pandas as pd
import numpy as np
import json
import folium
from folium.plugins import HeatMap
from streamlit_folium import folium_static
import matplotlib.pyplot as plt
from geopy.distance import geodesic
import geopandas as gpd
from shapely.geometry import Point
from streamlit_folium import st_folium

st.set_page_config(page_title="Park Volunteer Analysis", layout="wide")

def load_geojson(file_path="addresses.geojson"):
    """Load GeoJSON data and convert to DataFrame"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Extract features into a list of dictionaries
        features = []
        for feature in data['features']:
            lng, lat = feature['geometry']['coordinates']
            properties = feature['properties']
            features.append({
                'name': properties['name'],
                'email': properties['email'],
                'address': properties['address'],
                'latitude': lat,
                'longitude': lng
            })
        
        return pd.DataFrame(features)
    except Exception as e:
        st.error(f"Error loading GeoJSON file: {str(e)}")
        return None

def create_map(df, center=None, heatmap=False, radius=10, show_markers=False, show_dots=True, marker_size=3):
    """Create a Folium map with volunteer locations"""
    if center is None:
        # Default center is the mean of all points
        center = [df['latitude'].mean(), df['longitude'].mean()]
    
    m = folium.Map(location=center, zoom_start=11)
    
    # Add individual markers or circles based on preference
    if show_dots or show_markers:
        for idx, row in df.iterrows():
            if show_markers:
                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=f"<b>{row['name']}</b><br>{row['address']}<br>{row['email']}",
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            elif show_dots:
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=marker_size,
                    popup=f"<b>{row['name']}</b><br>{row['address']}<br>{row['email']}",
                    color='blue',
                    fill=True,
                    fill_color='blue',
                    fill_opacity=0.7
                ).add_to(m)
    
    # Add heatmap if requested
    if heatmap:
        heat_data = [[row['latitude'], row['longitude']] for idx, row in df.iterrows()]
        HeatMap(heat_data, radius=radius).add_to(m)
    
    # Add click handler for setting reference point
    folium.LatLngPopup().add_to(m)
    
    return m

def calculate_distances(df, reference_point):
    """Calculate distances from each volunteer to a reference point"""
    distances = []
    for idx, row in df.iterrows():
        volunteer_point = (row['latitude'], row['longitude'])
        distance = geodesic(reference_point, volunteer_point).kilometers
        distances.append(distance)
    
    return distances

def main():
    st.title("Park Volunteer Analysis Tool")
    
    # Initialize session state for reference point
    if 'ref_lat' not in st.session_state:
        st.session_state.ref_lat = None
    if 'ref_lng' not in st.session_state:
        st.session_state.ref_lng = None
    
    # Sidebar for file upload and options
    with st.sidebar:
        st.header("Data Input")
        uploaded_file = st.file_uploader("Upload GeoJSON file", type=['geojson', 'json'])
        
        if uploaded_file is not None:
            # Save the uploaded file temporarily
            with open("temp_upload.geojson", "wb") as f:
                f.write(uploaded_file.getbuffer())
            df = load_geojson("temp_upload.geojson")
        else:
            # Try to load the default file
            df = load_geojson()
        
        if df is not None:
            st.success(f"Loaded data for {len(df)} volunteers")
            
            # Set default reference point if not already set
            if st.session_state.ref_lat is None:
                st.session_state.ref_lat = 41.9067
            if st.session_state.ref_lng is None:
                st.session_state.ref_lng = -87.6244
            
            st.header("Map Options")
            show_markers = st.checkbox("Show Markers", value=False)
            show_dots = st.checkbox("Show Dots", value=True)
            marker_size = st.slider("Dot Size", 1, 10, 3, disabled=not show_dots)
            show_heatmap = st.checkbox("Show Heatmap", value=True)
            heatmap_radius = st.slider("Heatmap Radius", 5, 30, 15, disabled=not show_heatmap)
            
            st.header("Distance Analysis")
            st.write("Reference point for distance calculations:")
            ref_lat = st.number_input("Reference Latitude", value=st.session_state.ref_lat)
            ref_lng = st.number_input("Reference Longitude", value=st.session_state.ref_lng)
            
            # Update session state when inputs change
            if ref_lat != st.session_state.ref_lat:
                st.session_state.ref_lat = ref_lat
            if ref_lng != st.session_state.ref_lng:
                st.session_state.ref_lng = ref_lng
                
            reference_point = (ref_lat, ref_lng)
            
            st.write("*Tip: Click anywhere on the map to set a new reference point*")
            
            distance_unit = st.selectbox("Distance Unit", ["kilometers", "miles"])
            max_distance = st.slider("Max Distance to Consider", 1, 50, 20)
            
            # Add a park location selector
            st.header("Common Park Locations")
            park_locations = {
                "Main Park": (df['latitude'].mean(), df['longitude'].mean()),
                "North Entrance": (df['latitude'].mean() + 0.01, df['longitude'].mean() + 0.01),
                "South Entrance": (df['latitude'].mean() - 0.01, df['longitude'].mean() - 0.01),
            }
            selected_park = st.selectbox("Select Park Location", list(park_locations.keys()))
            if st.button("Use Selected Park as Reference"):
                st.session_state.ref_lat = park_locations[selected_park][0]
                st.session_state.ref_lng = park_locations[selected_park][1]
                st.rerun()
    
    # Main content area
    if df is not None:
        # Create tabs for different visualizations
        tab1, tab2, tab3 = st.tabs(["Map View", "Distance Analysis", "Volunteer Data"])
        
        with tab1:
            st.header("Volunteer Locations")
            
            # Create map with current reference point
            m = create_map(
                df, 
                center=[st.session_state.ref_lat, st.session_state.ref_lng], 
                heatmap=show_heatmap, 
                radius=heatmap_radius,
                show_markers=show_markers,
                show_dots=show_dots,
                marker_size=marker_size
            )
            
            # Add reference point marker
            folium.Marker(
                location=[st.session_state.ref_lat, st.session_state.ref_lng],
                popup="Reference Point",
                icon=folium.Icon(color='red', icon='star')
            ).add_to(m)
            
            # Display the map with increased height
            map_data = st_folium(m, width="100%", height=600)
            
            # Update reference point if map was clicked
            if map_data["last_clicked"] is not None:
                st.session_state.ref_lat = map_data["last_clicked"]["lat"]
                st.session_state.ref_lng = map_data["last_clicked"]["lng"]
                st.success(f"Reference point updated to: {st.session_state.ref_lat:.6f}, {st.session_state.ref_lng:.6f}")
                st.rerun()
        
        with tab2:
            st.header("Distance Analysis")
            
            # Calculate distances
            reference_point = (st.session_state.ref_lat, st.session_state.ref_lng)
            df['distance_km'] = calculate_distances(df, reference_point)
            
            if distance_unit == "miles":
                df['distance'] = df['distance_km'] * 0.621371
                unit_label = "miles"
            else:
                df['distance'] = df['distance_km']
                unit_label = "km"
            
            # Filter by max distance
            filtered_df = df[df['distance'] <= max_distance]
            
            # Create two columns for the top section
            col1, col2 = st.columns([3, 2])
            
            with col1:
                # Distance statistics in a more compact format
                st.subheader("Distance Statistics")
                stats_cols = st.columns(3)
                with stats_cols[0]:
                    st.metric("Average Distance", f"{filtered_df['distance'].mean():.2f} {unit_label}")
                with stats_cols[1]:
                    st.metric("Max Distance", f"{filtered_df['distance'].max():.2f} {unit_label}")
                with stats_cols[2]:
                    st.metric("Volunteers within range", f"{len(filtered_df)}/{len(df)}")
                
                # Distance distribution histogram
                st.subheader("Distance Distribution")
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.hist(filtered_df['distance'], bins=15, alpha=0.7, color='steelblue')
                ax.set_xlabel(f"Distance ({unit_label})")
                ax.set_ylabel("Number of Volunteers")
                ax.grid(axis='y', alpha=0.3)
                plt.tight_layout()
                st.pyplot(fig)
            
            with col2:
                # Distance rings visualization
                st.subheader("Distance Rings")
                
                # Create distance rings
                rings = [0, 1, 2, 5, 10, max_distance]
                ring_counts = []
                
                for i in range(len(rings)-1):
                    count = len(df[(df['distance'] >= rings[i]) & (df['distance'] < rings[i+1])])
                    ring_counts.append({
                        'Ring': f"{rings[i]}-{rings[i+1]} {unit_label}",
                        'Count': count,
                        'Percentage': f"{count / len(df) * 100:.1f}%"
                    })
                
                ring_df = pd.DataFrame(ring_counts)
                
                # Display the ring data in a clean format
                st.dataframe(
                    ring_df, 
                    hide_index=True,
                    use_container_width=True,
                    height=215
                )
            
            # Second row with two charts side by side
            st.subheader("Volunteer Distribution Analysis")
            chart_cols = st.columns(2)
            
            with chart_cols[0]:
                # Distance rings as a bar chart
                fig, ax = plt.subplots(figsize=(6, 4))
                ax.bar(
                    ring_df['Ring'], 
                    ring_df['Count'].astype(int), 
                    color='steelblue',
                    alpha=0.7
                )
                ax.set_xlabel(f"Distance Range ({unit_label})")
                ax.set_ylabel("Number of Volunteers")
                plt.xticks(rotation=45)
                plt.tight_layout()
                st.pyplot(fig)
            
            with chart_cols[1]:
                # Cumulative distance chart
                cumulative_data = []
                total = 0
                distances = sorted(df['distance'].tolist())
                
                for d in range(0, int(max_distance) + 1):
                    count = len([x for x in distances if x <= d])
                    cumulative_data.append({
                        'Distance': d,
                        'Volunteers': count,
                        'Percentage': count / len(df) * 100
                    })
                
                cumulative_df = pd.DataFrame(cumulative_data)
                
                fig, ax = plt.subplots(figsize=(6, 4))
                ax.plot(
                    cumulative_df['Distance'], 
                    cumulative_df['Percentage'],
                    marker='o',
                    markersize=4,
                    color='forestgreen'
                )
                ax.set_xlabel(f"Distance ({unit_label})")
                ax.set_ylabel("Cumulative % of Volunteers")
                ax.set_ylim(0, 100)
                ax.grid(alpha=0.3)
                plt.tight_layout()
                st.pyplot(fig)
            
            # Add a section for closest/furthest volunteers
            st.subheader("Volunteer Distance Details")
            detail_cols = st.columns(2)
            
            with detail_cols[0]:
                st.write("**Closest Volunteers**")
                closest_df = df.nsmallest(5, 'distance')[['name', 'address', 'distance']]
                closest_df['distance'] = closest_df['distance'].round(2).astype(str) + f" {unit_label}"
                st.dataframe(closest_df, hide_index=True, use_container_width=True)
            
            with detail_cols[1]:
                st.write("**Furthest Volunteers**")
                furthest_df = df.nlargest(5, 'distance')[['name', 'address', 'distance']]
                furthest_df['distance'] = furthest_df['distance'].round(2).astype(str) + f" {unit_label}"
                st.dataframe(furthest_df, hide_index=True, use_container_width=True)
        
        with tab3:
            st.header("Volunteer Data")
            
            # Add distance to the displayed dataframe
            display_df = df.copy()
            display_df['distance'] = display_df['distance'].round(2)
            display_df = display_df.sort_values('distance')
            
            # Display the data
            st.dataframe(display_df)
            
            # Allow downloading the data with distances
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name="volunteers_with_distances.csv",
                mime="text/csv",
            )

if __name__ == "__main__":
    main() 