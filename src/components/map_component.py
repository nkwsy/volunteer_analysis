import folium
from folium.plugins import HeatMap
import streamlit as st
from streamlit_folium import folium_static, st_folium
import pandas as pd
import geopandas as gpd
from typing import List, Dict, Optional, Tuple, Any


def create_map(df: pd.DataFrame, center: Optional[List[float]] = None, 
              zoom_start: int = 11, heatmap: bool = False, radius: int = 10, 
              show_markers: bool = False, show_dots: bool = True, 
              marker_size: int = 3, color_by: Optional[str] = None,
              exclude_zip_only: bool = False) -> folium.Map:
    """
    Create a Folium map with volunteer or opportunity locations.
    
    Args:
        df: DataFrame with latitude and longitude columns
        center: Center coordinates [lat, lng]
        zoom_start: Initial zoom level
        heatmap: Whether to show a heatmap
        radius: Heatmap radius
        show_markers: Whether to show markers
        show_dots: Whether to show dots
        marker_size: Size of dots
        color_by: Column to use for coloring markers/dots
        exclude_zip_only: Whether to exclude zip code-only addresses
        
    Returns:
        Folium map
    """
    # Filter out rows with missing coordinates
    if 'latitude' in df.columns and 'longitude' in df.columns:
        # Filter out zip code-only addresses if requested
        if exclude_zip_only and 'is_zip_only' in df.columns:
            df = df[~df['is_zip_only']].copy()
            
        df_valid = df.dropna(subset=['latitude', 'longitude']).copy()
        
        if len(df_valid) == 0:
            # No valid coordinates, create empty map with default center
            if center is None:
                center = [41.8781, -87.6298]  # Default to Chicago
            return folium.Map(location=center, zoom_start=zoom_start)
            
        if center is None:
            # Default center is the mean of all points
            center = [df_valid['latitude'].mean(), df_valid['longitude'].mean()]
    else:
        # No coordinate columns, create empty map with default center
        if center is None:
            center = [41.8781, -87.6298]  # Default to Chicago
        return folium.Map(location=center, zoom_start=zoom_start)
    
    m = folium.Map(location=center, zoom_start=zoom_start)
    
    # Add individual markers or circles based on preference
    if (show_dots or show_markers) and len(df_valid) > 0:
        # Determine color scale if color_by is provided
        color_scale = None
        if color_by and color_by in df_valid.columns:
            # Normalize values to 0-1 range for coloring
            min_val = df_valid[color_by].min()
            max_val = df_valid[color_by].max()
            
            if min_val != max_val:
                df_valid['color_value'] = (df_valid[color_by] - min_val) / (max_val - min_val)
                
                # Function to convert normalized value to color
                def get_color(val):
                    # Simple blue to red scale
                    if pd.isna(val):
                        return '#3186cc'  # Default blue for missing values
                    
                    # Convert value to color on blue-red scale
                    r = int(255 * val)
                    b = int(255 * (1 - val))
                    return f'#{r:02x}00{b:02x}'
                
                df_valid['marker_color'] = df_valid['color_value'].apply(get_color)
            else:
                # All values are the same, use default color
                df_valid['marker_color'] = '#3186cc'
        else:
            # No color column, use default color
            df_valid['marker_color'] = '#3186cc'
        
        # Add markers for each point
        for idx, row in df_valid.iterrows():
            # Skip if coordinates are missing
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                continue
                
            # Create popup with available information
            popup_html = "<div style='width: 200px'>"
            
            # Add name if available
            if 'name' in row and not pd.isna(row['name']):
                popup_html += f"<h4>{row['name']}</h4>"
            
            # Add other metrics if available
            metrics = ['total_hours', 'engagement_score', 'address', 'city', 'state']
            for metric in metrics:
                if metric in row and not pd.isna(row[metric]):
                    popup_html += f"<b>{metric.replace('_', ' ').title()}:</b> {row[metric]}<br>"
            
            popup_html += "</div>"
            
            if show_markers:
                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color=row['marker_color'], icon='info-sign')
                ).add_to(m)
            elif show_dots:
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=marker_size,
                    popup=folium.Popup(popup_html, max_width=300),
                    color=row['marker_color'],
                    fill=True,
                    fill_color=row['marker_color'],
                    fill_opacity=0.7
                ).add_to(m)
    
    # Add heatmap if requested
    if heatmap and len(df_valid) > 0:
        # If color_by is provided, use it for heatmap intensity
        if color_by and color_by in df_valid.columns:
            heat_data = [[row['latitude'], row['longitude'], row[color_by]] 
                         for idx, row in df_valid.iterrows() 
                         if pd.notna(row['latitude']) and pd.notna(row['longitude']) and pd.notna(row[color_by])]
        else:
            heat_data = [[row['latitude'], row['longitude']] 
                         for idx, row in df_valid.iterrows()
                         if pd.notna(row['latitude']) and pd.notna(row['longitude'])]
        
        if heat_data:  # Only add heatmap if we have data
            HeatMap(heat_data, radius=radius, blur=10, gradient={0.4: 'blue', 0.65: 'lime', 1: 'red'}).add_to(m)
    
    return m


def display_map(m: folium.Map, height: int = 600) -> Dict:
    """
    Display a Folium map in Streamlit and return click data.
    
    Args:
        m: Folium map to display
        height: Height of the map in pixels
        
    Returns:
        Dictionary with map interaction data
    """
    return st_folium(m, width="100%", height=height)


def add_reference_marker(m: folium.Map, lat: float, lng: float, 
                        popup: str = "Reference Point") -> folium.Map:
    """
    Add a reference point marker to a map.
    
    Args:
        m: Folium map
        lat: Latitude
        lng: Longitude
        popup: Popup text
        
    Returns:
        Updated Folium map
    """
    folium.Marker(
        location=[lat, lng],
        popup=popup,
        icon=folium.Icon(color='red', icon='star')
    ).add_to(m)
    
    return m


def create_choropleth_map(gdf: gpd.GeoDataFrame, value_column: str, 
                         title: str, center: Optional[List[float]] = None,
                         zoom_start: int = 11) -> folium.Map:
    """
    Create a choropleth map from a GeoDataFrame.
    
    Args:
        gdf: GeoDataFrame with geometry and value column
        value_column: Column to use for choropleth coloring
        title: Map title
        center: Center coordinates [lat, lng]
        zoom_start: Initial zoom level
        
    Returns:
        Folium map
    """
    if center is None:
        # Try to get center from GeoDataFrame
        try:
            center = [gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()]
        except:
            center = [0, 0]
    
    m = folium.Map(location=center, zoom_start=zoom_start)
    
    # Create choropleth layer
    folium.Choropleth(
        geo_data=gdf.__geo_interface__,
        name=title,
        data=gdf,
        columns=['id', value_column],
        key_on='feature.id',
        fill_color='YlOrRd',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name=title
    ).add_to(m)
    
    # Add hover functionality
    style_function = lambda x: {'fillColor': '#ffffff', 
                               'color': '#000000', 
                               'fillOpacity': 0.1, 
                               'weight': 0.1}
    highlight_function = lambda x: {'fillColor': '#000000', 
                                   'color': '#000000', 
                                   'fillOpacity': 0.5, 
                                   'weight': 0.1}
    
    # Add GeoJson layer with tooltips
    folium.GeoJson(
        gdf,
        style_function=style_function,
        control=False,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['name', value_column],
            aliases=['Area:', f'{title}:'],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
        )
    ).add_to(m)
    
    return m 