import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Optional, Tuple, Any


def create_hours_histogram(df: pd.DataFrame, hours_column: str = 'total_hours', 
                          bins: int = 15, title: str = "Hours Distribution") -> plt.Figure:
    """
    Create a histogram of volunteer hours.
    
    Args:
        df: DataFrame with hours data
        hours_column: Column containing hours data
        bins: Number of bins for histogram
        title: Chart title
        
    Returns:
        Matplotlib figure
    """
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(df[hours_column], bins=bins, alpha=0.7, color='steelblue')
    ax.set_xlabel("Hours")
    ax.set_ylabel("Number of Volunteers")
    ax.set_title(title)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    return fig


def create_hours_by_month_chart(hours_by_month: Dict[str, float], 
                               title: str = "Hours by Month") -> plt.Figure:
    """
    Create a line chart of hours by month.
    
    Args:
        hours_by_month: Dictionary mapping month to hours
        title: Chart title
        
    Returns:
        Matplotlib figure
    """
    # Convert dictionary to DataFrame
    df = pd.DataFrame(list(hours_by_month.items()), columns=['Month', 'Hours'])
    df['Month'] = pd.to_datetime(df['Month'])
    df = df.sort_values('Month')
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df['Month'], df['Hours'], marker='o', linestyle='-', color='forestgreen')
    ax.set_xlabel("Month")
    ax.set_ylabel("Hours")
    ax.set_title(title)
    ax.grid(alpha=0.3)
    
    # Format x-axis as month-year
    plt.xticks(rotation=45)
    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%b %Y'))
    
    plt.tight_layout()
    
    return fig


def create_top_volunteers_chart(top_volunteers: Dict[str, float], 
                               title: str = "Top Volunteers by Hours") -> plt.Figure:
    """
    Create a bar chart of top volunteers by hours.
    
    Args:
        top_volunteers: Dictionary mapping volunteer name to hours
        title: Chart title
        
    Returns:
        Matplotlib figure
    """
    # Convert dictionary to DataFrame
    df = pd.DataFrame(list(top_volunteers.items()), columns=['Volunteer', 'Hours'])
    df = df.sort_values('Hours', ascending=True)
    
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(df['Volunteer'], df['Hours'], color='steelblue', alpha=0.7)
    ax.set_xlabel("Hours")
    ax.set_ylabel("Volunteer")
    ax.set_title(title)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    
    return fig


def create_top_opportunities_chart(top_opportunities: Dict[str, float], 
                                  title: str = "Top Opportunities by Hours") -> plt.Figure:
    """
    Create a bar chart of top opportunities by hours.
    
    Args:
        top_opportunities: Dictionary mapping opportunity title to hours
        title: Chart title
        
    Returns:
        Matplotlib figure
    """
    # Convert dictionary to DataFrame
    df = pd.DataFrame(list(top_opportunities.items()), columns=['Opportunity', 'Hours'])
    df = df.sort_values('Hours', ascending=True)
    
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(df['Opportunity'], df['Hours'], color='darkorange', alpha=0.7)
    ax.set_xlabel("Hours")
    ax.set_ylabel("Opportunity")
    ax.set_title(title)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    
    return fig


def create_engagement_scatter_plot(df: pd.DataFrame, x_column: str = 'total_hours', 
                                  y_column: str = 'engagement_score',
                                  color_column: Optional[str] = None,
                                  title: str = "Volunteer Engagement") -> go.Figure:
    """
    Create a scatter plot of volunteer engagement.
    
    Args:
        df: DataFrame with volunteer data
        x_column: Column for x-axis
        y_column: Column for y-axis
        color_column: Column for color coding points
        title: Chart title
        
    Returns:
        Plotly figure
    """
    if color_column and color_column in df.columns:
        fig = px.scatter(
            df, x=x_column, y=y_column, color=color_column,
            hover_name="name" if "name" in df.columns else None,
            title=title,
            labels={
                x_column: x_column.replace('_', ' ').title(),
                y_column: y_column.replace('_', ' ').title(),
                color_column: color_column.replace('_', ' ').title()
            }
        )
    else:
        fig = px.scatter(
            df, x=x_column, y=y_column,
            hover_name="name" if "name" in df.columns else None,
            title=title,
            labels={
                x_column: x_column.replace('_', ' ').title(),
                y_column: y_column.replace('_', ' ').title()
            }
        )
    
    fig.update_traces(marker=dict(size=10))
    fig.update_layout(
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True)
    )
    
    return fig


def create_engagement_distribution_chart(df: pd.DataFrame, 
                                        engagement_column: str = 'engagement_score',
                                        title: str = "Engagement Score Distribution") -> go.Figure:
    """
    Create a distribution chart of engagement scores.
    
    Args:
        df: DataFrame with volunteer data
        engagement_column: Column containing engagement scores
        title: Chart title
        
    Returns:
        Plotly figure
    """
    # Define engagement categories
    df['engagement_category'] = pd.cut(
        df[engagement_column],
        bins=[0, 30, 60, 100],
        labels=['Low', 'Medium', 'High']
    )
    
    # Count volunteers in each category
    category_counts = df['engagement_category'].value_counts().reset_index()
    category_counts.columns = ['Category', 'Count']
    
    # Sort by engagement level
    category_order = ['Low', 'Medium', 'High']
    category_counts['Category'] = pd.Categorical(
        category_counts['Category'], 
        categories=category_order, 
        ordered=True
    )
    category_counts = category_counts.sort_values('Category')
    
    # Create color map
    colors = {'Low': 'red', 'Medium': 'orange', 'High': 'green'}
    
    fig = px.bar(
        category_counts, 
        x='Category', 
        y='Count',
        title=title,
        color='Category',
        color_discrete_map=colors,
        text='Count'
    )
    
    fig.update_traces(textposition='outside')
    fig.update_layout(
        xaxis_title="Engagement Level",
        yaxis_title="Number of Volunteers",
        showlegend=False
    )
    
    return fig


def create_hours_cumulative_chart(df: pd.DataFrame, hours_column: str = 'total_hours',
                                 max_hours: Optional[int] = None,
                                 title: str = "Cumulative Volunteer Hours") -> go.Figure:
    """
    Create a cumulative chart of volunteer hours.
    
    Args:
        df: DataFrame with volunteer data
        hours_column: Column containing hours data
        max_hours: Maximum hours to include
        title: Chart title
        
    Returns:
        Plotly figure
    """
    # Sort hours
    hours = sorted(df[hours_column].tolist())
    
    if max_hours is None:
        max_hours = int(max(hours)) + 1
    
    # Create cumulative data
    cumulative_data = []
    
    for h in range(0, max_hours + 1):
        count = len([x for x in hours if x <= h])
        percentage = count / len(hours) * 100 if len(hours) > 0 else 0
        
        cumulative_data.append({
            'Hours': h,
            'Volunteers': count,
            'Percentage': percentage
        })
    
    cumulative_df = pd.DataFrame(cumulative_data)
    
    # Create figure
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=cumulative_df['Hours'],
        y=cumulative_df['Percentage'],
        mode='lines+markers',
        name='Cumulative %',
        line=dict(color='forestgreen', width=2),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Hours",
        yaxis_title="Cumulative % of Volunteers",
        yaxis=dict(range=[0, 100]),
        hovermode="x unified"
    )
    
    return fig


def create_opportunity_participation_chart(df: pd.DataFrame, 
                                          title: str = "Opportunity Participation") -> go.Figure:
    """
    Create a bubble chart of opportunity participation.
    
    Args:
        df: DataFrame with opportunity participation data
        title: Chart title
        
    Returns:
        Plotly figure
    """
    fig = px.scatter(
        df,
        x="volunteer_count",
        y="total_hours",
        size="average_hours_per_volunteer",
        hover_name="opportunity_title",
        title=title,
        labels={
            "volunteer_count": "Number of Volunteers",
            "total_hours": "Total Hours",
            "average_hours_per_volunteer": "Avg Hours per Volunteer"
        }
    )
    
    fig.update_traces(marker=dict(opacity=0.7))
    fig.update_layout(
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True)
    )
    
    return fig 