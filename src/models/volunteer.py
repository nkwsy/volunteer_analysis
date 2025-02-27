from typing import List, Optional, Dict
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from dataclasses import dataclass, field


class VolunteerHours(BaseModel):
    """Model representing volunteer hours logged."""
    id: str
    volunteer_id: str
    opportunity_id: str
    hours: float
    date: datetime
    notes: Optional[str] = None
    status: str = "approved"
    
    @property
    def formatted_date(self) -> str:
        """Return the date formatted as YYYY-MM-DD."""
        return self.date.strftime("%Y-%m-%d")


class Volunteer(BaseModel):
    """Model representing a volunteer."""
    id: str
    first_name: str
    last_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    join_date: Optional[datetime] = None
    status: str = "active"
    hours: List[VolunteerHours] = Field(default_factory=list)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    @property
    def full_name(self) -> str:
        """Return the volunteer's full name."""
        return f"{self.first_name} {self.last_name}"
    
    @property
    def full_address(self) -> str:
        """Return the volunteer's full address."""
        components = []
        if self.address:
            components.append(self.address)
        if self.city:
            components.append(self.city)
        if self.state:
            components.append(self.state)
        if self.zip_code:
            components.append(self.zip_code)
        
        return ", ".join(components) if components else ""
    
    @property
    def total_hours(self) -> float:
        """Calculate total hours logged by the volunteer."""
        return sum(hour.hours for hour in self.hours)
    
    def hours_by_opportunity(self) -> Dict[str, float]:
        """Group hours by opportunity ID."""
        result = {}
        for hour in self.hours:
            if hour.opportunity_id not in result:
                result[hour.opportunity_id] = 0
            result[hour.opportunity_id] += hour.hours
        return result
    
    def hours_by_month(self) -> Dict[str, float]:
        """Group hours by month."""
        result = {}
        for hour in self.hours:
            month_key = hour.date.strftime("%Y-%m")
            if month_key not in result:
                result[month_key] = 0
            result[month_key] += hour.hours
        return result
    
    def hours_in_date_range(self, start_date: datetime, end_date: datetime) -> float:
        """Calculate hours within a date range."""
        return sum(hour.hours for hour in self.hours 
                  if start_date <= hour.date <= end_date)
    
    def is_long_term(self, min_months: int = 6) -> bool:
        """
        Determine if volunteer is long-term (active for at least min_months).
        
        Args:
            min_months: Minimum number of months to be considered long-term
            
        Returns:
            True if volunteer is long-term, False otherwise
        """
        if not self.join_date:
            return False
            
        months_active = (datetime.now() - self.join_date).days // 30
        return months_active >= min_months
    
    def engagement_score(self, recency_weight: float = 0.4, 
                        frequency_weight: float = 0.3,
                        hours_weight: float = 0.3) -> float:
        """
        Calculate an engagement score for the volunteer.
        
        Uses a weighted combination of:
        - Recency: How recently they volunteered
        - Frequency: How often they volunteer
        - Hours: Total hours contributed
        
        Args:
            recency_weight: Weight for recency component
            frequency_weight: Weight for frequency component
            hours_weight: Weight for hours component
            
        Returns:
            Engagement score from 0-100
        """
        if not self.hours:
            return 0
            
        # Recency: days since last volunteer activity (inverse)
        latest_date = max(hour.date for hour in self.hours)
        days_since = (datetime.now() - latest_date).days
        recency_score = max(0, 100 - min(days_since, 100))
        
        # Frequency: number of distinct days volunteered in last 90 days
        ninety_days_ago = datetime.now() - timedelta(days=90)
        recent_days = set(hour.date.date() for hour in self.hours 
                         if hour.date >= ninety_days_ago)
        frequency_score = min(100, len(recent_days) * (100/30))  # Scale to 100
        
        # Hours: total hours in last 90 days (capped at 100)
        recent_hours = sum(hour.hours for hour in self.hours 
                          if hour.date >= ninety_days_ago)
        hours_score = min(100, recent_hours * 5)  # 20 hours = 100 score
        
        # Weighted score
        return (recency_weight * recency_score +
                frequency_weight * frequency_score +
                hours_weight * hours_score) 