from typing import List, Optional, Dict
from datetime import datetime
from pydantic import BaseModel, Field


class Opportunity(BaseModel):
    """Model representing a volunteer opportunity."""
    id: str
    title: str
    description: Optional[str] = None
    location: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: str = "active"
    category: Optional[str] = None
    organization: Optional[str] = None
    
    @property
    def full_address(self) -> str:
        """Return the opportunity's full address."""
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
    def duration_hours(self) -> Optional[float]:
        """Calculate the duration of the opportunity in hours."""
        if self.start_date and self.end_date:
            delta = self.end_date - self.start_date
            return delta.total_seconds() / 3600
        return None
    
    @property
    def is_past(self) -> bool:
        """Check if the opportunity is in the past."""
        if self.end_date:
            return self.end_date < datetime.now()
        return False
    
    @property
    def is_upcoming(self) -> bool:
        """Check if the opportunity is upcoming."""
        if self.start_date:
            return self.start_date > datetime.now()
        return False
    
    @property
    def is_ongoing(self) -> bool:
        """Check if the opportunity is currently ongoing."""
        now = datetime.now()
        if self.start_date and self.end_date:
            return self.start_date <= now <= self.end_date
        return False


class OpportunityParticipation(BaseModel):
    """Model representing participation in an opportunity."""
    opportunity_id: str
    volunteer_ids: List[str] = Field(default_factory=list)
    total_hours: float = 0
    average_hours_per_volunteer: float = 0
    
    def add_volunteer(self, volunteer_id: str, hours: float = 0):
        """Add a volunteer to the participation record."""
        if volunteer_id not in self.volunteer_ids:
            self.volunteer_ids.append(volunteer_id)
            self.total_hours += hours
            self.update_average()
    
    def update_average(self):
        """Update the average hours per volunteer."""
        if self.volunteer_ids:
            self.average_hours_per_volunteer = self.total_hours / len(self.volunteer_ids)
        else:
            self.average_hours_per_volunteer = 0 