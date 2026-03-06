"""Domain models for the movie analytics pipeline."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Movie:
    """Represents a single movie record after cleaning."""

    movie_id: int
    title: Optional[str]
    release_date: Optional[datetime]
    budget: Optional[float]
    revenue: Optional[float]
    is_budget_reported: bool = field(init=False)
    is_revenue_reported: bool = field(init=False)
    is_roi_eligible: bool = field(init=False)

    def __post_init__(self):
        self.is_budget_reported = self.budget is not None and self.budget > 0
        self.is_revenue_reported = self.revenue is not None and self.revenue > 0
        self.is_roi_eligible = self.is_budget_reported and self.is_revenue_reported

    @property
    def roi(self) -> Optional[float]:
        if self.is_roi_eligible:
            return (self.revenue - self.budget) / self.budget
        return None

    @property
    def profit(self) -> Optional[float]:
        if self.is_roi_eligible:
            return self.revenue - self.budget
        return None


@dataclass
class RatingSummary:
    """Represents a flattened rating summary for a movie."""

    movie_id: int
    avg_rating: Optional[float]
    total_ratings: Optional[int]
    std_dev: Optional[float]
    last_rated: Optional[datetime]
