from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator


class JobPostingStatisticsResponse(BaseModel):
    standard_job_id: str
    country_code: Optional[str] = None
    min_days: float = Field(alias="min_days_to_hire")
    avg_days: float = Field(alias="average_days_to_hire")
    max_days: float = Field(alias="max_days_to_hire")
    job_postings_number: int = Field(alias="job_postings_count")

    class Config:
        orm_mode = True

    @validator("min_days", "avg_days", "max_days", pre=True)
    def round_to_one_decimal(cls, v):
        return round(v, 1) if v is not None else None
