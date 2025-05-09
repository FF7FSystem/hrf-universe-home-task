from dataclasses import dataclass
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Table,
BigInteger,
UniqueConstraint
)
from sqlalchemy.orm import registry

mapper_registry = registry()


class Model:
    pass


@mapper_registry.mapped
@dataclass
class StandardJobFamily(Model):
    __table__ = Table(
        "standard_job_family",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        schema="public",
    )

    id: str
    name: str


@mapper_registry.mapped
@dataclass
class StandardJob(Model):
    __table__ = Table(
        "standard_job",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        Column("standard_job_family_id", String, nullable=False),
        schema="public",
    )

    id: str
    name: str
    standard_job_family_id: str


@mapper_registry.mapped
@dataclass
class JobPosting(Model):
    __table__ = Table(
        "job_posting",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("title", String, nullable=False),
        Column("standard_job_id", String, nullable=False),
        Column("country_code", String, nullable=True),
        Column("days_to_hire", Integer, nullable=True),
        schema="public",
    )

    id: str
    title: str
    standard_job_id: str
    country_code: Optional[str] = None
    days_to_hire: Optional[int] = None


@mapper_registry.mapped
@dataclass
class JobPostingStatistics:
    __table__ = Table(
        "job_posting_statistics",
        mapper_registry.metadata,
        Column('id', BigInteger, primary_key=True, autoincrement=True),
        Column("standard_job_id", String, nullable=False),
        Column("average_days_to_hire", Float, nullable=False),
        Column("min_days_to_hire", Integer, nullable=False),
        Column("max_days_to_hire", Integer, nullable=False),
        Column("job_postings_count", Integer, nullable=False),
        Column("country_code", String, nullable=True),
        UniqueConstraint("standard_job_id", "country_code", name="uq_job_country"),
        schema="public",
    )

    standard_job_id: str
    average_days_to_hire: float
    min_days_to_hire: int
    max_days_to_hire: int
    job_postings_count: int
    country_code: Optional[str] = None
