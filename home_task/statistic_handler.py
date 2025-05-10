from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, Generator

from sqlalchemy import func, literal
from models import JobPosting, JobPostingStatistics
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.engine import Row

DEFAULT_MIN_PERCENTILE = 0.1
DEFAULT_MAX_PERCENTILE = 0.9


class StatisticHandler:

    def __init__(self, session: Session | Generator) -> None:
        self.batch_size = 5
        self.session = next(session) if isinstance(session, Generator) else session

    def update_posting_statistic(self, threshold: int) -> None:

        #  data is frozen and will not change during script execution
        country_job_unique_pairs = self._get_unique_pair()
        job_unique_group = self._get_unique_jobs()

        records = []
        for country_code, job_id in country_job_unique_pairs:
            self.processing_statistics(job_id, country_code, threshold, records)

        for job_id in job_unique_group:
            self.processing_statistics(job_id[0], None, threshold, records)

        self.insert_records(records)

    def processing_statistics(self, job_id: str, country_code: str | None, threshold: int, records: list) -> None:

        min_threshold, max_threshold = self._get_min_max_thresholds(job_id, country_code)
        aggregated_data = self._create_statistic(job_id, min_threshold, max_threshold, country_code)

        if aggregated_data.total_count > threshold:
            records.append(self.convert_record(job_id, country_code, aggregated_data))

    def _get_unique_pair(self) -> list[JobPosting]:
        query = self.session.query(JobPosting).with_entities(
            JobPosting.country_code,
            JobPosting.standard_job_id
        ).filter(
            JobPosting.country_code.isnot(None)
        ).distinct().order_by(JobPosting.country_code)

        return query.all()

    def _get_unique_jobs(self) -> list[JobPosting]:
        query = self.session.query(JobPosting).with_entities(JobPosting.standard_job_id).distinct()
        return query.all()

    def _get_min_max_thresholds(self, job_id: str, country_code: str = None) -> Tuple[Decimal, Decimal]:
        query = self.session.query(
            func.percentile_cont(DEFAULT_MIN_PERCENTILE).within_group(JobPosting.days_to_hire).label('p10'),
            func.percentile_cont(DEFAULT_MAX_PERCENTILE).within_group(JobPosting.days_to_hire).label('p90')
        ).filter(JobPosting.standard_job_id == job_id, JobPosting.days_to_hire.isnot(None))

        if country_code:
            query = query.filter(JobPosting.country_code == country_code)

        percentiles = query.one()
        min_threshold = Decimal(percentiles.p10).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        max_threshold = Decimal(percentiles.p90).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        return min_threshold, max_threshold

    def _create_statistic(self, job_id: str, min_threshold: Decimal, max_threshold: Decimal,
                          country_code: str = None) -> Row:

        query = self.session.query(
            func.count(JobPosting.id).label('total_count'),
            func.sum(JobPosting.days_to_hire).label('total_days'),
            func.avg(JobPosting.days_to_hire).label('average_days'),
            func.min(JobPosting.days_to_hire).label('min_days'),
            func.max(JobPosting.days_to_hire).label('max_days')
        ).filter(
            JobPosting.standard_job_id == job_id,
            JobPosting.days_to_hire > min_threshold,
            JobPosting.days_to_hire < max_threshold
        )

        if country_code:
            query = query.filter(JobPosting.country_code == country_code)

        aggregated_data = query.one()

        return aggregated_data

    @staticmethod
    def convert_record(job_id: str, country_code: str | None, aggregated_data: Row) -> dict:
        average_days = Decimal(aggregated_data.average_days).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        return {
            'standard_job_id': job_id,
            'country_code': country_code,
            'average_days_to_hire': average_days,
            'min_days_to_hire': aggregated_data.min_days,
            'max_days_to_hire': aggregated_data.max_days,
            'job_postings_count': aggregated_data.total_count
        }

    def insert_records(self, records: list[dict]) -> None:
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]

            stmt = insert(JobPostingStatistics.__table__).values(batch)

            conflict_stmt = stmt.on_conflict_do_update(
                index_elements=[
                    JobPostingStatistics.__table__.c.standard_job_id,
                    func.coalesce(JobPostingStatistics.__table__.c.country_code, literal('__NULL__'))
                ],
                set_={
                    'average_days_to_hire': stmt.excluded.average_days_to_hire,
                    'min_days_to_hire': stmt.excluded.min_days_to_hire,
                    'max_days_to_hire': stmt.excluded.max_days_to_hire,
                    'job_postings_count': stmt.excluded.job_postings_count,
                    'country_code': stmt.excluded.country_code
                }
            )

            self.session.execute(conflict_stmt)
            self.session.commit()

    def get_statistic(self, job_id: str, country_code: str | None) -> Row | None:
        query = self.session.query(JobPostingStatistics).filter(JobPostingStatistics.standard_job_id == job_id)

        if country_code is None:
            query = query.filter(JobPostingStatistics.country_code.is_(None))
        else:
            query = query.filter(JobPostingStatistics.country_code == country_code)

        return query.first()
