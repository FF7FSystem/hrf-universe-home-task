from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple

from sqlalchemy import func
from db import get_session
from models import JobPosting, JobPostingStatistics
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.engine import Row


class StatisticHandler:
    BATCH_SIZE = 5

    @classmethod
    def update_posting_statistic(cls, threshold: int) -> None:

        with get_session() as session:
            #  data is frozen and will not change during script execution

            country_job_unique_pairs = cls._get_unique_pair(session)
            job_unique_group = cls._get_unique_jobs(session)

            records = []
            for country_code, job_id in country_job_unique_pairs:
                cls.processing_statistics(session, job_id, country_code, threshold, records)

            for job_id in job_unique_group:
                cls.processing_statistics(session, job_id[0], None, threshold, records)

            cls.insert_records(session, records)


    @classmethod
    def processing_statistics(cls,session: Session,job_id:str, country_code: str|None,threshold:int, records:list) -> None:

        min_threshold, max_threshold = cls._get_min_max_thresholds(session, job_id, country_code)
        aggregated_data = cls._get_certain_statistic(session, job_id, min_threshold, max_threshold, country_code)

        if aggregated_data.total_count > threshold:
            records.append(cls.convert_record(job_id, country_code, aggregated_data))


    @staticmethod
    def _get_unique_pair(session: Session) -> list[Row]:
        query = session.query(JobPosting).with_entities(
            JobPosting.country_code,
            JobPosting.standard_job_id
        ).filter(
            JobPosting.country_code.isnot(None)
        ).distinct().order_by(JobPosting.country_code)

        return query.all()

    def _get_unique_jobs(session: Session) -> list[Row]:
        query = session.query(JobPosting).with_entities(JobPosting.standard_job_id).distinct()
        return query.all()


    @staticmethod
    def _get_min_max_thresholds(session: Session,  job_id: str,country_code: str = None) -> Tuple[Decimal, Decimal]:
        query = session.query(
            func.percentile_cont(0.1).within_group(JobPosting.days_to_hire).label('p10'),
            func.percentile_cont(0.9).within_group(JobPosting.days_to_hire).label('p90')
        ).filter(JobPosting.standard_job_id == job_id, JobPosting.days_to_hire.isnot(None))

        if country_code:
            query = query.filter(JobPosting.country_code == country_code)

        percentiles = query.one()
        min_threshold = Decimal(percentiles.p10).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        max_threshold = Decimal(percentiles.p90).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        return min_threshold, max_threshold

    @staticmethod
    def _get_certain_statistic(session: Session, job_id: str, min_threshold: Decimal, max_threshold: Decimal, country_code: str=None) -> Row:

        query = session.query(
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
        average_days =  Decimal(aggregated_data.average_days).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        return {
            'standard_job_id': job_id,
            'country_code': country_code,
            'average_days_to_hire':average_days,
            'min_days_to_hire': aggregated_data.min_days,
            'max_days_to_hire': aggregated_data.max_days,
            'job_postings_count': aggregated_data.total_count
        }

    @classmethod
    def insert_records(cls, session: Session, records: list[dict]) -> None:
        for i in range(0, len(records), cls.BATCH_SIZE):
            batch = records[i:i + cls.BATCH_SIZE]
            stmt = insert(JobPostingStatistics.__table__).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["standard_job_id", "country_code"],
                set_={
                    'average_days_to_hire': stmt.excluded.average_days_to_hire,
                    'min_days_to_hire': stmt.excluded.min_days_to_hire,
                    'max_days_to_hire': stmt.excluded.max_days_to_hire,
                    'job_postings_count': stmt.excluded.job_postings_count,
                    'country_code': stmt.excluded.country_code
                }
            )

            session.execute(stmt)
            session.commit()
