"""add job_posting_statistics table with COALESCE index

Revision ID: a51f8f8305fc
Revises: 991ecb2bf269
Create Date: 2025-05-09 22:18:39.656392
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'a51f8f8305fc'
down_revision = '991ecb2bf269'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'job_posting_statistics',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('standard_job_id', sa.String(), nullable=False),
        sa.Column('average_days_to_hire', sa.Float(), nullable=False),
        sa.Column('min_days_to_hire', sa.Integer(), nullable=False),
        sa.Column('max_days_to_hire', sa.Integer(), nullable=False),
        sa.Column('job_postings_count', sa.Integer(), nullable=False),
        sa.Column('country_code', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )

    op.execute("""
        CREATE UNIQUE INDEX job_posting_statistics_unique_idx
        ON public.job_posting_statistics (
            standard_job_id,
            COALESCE(country_code, '__NULL__')
        );
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS public.job_posting_statistics_unique_idx")
    op.drop_table('job_posting_statistics', schema='public')
