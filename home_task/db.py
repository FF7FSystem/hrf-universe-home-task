from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

engine = create_engine(
    "postgresql+psycopg2://admin:adm1n_password@localhost/home_task",
)
pg_session_factory = sessionmaker(
    engine, Session, autocommit=False, autoflush=False, expire_on_commit=False
)
SessionFactory = scoped_session(pg_session_factory)


def get_session() -> Generator[Session, None, None]:
    session = SessionFactory()
    try:
        yield session
    finally:
        session.close()
