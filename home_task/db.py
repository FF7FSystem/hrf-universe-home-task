from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from contextlib import contextmanager

engine = create_engine("postgresql+psycopg2://admin:adm1n_password@localhost/home_task",)
pg_session_factory = sessionmaker(engine, Session, autocommit=False, autoflush=False, expire_on_commit=False
)
SessionFactory = scoped_session(pg_session_factory)


# def get_session() -> Session:
#     return SessionFactory()

@contextmanager
def get_session() -> Session:
    session = SessionFactory()
    try:
        yield session
    except:
        session.rollback()
        raise
    finally:
        session.close()