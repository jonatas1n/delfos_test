from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from . import common


_source_engine = None
_SourceSessionLocal = None


def _ensure_source_session_factory() -> None:
    global _source_engine, _SourceSessionLocal
    if _SourceSessionLocal is not None:
        return
    url = common.build_db_url(common.DB_SOURCE_NAME)
    common.wait_for_connection(url)
    _source_engine = create_engine(url, pool_pre_ping=True)
    _SourceSessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=_source_engine
    )


def get_source_session() -> Generator[Session, None, None]:
    _ensure_source_session_factory()
    session: Session = _SourceSessionLocal()
    try:
        yield session
    finally:
        session.close()
