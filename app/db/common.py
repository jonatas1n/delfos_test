import os
import time
from sqlalchemy import create_engine, text


DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_SOURCE_NAME = os.getenv("DB_SOURCE_NAME", os.getenv("DB_NAME", "source"))
DB_TARGET_NAME = os.getenv("DB_TARGET_NAME", "target")

CONNECTION_ATTEMPTS_DEFAULT = int(os.getenv("DB_CONN_ATTEMPTS", "30"))
CONNECTION_DELAY_S_DEFAULT = float(os.getenv("DB_CONN_DELAY_S", "1.0"))


def build_db_url(db_name: str) -> str:
    return (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}"
    )


def wait_for_connection(
    url: str,
    attempts: int = CONNECTION_ATTEMPTS_DEFAULT,
    delay_s: float = CONNECTION_DELAY_S_DEFAULT,
) -> None:
    last_error = None
    for _ in range(attempts):
        try:
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(delay_s)
    raise RuntimeError(f"Could not connect to {url}: {last_error}")


def create_database_if_not_exists(db_name: str) -> None:
    server_url = build_db_url("postgres")
    wait_for_connection(server_url)
    engine = create_engine(server_url, pool_pre_ping=True, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": db_name},
        ).scalar()
        if not exists:
            if not db_name.replace("_", "").isalnum():
                raise ValueError("Invalid database name")
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    engine.dispose()
