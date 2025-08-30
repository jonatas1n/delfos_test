import os

from dagster import (
    Definitions,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    resource,
)
from sqlalchemy import create_engine

from db import common
from db.target_setup import create_target_schema
from .etl_daily import (
    DEFAULT_BASE_URL,
    aggregate_10min,
    build_day_window_utc,
    fetch_source_data,
    write_target,
)


@resource
def source_engine():
    """Recurso: engine do banco de dados Fonte (SQLAlchemy)."""
    url = common.build_db_url(common.DB_SOURCE_NAME)
    common.wait_for_connection(url)
    engine = create_engine(url, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


@resource
def target_engine():
    """Recurso: engine do banco de dados Alvo (SQLAlchemy)."""
    url = common.build_db_url(common.DB_TARGET_NAME)
    common.wait_for_connection(url)
    engine = create_engine(url, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    partitions_def=daily_partitions,
    required_resource_keys={"source_engine", "target_engine"},
)
def etl_daily_asset(context, source_engine, target_engine):
    """Asset particionado diário que executa o ETL do dia."""
    date_str = context.partition_key
    base_url = os.getenv("API_BASE_URL", DEFAULT_BASE_URL)

    start, end = build_day_window_utc(date_str)
    df = fetch_source_data(base_url, start, end)
    agg_df = aggregate_10min(df)

    create_target_schema(target_engine)
    inserted = write_target(target_engine, agg_df, start, end)

    return Output(
        value=None,
        metadata={
            "date": MetadataValue.text(date_str),
            "source_rows": MetadataValue.int(len(df)),
            "agg_rows": MetadataValue.int(len(agg_df)),
            "inserted": MetadataValue.int(inserted),
        },
    )


etl_job = define_asset_job("etl_job", selection=[etl_daily_asset])
etl_daily_schedule = build_schedule_from_partitioned_job(
    etl_job, cron_schedule="0 1 * * *"
)


defs = Definitions(
    assets=[etl_daily_asset],
    jobs=[etl_job],
    schedules=[etl_daily_schedule],
    resources={
        "source_engine": source_engine,
        "target_engine": target_engine,
    },
)
