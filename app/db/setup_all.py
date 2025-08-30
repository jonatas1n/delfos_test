import sys
from sqlalchemy import create_engine

from . import common
from .source_setup import create_source_schema, seed_source_data
from .target_setup import create_target_schema, ensure_signals_and_seed_target


def main() -> None:
    common.create_database_if_not_exists(common.DB_SOURCE_NAME)
    common.create_database_if_not_exists(common.DB_TARGET_NAME)

    src_url = common.build_db_url(common.DB_SOURCE_NAME)
    common.wait_for_connection(src_url)
    src_engine = create_engine(src_url, pool_pre_ping=True)

    tgt_url = common.build_db_url(common.DB_TARGET_NAME)
    common.wait_for_connection(tgt_url)
    tgt_engine = create_engine(tgt_url, pool_pre_ping=True)

    create_source_schema(src_engine)
    create_target_schema(tgt_engine)

    start_ts, end_ts, num_rows = seed_source_data(src_engine)
    inserted = ensure_signals_and_seed_target(tgt_engine, src_engine)

    print("Source DB:")
    print(f"  database: {common.DB_SOURCE_NAME}")
    print(f"  period: {start_ts.isoformat()} -> {end_ts.isoformat()}")
    print(f"  rows: {num_rows}")
    print("Target DB:")
    print(f"  database: {common.DB_TARGET_NAME}")
    if inserted:
        return print(f"  inserted measurements: {inserted}")
    print("  measurements not inserted (already present)")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}")
        sys.exit(1)
