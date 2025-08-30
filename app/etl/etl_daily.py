import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import httpx
import pandas as pd
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session

from db import common
from db.target_setup import Signal, Measurement, create_target_schema


FALLBACK_URL = "http://localhost:8000"
DEFAULT_BASE_URL = os.getenv("API_BASE_URL", FALLBACK_URL)

# Constantes de agregação e nomes de variáveis
RESAMPLE_RULE = "10min"
REQUEST_VARS = ["wind_speed", "power"]
AGG_FUNCS = ["mean", "min", "max", "std"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa ETL diário da API de origem para o banco de destino"
    )
    parser.add_argument(
        "--date", required=True, help="Data no formato YYYY-MM-DD (dia em UTC)"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"URL base da API, padrão {FALLBACK_URL}",
    )
    return parser.parse_args()


def build_day_window_utc(date_str: str) -> tuple[datetime, datetime]:
    day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = day
    end = day + timedelta(days=1)
    return start, end


def fetch_source_data(base_url: str, start: datetime, end: datetime) -> pd.DataFrame:
    params = {
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "variables": REQUEST_VARS,
    }
    date_params = [
        ("start", params["start"]),
        ("end", params["end"]),
    ]
    variables_params = [("variables", v) for v in params["variables"]]
    query_params = date_params + variables_params
    url = f"{base_url.rstrip('/')}/source/data"
    with httpx.Client(timeout=60) as client:
        resp = client.get(url, params=query_params)
        resp.raise_for_status()
        data = resp.json()

    if not data:
        return pd.DataFrame(columns=["timestamp", *REQUEST_VARS]).set_index("timestamp")

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp").sort_index()

    for col in REQUEST_VARS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[REQUEST_VARS]


def aggregate_10min(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        # Cria DataFrame vazio com as colunas esperadas quando não há dados
        cols = [
            f"{var}_{agg}_10m"
            for var in ["wind_speed", "power"]
            for agg in ["mean", "min", "max", "std"]
        ]
        return pd.DataFrame(columns=cols)
    agg = df.resample(RESAMPLE_RULE).agg(AGG_FUNCS)
    agg.columns = [f"{var}_{stat}_10m" for var, stat in agg.columns]
    return agg


def ensure_signals(session: Session, signal_names: List[str]) -> Dict[str, int]:
    existing = session.query(Signal).filter(Signal.name.in_(signal_names)).all()
    name_to_id = {s.name: s.id for s in existing}
    missing = [n for n in signal_names if n not in name_to_id]
    if missing:
        session.add_all([Signal(name=n) for n in missing])
        session.commit()
        existing = session.query(Signal).filter(Signal.name.in_(signal_names)).all()
        name_to_id = {s.name: s.id for s in existing}
    return name_to_id


def write_target(engine, agg_df: pd.DataFrame, start: datetime, end: datetime) -> int:
    if agg_df.empty:
        return 0

    with Session(engine) as session:
        create_target_schema(engine)

        signal_names = [
            f"{var}_{stat}_10m" for var in REQUEST_VARS for stat in AGG_FUNCS
        ]
        name_to_id = ensure_signals(session, signal_names)

        # Idempotente: remove linhas existentes na janela para esses sinais
        signal_ids = list(name_to_id.values())
        session.execute(
            delete(Measurement)
            .where(Measurement.timestamp >= start)
            .where(Measurement.timestamp < end)
            .where(Measurement.signal_id.in_(signal_ids))
        )
        session.commit()

        # Insere por sinal em lotes usando pandas para eficiência
        inserted = 0
        for signal_name, signal_id in name_to_id.items():
            if signal_name not in agg_df.columns:
                continue

            sub = agg_df[[signal_name]].dropna().rename(columns={signal_name: "value"})
            if sub.empty:
                continue

            # Converte índice de timestamp para coluna
            sub = sub.reset_index()
            sub["signal_id"] = signal_id

            sub.to_sql(
                name=Measurement.__tablename__,
                con=engine,
                if_exists="append",
                index=False,
            )
            inserted += len(sub)

    return inserted


def run_etl_for_date(date_str: str, base_url: str = DEFAULT_BASE_URL) -> dict:
    start, end = build_day_window_utc(date_str)

    df = fetch_source_data(base_url, start, end)
    agg_df = aggregate_10min(df)

    common.create_database_if_not_exists(common.DB_TARGET_NAME)
    tgt_url = common.build_db_url(common.DB_TARGET_NAME)
    common.wait_for_connection(tgt_url)
    tgt_engine = create_engine(tgt_url, pool_pre_ping=True)
    inserted = write_target(tgt_engine, agg_df, start, end)

    return {
        "date": date_str,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "source_rows": int(len(df)),
        "agg_rows": int(len(agg_df)),
        "inserted": int(inserted),
    }


def main() -> None:
    args = parse_args()
    result = run_etl_for_date(args.date, args.base_url)
    print(
        f"ETL date={result['date']} window=[{result['window_start']}, {result['window_end']})\n"
        f"Source rows: {result['source_rows']} -> 10-min rows: {result['agg_rows']}\n"
        f"Inserted measurements: {result['inserted']}"
    )


if __name__ == "__main__":
    main()
