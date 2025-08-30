from datetime import timedelta

import httpx
from sqlalchemy import text

from .conftest import DatabaseHelper, TestConfig


def test_api_health_check():
    resp = httpx.get(f"{TestConfig.API_BASE_URL}/docs", timeout=TestConfig.API_TIMEOUT)
    assert resp.status_code == 200


def test_rota_data_basica():
    engine = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MIN(timestamp) FROM data")).fetchone()
        start = row[0].isoformat()
        end = (row[0] + timedelta(hours=1)).isoformat()

    resp = httpx.get(
        f"{TestConfig.API_BASE_URL}/source/data",
        params=[("start", start), ("end", end), ("variables", "wind_speed"), ("variables", "power")],
        timeout=TestConfig.API_TIMEOUT,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) > 0


def test_filtro_variaveis():
    engine = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MIN(timestamp) FROM data")).fetchone()
        start = row[0].isoformat()
        end = (row[0] + timedelta(hours=1)).isoformat()

    resp = httpx.get(
        f"{TestConfig.API_BASE_URL}/source/data",
        params=[("start", start), ("end", end), ("variables", "wind_speed"), ("variables", "power")],
        timeout=TestConfig.API_TIMEOUT,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) > 0


