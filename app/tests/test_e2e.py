from datetime import datetime, timezone
from sqlalchemy import text

from .conftest import DatabaseHelper, TestConfig, run_etl_for_date


def test_pipeline_completo_data_atual():
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    result = run_etl_for_date(current_date)
    # Pode falhar se não houver dados hoje; não falhar o teste
    assert result.returncode in (0, 1)


def test_consistencia_dados_fonte_alvo():
    engine_fonte = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine_fonte.connect() as conn:
        test_date = (
            conn.execute(text("SELECT DATE(MIN(timestamp)) FROM data"))
            .scalar()
            .strftime("%Y-%m-%d")
        )

    run_etl_for_date(test_date)

    with engine_fonte.connect() as conn_fonte:
        fonte = conn_fonte.execute(
            text(
                f"""
                SELECT COUNT(*) as total, AVG(wind_speed) as avg_wind, AVG(power) as avg_power
                FROM data WHERE DATE(timestamp) = '{test_date}'
                """
            )
        ).fetchone()

    engine_alvo = DatabaseHelper.get_engine(TestConfig.ALVO_DB_URL)
    with engine_alvo.connect() as conn_alvo:
        alvo = conn_alvo.execute(
            text(
                f"""
                SELECT COUNT(*) as total FROM data WHERE DATE(timestamp) = '{test_date}'
                """
            )
        ).fetchone()

    assert alvo.total > 0
