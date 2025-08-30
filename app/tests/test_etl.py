from sqlalchemy import text

from .conftest import DatabaseHelper, TestConfig, run_etl_for_date


def test_etl_execucao_basica():
    engine = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine.connect() as conn:
        test_date = (
            conn.execute(text("SELECT DATE(MIN(timestamp)) FROM data"))
            .scalar()
            .strftime("%Y-%m-%d")
        )

    result = run_etl_for_date(test_date)

    assert result.returncode == 0


def test_etl_agregacoes():
    engine_fonte = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine_fonte.connect() as conn:
        test_date = (
            conn.execute(text("SELECT DATE(MIN(timestamp)) FROM data"))
            .scalar()
            .strftime("%Y-%m-%d")
        )

    run_etl_for_date(test_date)

    engine_alvo = DatabaseHelper.get_engine(TestConfig.ALVO_DB_URL)
    with engine_alvo.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT DATE_TRUNC('hour', timestamp) as hour, COUNT(*) as records_per_hour
                FROM data 
                WHERE DATE(timestamp) = '{test_date}'
                GROUP BY DATE_TRUNC('hour', timestamp)
                ORDER BY hour
                LIMIT 5
                """
            )
        ).fetchall()

        if rows:
            for row in rows:
                assert row.records_per_hour > 0
