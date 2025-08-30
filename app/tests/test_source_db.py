from sqlalchemy import text

from .conftest import DatabaseHelper, TestConfig


def test_banco_fonte_conectividade():
    engine = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT 1")).scalar() == 1


def test_estrutura_tabela_data():
    engine = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine.connect() as conn:
        exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'data'
                )
                """
            )
        ).scalar()
        assert exists, "Tabela 'data' não existe"

        cols = conn.execute(
            text(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'data'
                ORDER BY column_name
                """
            )
        ).fetchall()
        names = {c[0] for c in cols}
        assert 'timestamp' in names
        assert 'wind_speed' in names
        assert 'power' in names
        assert 'ambient_temperature' in names


def test_frequencia_minutal():
    engine = DatabaseHelper.get_engine(TestConfig.FONTE_DB_URL)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                WITH sample_hour AS (
                    SELECT timestamp FROM data ORDER BY timestamp LIMIT 3600
                ), intervals AS (
                    SELECT EXTRACT(EPOCH FROM (
                        LEAD(timestamp) OVER (ORDER BY timestamp) - timestamp
                    )) AS seconds_diff FROM sample_hour
                )
                SELECT AVG(seconds_diff) AS avg_interval
                FROM intervals WHERE seconds_diff IS NOT NULL
                """
            )
        ).fetchone()
        assert 50 <= row.avg_interval <= 70


