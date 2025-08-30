from sqlalchemy import text

from .conftest import DatabaseHelper, TestConfig


def test_banco_alvo_conectividade():
    engine = DatabaseHelper.get_engine(TestConfig.ALVO_DB_URL)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT 1")).scalar() == 1


def test_estrutura_tabelas_alvo():
    engine = DatabaseHelper.get_engine(TestConfig.ALVO_DB_URL)
    with engine.connect() as conn:
        signal_exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables WHERE table_name = 'signal'
                )
                """
            )
        ).scalar()
        assert signal_exists

        data_exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables WHERE table_name = 'data'
                )
                """
            )
        ).scalar()
        assert data_exists


def test_dados_auxiliares_signals():
    engine = DatabaseHelper.get_engine(TestConfig.ALVO_DB_URL)
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM signal ORDER BY name")).fetchall()
        assert len(rows) > 0


