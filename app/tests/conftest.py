import os
import time
import subprocess

import httpx
import pytest
from sqlalchemy import create_engine, text


class TestConfig:
    RUN_IN_CONTAINER = os.getenv("RUN_IN_CONTAINER") is not None
    DB_HOST = "postgres" if RUN_IN_CONTAINER else "localhost"
    DB_PORT = 5432
    FONTE_DB_URL = f"postgresql://postgres:postgres@{DB_HOST}:{DB_PORT}/source"
    ALVO_DB_URL = f"postgresql://postgres:postgres@{DB_HOST}:{DB_PORT}/target"
    API_BASE_URL = "http://localhost:8000"

    SETUP_TIMEOUT = 300
    API_TIMEOUT = 30


class DatabaseHelper:
    @staticmethod
    def get_engine(db_url: str):
        return create_engine(db_url)

    @staticmethod
    def wait_for_db(db_url: str, timeout: int = 60) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                engine = create_engine(db_url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True
            except Exception:
                time.sleep(2)
        return False


class APIHelper:
    @staticmethod
    def wait_for_api(base_url: str, timeout: int = 60) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = httpx.get(f"{base_url}/docs", timeout=5)
                if response.status_code == 200:
                    return True
            except Exception:
                time.sleep(2)
        return False


def run_etl_for_date(date_str: str) -> subprocess.CompletedProcess:
    if TestConfig.RUN_IN_CONTAINER:
        return subprocess.run(
            [
                "python",
                "-m",
                "etl.etl_daily",
                "--date",
                date_str,
                "--base-url",
                TestConfig.API_BASE_URL,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
    return subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "api",
            "python",
            "-m",
            "etl.etl_daily",
            "--date",
            date_str,
            "--base-url",
            TestConfig.API_BASE_URL,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    print("\n[SETUP] Iniciando ambiente de teste...")

    if TestConfig.RUN_IN_CONTAINER:
        if not DatabaseHelper.wait_for_db(TestConfig.FONTE_DB_URL):
            pytest.fail("Banco Fonte não ficou disponível")
        if not DatabaseHelper.wait_for_db(TestConfig.ALVO_DB_URL):
            pytest.fail("Banco Alvo não ficou disponível")
        result = subprocess.run(
            ["python", "-m", "db.setup_all"],
            capture_output=True,
            text=True,
            timeout=TestConfig.SETUP_TIMEOUT,
        )
        if result.returncode != 0:
            pytest.fail(f"Falha na inicialização dos bancos: {result.stderr}")
        if not APIHelper.wait_for_api(TestConfig.API_BASE_URL):
            pytest.fail("API não ficou disponível")
    else:
        result = subprocess.run(
            ["docker", "compose", "up", "-d", "--build"],
            capture_output=True,
            text=True,
            timeout=TestConfig.SETUP_TIMEOUT,
        )
        if result.returncode != 0:
            pytest.fail(f"Falha ao subir serviços Docker: {result.stderr}")

        if not DatabaseHelper.wait_for_db(TestConfig.FONTE_DB_URL):
            pytest.fail("Banco Fonte não ficou disponível")
        if not DatabaseHelper.wait_for_db(TestConfig.ALVO_DB_URL):
            pytest.fail("Banco Alvo não ficou disponível")

        result = subprocess.run(
            ["docker", "compose", "run", "--rm", "api", "python", "-m", "db.setup_all"],
            capture_output=True,
            text=True,
            timeout=TestConfig.SETUP_TIMEOUT,
        )
        if result.returncode != 0:
            pytest.fail(f"Falha na inicialização dos bancos: {result.stderr}")
        if not APIHelper.wait_for_api(TestConfig.API_BASE_URL):
            pytest.fail("API não ficou disponível")

    print("[SETUP] Ambiente pronto!")

    yield

    print("\n[TEARDOWN] Limpando ambiente...")
    if not TestConfig.RUN_IN_CONTAINER:
        subprocess.run(["docker", "compose", "down", "-v"], capture_output=True)
