#!/usr/bin/env bash
set -euo pipefail

# Script de demonstração: sobe serviços, instala deps, inicializa DBs e roda ETL para o dia atual

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATE_UTC="$(date -u +%F)"

echo "[1/4] Subindo serviços..."
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d --build

echo "[2/4] Inicializando bancos e populando dados de exemplo..."
docker compose -f "$ROOT_DIR/docker-compose.yml" run --rm api python -m db.setup_all

echo "[3/4] Executando ETL (data UTC=$DATE_UTC)..."
docker compose -f "$ROOT_DIR/docker-compose.yml" exec api python -m etl.etl_daily --date "$DATE_UTC" --base-url http://localhost:8000

echo "[4/4] Mostrando logs da API (Ctrl+C para sair)..."
docker compose -f "$ROOT_DIR/docker-compose.yml" logs -f api
