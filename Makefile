SHELL := /bin/zsh

PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

.PHONY: up down rebuild logs ps psql clean initdb etl etl_dagster run_all test lint

# Sobe os serviços (constrói imagens se necessário) em background
up:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml up -d --build

# Encerra os serviços
down:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml down

# Reconstrói as imagens sem cache
rebuild:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml build --no-cache

# Segue os logs de todos os serviços
logs:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml logs -f

# Lista o status dos serviços
ps:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml ps

# Abre um psql no container do Postgres (banco postgres)
psql:
	docker exec -it postgres-dual-db psql -U postgres -d postgres

# Inicializa bancos: cria DBs, schemas, popula fonte e normaliza dados no alvo
initdb:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml run --rm api python -m db.setup_all

# Remove serviços, volumes e órfãos (ATENÇÃO: remove dados)
clean:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml down -v --remove-orphans
	docker volume prune -f

# Executa ETL diário para a data informada (YYYY-MM-DD)
etl:
	@if [ -z "$(DATE)" ]; then echo "Uso: make etl DATE=YYYY-MM-DD"; exit 1; fi
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml exec api python -m etl.etl_daily --date $(DATE) --base-url http://localhost:8000

# Executa job do Dagster para a data informada (YYYY-MM-DD)
etl_dagster:
	@if [ -z "$(DATE)" ]; then echo "Uso: make etl_dagster DATE=YYYY-MM-DD"; exit 1; fi
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml exec api dagster job execute -m etl.dagster -j etl_job --partition $(DATE)

# Executa o fluxo completo de demonstração via script shell
all:
	./run_all.sh

# Executa testes com pytest dentro do container da API
test:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml exec -e RUN_IN_CONTAINER=1 api pytest -q

lint:
	docker compose -f $(PROJECT_ROOT)/docker-compose.yml exec api black .
