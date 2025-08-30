# Pipeline ETL para Processamento de Dados

Implementação um pipeline ETL (Extract, Transform, Load) para processamento de dados de turbinas eólicas, desenvolvido como parte do teste técnico da Delfos.

## Como executar (etapas)
1. Subir serviços e instalar dependências:
   - Todos os comandos estão descritos no Makefile com comentários.
   - Você pode executar tudo de uma vez pelo script:
   ```bash
   make run_all
   ```
   Caso o `make all` não funcione, rode diretamente:
   ```bash
   ./run_all.sh
   ```
2. Acompanhar logs da API (já feito pelo script). Acesse:
   - http://localhost:8000/
   - http://localhost:8000/health
3. (Opcional) Executar ETL manualmente para uma data específica:
   ```bash
   make etl DATE=2024-01-01
   ```

## Script de execução (run_all.sh)
Etapas executadas:
- Sobe os serviços (build + up -d)
- Instala dependências no container da API
- Inicializa bancos e popula dados de exemplo (source/target)
- Executa ETL para o dia atual (UTC)
- Mostra logs da API

## Observação
Este projeto foi desenvolvido por Jônatas Gomes Barbosa da Silva, utilizando ferramentas de IA como suporte ao desenvolvimento, mantendo a autoria intelectual e implementação técnica.