Projeto Previsão do Tempo com Airflow e Postgres
Descrição

Este projeto extrai dados meteorológicos de uma API, processa os dados e salva em um banco Postgres. O fluxo de dados é orquestrado pelo Apache Airflow, usando DAGs e tasks decoradas. O objetivo é manter uma base de dados atualizada para análises futuras, como visualizações no Power BI.

Tecnologias

Python 3.10
Apache Airflow
PostgreSQL 13
Docker & Docker Compose
Pandas
psycopg2
DBeaver (opcional, para visualização do banco)
Power BI
Visual Crossing Weather API (fonte de dados meteorológicos)

Estrutura do Projeto
airflow-tempo-projeto/
├─ docker/                  # Configuração do Docker
│  └─ docker-compose.yml
├─ airflow/                  # Dados do Airflow
│  ├─ dags/                  # DAGs do Airflow
│  │  └─ pipeline_clima.py
│  ├─ logs/
│  └─ plugins/
├─ src/                      # Código Python da pipeline
│  └─ pipeline_teste.py
├─ data/                     # CSVs brutos e processados
│  ├─ raw/
│  └─ processed/
├─ sql/                      # Scripts SQL
│  └─ create_tables.sql
├─ .env                      # Variáveis de ambiente (API key, DB)
└─ README.md

Configuração do Docker

No docker-compose.yml, temos os serviços:

postgres: banco de dados Postgres

airflow-webserver: interface web do Airflow

airflow-scheduler: executa as DAGs

airflow-init: inicializa o banco do Airflow e cria usuário admin

Portas mapeadas:

8080 → Airflow Web

5432 → Postgres

Variáveis de Ambiente (.env)
VISUAL_CROSSING_KEY=your_api_key
DB_HOST=localhost
DB_NAME=clima_db
DB_USER=postgres
DB_PASSWORD=admin
DB_PORT=5432

Criação do Banco de Dados

Acesse o container Postgres:

docker compose exec postgres bash


Entre no psql:

psql -U airflow


Crie o banco de dados:

CREATE DATABASE clima_db;


Saia com \q.

Obs.: usuário e senha para conexão externa com DBeaver: postgres / admin.

Tabela do Banco de Dados
CREATE TABLE IF NOT EXISTS previsao_tempo (
    name TEXT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    tempmax DOUBLE PRECISION,
    tempmin DOUBLE PRECISION,
    temp DOUBLE PRECISION,
    feelslike DOUBLE PRECISION,
    precip DOUBLE PRECISION,
    humidity INTEGER,
    windspeed DOUBLE PRECISION,
    avg_temp DOUBLE PRECISION,
    temp_range DOUBLE PRECISION,
    day_of_week TEXT,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name, datetime)
);

Pipeline Python (pipeline_teste.py)

Funções principais:

extrair_dados(cidade, dias=7): busca dados da API Visual Crossing e retorna DataFrame.

processar_dados(df): calcula colunas derivadas (avg_temp, temp_range, day_of_week).

carregar_postgres(df): envia os dados processados para a tabela do Postgres usando UPSERT (ON CONFLICT).

DAG Airflow (pipeline_clima.py)

extrair(): extrai dados da API e salva CSV bruto.

processar(df_raw): processa CSV e salva CSV processado.

carregar(df_limpo): insere ou atualiza dados no Postgres.

Schedule: @daily

Start date: 2026-02-07

max_active_runs=1 → evita acumular runs pendentes

Execução

Levante o Docker Compose:

docker compose up -d


Teste a DAG no Airflow UI:

http://localhost:8080


Trigger manual:

No Airflow UI → DAGs → pipeline_clima → Trigger DAG

Dicas

Para visualizar dados no DBeaver, use conexão:

Host: localhost

Port: 5432

Database: clima_db

User: postgres

Password: admin

CSVs brutos e processados ficam em data/raw e data/processed.

Sempre rode a DAG com catchup=False para não processar datas passadas.