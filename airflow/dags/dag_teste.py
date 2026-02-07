from airflow.decorators import dag, task
from datetime import timedelta
import sys
import pandas as pd
import pendulum 

sys.path.append('/opt/airflow/src')
from pipeline_teste import extrair_dados, processar_dados , carregar_postgres


@dag(
    dag_id="pipeline_climateste",
    schedule="@daily",
    start_date= pendulum.datetime(2026, 2, 7, tz="UTC"),
    catchup=False,
    tags=['clima', 'previsao_tempo', 'caxias do sul'],
    default_args={
        'owner': 'guiandreis',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    max_active_runs=1  # ISSO AQUI impede acumular infinitos runs
)
def pipeline_tempo():

    @task()
    def extrair():
        print("📥 Extraindo dados da API...")
        
        df_raw = extrair_dados("Caxias do Sul", dias=7)

        path_raw = f"/opt/airflow/data/raw/previsao_tempo.csv"

        df_raw.to_csv(path_raw, index=False)
        print(f"✅ Dados brutos salvos em {path_raw}")

        return df_raw

    @task()
    def processar(df_raw):
        print("⚙️ Processando dados...")
        df_limpo = processar_dados(df_raw)

        path_processed = f"/opt/airflow/data/processed/previsao_tempo_limpo.csv"
    
        df_limpo.to_csv(path_processed, index=False)
        print(f"✅ Dados processados salvos em {path_processed}")

        return df_limpo
    @task()
    def carregar(df_limpo):
        print("📤 Carregando dados para o Postgres...")
        
        
        carregar_postgres(df_limpo)
        
        print("✅ Dados carregados no Postgres (simulado)")
    
    df_raw = extrair()
    df_limpo = processar(df_raw)
    carregar(df_limpo)



pipeline_tempo_dag = pipeline_tempo()
