import pendulum
from airflow.decorators import dag, task
from datetime import timedelta
import sys
import pandas as pd

sys.path.append('/opt/airflow/src')
from oldfiles.pipeline_tempoteste import extrair_dados, processar_dados


ORIGEM = r"/opt/airflow/data/origem/previsao_tempo.csv"


@dag(
    dag_id="pipeline_teste_cluma",
    schedule="*/1 * * * *",
    start_date= pendulum.datetime(2026, 2, 7),#TODO antes eu tinha trocado pra 6 e tava certo só mudei pra 7
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
        df_raw = extrair_dados(origem=ORIGEM)

        
        path_raw = f"/opt/airflow/data/raw/previsaotempoTESTE.csv"

        df_raw.to_csv(path_raw, index=False)
        print(f"✅ Dados brutos salvos em {path_raw}")

        return path_raw

    @task()
    def processar(path_raw):
        
        
        
        print("⚙️ Processando dados...")
        df_raw = pd.read_csv(path_raw)
        df_limpo = processar_dados(df_raw)

      
        path_processed = f"/opt/airflow/data/processed/previsao_tempo_limpoTESTE.csv"

        df_limpo.to_csv(path_processed, index=False)
        print(f"✅ Dados processados salvos em {path_processed}")

        return path_processed

    path_raw = extrair()
    processar(path_raw)


pipeline_tempo_dag = pipeline_tempo()

