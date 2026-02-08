from airflow.decorators import dag, task
from datetime import timedelta
import sys
import pendulum 

sys.path.append('/opt/airflow/src') # Adiciona o diretório src ao sys.path para importar pipeline_tempo
from pipeline_tempo import extrair_dados, processar_dados , carregar_postgres


@dag(
    dag_id="pipeline_clima",
    schedule="@daily",
    start_date = pendulum.today(tz="UTC"),
    catchup=False,
    tags=['clima', 'previsao_tempo', 'caxias do sul'],
    default_args={
        'owner': 'guiandreis',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    max_active_runs=1 
)
def pipeline_tempo():

    @task()
    def extrair():
        """
        Extrai os dados da API e salva o CSV bruto, retornando o df_raw
        """
        print(" - -  Extraindo dados da API...")
        df_raw = extrair_dados("Caxias do Sul", dias=7)
        
        path_raw = f"/opt/airflow/data/raw/previsao_tempo.csv"
        
        df_raw.to_csv(path_raw, index=False)
        print(f" - - Dados brutos salvos em {path_raw}")
        return df_raw

    @task()
    def processar(df_raw):
        """
        Pega o df_raw e processa, limpando e criando colunas, depois salva o CSV limpo e retorna o df_limpo
        """
        print(" - -  Processando dados...")
        df_limpo = processar_dados(df_raw)

        path_processed = f"/opt/airflow/data/processed/previsao_tempo_limpo.csv"

        df_limpo.to_csv(path_processed, index=False)
        print(f" - -  Dados processados salvos em {path_processed}")
        return df_limpo
    
    @task()
    def carregar_db(df_limpo):
        """
        Carrega os dados processados no Postgres, se não existir, ou atualiza se já existir (UPSERT)
        """
        print(" - -  Carregando dados para o Postgres...")
        carregar_postgres(df_limpo)
        
        print(" - - Dados carregados no Postgres")
    
    
    df_raw = extrair()
    df_limpo = processar(df_raw)
    carregar_db(df_limpo)

pipeline_tempo_dag = pipeline_tempo()