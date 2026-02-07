import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
import subprocess
import sys 
sys.path.append("/opt/airflow/src")
from moverpradatatesteprocesso import extrair_dados, salvar_em_csv

@dag(
    dag_id="teste-rondando-em-python",
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(2026, 2, 7, tz="UTC"),
    tags=['clima', 'previsao_tempo', 'caxias do sul'],
    default_args={
        'owner': 'guiandreis',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    catchup=False,
    max_active_runs=1  # ISSO AQUI impede acumular infinitos runs
)
def pipeline_teste():

    @task()
    def rodar_script():
        df = extrair_dados()
        salvar_em_csv(df)


    rodar_script()


pipeline_teste_dag = pipeline_teste()
