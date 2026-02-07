# pipeline_tempo.py
import os
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
from dotenv import load_dotenv
import pandas as pd


ORIGEM = r"/opt/airflow/data/raw/previsao_tempo.csv"
DESTINO = r"/opt/airflow/data/processed/previsao_tempo.csv"



def extrair_dados(origem=ORIGEM):


    url = origem
    df_raw = pd.read_csv(url)
    
    
    return df_raw

def processar_dados(df):
    """Processa e cria colunas derivadas"""
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['temp_media'] = (df['tempmax'] + df['tempmin']) / 2
    df['amplitude_termica'] = df['tempmax'] - df['tempmin']
    df['dia_semana'] = df['datetime'].dt.day_name()
    
    colunas_importantes = ['name', 'datetime', 'tempmax', 'tempmin', 'temp', 
                          'feelslike', 'precip', 'humidity', 'windspeed', 
                          'temp_media', 'amplitude_termica', 'dia_semana']
    
    return df[colunas_importantes]

def salvar_dados(df_raw, df_processed):
    os.makedirs('/opt/airflow/data/raw', exist_ok=True)
    os.makedirs('/opt/airflow/data/processed', exist_ok=True)
    df_raw.to_csv('/opt/airflow/data/raw/previsao_tempo.csv', index=False)
    df_processed.to_csv('/opt/airflow/data/processed/previsao_tempo_limpo.csv', index=False)
# Executar o pipeline
if __name__ == "__main__":
    print("🔄 Iniciando pipeline...")
    
    # 1. Extrair
    print("📥 Extraindo dados da API...")
    df_raw = extrair_dados('Caxias do Sul')
    
    # 2. Processar
    print("⚙️ Processando dados...")
    df_limpo = processar_dados(df_raw)
    
    # 3. Salvar
    print("💾 Salvando dados...")
    salvar_dados(df_raw, df_limpo)
    
    print(df_limpo.head())
    print("\n✅ Pipeline concluído!")